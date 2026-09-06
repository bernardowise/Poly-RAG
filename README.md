# Poly-RAG

A RAG assistant over live Polymarket prediction-market data, correlated with news
coverage and trader sentiment. Guided by Chip Huyen's *AI Engineering* book — a
personal, long-term AI engineering learning project, not a client engagement.

## Why this exists

A general-purpose LLM with web search can tell you what a market's odds are *right
now*. It cannot tell you how those odds moved over time in correlation with specific
news events or trader sentiment — that history isn't logged anywhere unless someone
is actively collecting it. Poly-RAG is that someone: a self-built historical
time-series of odds movement, concurrent news, and trader comments, queryable as one
coherent view. That proprietary time-series — not the LLM, not the retrieval code —
is the actual differentiator.

Agentic-first from day one: Claude Code is the primary development environment, and
multi-agent orchestration (data-collection, retrieval, synthesis) is a first-class
design goal, not something bolted onto a finished pipeline afterward.

## Architecture

Four AWS Lambdas, strictly chained (not independent timers — see
`.claude/claude_docs/tech_debt.md`, "Strict Ingestion Chaining"):

```
EventBridge (00:00 / 12:00 UTC)
        |
        v
ingest_polymarket --invoke--> ingest_news --invoke--> ingest_comments --invoke--> send_digest
   (top-500 by            (Google News RSS,          (Polymarket's own      (synthesizes all
    volume24hr, LLM        one search per open         comment API,          3 sources into one
    verifiability          + resolved-in-window        grouped by Event/     JSON artifact +
    filter, registry       market, fan-out              Series, deduped       HTML email, its
    diff, odds+history     batching + parallel          by comment_id)        own Bedrock call)
    for new markets)        merge, deduped by URL)
                                                                                     |
                                                                                     v
                                                                          embed_orchestrator
                                                                              (Phase 2 entry)
                                                                                     |
                          +--------------------+--------------------+--------------------+
                          v                     v                     v                     v
                   chunk_registry        chunk_comments          chunk_digest       chunk_news_article
                  (this cycle's         (entity-grouped         (one narrative      (whole-article,
                   new markets           comment chunks)         chunk)              overflow split)
                   only)
                          |                     |                     |                     |
                          +--------------------+--------------------+--------------------+
                                                     | (fan-out, all 4 in PARALLEL)
                                                     v
                                              embed_digest
                                                     | --invoke-->
                                                     v
                                             embed_comments
                                                     | --invoke-->
                                                     v
                                             embed_registry
                                                     | --invoke-->
                                                     v
                                          embed_news_article
                                   (strict SEQUENCE, deliberately not parallel --
                                    the 4 embed Lambdas share one Cohere TPM ceiling)
                                                     |
                                                     v
                                              digest_metrics
                                        (Phase 1+2 cost/latency/tokens
                                         report email, second email)
                                                     |
                                                     v
                                              write_lancedb
                                    (Phase 3 -- per-cycle merge_insert into
                                     each source's LanceDB table, terminal)
```

Only `ingest_polymarket` has its own EventBridge trigger. Every other stage is
invoked directly by the one before it (`lambda.invoke`), threading a single
`cycle_started_at` through the whole chain so every stage's S3 output lands under the
same cycle's date/hour partition, regardless of how long an earlier stage took. A
separate watchdog Lambda (`poly-rag-watchdog-ingest-news`, 10-min cron) detects a
stuck News cycle and retries only the missing batches.

Everything above is **Phase 1 (ingestion)**. **Phase 2 (chunking + embedding)** is
built and connected as of 2026-08-22: `send_digest` invokes `embed_orchestrator`,
which fans out in parallel to 4 chunking Lambdas (`chunk_registry`,
`chunk_comments`, `chunk_digest`, `chunk_news_article` — the `news_paragraph`
variant is deliberately not built yet) and then runs 4 embedding Lambdas in
strict SEQUENCE (`embed_digest` → `embed_comments` → `embed_registry` →
`embed_news_article`), all calling Cohere Embed v4 via its cross-region
inference profile. Sequential, not parallel, on purpose: the 4 embed Lambdas
share one account-wide token-per-minute ceiling, and running them concurrently
would recreate the exact uncoordinated-competing-invocations bug that already
caused a real News double-invocation incident. See architecture_canon.md for
the full per-Lambda design and tech_debt.md for how two real Cohere
daily-quota outages (on the bare on-demand model id and the `us.` cross-region
route) were diagnosed and fixed by switching to `global.cohere.embed-v4:0`.

**Phase 3 (write to LanceDB) is also built and connected, same day
(2026-08-22):** `embed_news_article` invokes `digest_metrics` (see below), which
invokes `write_lancedb` — it merges each source's new rows into its LanceDB
table (`registry_cohere`, `comments_cohere`, `digest_cohere`,
`news_article_cohere`), then invokes `build_sql_parquet` (Phase 4).
`write_lancedb` is a container-image Lambda (LanceDB's real dependency footprint
is 339MB unzipped, over Lambda's 250MB zip/Layer limit), deployed via its own
ECR repo. It never rebuilds the vector index — `merge_insert`s new rows, then
runs `optimize()` as a maintenance pass (which folds new rows into the existing
index incrementally). That `optimize()` pass runs *last*, after the Phase 3
email and the Phase 4 invoke: `optimize()` on the big table can exhaust memory,
and `Runtime.OutOfMemory` is uncatchable and would truncate the rest of the
handler (it did, silently, for ~6 cycles from 2026-09-03 — see
`.claude/claude_docs/tech_debt.md`, "write_lancedb OOM on optimize()"; the
fix was 2048MB of memory plus this reordering).

Every embedding Lambda writes cost/latency/token rows to
`poly-rag-embedding-metrics`. Each phase sends its own checkpoint email, four
total, landing at genuinely different times so each phase's real wall-clock
duration is measurable from the gaps between their timestamps: `send_digest`
(Phase 1's market-content digest), `digest_metrics` (split out of
`embed_news_article` 2026-08-22 — a cost/latency/tokens report covering
Phases 1 and 2), `write_lancedb` (a per-source status/before/after/written/
missing table for Phase 3), and `build_sql_parquet` (Phase 4 — Parquet
sizes plus eight DuckDB smoke-test queries over the tables it just wrote).
The per-phase emails mean a human can tell which phase succeeded or failed
straight from the inbox, no CloudWatch needed for the common case.

**Data flow, per cycle:**
1. **Registry** (DynamoDB, `poly-rag-market-registry`) — one item per market, updated
   in place. A market enters only after an LLM verifiability check (does its outcome
   resolve against a citable public record, not human judgment over ambiguous
   evidence?). Never deleted — resolved markets stay in the registry with their final
   outcome, so the full open-to-resolution history is preserved. Each item also
   carries `created_at` (the market's real Polymarket creation date, distinct from
   `first_seen` — when *we* started tracking it) and
   `post_resolution_cycles_remaining` (a counter that arms at 4 the exact cycle a
   market resolves, driving the post-resolution News capture below).
2. **Odds time-series** (S3, `odds/<market_id>.json`) — one file per market,
   append-only. This is the actual differentiator; nothing else in the pipeline
   matters if this isn't clean. Every snapshot carries an explicit `source`:
   `cycle` (written every 12h, includes `volume`/`volume24hr`/`liquidity`) or
   `clob_backfill` (pre-tracking price history recovered from Polymarket's free
   CLOB API, back to the market's `created_at` — price only, no volume/liquidity,
   and never overlapping the tracked window). The CLOB backfill runs two ways: a
   one-off pass over the markets already tracked before 2026-08-18, and natively
   inside `ingest_polymarket` for any market entering the registry from that date
   forward — no market starts with an empty time-series anymore. See
   `.claude/claude_docs/tech_debt.md`, "Odds History Backfill from Polymarket CLOB."
3. **News** (S3, `news/YYYY-MM-DD/HH.json`) — full article text (via
   `googlenewsdecoder` + `trafilatura`), tagged with the specific `market_id`(s) it's
   about. Also classified by `temporal_tier` (when the article was published
   relative to the market's lifecycle — before it existed, after it existed but
   before we tracked it, or while we were tracking it) and
   `market_status_at_publish` (whether the market was still open or had already
   resolved) — two independent questions, see tech_debt.md, "News Temporal Tiers."
   Search now covers open markets plus recently-resolved ones still inside their
   4-cycle post-resolution window, to capture how coverage/reaction looks right
   after a market's outcome is fixed.
4. **Comments** (S3, `comments/YYYY-MM-DD/HH.json`) — real trader discussion from
   Polymarket's own comment sections, diffed against `poly-rag-processed-comments` so
   only genuinely new comments are pulled in each cycle.
5. **Digest** (S3, `digest/YYYY-MM-DD/HH.json` + email) — a structured artifact built
   for future RAG ingestion, not just a human-readable email. Includes
   `top_volatility` (what moved this cycle) and `world_snapshot` (what the market
   currently believes, independent of movement: highest-conviction and most-disputed
   open bets), plus an LLM-synthesized executive summary that sees all three sources
   together.

## Stack

| Layer | Choice |
|---|---|
| Compute | AWS Lambda (Python 3.12), event-driven, no always-on servers |
| Storage | S3 (raw JSON, partitioned by source/date/hour) + DynamoDB (registry, dedup, metrics, PAY_PER_REQUEST, point-in-time recovery enabled) |
| LLM | Claude Sonnet 4.5 via Bedrock, IAM-authenticated (no separate API key) |
| Orchestration | EventBridge (single cron trigger) + direct Lambda-to-Lambda chaining |
| IaC | Terraform (`terraform/`), one file per resource domain |
| Exploration | Databricks (Delta Lake + Unity Catalog), separate from the live pipeline |

Budget discipline: operates against a $5/month AWS Budget with automated
Deny-policy guardrails at $10, spending real promotional credits deliberately, not
treating them as free.

## Repo structure

```
lambdas/
  ingest_polymarket/    registry + odds time-series (+ CLOB backfill for new
                         markets), LLM verifiability filter
  ingest_news/           Google News RSS (open + post-resolution markets),
                          fan-out batching, article extraction
  ingest_comments/       Polymarket comments, entity grouping, comment_id dedup
  send_digest/           cross-source synthesis, JSON artifact + email, invokes
                          embed_orchestrator at the end (Phase 2 entry point)
  watchdog_ingest_news/  stuck-cycle detection and retry
  embed_orchestrator/    Phase 2 entry point -- fans out chunking, starts the
                          sequential embedding chain
  chunk_registry/        this cycle's new markets only (first_seen >=
                          cycle_started_at, no lookback window needed)
  chunk_comments/        entity-grouped comment chunks for this cycle
  chunk_digest/          this cycle's digest as one narrative chunk
  chunk_news_article/    this cycle's articles, whole-article chunking with
                          overflow split for oversized articles
  embed_digest/          1st in the sequential embed chain
  embed_comments/        2nd -- invoked by embed_digest
  embed_registry/        3rd -- invoked by embed_comments
  embed_news_article/    4th -- invoked by embed_registry, writes embedding
                          metrics, invokes digest_metrics
  digest_metrics/        Phase 1+2 cost/latency/tokens report email, split out
                          of embed_news_article 2026-08-22; invokes write_lancedb
  write_lancedb/         Phase 3, true last stage -- per-cycle merge_insert into
                          each source's LanceDB table, container-image Lambda
scripts/                  one-off scripts, run manually, not part of the 12h chain
                           (odds/registry backfills, News temporal/status tagging,
                           corpus chunking + embedding bootstrap — see its README)
terraform/                all AWS infrastructure as code
.claude/
  claude_docs/            architecture_canon.md, tech_debt.md, session_ledger.md,
                           knowledge.md, hooks.md, infra_design.md (see CLAUDE.md
                           for what each one is for)
  commands/                slash commands for repeatable workflows
  skills/, .databricks/    Databricks CLI integration for Claude Code
CLAUDE.md                  project instructions, loaded automatically every session
```

`.claude/claude_docs/architecture_canon.md` is the authoritative, current-state
snapshot of everything above — this README is a summary of it, not a replacement.

## Status

The full pipeline runs end to end, unattended, on a 12-hour cycle: ingestion,
chunking and embedding, and vector-store writes are all live in production.
The corpus grows automatically every cycle — 1,800+ tracked markets, ~20,000
news articles, 1,400+ trader comments, over 23,000 vectors indexed for
semantic search.

Retrieval (a Registry-first cascade over the accumulated corpus, correlating
market data, news, odds history, and trader comments) is live in a public
Gradio app: https://huggingface.co/spaces/bernardolw/poly-rag — built as an
evaluation instrument rather than a consumer chatbot. It carries a sliding
context window (the full retrieved context of prior turns, merged and
de-duplicated, bounded by a configurable token budget), a per-turn metrics
panel (latency, token usage, estimated cost), an optional in-line LLM-judge
scoring the answer (faithfulness, answer relevancy, context relevance), and
opt-out per-turn logging of every interaction to S3 for later evaluation.
Deploys are automatic: a GitHub Action flattens `gradio_app/` + `retrieval/`
and pushes to the Space on every push to `main`.

**Phase 4 (SQL layer)** — the registry and the odds time-series, flattened
into SQL-queryable Parquet on S3 (`sql/markets.parquet`,
`sql/odds_snapshots/YYYY-MM.parquet`), so retrieval can answer aggregate /
ranking / point questions ("top 10 markets by volume last week") that
semantic search structurally cannot. The one-off retroactive run
(`scripts/build_sql_parquet.py`, 2,643 markets, 201,692 snapshot rows) and
the per-cycle `build_sql_parquet` Lambda — chained after `write_lancedb`,
container-image, refreshing `markets.parquet` plus the current month's
`odds_snapshots` partition each cycle — are both built and deployed. The
Lambda's report email (the cycle's fourth) runs eight DuckDB smoke-test
queries over the Parquet it just wrote. The retrieval side is live too:
`rewrite_query` flags aggregation/ranking questions, `text_to_sql` generates
a guarded `SELECT` (non-destructive checks, no DDL, forced `LIMIT`),
`run_sql` runs it read-only against S3 via DuckDB, and `search_cascade`
feeds any `market_id`s in the result into the semantic news/comments/odds
lookups so a question like "top 10 markets by volume last week — and what's
the news" cross-references. `duckdb` is on the Space. Still pending: verify
the Lambda against a real automatic cycle.

**Phase 5 (`rag_eval`)** — a fixed set of temporal, verifiable questions
scored each cycle against programmatically-computed ground truth, plus a
longitudinal drift judge over the stored history — is designed but not yet
built. It is where the canonical `ragas` library runs, in an isolated
environment reading the S3 session logs (`ragas` cannot coexist with the
`langchain-aws` pin the retrieval path needs).

See `.claude/claude_docs/architecture_canon.md` for the full architecture and
design rationale.

