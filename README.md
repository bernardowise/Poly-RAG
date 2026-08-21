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
```

Only `ingest_polymarket` has its own EventBridge trigger. Every other stage is
invoked directly by the one before it (`lambda.invoke`), threading a single
`cycle_started_at` through the whole chain so every stage's S3 output lands under the
same cycle's date/hour partition, regardless of how long an earlier stage took. A
separate watchdog Lambda (`poly-rag-watchdog-ingest-news`, 10-min cron) detects a
stuck News cycle and retries only the missing batches.

Everything above is **Phase 1 (ingestion)**. **Phase 2 (embedding)** is designed
but not built: a separate set of Lambdas that reads what Phase 1 already wrote to
S3 and never modifies it — an orchestrator invoked by `send_digest` as one more
link in the same chain, fanning out to per-source chunking Lambdas, then to
embedding Lambdas that only turn text into vectors and persist them to S3, and
finally to store-write Lambdas kept deliberately separate so adding a new vector
store touches none of the already-proven embedding code. Only the one-off
bootstrap scripts exist so far; none of these Lambdas are written yet. See
architecture_canon.md.

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
  send_digest/           cross-source synthesis, JSON artifact + email
  watchdog_ingest_news/  stuck-cycle detection and retry
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

Verified in production: the full 4-Lambda chain runs unattended on the 00:00/12:00
UTC schedule with no manual intervention, cost/latency/tokens logged per invocation
to `poly-rag-architecture-metrics` for all 4 Lambdas. Registry currently tracks
markets under the current (post-2026-08-16) design only — an early-pipeline cleanup
removed all registry/odds data tied to the deprecated Bluesky + keyword-matching
design.

**Corpus, as of 2026-08-21 (11 complete cycles, registry growing every cycle):**
981 registry markets, 7,429 news articles (100% linked to exactly one market_id,
all classified by temporal tier and market status at publish time), 8,858
comments (News keeps growing steadily per cycle, Comments flattened to
steady-state volume once its dedup table caught up — 3,080 orphaned comments
referencing markets purged in an earlier registry cleanup were removed on
2026-08-20 by `scripts/purge_orphan_comments.py`), and 63,641+ odds snapshots (a
growing mix of `cycle` and `clob_backfill` provenance — most markets now carry
history back to their real creation date, not just since we started tracking
them).

Chunked and ready for embedding in `chunks/`: 981 registry + 120,568
news_paragraph + 7,429 news_article + 681 comments + 11 digest. Nothing is
embedded yet — see Day 4 below.

## Pending / TODO

- **RAG retrieval (Day 4)** — chunking **done and written to S3**; embedding
  **blocked**; retrieval not started.
  - **Chunking (step 1, complete):** strategy closed for all sources and executed
    by `scripts/bootstrap_chunk_corpus.py` over the full existing corpus. News
    splits by paragraph (with a whole-article variant kept as a comparison axis),
    Comments group by `link_type` (`direct` → `market_id`, shared → the
    `comment_entity_id` from the registry, so one stream is never re-embedded per
    market), the registry's `question`+`description` become one vector per market
    (the semantic half of the Polymarket source, next to odds as the structured
    half), and each digest becomes one chunk via a deterministic text template —
    no new LLM call. Live counts in `chunks/`: 981 registry, 120,568
    news_paragraph, 7,429 news_article, 681 comments, 11 digest.
  - **Embedding (step 2, blocked — no model has completed a corpus).** Every
    candidate hit a real, verified account limit. Titan v2 was **dropped from the
    project**: its Bedrock API has no batch field at all and the account cap is
    600 req/min, which would bottleneck every future incremental cycle, not just
    the bootstrap. Voyage (`voyage-finance-2`) was the one model that actually
    worked (122K vectors in ~30 min, no throttling) but a single run consumed
    56.5% of its 50M free tier, projecting exhaustion in ~4 days at current
    ingestion rate. Cohere v4 is throttled at ~15-20 rejections/min against 1-2
    successful calls/min, and neither exponential backoff nor fixed pacing moved
    it. Leading unverified hypothesis: the **daily** 8.1M-token cap (already 65%
    consumed that day), not the per-minute one. Full detail, including the real
    CLI/CloudWatch evidence for each limit, in tech_debt.md, "Phase 2 Embedding
    Bootstrap BLOCKED".
  - **Design context that still holds:** retrieval is a single path — metadata
    filter (`market_id`, time, source) plus semantic ranking. The earlier
    two-layer model (explicit linkage + ambient time-window) is deprecated and
    its implementation deleted: the per-market News redesign left 100% of
    articles linked to exactly one market, so the ambient pool it read from was
    empty. Odds history reaches back to each market's creation (CLOB backfill),
    unblocking correlation against older news; News carries `temporal_tier` and
    `market_status_at_publish`; post-resolution News capture (4 cycles / 48h) is
    live. A real orphan-data finding (305 articles referencing purged markets)
    was left untouched by choice, to be handled at query time.
  - **Vector store: still undecided, and the constraint changed.** Databricks
    Vector Search had been ruled out for allowing only 1 active endpoint per
    account — a blocker only under the earlier plan of many parallel indices, and
    one that no longer applies now that the design has collapsed toward a single
    model. Pinecone and Qdrant accounts exist (keys in `.secrets`, gitignored);
    LanceDB-in-S3 remains a candidate. OpenSearch Serverless stays out on cost
    (~$700/month).
- **Synthesis agent (Day 5)** — not started. `send_digest`'s executive-summary call
  is the closest existing precedent (multi-source context → Bedrock → synthesis) but
  there's no user-facing query interface yet. Includes an open, explicitly deferred
  decision: LangChain/LlamaIndex vs. continuing with direct boto3 calls. CI/CD
  (remote Terraform state → first Python tests → CI → CD via GitHub OIDC) is
  scheduled here too, deliberately deferred out of Day 4.
- **Databricks Delta Lake / Unity Catalog** — tables created and verified
  (`workspace.poly_rag.market_registry`, `odds_snapshots`), real time-travel
  demonstrated (comparing two live registry versions via `VERSION AS OF`).
- **LLM enrichment output not yet optimized for RAG** — current `llm_summary` /
  `executive_summary` fields were designed for human readability (the digest email),
  not evaluated against real retrieval requirements. Deliberately deferred until the
  retrieval layer exists (see tech_debt.md).
- **Sports-market resolution-horizon bug** — the minimum-horizon filter uses a
  market's `endDate`, which for sports markets is an administrative deadline, not the
  actual game time. Confirmed but not fixed (see tech_debt.md).
- **Self-referential corpus** — a RAG index over this repo's own git history
  (querying how the architecture evolved) is designed but not built.
- **DKIM/deliverability** — digest emails land in spam; the real fix requires a
  project-owned domain, deferred by explicit choice.
- **No CI/CD** — deploys are manual (`terraform apply`, scoped with `-target`),
  Terraform state is local and gitignored, no remote backend. Scheduled for Day 5
  (see above), not before.
