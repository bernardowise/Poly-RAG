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
invokes `write_lancedb`, the true last stage of the whole cycle — it merges
each source's new rows into its LanceDB table (`registry_cohere`,
`comments_cohere`, `digest_cohere`, `news_article_cohere`) and invokes nothing
further. `write_lancedb` is this project's only container-image Lambda
(LanceDB's real dependency footprint is 339MB unzipped, over Lambda's 250MB
zip/Layer limit), deployed via its own ECR repo. It deliberately does not
rebuild the vector index every cycle — only `merge_insert`s new rows — since
index-rebuild cost scales with the table's total size, not how many rows are
new; index maintenance is a separate, lower-cadence concern.

Every embedding Lambda writes cost/latency/token rows to
`poly-rag-embedding-metrics`. Each phase sends its own checkpoint email, three
total, landing at genuinely different times so each phase's real wall-clock
duration is measurable from the gaps between their timestamps: `send_digest`
(Phase 1's market-content digest), `digest_metrics` (split out of
`embed_news_article` 2026-08-22 so the Lambda that embeds isn't also the one
that reports — a plain cost/latency/tokens report covering both Phase 1 and
Phase 2), and `write_lancedb` (added 2026-08-23, a per-source
status/before/after/written/missing table for Phase 3). The three-email
pattern means a human can tell which phase succeeded or failed straight from
the inbox, no CloudWatch needed for the common case.

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

Verified in production: the full Phase 1 chain (4 Lambdas) runs unattended on the
00:00/12:00 UTC schedule with no manual intervention, cost/latency/tokens logged
per invocation to `poly-rag-architecture-metrics`. The full Phase 2 chain (9
Lambdas — chunking + embedding) is deployed and connected to fire automatically
at the end of every Phase 1 cycle. Its first real production test (cycle 14,
2026-08-22 12:00 UTC) surfaced two real bugs, both diagnosed and fixed the same
day without any manual Lambda invocation: an IAM policy scoped to only 3 AWS
regions, insufficient for where the `global.` cross-region Bedrock profile
actually routes (`AccessDeniedException` in `embed_digest`, broke the chain
before it reached the other 3 embed Lambdas); and `chunk_registry` comparing
`first_seen > cycle_started_at` when `ingest_polymarket` sets both fields to the
exact same timestamp, so the comparison must be `>=` — this one silently
returned 0 new markets every cycle since the Lambda was written, not just on
cycle 14. Both fixed and deployed, cycle 14's registry gap closed via a local
one-off (chunk_registry + embed_registry re-run against real chunk data, no
Lambda invoked). A dedicated Phase 2 healthcheck runbook
(`.claude/claude_docs/runbook_verify_phase2_health.md`) was written the same
day, encoding checks that would have caught both bugs — cross-checking
`chunk_registry`'s output against `send_digest`'s already-verified
`newly_tracked_markets` count, and a CloudWatch error sweep across all 8 Phase 2
Lambdas.

**Corpus, as of 2026-08-22 (14 complete cycles, registry growing every cycle):**
1,090+ registry markets, 8,980+ news articles, 12,108+ comments, and 102,997+
odds snapshots (a growing mix of `cycle` and `clob_backfill` provenance — most
markets now carry history back to their real creation date, not just since we
started tracking them).

**Embedded and verified in a real vector space** (Cohere Embed v4, via the
`global.cohere.embed-v4:0` cross-region inference profile): the Friday/Saturday
bootstrap (1,090 registry + 749 comments + 13 digest + 9,235 news_article
chunks, 11,087 vectors) plus cycle 14's own automatic-cycle vectors (25
registry + 37 comments + 1 digest + 603 news_article, 666 vectors, verified
zero gaps against their source chunk files). `news_paragraph` is deliberately
not built yet (see Pending below).

**Phase 3 (write to LanceDB) closed for all 4 embedded sources AND connected to
the automatic cycle, same day (2026-08-22):** `scripts/write_to_lancedb.py`
was first run manually against the full 14-cycle corpus for all 4 sources —
`registry_cohere` 1,115 rows, `comments_cohere` 786, `digest_cohere` 14,
`news_article_cohere` 9,832 rows (11,747 vectors total), all in S3 under
`lancedb/`. Fixed three real schema-drift bugs in the script along the way
(wrong vector column name for index creation, and two cases of LanceDB
rejecting a later write that introduced a field — `_lineage`,
`cycle_started_at` — the first-created table's schema didn't have; see
tech_debt.md). That logic was then ported into a new Lambda,
`poly-rag-write-lancedb` — this project's only container-image Lambda
(LanceDB's real dependency footprint is 339MB unzipped, over the 250MB
zip/Layer limit), deployed via a new ECR repo. Chained after a new
`poly-rag-digest-metrics` Lambda, split out of `embed_news_article` the same
day (the cycle cost/latency/tokens report email was living in a Lambda named
for embedding, not reporting — see tech_debt.md). Full chain: `embed_digest → embed_comments → embed_registry →
embed_news_article → digest_metrics → write_lancedb`. Deliberately does not
rebuild the vector index every cycle (only `merge_insert`s new rows) — index
rebuild cost scales with table size, not new-row count, so it's a separate
lower-cadence concern.

**Verified in production the very next cycle (2026-08-23 00:00 UTC), which
surfaced and closed one more real bug the same day:** `write_lancedb` timed
out at its 120s limit on all 3 attempts (the original invocation plus
Lambda's 2 automatic async retries) — `registry`/`comments`/`digest` wrote
fine, but `news_article` (the largest source) never completed. Root cause:
`load_vectors` was reading every checkpoint ever written for a source, not
just the current cycle's — harmless for a one-off run, but unbounded cost on
a Lambda that runs every 12h forever as the corpus grows. Fixed to filter by
S3 `LastModified >= cycle_started_at` instead, verified locally (35s, well
under the timeout) before redeploying via `aws lambda update-function-code`
(Terraform's `image_uri` is a `:latest` tag and doesn't diff on a new
digest, so this deploy step is manual, not `terraform apply`), and used to
close that cycle's real data gap in the same run (`news_article_cohere`
9,832 → 10,580 rows, exactly the missing 748). See tech_debt.md for the full
incident writeup. Both Phase 1 and Phase 2 healthcheck runbooks passed clean
on this same cycle — the Phase 1 runbook's own post-resolution-counter check
turned out to have a false positive (fixed in the runbook itself, see its own
changelog), not a pipeline bug.

## Pending / TODO

- **RAG retrieval (Day 4)** — chunking, embedding, and vector store write for 4
  of 5 sources **done, connected to the automatic cycle** (verification of the
  full new chain still pending its first real automatic cycle, see Status);
  retrieval itself (the actual query layer) not started.
  - **Chunking + embedding (steps 1-2, done for registry/comments/digest/
    news_article).** Strategy closed for all sources; `news_paragraph` was
    deliberately paused (see below) so the whole pipeline could be proven
    end-to-end on one variant first. Comments group by `link_type` (`direct` →
    `market_id`, shared → the `comment_entity_id` from the registry, so one
    stream is never re-embedded per market), the registry's
    `question`+`description` become one vector per market (the semantic half of
    the Polymarket source, next to odds as the structured half), digest becomes
    one chunk via a deterministic text template, and news_article chunks whole
    articles with overflow splitting for anything over ~32K chars — no new LLM
    call anywhere in this path.
  - **Embedding model: Cohere Embed v4 only, for now.** Titan v2 was dropped
    outright (no batch API, 600 req/min account ceiling). Voyage was ruled out
    after a single run consumed 56.5% of its free tier with no spend guardrail
    in place. Two real Cohere daily-quota outages were hit and fixed during the
    2026-08-21/22 bootstrap — see tech_debt.md, "Phase 2 Embedding Bootstrap",
    for the full diagnosis (the quota actually blocking calls was never the one
    being monitored) and why `global.cohere.embed-v4:0` is the model id now used
    everywhere in Phase 2.
  - **Design context that still holds:** retrieval is a single path — metadata
    filter (`market_id`, time, source) plus semantic ranking. The earlier
    two-layer model (explicit linkage + ambient time-window) is deprecated and
    its implementation deleted: the per-market News redesign left 100% of
    articles linked to exactly one market, so the ambient pool it read from was
    empty. Odds history reaches back to each market's creation (CLOB backfill),
    unblocking correlation against older news. A real orphan-data finding (305
    articles referencing purged markets) was left untouched by choice, to be
    handled at query time.
  - **Vector store: decided (LanceDB), written for all 4 sources, connected to
    the automatic cycle.** Chosen 2026-08-22 on measured 3-12 month storage
    growth against real free-tier ceilings (Qdrant/Pinecone would be exhausted
    in 2-4 months at current growth; LanceDB's real cost at 12 months is
    ~$0.33/month in S3 storage, no managed-service tier to outgrow). The
    one-off write script (`scripts/write_to_lancedb.py`) proved the design
    against the full 14-cycle corpus (11,747 vectors total, see Status above),
    then its logic was ported into `poly-rag-write-lancedb`, a container-image
    Lambda (LanceDB's real dependency footprint measured at 339MB unzipped,
    over Lambda's 250MB zip/Layer limit) chained after the new
    `poly-rag-digest-metrics` Lambda — see Status above for the full chain.
  - **`news_paragraph` deliberately paused**, not abandoned — explicit user
    decision to prove one chunking variant end-to-end (chunk → embed → store →
    query) before doubling the corpus with a second one. Revisit once retrieval
    against `news_article` is working.
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
