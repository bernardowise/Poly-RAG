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
    verifiability          market, fan-out             grouped by Event/     JSON artifact +
    filter, registry       batching + parallel         Series, deduped       HTML email, its
    diff, odds             merge, deduped by            by comment_id)        own Bedrock call)
    time-series)            URL)
```

Only `ingest_polymarket` has its own EventBridge trigger. Every other stage is
invoked directly by the one before it (`lambda.invoke`), threading a single
`cycle_started_at` through the whole chain so every stage's S3 output lands under the
same cycle's date/hour partition, regardless of how long an earlier stage took. A
separate watchdog Lambda (`poly-rag-watchdog-ingest-news`, 10-min cron) detects a
stuck News cycle and retries only the missing batches.

**Data flow, per cycle:**
1. **Registry** (DynamoDB, `poly-rag-market-registry`) — one item per market, updated
   in place. A market enters only after an LLM verifiability check (does its outcome
   resolve against a citable public record, not human judgment over ambiguous
   evidence?). Never deleted — resolved markets stay in the registry with their final
   outcome, so the full open-to-resolution history is preserved.
2. **Odds time-series** (S3, `odds/<market_id>.json`) — one file per market,
   append-only, one snapshot per cycle for every open market. This is the actual
   differentiator; nothing else in the pipeline matters if this isn't clean.
3. **News** (S3, `news/YYYY-MM-DD/HH.json`) — full article text (via
   `googlenewsdecoder` + `trafilatura`), tagged with the specific `market_id`(s) it's
   about.
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
  ingest_polymarket/    registry + odds time-series, LLM verifiability filter
  ingest_news/           Google News RSS, fan-out batching, article extraction
  ingest_comments/       Polymarket comments, entity grouping, comment_id dedup
  send_digest/           cross-source synthesis, JSON artifact + email
  watchdog_ingest_news/  stuck-cycle detection and retry
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

## Pending / TODO

- **RAG retrieval (Day 4)** — in design. The earlier two-layer model (explicit
  linkage + ambient time-window) was deprecated on 2026-08-18 and its implementation
  (`retrieval/time_window.py`) deleted: the per-market News redesign left 100% of
  articles linked to exactly one market, so the ambient pool it retrieved from is
  empty. Current model is a single path — metadata filter (`market_id`, time, source)
  plus semantic ranking. Chunking strategy, embedding model, and vector store are the
  open decisions; embedding must be incremental within the ingestion chain, since the
  corpus accumulates ~1M tokens per cycle.
- **Synthesis agent (Day 5)** — not started. `send_digest`'s executive-summary call
  is the closest existing precedent (multi-source context → Bedrock → synthesis) but
  there's no user-facing query interface yet. Includes an open, explicitly deferred
  decision: LangChain/LlamaIndex vs. continuing with direct boto3 calls.
- **Databricks Delta Lake / Unity Catalog** — tables created and verified
  (`workspace.poly_rag.market_registry`, `odds_snapshots`), but only one version
  exists so far; real time-travel (comparing two versions) not yet demonstrated.
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
  Terraform state is local and gitignored, no remote backend.
