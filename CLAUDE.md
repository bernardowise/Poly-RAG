# Poly-RAG — Claude Code Guide

## Project Purpose

A personal AI engineering learning project: a RAG assistant over live Polymarket prediction market data, correlated with news and Bluesky sentiment. The differentiator is a self-built historical time-series of odds movement + concurrent news/sentiment — data no general-purpose LLM can reconstruct retroactively.

Guided by Chip Huyen's *AI Engineering* book. This is a long-term side project, not a client engagement.

## Architecture Philosophy

**Agentic-first from day one.** This is not a data science project with agents bolted on — agents and orchestration are a first-class design goal.

- Multi-agent system: data-collection agent, retrieval/research agent, synthesis agent
- MCP integration for structured tool-based access to data sources
- Claude Code is the primary development environment from commit one

## Stack

| Layer | Choice | Reason |
|---|---|---|
| Cloud | AWS | Deliberately unfamiliar — building proficiency in the production AI ecosystem |
| Compute | Lambda, Kinesis | Pay-per-use, fits $5/month hard budget cap |
| Storage | S3, DynamoDB | No idle cost |
| LLM | Bedrock, Claude Sonnet 4.5 (`us.anthropic.claude-sonnet-4-5-20250929-v1:0`, inference profile) | Used in both ingestion (trial, see Development Conventions) and the Day 5 synthesis agent for now, to keep cost/latency comparisons consistent across the pipeline. Auth via IAM (boto3 bedrock-runtime client using the same credentials as S3/DynamoDB) — no separate Anthropic API key needed. Sonnet 5 not yet available without AWS Sales contact; Sonnet 4.5 requires the `us.` inference-profile prefix, not the bare model ID. Model choice may become user-selectable (multimodal options) in a later iteration — not part of the current MVP. |
| Data sources | Polymarket Gamma API, news RSS (10 feeds), Bluesky AT Protocol | All free |

**Budget constraint: spend as if it were real money, not free credits.** The AWS account carries ~$120 in promotional credits ($100 signup + $20 for completing a Budgets activity), but this is a discipline exercise, not a spending allowance — design and operate as if every dollar were out of pocket. Avoid always-on compute (EC2, MSK/Kafka) and frequent Bedrock calls. Batch writes where possible (S3 free tier caps at 2,000 PUT requests/month — request count matters more than payload size).

**Guardrail:** AWS Budgets is configured with alerts at $1 (20%) and $5 (100%) against a $5/month budget. A Budget Action should additionally attach a Deny IAM policy at a higher threshold (~$10) to halt active spend (Bedrock InvokeModel, Lambda Invoke, S3/DynamoDB writes) automatically — never rely on manual monitoring alone to prevent runaway cost.

## Data Collection

- Polymarket Gamma API: live market questions, odds, volume, order book. No auth required.
- News: 10 curated RSS feeds (BBC, CBC, NYT, CNN, France 24), tagged by vertical at ingestion, matched to markets via keyword/entity matching.
- Bluesky (AT Protocol): sentiment analysis, topic extraction, entity recognition. Uses `app.bsky.feed.searchPosts` (public REST/JSON, no auth) — not the firehose/Jetstream, which requires a persistent connection incompatible with the serverless/budget model.
- Reddit was evaluated and dropped (see tech_debt.md) — Reddit's Responsible Builder Policy explicitly prohibits using API data to train/feed AI or ML models, and app registration was blocked accordingly.
- Cadence: every 12 hours across all 3 sources, 3 independent Lambdas (not one orchestrator) so a failure in one source doesn't block the others or waste PUT requests on retries. Reassess cadence after 3-7 days of observed cost.
- Markets/articles are curated into 3 verticals (Macro/Central Banks, Geopolitics/Elections, Regulatory/Tech) via keyword filtering at ingestion — see architecture_canon.md for the full taxonomy. Pop-culture/entertainment markets are out of scope: poor RAG material, resolution depends on gossip rather than structured, correlatable text.

## Out of Scope

- Real-time charting or visualization dashboards **as a user-facing product feature** — Poly-RAG is an AI/RAG assistant, not a Looker/Tableau-style analytics tool for end users. This does NOT rule out internal engineering observability (e.g. an architecture-decisions metrics table for cost/latency/benefit tracking, queried via CLI/notebook, not built as a polished dashboard) — that's tooling to evaluate our own decisions, not a product feature.
- Training predictive models (that was Pienza 1.0)
- GCP (even though it's familiar — the point is to learn AWS)

## Development Conventions

- LLM calls in ingestion are being trialed (2026-08-13), not dogmatically banned. Original rule was "on-demand only, not on every data pull" under the $5 hard-cap regime — now operating with a $120 promotional credit buffer and "spend deliberately, not miserly" philosophy. Currently trialing Bedrock summarization/entity-extraction at ingestion time for all 3 sources (News, Bluesky, Polymarket) for 3-4 days, measuring actual cost/latency/benefit via the architecture-decisions metrics table before deciding whether to keep, scope down, or revert to on-demand-only. See architecture_canon.md for the measured tradeoff.
- Prefer Lambda + event-driven patterns over always-on services
- Follow Chip Huyen's *AI Engineering* architecture patterns as primary reference (compute budget / FLOPs tradeoffs to be tackled later, per the book's framing)
- Explore LangChain and LlamaIndex alongside Claude Code for agent orchestration comparisons
- Architectural decisions with real cost/latency/benefit tradeoffs (e.g. LLM-in-ingestion vs on-demand-only) should be measured, not guessed — instrument with real Bedrock calls and CloudWatch timing, not estimates, and record the outcome as an ADR (Architecture Decision Record).

## Documentation Map (`.claude/claude_docs/`)

| File | Purpose |
|---|---|
| `architecture_canon.md` | **Current-state snapshot** of Poly-RAG's architecture (data sources, vertical taxonomy, LLM enrichment trial, AWS infrastructure inventory). Overwritten/updated in place as the architecture evolves — not a changelog. Start here to understand what's actually built right now. |
| `tech_debt.md` | **Open items** — known limitations, rejected alternatives (Reddit/X/Truth Social), and unresolved tradeoffs, each with Issue/Debt/Mitigation/Revisit structure. Entries close or get superseded as they're resolved. |
| `session_ledger.md` | **Chronological log** of work sessions, most recent first, added via `/end`. Immutable history — what happened and when, never edited after the fact. |
| `knowledge.md` | **Concept archive** — technical explanations (RAG, NLP, LLM deployment, AWS/GCP mappings, etc.) surfaced during sessions, added via `/knowledge` or proactively when a new concept comes up. |
| `hooks.md` | Documents this repo's Claude Code hooks (auto-updated when `.claude/settings.json` changes — see the hook itself). |
| `infra_design.md` | Claude Code environment design **for this repo** — hooks, skills, agents, MCP, memory conventions. About the dev environment, not about Poly-RAG's product architecture (that's `architecture_canon.md`). |
| `memory_mirror/` | Git-tracked mirror of Claude Code's internal memory store, kept in sync via hooks (union sync, no deletions). |
| `gerdau/` | Interview-prep sprint materials — gitignored, never tracked in version control. |

## Timezone Convention

Two clocks, deliberately. The split exists because the pipeline's day and the user's day
are not the same day, and conflating them silently misfiles things.

| Domain | Timezone | Why |
|---|---|---|
| **Corpus, RAG, data, infrastructure** | **UTC** (canon) | S3 partitions (`<source>/YYYY-MM-DD/HH.json`), cycle timestamps, odds snapshot `timestamp`, registry `first_seen`/`resolution_date`, EventBridge crons (`0 0,12 * * ? *`), CloudWatch logs. Never convert these -- a cycle is identified by its UTC hour and nothing else. |
| **Conversation with the user** | **UTC-6 (Mexico City)** | When talking about when things happened -- today, yesterday, this morning, last night -- always mean the user's local time, not UTC. State the offset explicitly if a sentence could be read either way. |
| **`session_ledger.md` entries** | **UTC-6 (Mexico City)** | The ledger is a log of the user's WORK SESSIONS, so its `## YYYY-MM-DD` headers must match the user's day. At UTC-6, anything worked on after 18:00 local falls on the NEXT UTC date -- dating the ledger by UTC would file an evening session under tomorrow. |

**The trap to watch:** at UTC-6 the two dates disagree for six hours of every day (18:00-23:59
local = 00:00-05:59 UTC of the following date). A ledger entry written at 20:00 Mexico City on
the 17th belongs under `## 2026-08-17`, even though `date -u` says the 18th. Get the local date
explicitly (e.g. `TZ=America/Mexico_City date +%F`) rather than assuming the environment's
default date is the right one.

Mexico City observes no DST as of 2022 (permanent UTC-6), so the offset is fixed year-round --
no seasonal adjustment needed.

## Git & Commit Rules

**Never commit on your behalf.** When you request a commit, I will provide the commit message only. You decide when and whether to run the actual commit.

Commit message format is canon, defined in `.claude/commands/commit-msg.md` (invoke with `/commit-msg`):
- Format: `type(scope): short summary`, blank line, bullets with `-`
- ZERO quotes of any kind (no single, double, typographic, or backticks), no emoji, plain ASCII only
- Hard limit of 500 characters total (subject + body)
- NEVER run `git commit` — only deliver the message text and stop

You then run `git add` and `git commit -m "..."` yourself.
