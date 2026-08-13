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
| LLM | Bedrock | On-demand only (not on every data pull) |
| Data sources | Polymarket Gamma API, news RSS (10 feeds), Bluesky AT Protocol | All free |

**Budget constraint: spend as if it were real money, not free credits.** The AWS account carries ~$120 in promotional credits ($100 signup + $20 for completing a Budgets activity), but this is a discipline exercise, not a spending allowance — design and operate as if every dollar were out of pocket. Avoid always-on compute (EC2, MSK/Kafka) and frequent Bedrock calls. Batch writes where possible (S3 free tier caps at 2,000 PUT requests/month — request count matters more than payload size).

**Guardrail:** AWS Budgets is configured with alerts at $1 (20%) and $5 (100%) against a $5/month budget. A Budget Action should additionally attach a Deny IAM policy at a higher threshold (~$10) to halt active spend (Bedrock InvokeModel, Lambda Invoke, S3/DynamoDB writes) automatically — never rely on manual monitoring alone to prevent runaway cost.

## Data Collection

- Polymarket Gamma API: live market questions, odds, volume, order book. No auth required.
- News: 10 curated RSS feeds (BBC, CBC, NYT, CNN, France 24), tagged by vertical at ingestion, matched to markets via keyword/entity matching.
- Bluesky (AT Protocol): sentiment analysis, topic extraction, entity recognition. Uses `app.bsky.feed.searchPosts` (public REST/JSON, no auth) — not the firehose/Jetstream, which requires a persistent connection incompatible with the serverless/budget model.
- Reddit was evaluated and dropped (see tech_debt.md) — Reddit's Responsible Builder Policy explicitly prohibits using API data to train/feed AI or ML models, and app registration was blocked accordingly.
- Cadence: every 12 hours across all 3 sources, 3 independent Lambdas (not one orchestrator) so a failure in one source doesn't block the others or waste PUT requests on retries. Reassess cadence after 3-7 days of observed cost.
- Markets/articles are curated into 3 verticals (Macro/Central Banks, Geopolitics/Elections, Regulatory/Tech) via keyword filtering at ingestion — see infra_design.md for the full taxonomy. Pop-culture/entertainment markets are out of scope: poor RAG material, resolution depends on gossip rather than structured, correlatable text.

## Out of Scope

- Real-time charting or visualization dashboards
- Training predictive models (that was Pienza 1.0)
- GCP (even though it's familiar — the point is to learn AWS)

## Development Conventions

- LLM calls are on-demand only (triggered by user queries, not data ingestion jobs)
- Prefer Lambda + event-driven patterns over always-on services
- Follow Chip Huyen's *AI Engineering* architecture patterns as primary reference
- Explore LangChain and LlamaIndex alongside Claude Code for agent orchestration comparisons

## Git & Commit Rules

**Never commit on your behalf.** When you request a commit, I will provide the commit message only. You decide when and whether to run the actual commit.

Commit message format is canon, defined in `.claude/commands/commit-msg.md` (invoke with `/commit-msg`):
- Format: `type(scope): short summary`, blank line, bullets with `-`
- ZERO quotes of any kind (no single, double, typographic, or backticks), no emoji, plain ASCII only
- Hard limit of 500 characters total (subject + body)
- NEVER run `git commit` — only deliver the message text and stop

You then run `git add` and `git commit -m "..."` yourself.
