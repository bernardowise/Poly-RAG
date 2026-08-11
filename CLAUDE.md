# Poly-RAG — Claude Code Guide

## Project Purpose

A personal AI engineering learning project: a RAG assistant over live Polymarket prediction market data, correlated with news and Reddit sentiment. The differentiator is a self-built historical time-series of odds movement + concurrent news/sentiment — data no general-purpose LLM can reconstruct retroactively.

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
| Data sources | Polymarket Gamma API, news RSS/free tier, Reddit API | All free |

**Budget constraint: ~$5/month hard cap.** Avoid always-on compute (EC2, MSK/Kafka) and frequent Bedrock calls.

## Data Collection

- Polymarket API: live market questions, odds, volume, order book
- News: matched to markets via keyword/entity matching
- Reddit: sentiment analysis, topic extraction, entity recognition
- Cadence: recurring schedule (every few hours), building a proprietary historical dataset over time

## Out of Scope

- Real-time charting or visualization dashboards
- Training predictive models (that was Pienza 1.0)
- GCP (even though it's familiar — the point is to learn AWS)

## Development Conventions

- LLM calls are on-demand only (triggered by user queries, not data ingestion jobs)
- Prefer Lambda + event-driven patterns over always-on services
- Follow Chip Huyen's *AI Engineering* architecture patterns as primary reference
- Explore LangChain and LlamaIndex alongside Claude Code for agent orchestration comparisons
