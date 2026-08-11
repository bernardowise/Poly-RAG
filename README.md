# Poly-RAG
Poly-RAG is Yet another IS-ALL-YOU-NEED LLM project that you don't need... yet.


Pienza 1.0's origin and framing

I was obsessed with digitizing my experience as an Uber driver in Mexico City while I transitiond my career into Data Science— capturing ~4,700 real accept/reject decisions over a controlled observation window, building the whole pipeline from raw OCR/webapp capture through classification, causal inference, synthetic data, and eventually a RAG layer on top. It was deliberately personal: I wasn't building for a market, I was proving to myself, that I could take a lived, messy dataset all the way through the ML lifecycle. That's exactly why for me Pienza was valuable despite the narrowness — it wasn't theoretical, I had real skin in the game with data I generated myself.


The two bottlenecks that define its ceiling

Static data, zero MLOps: everything in Pienza runs on a closed dataset. There's no live traffic, no continuous ingestion, no inference serving loop — which means it's a strong showcase of data science depth (stats, ML, GenAI) but doesn't demonstrate the engineering muscle of keeping a system alive against a moving target.

Narrow audience: n=1 driver, one city, one platform. Even a technically excellent project here has a ceiling on who cares, because the domain itself is hyper-specific.

Why the RL/MDP "Pienza 2.0" idea got cut

My original sequel concept doubled down on the same niche — autonomous driving decision-making via Markov Decision Processes (MDPs) — which would have taken me further into Waymo-adjacent territory instead of away from the two bottlenecks above. It solves neither the static-data problem in a meaningful new way nor the narrow-audience problem; if anything it deepens the audience problem, since full autonomous-driving RL is an even more specialized field than ride-hailing itself.

Why the Polymarket RAG project is the actual answer to both bottlenecks

It directly attacks the static-data ceiling: Polymarket's free Gamma API gives me a live, refreshable batch-data source, so for the first time I'm forced to design around data that changes daily instead of a dataset I already finished capturing.

It attacks the narrow-audience ceiling: financial markets and prediction data are a broadly legible domain — recognizable to way more people (and employers) than ride-hailing driver economics.

It shifts my skill focus from training predictive models (what Pienza 1.0 proved I can do) to AI engineering — retrieval, orchestration, tool-calling over live tabular + unstructured data — which is a distinct, currently-hot skill set I'm deliberately building in parallel with reading Chip Huyen's book, chapter by chapter.

While I'm currently job hunting, this repo will serve as a side personal year-long project that hopefully has a broader audience and allows me to upskill myself by learning by doing. 







Project Overview

This is a personal learning project — an AI application built on top of foundation models (LLMs), applied to the finance domain, specifically prediction markets (Polymarket). The project is guided by Chip Huyen's AI Engineering book, which serves as the primary reference for architecture and design decisions as the project evolves.

Unlike a prior project (Pienza), which relied on a single static dataset sitting in Google Cloud Storage, this project is explicitly designed to be dynamic — continuously ingesting live data rather than working off a frozen snapshot.

What This Is (and Isn't)

This is a RAG (retrieval-augmented generation) assistant with NLP components, built on top of LLMs — not a real-time charting dashboard. The visualization/analytics side is explicitly out of scope; the focus is on retrieval, reasoning, and language understanding over market-related data.

The Core Problem: Why Build This At All?

A central design constraint driving this project: it must do something a general-purpose LLM with web search (Claude, Gemini, etc.) genuinely cannot do. If a user could get an equally good answer by just asking Claude or Gemini directly, the project wouldn't be solving a real problem.

The differentiator is proprietary, self-collected historical data. General LLMs with web search can retrieve current information, but they can't retroactively reconstruct how a market's odds moved over time in correlation with specific news events or sentiment shifts — because that data isn't logged anywhere unless someone is actively collecting it. This project aims to be that someone: building a time-series dataset over weeks/months that ties together odds movement, news, and sentiment in one coherent, queryable view.

Data Sources

All three sources are being pulled from day one, rather than starting narrow:

Polymarket API — live market data: questions, current odds, trading volume, order book depth. This is the core structured time-series data.
News (free tier) — a free news API or RSS feeds from financial outlets, matched to specific markets via keyword/entity matching.
Reddit API — free access to relevant finance/politics subreddits, used as an unstructured text source for NLP tasks like sentiment analysis, topic extraction, and entity recognition.

Collection cadence: Data is logged on a recurring schedule (e.g. every few hours), building a proprietary historical dataset over time that ties odds movement to concurrent news and sentiment shifts.

Learning Objectives

This project is twofold in purpose:

Domain learning — becoming proficient in prediction markets and how they function as a financial product.
AI/MLOps engineering learning — the bigger motivator. Despite strong data science proficiency, MLOps is a current gap. This project is meant to force hands-on learning of:
Batch and real-time/streaming data processing (e.g. via Kinesis)
Building and orchestrating a live data pipeline rather than working from a static dataset
AWS-native MLOps tooling (Bedrock, SageMaker) as opposed to GCP, which is already familiar
Infrastructure & Cloud Strategy
Cloud provider: AWS, chosen deliberately over the already-familiar GCP, specifically to build proficiency in the ecosystem most AI companies use in production (Bedrock, SageMaker).
Approach: Full commitment to AWS from the start rather than prototyping on GCP first — the friction of the unfamiliar stack is considered part of the point.
Budget constraint: A hard cap of roughly $5/month for hosting. This shapes architecture choices significantly:
Favored: Lambda, S3, DynamoDB, Kinesis — all pay-per-use with no idle cost
Avoided: always-on compute like EC2 or MSK (Kafka), and frequent/high-volume Bedrock LLM calls, both of which can quickly exceed the budget
LLM calls should be made on-demand (e.g. when a user asks a question) rather than on every scheduled data pull, to keep Bedrock usage low
Context

This is a personal project, not built for a client or employer. It exists purely for learning purposes, with an underlying secondary motivation of building demonstrable, paid-work-adjacent skilled experience.





Development Philosophy: Agentic-First From Day One

This is the key architectural difference from Pienza. In Pienza, Claude Code was adopted late — introduced only when migrating from Jupyter Notebooks to Codespaces, effectively bolted onto an already-mature data science project. That meant the agentic tooling never got to shape the project's foundations.

This project inverts that. Agentic engineering is a first-class design goal from the start, not an afterthought layered on top of a finished data pipeline. Concretely, this means:

Building with Claude Code from scratch, with the codebase, project structure, and workflows designed around agentic development from commit one.
Multi-agent workflows and orchestration as a core architectural pattern — not a single assistant bolted onto a RAG pipeline, but a system of agents and sub-agents with defined responsibilities (e.g. a data-collection agent, a retrieval/research agent, a synthesis agent).
MCP (Model Context Protocol) integration, to give agents structured, tool-based access to data sources and services.
Reusing prior work — skills, commands, and hooks already developed during Pienza will be ported over as a starting foundation, since Pienza's own growth had plateaued.
Exploring frontier frameworks beyond just Claude Code — including LangChain and LlamaIndex — to compare approaches to agent orchestration, retrieval, and tool use.

This reflects a broader maturity point: having gone through Pienza already, the goal now isn't just to build another data science project, but to design the foundations — the agentic architecture itself — well enough that the project can keep growing and scaling, rather than plateauing the way Pienza did.



Why "Polymarket Agents" Doesn't Replace Poly-RAG???
 
Existing frameworks like Polymarket Agents focus on live execution and real-time execution loops (programmatic trading, querying current order books, immediate automated workflows). They are built to act now.Your project is built to answer: "What happened, why did it happen, and how did the market react over time?"FeatureExisting Polymarket FrameworksYour Poly-RAG ProjectPrimary GoalAutomated programmatic trading & live state querying.Historical context reconstruction & longitudinal reasoning.Data PhilosophyReal-time stateless API polling.Proprietary, stateful time-series data storage.The "Moat"Execution speed and API connectivity.Self-collected historical correlation (Odds + News + Sentiment).Core LLM ActionTool-calling for immediate market actions.RAG over an unstructured/structured temporal database.A general-purpose LLM or a standard trading agent cannot look back and explain how a sudden Reddit sentiment spike on a Tuesday correlated with an odds drop on Thursday because that historical intersection isn't preserved in a single, open API. 
