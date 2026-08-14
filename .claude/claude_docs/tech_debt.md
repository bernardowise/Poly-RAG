# Tech Debt

Track known limitations, architectural compromises, and planned refactors that may constrain the project as it grows.

---

## Reddit Dropped as Data Source — Replaced by Bluesky

**Issue (resolved 2026-08-13):** Reddit's OAuth2 app registration form failed silently
(reset after reCAPTCHA + submit, no error). Root cause confirmed by reading Reddit's
"Responsible Builder Policy" directly: it explicitly states data obtained via their API
"must not" be used for "commercial and non-commercial mining, scraping, or using data for
purposes like ads targeting or **to train machine learning or AI models**" — this is a
deliberate policy restriction, not a bug or account issue. The classic `/prefs/apps`
registration path also appears to be getting deprecated in favor of Devvit (Reddit's
in-platform app framework, not suited for external data extraction).

**Decision:** Reddit is dropped entirely as a data source rather than pursued via formal
written approval (too slow for the sprint timeline, and the use case — feeding a RAG
pipeline — squarely falls under what the policy prohibits). Also evaluated and rejected:

- **X (Twitter):** no viable free read tier for new developers (pay-per-use only, ~$0.005/read,
  budget-incompatible at meaningful volume) AND explicit contractual ban on RAG/model-training
  use in the Developer Agreement — disqualified on both cost and policy grounds.
- **Truth Social:** no public developer API exists at any price for individuals (their 2026
  "Truth API" targets institutional financial clients only); ToS broadly prohibits any
  automated scraping. No legitimate access path.

**Replacement: Bluesky (AT Protocol).** Uses `app.bsky.feed.searchPosts` — REST/JSON,
keyword search built in (`q=<term>` param). No explicit ToS restriction on third-party
AI/RAG use (contrast with X). Architecturally compatible with the serverless Lambda
pattern — critically, uses the REST search endpoint, NOT the firehose/Jetstream (which
requires a persistent WebSocket connection and would be incompatible with short,
scheduled Lambda invocations under the budget cap).

**Correction (2026-08-14):** initial research said `searchPosts` was public/no-auth via
`public.api.bsky.app`. Confirmed false by direct reproduction — that endpoint returns
403 on `searchPosts` specifically (other read endpoints like `getProfile` remain open
there), even with a valid Bearer token. The actual working setup: authenticate via
`com.atproto.server.createSession` against `bsky.social` (the PDS) using an app
password (bsky.app -> Settings -> App Passwords, not the account password), then call
`searchPosts` against `bsky.social` as well — not `public.api.bsky.app`. Lambda
re-authenticates once per invocation rather than persisting/refreshing the short-lived
JWT, which is simpler and correct at a 12h cadence. Credentials stored as Lambda
environment variables (BLUESKY_HANDLE, BLUESKY_APP_PASSWORD), never in code or git.

**Lesson for future source evaluation:** always check a platform's ToS/developer policy
for AI/ML-training restrictions before investing implementation time — Reddit and X both
have explicit bans that would have blocked this project regardless of technical feasibility
or payment. This is now a standing check for any future data source candidate. Also:
verify auth requirements empirically (curl/direct test) rather than trusting docs/research
alone — platforms tighten previously-open endpoints without much notice.

---

## Budget Constraint → Always-On Compute Avoidance

**Issue:** $5/month hard cap forces event-driven architecture (Lambda, Kinesis) over conventional approaches.

**Debt:** 
- No always-on message queue (MSK/Kafka) means polling-based data ingestion instead of push-based streams
- No persistent compute means cold-start latency on first query after inactivity
- LLM calls strictly on-demand (not during ingestion) limits real-time synthesis capabilities

**Mitigation:**
- Kinesis + Lambda for efficient batch processing within budget
- Pre-cache common queries in DynamoDB to reduce Bedrock calls on repeat queries

**Revisit if:** Budget increases or AWS credits become available.

---

## Static → Live Data Transition (Early Stage)

**Issue:** Unlike Pienza's frozen dataset, this project ingests live data daily but only recently shifted to this model.

**Debt:**
- Data schema and pipeline architecture not yet battle-tested at scale
- No established SLOs for data freshness, latency, or accuracy
- Incremental ingestion logic may have edge cases (duplicate detection, late arrivals, schema drift)

**Mitigation:**
- Monitor ingestion logs for anomalies in early weeks
- Build monitoring/alerting around Polymarket API reliability
- Validate data against manual spot checks

**Revisit if:** Data quality issues emerge or pipeline reliability becomes a blocker.

---

## Narrow Prototype → Multi-Agent Orchestration

**Issue:** Starting agentic-first, but initially only one agent (retrieval/synthesis). Multi-agent coordination patterns not yet proven.

**Debt:**
- Agent isolation and communication protocols still being designed (no canonical MCP server layout yet)
- No established patterns for agent memory sharing or conflict resolution
- Tool-call tracing and observability across agents rudimentary

**Mitigation:**
- Start with tight agent coupling, decouple later as patterns emerge
- Log all agent→tool→response chains for debugging
- Document agent responsibilities as they stabilize

**Revisit if:** Adding more agents reveals coordination challenges.

---

## AWS Proficiency Gap

**Issue:** Deliberately chose unfamiliar AWS stack to build production-relevant expertise. This is a learning curve.

**Debt:**
- Lambda cold-start behavior not yet fully characterized
- Kinesis shard scaling, retention policies being learned via trial-and-error
- Bedrock cost modeling still rough; actual usage patterns may surprise

**Mitigation:**
- Set up CloudWatch dashboards early to track Bedrock tokens/cost
- Test Lambda concurrency limits with realistic traffic
- Document learnings in CLAUDE.md as they emerge

**Revisit if:** AWS costs spike unexpectedly or performance becomes unpredictable.

---

## Proprietary Data Collection Not Yet Validated

**Issue:** Core differentiator is self-built historical time-series (odds + news + sentiment). Hasn't yet proven this data is actually useful.

**Debt:**
- Correlation analysis between odds and news/sentiment is exploratory; success metrics undefined
- No external benchmark (can't compare to paid market research services, which are proprietary)
- If news/sentiment turns out to be weakly correlated with odds, the entire project's premise collapses

**Mitigation:**
- Define success metrics early: which correlations matter, what p-values are acceptable
- Start with simple synthetic experiments to validate assumptions
- Be prepared to pivot if initial data exploration disappoints

**Revisit if:** Exploratory analysis reveals weak signal or no actionable insights.

---

## Framework Comparison Not Yet Complete

**Issue:** README mentions exploring LangChain and LlamaIndex alongside Claude Code. Haven't yet settled on canonical patterns.

**Debt:**
- Code may contain experimental branches or duplicate logic if frameworks are being compared in parallel
- No established conventions for agent orchestration (custom vs. LangChain vs. LlamaIndex)
- Refactoring risk if framework choice changes mid-project

**Mitigation:**
- Make explicit framework choice within first month of development
- Keep framework-specific code isolated so pivoting is lower-cost
- Document pros/cons of each approach in decision log

**Revisit if:** Framework maturity or API stability changes unexpectedly.

---

## Data Source Dependency Risk

**Issue:** Polymarket Gamma API, news feeds, Reddit API are all free but not guaranteed to persist.

**Debt:**
- No fallback if Polymarket API becomes unavailable or rate-limits aggressively
- News feed quality depends on free tier availability and may be discontinued
- Reddit API access subject to policy changes (rate limits, authentication)

**Mitigation:**
- Archive ingested data to S3 for offline analysis
- Monitor API health and build alerting for outages
- Keep data models flexible to adapt to API schema changes

**Revisit if:** Any data source becomes unavailable or unreliable.

---

## MLOps → Production Handoff Unknown

**Issue:** Project is explicitly a learning exercise, not a production system. Handoff path unclear.

**Debt:**
- No CI/CD pipeline yet; manual deployments via Claude Code
- No production monitoring or runbooks
- Unclear if/how this evolves into a real product or stays a learning project

**Mitigation:**
- Assume learning phase lasts 6 months; plan infrastructure review at that point
- Build runbooks as you go (deploy docs, rollback procedures)
- Revisit project scope when learning goals are met

**Revisit if:** Project matures or external audience emerges.

---

## Self-Referential Corpus Not Yet Built

**Issue:** Core goal is to interact with project evolution via RAG — querying how README and claude_docs changed over git history. Not yet implemented.

**Debt:**
- No git-history-aware corpus; can't yet ask "how did architecture philosophy evolve?" or "what was the original tech debt list at commit X?"
- Docs exist in version control but aren't indexed by commit timestamp
- No retrieval mechanism for historical doc state (what was true 3 months ago vs. now?)

**Mitigation:**
- Design corpus schema to track doc versions by commit (`.claude/corpus/manifest.json` or similar)
- Build ingestion script (`.claude/scripts/build_corpus.sh`) to parse git history and extract doc snapshots
- Define query interface (MCP server? embedding store? simple search?)
- Start ingesting incrementally as project history accumulates (revisit after 1-2 months of commits)

**Revisit if:** Enough commit history exists to make historical queries meaningful (target: 2-3 months in).
