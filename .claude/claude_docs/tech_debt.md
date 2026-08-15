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

**Issue:** Polymarket Gamma API, news RSS feeds, and Bluesky AT Protocol are all free but not guaranteed to persist.

**Debt:**
- No fallback if Polymarket API becomes unavailable or rate-limits aggressively
- News feed quality depends on free tier availability and may be discontinued
- Bluesky API access subject to policy changes (rate limits, authentication) — see the Reddit/X/Truth Social entry above for why Bluesky was chosen and what was rejected

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

---

## Ingestion Redesign: Verifiability Filter + ID Registry + Dynamic Cross-Source Linking (Not Yet Executed)

**Issue (raised 2026-08-15, Day 3):** The current ingestion architecture has three separate
problems that turned out to share one root cause and, potentially, one fix. Discovered while
exploring real Polymarket data in the Day 3 Databricks notebook — this is a design decision,
recorded here deliberately BEFORE execution, per explicit instruction: draft the full redesign
now, keep building the rest of the sprint, come back and implement later.

**Problem 1 — the 3 verticals are a keyword proxy for something else.** Macro/Geopolitics/
Regulatory-Tech (and the blanket sports/pop-culture exclusion) were meant to filter for "good
RAG material." But interrogating the actual criterion revealed the real axis isn't topic
seriousness — it's **whether a market's outcome is objectively verifiable against a citable
public record, vs. resolved by human judgment over ambiguous social evidence.** Example:
"does the Fed raise rates in March" resolves against an FOMC statement (unambiguous); "do
celebrities X and Y break up this year" resolves by a human/UMA oracle judging rumors and
social posts (disputable, contested in Polymarket's own resolution forums). Under this
corrected axis, a sports market with an official scoreboard is MORE verifiable than a
celebrity-relationship market, even though today's `SPORTS_EXCLUSION_PATTERNS` blocks the
former outright and no filter exists for the latter's actual weakness.

**Problem 2 — no cross-source linkage.** Polymarket, News, and Bluesky run as 3 fully parallel,
mutually blind pipelines. News/Bluesky search against a static, hand-written keyword dict
(`VERTICAL_KEYWORDS`) that has no relationship to which specific markets Polymarket actually
pulled that cycle. There is no field anywhere that says "this article is about market id X."
This blocks the project's actual differentiator (self-built historical time-series of odds +
concurrent correlatable news/sentiment) — without id-level linkage, correlation has to be
reconstructed by hand after the fact, if at all.

**Problem 3 — no lifecycle tracking.** Markets are pulled fresh every 12h via `active=true`
with no memory of what was seen before. There's no registry distinguishing "still open, being
tracked" from "resolved, and here's the final outcome" — so the project can never build the
one dataset that would make retroactive analysis possible: odds trajectory + surrounding
text, from open to resolution.

**Proposed redesign (drafted, not implemented):**

1. **Verifiability filter as the first agentic step in ingestion.** Replace (not supplement)
   keyword-based vertical tagging and the sports exclusion with an LLM classification step:
   for each candidate market, the LLM judges "is this market's outcome objectively verifiable
   against a citable public record, or does it depend on human interpretation of ambiguous
   evidence?" Pass -> ingest. Fail -> discard. This is a stronger justification for
   LLM-in-ingestion than the current summarization trial — the LLM becomes the quality gate
   itself, not an add-on. Candidate universe to run this against: top N markets by
   `volume24hr` ("trending" — NOT `volume` total, which the current Lambda uses today and
   favors old, possibly-dead markets over what's actually moving right now), not a
   pre-filtered keyword subset — volume already does cheap, partial verifiability filtering
   for free (large money rarely backs disputable-resolution markets), so this also resolves
   the open "4th vertical" question (a trending, topic-agnostic pull) without needing a
   separate tag or pipeline — trending and the old verticals converge into one mechanism.

   **Measured real cutoff data (2026-08-15, Gamma API, `order=volume24hr`).** First pass used
   only 500 fetched markets as the denominator, which inflated every percentage (the top-500
   sum was miscounted as "100% of the total" simply because 500 was all that had been
   fetched). Corrected by paginating the full active pool: standard `offset` pagination breaks
   around offset ~2100 with an explicit API error (`"offset too large, use /markets/keyset for
   deeper pagination"` — `/markets/keyset` cursor-based pagination not yet implemented, so the
   true universe may be larger than 2,100). Using the confirmed ~2,100 active markets
   ($30,976,726 combined volume24hr) as the denominator:

   | Top N | % of total volume24hr captured | Cutoff value |
   |---|---|---|
   | 10 | 15.9% | $287K |
   | 20 | 24.2% | $229K |
   | 30 | 30.2% | $167K |
   | 50 | 38.4% | $104K |
   | 100 | 49.7% | $52K |
   | 200 | 62.8% | $31K |
   | 300 | 70.7% | $20K |
   | 500 | 80.7% | $11K |

   This corrects TWO prior assumptions in sequence: an initial genericized claim ("top 100-300
   captures ~90%"), and a first-pass measurement inflated by an incomplete denominator. With
   the real denominator, the tail is even fatter than either prior estimate — top 100 barely
   captures half the platform's 24h volume, and even top 500 doesn't reach 90%. Also worth
   implementing `/markets/keyset` pagination eventually to get the exact total universe size
   instead of the ~2,100 floor established here.

   **Cost correction (2026-08-15):** the "wider N multiplies LLM cost" tension above assumed
   every candidate gets re-evaluated every cycle — wrong once the market registry (piece 2
   below) exists. Real model: the LLM verifiability pass only needs to run on IDs NOT already
   in the registry. Cycle 1 (bootstrap) evaluates the full top-N candidate set; every cycle
   after that, re-fetching top-N and diffing against the registry should surface a small
   number of genuinely new IDs (the top-500-by-volume set likely doesn't churn much run to
   run), plus IDs that dropped out of top-N (candidates for marking `resolved`). So per-cycle
   LLM cost scales with new-ID arrival rate, not with N — which removes most of the pressure
   to keep N small, and makes a wider initial candidate pool (300-500) more affordable than
   it first appeared. Still needs real measurement: how many genuinely new IDs actually
   appear per 12h cycle in practice (unmeasured, first bootstrap run not yet executed).
2. **A master market registry, split into two pieces with different change patterns** —
   conflating them into one table either duplicates static text every cycle for nothing, or
   overwrites historical odds when only the current price should update:
   - **Registry (metadata, updates in place, one row per market id):** id, question,
     description, endDate, resolutionSource, first_seen, status (open/resolved),
     resolution_date, final_outcome (Yes/No, captured once available). Changes rarely —
     mostly once at creation, once at resolution.
   - **Odds time-series (append-only, one row per market id PER observation/cycle):**
     market_id, timestamp, outcomePrices, volume, volume24hr, liquidity. This IS the
     project's actual differentiator (self-built historical time-series of odds movement) —
     must be appended every 12h cycle, never overwritten, or the time-series is lost.
   Not yet decided: DynamoDB item per id (natural fit, already using DynamoDB for metrics)
   vs. a Delta table (fits the Day 3 Databricks work already in progress) vs. S3 with
   overwrite — and the two pieces above may not even want the same storage choice (registry
   is small/mutable, time-series is append-heavy/growing). Needs its own design pass.
3. **Dynamic, per-market keywords replacing the static `VERTICAL_KEYWORDS` dict.** While a
   market is open, keywords/entities extracted from its own `question`/`description` (LLM or
   NER) drive what News/Bluesky search for. When a market resolves, its keywords stop being
   queried ("keywords die with the market" — user's framing). Open question: who/what
   triggers News/Bluesky re-pulls when the active keyword set changes — still independent
   12h-cadence Lambdas, or does this require tighter coupling than today's 3-Lambda isolation
   design allows?
4. **Explicit id-level linkage on ingested News/Bluesky items** — each article/post tagged
   with the specific market id(s) it relates to, not just a generic vertical label. This is
   the actual prerequisite for meaningful sentiment analysis correlated to odds movement,
   which is the project's core differentiator per README/CLAUDE.md.

**Design decisions closed (2026-08-15, before execution):**
- **Registry storage:** DynamoDB, one item per `market_id`, update in place — reuses existing
  IAM/write patterns from `poly-rag-architecture-metrics`. Explicitly not final: may move to a
  Delta table once Databricks work matures; revisit after Day 3 if that looks better.
- **Odds time-series storage:** S3, one JSON file per `market_id`, read-modify-write each
  cycle (append the new odds snapshot, rewrite the file) — reuses the existing S3 partitioning
  pattern instead of introducing a new service. Tradeoff accepted: this means one S3 write per
  tracked market per cycle, not one write per Lambda run as today — watch the 2,000 PUT/month
  free-tier ceiling as the tracked-market count grows.
- **Resolved detection:** query the Gamma API per-id endpoint (`markets/{id}`) directly for
  ids that drop out of the `active=true` pull, reading `closed` + the real final outcome —
  chosen over inferring resolution from absence alone, because inference doesn't give the
  actual Yes/No outcome, which is required data (not optional) for the historical dataset.
  Cost: one extra API call per id that disappears from the top-N pull each cycle (Polymarket's
  Gamma API, not Bedrock — no LLM cost implication).
- **News/Bluesky re-pull trigger:** no new trigger — same independent 12h EventBridge cron as
  today. News/Bluesky read active keywords from the registry (populated by the Polymarket
  Lambda earlier in the same cycle, same 5-min-offset pattern already used by send_digest)
  instead of the static `VERTICAL_KEYWORDS` dict. Preserves the Day 2 rationale for 3
  independent Lambdas (failure isolation) — explicitly rejected chaining Polymarket ->
  News/Bluesky invocation directly, since that would reintroduce the single-point-of-failure
  coupling the 3-Lambda split was designed to avoid.
- **Keyword generation:** the LLM produces search terms per market in the SAME Bedrock call
  as the verifiability judgment — single structured (JSON) response returns `is_verifiable`
  plus search terms, not two separate calls. Zero additional Bedrock cost for this piece;
  keeps the "one batched call" cost discipline already applied to the existing summarization
  trial. Rejected: cheap regex/NER extraction without an LLM call — less precise (misses
  context/relationships an LLM would catch), and the LLM call already has to happen for
  verifiability, so piggy-backing costs nothing extra.
- **Two search representations, not one (corrected 2026-08-15, same day):** an isolated-
  keywords-list format was tried first and rejected — e.g. `["Elon Musk", "tweets", "August
  2026"]` used as 3 independently-searched terms loses the connection between them (a search
  for "August 2026" alone matches almost anything). The fix isn't a single combined string
  either, because News and Bluesky match text through fundamentally different mechanisms:
  - **`search_query`** (single combined free-text string, e.g. "Elon Musk Twitter posting
    frequency August 2026"): for Bluesky's `searchPosts`, which accepts natural-language
    search-engine-style queries.
  - **`news_match_terms`** (short list of 1-3 distinctive multi-word phrases, e.g.
    `["Federal Reserve", "September 2026"]`, not `["Fed", "rate", "2026"]`): for News, which
    has no search API — it downloads full RSS article text and greps it, so matching needs
    AND logic (every term must co-occur in the SAME article) against terms specific enough
    that co-occurrence is meaningful, not generic words that appear everywhere.
  Both are produced in the same LLM call as the verifiability judgment — still zero additional
  Bedrock cost. Verified via a real Bedrock invoke-model test (2026-08-15, 20-market sample,
  outside the Lambda) before wiring into the handler — see example outputs in the prompt
  itself (`_classify_batch` in `lambdas/ingest_polymarket/handler.py`).
- **Initial candidate N:** top 500 by `volume24hr` for the bootstrap run. Affordable per the
  cost-correction note above (steady-state cost scales with new-id arrival rate, not N).

**Still explicitly open (may surface during implementation):**
- Real cost of per-market LLM verifiability classification vs. today's batched-summary trial
  (must be measured, not assumed, per CLAUDE.md's cost/latency/benefit discipline)
- Real new-ID arrival rate per 12h cycle (determines actual steady-state LLM cost — see cost
  correction note above)
- Whether vertical/topic labels survive as descriptive metadata even after the filtering
  mechanism changes

**Status:** design closed 2026-08-15, execution starting same day — S3 data ingested under the
OLD 3-vertical-keyword shape (`polymarket/`, `news/`, `bluesky/` prefixes as they exist today)
is retained as-is, not deleted, and treated as obsolete/pre-redesign once the new pipeline's
first manual pull runs. First run of the new design will be a manual pull (not cron), followed
by re-enabling the 12h EventBridge schedule once verified.

**Revisit if:** Real new-ID arrival rate or per-market LLM cost, once measured, invalidates the
"wider N is affordable because only new ids get classified" assumption above.

---

## Known Limitation: Explicit ID-Linkage Only Captures Direct Correlation

**Issue (raised 2026-08-15, during bootstrap review):** The keyword-based linkage between
market ids and News/Bluesky items (see "Ingestion Redesign" entry above) only links content
that explicitly matches a market's extracted keywords. This is a structural limitation of any
keyword-matching approach, not a bug to fix in the current design.

**Concrete example:** a generic article about "crypto market sentiment souring" could move a
Bitcoin-price market's odds without ever mentioning "Bitcoin" or the specific price threshold
literally in a way keyword matching catches — it would never get linked to that market_id,
even though it may be the real driver of the odds movement.

**Why this can't be fully solved by better keywords:** even a well-designed keyword/query
(see the "Elon Musk tweets" example — keywords need to work as a combined, reasoned search
query, not isolated terms, which the redesign above addresses) only catches DIRECT mentions.
Ambient/indirect correlation (sentiment shifts, related-but-not-explicitly-connected events)
requires comparing the full ingested corpus against odds movement using semantic similarity
(embeddings), not keyword presence.

**Correction (2026-08-15, same day):** retrieval should NOT be limited to explicitly-linked
items only — that throws away real signal for no good reason. Every News/Bluesky item already
carries an ingestion timestamp regardless of whether it matched any market's keywords, so a
second, cheap retrieval layer is available today, no embeddings required: a TIME-WINDOW query
("everything ingested in the 12h window around when market X's odds moved") over the existing
raw S3 storage. This is not the same fix as better keywords (which only improves the
high-confidence layer) — it's an entirely separate, complementary retrieval path.

**Corrected framing — retrieval has (at least) two layers, not one:**
1. **High-confidence (explicit id-linkage, Silver):** "we know with certainty this item is
   about this market" — shown first, treated as likely-causal.
2. **Contextual/temporal (raw timestamp window, no linkage required):** "this happened in the
   world while this market moved" — shown as supporting context, weaker/noisier signal, but
   real and available immediately, not gated on Day 4 work.
Within layer 2's (often large, noisy) time window, semantic similarity (embeddings, Day 4)
becomes useful for RANKING relevance — not for discovering the window itself, which timestamp
filtering already does for free.

**Revisit if:** Implementing this — building the time-window retrieval query is not blocked on
anything else in the current redesign and could be done alongside or right after News/Bluesky
keyword-reading (tasks 3/4 in progress as of 2026-08-15).

---

## Future Consideration: Live Web Access for the Synthesis/RAG Agent

**Issue (raised 2026-08-15):** Today's Bedrock usage (both the ingestion-time verifiability
classifier and the planned Day 5 synthesis agent) is a closed model with no live internet
access — it only knows what's explicitly placed in the prompt from Poly-RAG's own S3/DynamoDB
storage, plus frozen training-time knowledge. This is a deliberate, correct constraint for the
ingestion Lambdas (keeps cost/latency bounded, batch-shaped, and reproducible — see the
verifiability-classifier design above). It becomes a real limitation for the Day 5 synthesis
agent, whose whole point is answering user questions that may need context beyond what any
12h ingestion cycle happened to capture.

**Capability that exists but isn't used here:** Claude supports tool-use-based web browsing /
"computer use" (agentic tool calls that let the model fetch live web content mid-conversation)
via the Claude API / Agent SDK. Not wired into Poly-RAG today.

**Why not adopted yet:** wiring this into the current ingestion Lambdas would break the
batch cost/latency model this whole redesign is built on (single bounded Bedrock call per
Lambda run, cost that scales with new-id arrival rate, not with open-ended browsing). This is
a different architectural shape — synchronous, user-facing, unbounded-latency-tolerant — that
belongs to the Day 5 synthesis agent, not to ingestion.

**Likely real use case:** the Day 5 synthesis/RAG agent, when answering a live user question,
could optionally reach beyond the ingested corpus (e.g. "what's the latest on X" when the most
recent relevant ingestion cycle is 10h stale) via live web tool-use, clearly distinguished in
the response from grounded-in-our-own-corpus answers.

**Revisit if:** Day 5 synthesis agent work begins — evaluate whether user questions actually
need live-web reach beyond the ingested corpus before adding the complexity/cost of tool-use
web browsing, rather than assuming it's needed upfront.

---

## Future Consideration: Overnight/Unattended Agent Work (Cloud-Isolated Agents)

**Issue (raised 2026-08-15, end of Day 3):** User asked whether Claude Code could keep
executing pending ingestion-redesign work (bootstrap re-run, News/Bluesky migration) while
the Codespace machine is powered off overnight. Answer today is no: Claude Code runs as a
process inside this Codespace — powering off the machine stops the process entirely, there is
no separate "Claude" running elsewhere that survives the host shutting down. This is directly
relevant to the project's explicit "agentic-first from day one" architecture philosophy in
CLAUDE.md, so it's recorded here rather than treated as a one-off answer.

**What does exist for this pattern:** Claude Code supports launching agents with `isolation:
"remote"`, which runs in a cloud environment separate from the local machine — this COULD
survive a local machine being powered off, since the agent isn't tied to the Codespace's
lifecycle.

**Why not set up tonight:** running Poly-RAG's actual pending work (re-running the ingestion
bootstrap, editing/deploying Lambdas via Terraform) in a remote environment requires that
environment to have safe access to AWS credentials and the Databricks token currently sitting
in the gitignored `.secrets` file — wiring that up safely is its own task, not something to
improvise late at night right before the credentials would need to be trusted to a new
environment. Deferred deliberately, not forgotten.

**Revisit if:** The project reaches a point where genuinely unattended/overnight agent work
(scheduled ingestion redesign steps, batch backfills, etc.) becomes valuable enough to justify
setting up secure credential access in a remote-isolated agent environment — evaluate the
`isolation: "remote"` Agent option and/or the `schedule` skill (cron-based cloud agents) at
that point, rather than defaulting to "just leave the Codespace running overnight," which
defeats the cost-discipline principle in CLAUDE.md if left unattended repeatedly.
