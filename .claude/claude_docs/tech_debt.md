# Tech Debt

Track known limitations, architectural compromises, and planned refactors that may constrain the project as it grows.

---

## Minimum-Horizon Filter Uses the Wrong Field for Sports Markets (found 2026-08-16)

**Issue:** `MIN_HORIZON_HOURS = 48` in `ingest_polymarket` (see "Ingestion Redesign" entry
below) was added specifically to exclude markets that resolve too soon to demonstrate the
project's thesis (news/sentiment influencing odds while a market is open) -- the original
motivating case was live sports/esports matches resolving within minutes. The filter compares
`now` against the market's `endDate` field. Confirmed via direct Gamma API inspection
(2026-08-16) that `endDate` for sports markets is NOT the actual game time -- it is a later
administrative deadline (observed case: market 3448712, Kansas City Royals vs. Los Angeles
Angels, `endDate: 2026-08-23T01:38:00Z`, roughly a week after `event.eventDate: 2026-08-15`,
which is the real game date). The filter compared against the wrong timestamp and let a
same-day game through the 48h horizon check.

**Root cause:** `endDate` appears to represent something like "deadline for the market to be
administratively resolved/closed" (likely with buffer for disputes, postponements, etc.), not
"when the outcome becomes known." For non-sports markets (elections, macro events) these two
concepts are usually close together; for sports, they can diverge by over a week.

**Candidate fix (not yet implemented):** `event.eventDate` (or `event.startDate`) looks like a
better proxy for "when does this actually resolve" for sports/recurring markets specifically --
needs verification across a wider sample before wiring in, since the field may not exist or
may behave differently for non-sports event types.

**Revisit if:** building on the market registry's sports-market horizon accuracy becomes load-
bearing for something (e.g. the Comments ingestion work below, which surfaced this bug while
investigating a specific sports market) -- fix the filter to use the right field instead of
just widening MIN_HORIZON_HOURS, which wouldn't actually solve a wrong-field bug.

---

## Reddit Dropped as Data Source — Replaced by Bluesky

**Update (2026-08-16): Bluesky itself replaced by Polymarket Comments, deployed and verified.**
See the "Comments Source Replaces Bluesky" entry below for the full investigation, design, and
production verification. This entry (Reddit vs. the alternatives evaluated in 2026-08-13) stays
as historical record of why Bluesky was chosen originally -- not rewritten, since Reddit's
rejection reasoning is still accurate and unrelated to the Bluesky->Comments swap.

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

---

## News Source Redesign: Google News RSS Replaces the 10 Curated Feeds (Conscious ToS Exception)

**Issue (raised 2026-08-15):** Auditing real News linkage results (367 articles, only 3 linked
to any tracked market via AND-match) revealed the root cause was NOT a broken matching
mechanism — it was that the LLM-generated `news_match_terms` used Polymarket's formal contract
language ("Federal Reserve", "President of Russia") instead of how journalism actually refers
to these entities ("Fed", "Putin"). Confirmed empirically: searching the day's real articles
for "federal reserve" (exact) returned 0 matches; "fed " returned 1; "putin" alone returned 2,
but the market's required second AND term ("President of Russia") never appeared verbatim
anywhere. Root cause: the classification prompt only receives a market's `question` field (one
short formal sentence) as its only source text — with that narrow a "nanocorpus," the LLM has
no way to infer real-world journalistic vocabulary, it can only paraphrase the formal wording
it was given.

**Decision:** replace the 10 fixed curated RSS feeds (BBC, CBC, NYT, CNN, France24) with
targeted Google News RSS searches (`news.google.com/rss/search?q=<query>`), one per tracked
market, using the market's own `question` text directly as the search query — Google's own
search relevance engine handles the synonym/paraphrase problem instead of a hand-tuned
AND-term list.

**Conscious ToS exception (per CLAUDE.md's own "check ToS before investing implementation
time" discipline, established after the Reddit rejection — see that entry above):** the
endpoint's own response copyright is explicit: *"made available solely for the purpose of
rendering Google News results within a personal feed reader for personal, non-commercial
use. Any other use of the feed is expressly prohibited."* An automated, scheduled Lambda
pipeline feeding a stored dataset does not fit "personal feed reader" — this is knowingly
outside the license grant, unlike Bluesky (no prohibition existed) or GDELT (explicit
unrestricted commercial/research grant, verified directly from gdeltproject.org/about.html).
User made this choice explicitly and consciously after being shown the Reddit-parallel risk:
undocumented endpoint, no API contract, could break/block without notice at any time. Revisit
if the endpoint breaks or Google enforces the restriction in practice.

**GDELT evaluated and available as a ToS-clean alternative, not chosen for now:** confirmed
working (real recent articles returned, e.g. seendate 2026-07-30) and explicitly licensed
("available for unlimited and unrestricted use for any academic, commercial, or governmental
use of any kind without fee," gdeltproject.org/about.html Terms of Use) but has an aggressive
rate limit (429 "one request per 5 seconds" that in practice held even after 20-40s waits
during testing — possibly a shared-IP-range limit affecting the whole Codespace network path,
not just per-caller) that would need real handling (backoff, or GDELT's own suggested ngrams
bulk dataset for high-traffic use) before it's viable at ~200+ queries/cycle. Also
investigated and ruled out: Brave Search API (ToS explicitly bans AI/RAG training use, same
category of mistake as Reddit), Bing/Azure Search (fully retired Aug 2025), DuckDuckGo (no
real search API, only unauthorized HTML scraping), Google Custom Search JSON API (closed to
new signups since 2025), NewsAPI.org (free tier explicitly bans production use regardless of
volume), SerpApi/Serper (Google-scraping-as-a-service, live but under contested litigation).

**Content extraction pipeline (new complexity, not previously needed with plain RSS):**
1. Google News RSS `<link>`/`<description>` fields only contain an obfuscated Google redirect
   URL (`news.google.com/rss/articles/CBMi...`), not the real article URL or any article text
   — confirmed via `curl -I -L`, the redirect resolves to a client-side JS splash page
   (`content-length: 0`), not a simple HTTP 3xx to the real source.
2. **`googlenewsdecoder`** (PyPI package) decodes the obfuscated URL to the real source URL
   without needing a headless browser — verified working against a real link, resolved to the
   actual `reuters.com` article URL.
3. **`trafilatura`** (PyPI package) extracts clean article body text from the real URL's HTML.
   Confirmed real-world limitation: large outlets (verified with Reuters) return `401` even
   with a browser User-Agent — anti-scraping/paywall blocking, not a trafilatura bug. Handled
   by design, not worked around: articles that fail extraction fall back to title+source only
   (never dropped entirely), same graceful-degradation principle already used elsewhere
   (send_digest's "no summary available" pattern). Some outlets WILL succeed (smaller/regional
   sites, aggregators that republish wire content) and that's accepted as sufficient —
   see next point for why redundant coverage of the same story makes this fine.
4. **Dedup by exact URL, not by story/event similarity.** User's framing: Google News keeps
   showing the same article across multiple days of RSS results, and the SAME underlying event
   is often covered by multiple outlets (e.g. NYT paywalled + a regional outlet that
   republishes/cites the same wire story) — so blocking on "extract every outlet" is
   unnecessary; extracting whichever outlets are actually reachable is sufficient signal, as
   long as the same exact URL is never re-processed across cycles. A new DynamoDB table
   (`poly-rag-processed-urls`, hash key = URL) records every URL once successfully handled.
   **No TTL** — considered and explicitly rejected: DynamoDB pay-per-request charges by
   operation, not by idle storage of small items, so there's no real cost pressure to expire
   entries, and permanent dedup is simpler to reason about than picking an arbitrary "safe"
   expiry window. The underlying article content itself is NOT affected by this table either
   way — that's permanently retained in the existing S3 `news/YYYY-MM-DD/HH.json` payloads,
   which have no TTL and never did.

**Status:** design closed 2026-08-15, not yet implemented — Terraform (new DynamoDB table),
`ingest_news` handler rewrite (per-market Google News queries instead of the 10 fixed feeds,
decode + extract + dedup pipeline), and dependency packaging (googlenewsdecoder + trafilatura
in the Lambda zip, not currently used by any handler) are the remaining implementation steps.

**Revisit if:** Google blocks/breaks the RSS endpoint in practice (see ToS exception note
above), or extraction success rate turns out too low to be useful (not yet measured against
the full market_id set).

**Update (2026-08-16): implemented, and hit a real scale wall in production.** Deployed with
trafilatura's DOWNLOAD_TIMEOUT lowered from its 30s default to 8s (production observed a
single slow/blocked outlet, e.g. washingtonpost.com, eating a full 30s before failing, which
compounds badly across ~230 markets x up to 5 candidates each). Even with the 8s fix, 3
consecutive real invocations still hit the Lambda's 300s timeout, and worse -- each timeout's
automatic retry restarted from market #1 with no memory of prior progress, re-processing (and
re-failing against) the exact same blocked URLs each time.

**Root cause, measured, not assumed:** timed 3 real markets end-to-end (search + decode +
extract) locally against the real registry and real network: 23.7s, 24.1s, 14.9s -- averaging
~21s/market. At that rate, 228 markets is ~4,800s (80 minutes) of sequential work, which
cannot fit in a single invocation under ANY Lambda timeout setting, since AWS Lambda's hard
maximum is 900s (15 min). This was a structural capacity problem, not a tuning problem --
raising the timeout further was never going to fix it.

**Fix: self-chaining batched invocations with an offset checkpoint.** At ~21s/market, roughly
35-40 markets fit safely within one invocation's time budget. The Lambda processes a batch
starting at an `offset` passed in via the invocation event payload (defaults to 0 if absent —
first invocation of a cycle), and if `offset + batch_size < total_open_markets`, it invokes
itself asynchronously (`lambda.invoke(InvocationType="Event")`) with the next offset before
returning, chaining until the full registry is covered each cycle.

**Explicitly chosen over a DynamoDB-persisted checkpoint:** the offset lives only in the
invocation chain itself, not in a table — simpler, no new schema, but means a broken chain
(one invocation crashes without invoking the next) silently stops progress for that cycle with
no automatic recovery, rather than resuming from stored state on the next trigger. Accepted for
now given the chain is short (roughly 6-7 hops at ~35/batch for 228 markets) and each cycle
starts a fresh chain from offset 0 regardless of how far the prior cycle's chain got.

**Revisit if:** the chain proves unreliable in practice (silent stalls), or the registry grows
large enough that a persisted, resumable checkpoint becomes worth the added complexity.

**Update (2026-08-16): sequential chain replaced by parallel fan-out, same day as first deploy.**
The first production run of the batched design showed each batch taking ~10-13 minutes real
wall-clock time, projecting to ~1.5h for the full 7-batch sequential chain -- too slow. Redesigned
mid-run: instead of each batch invoking only the next one, the first invocation (offset=0) fans
out ALL remaining batch offsets at once via async `lambda.invoke()` calls, staggered by
`DISPATCH_STAGGER_SECONDS = 3` between each dispatch (small gap to avoid a simultaneous burst
against the same outlets/Google, without adding proxy/VPN infrastructure -- considered and
rejected: no cheap way to rotate Lambda's egress IP per batch, NAT Gateway is a fixed IP and
breaks the budget on its own, third-party rotating proxies add cost and an external dependency
for what's really a request-concurrency concern, not an IP-identity one).

**Shared-state race fixed by giving each batch its own S3 key.** Concurrent batches all
read-modify-writing the SAME cycle payload key would race (last writer wins, earlier batches'
articles silently lost) -- this is exactly why the sequential design used one shared key safely
(only ever one writer at a time) but parallel fan-out cannot. Fix: each batch writes to its own
key (`news/.../HH_batch<offset>.json`); a merge step (`merge_batch_payloads`) combines all
per-batch files into the final cycle payload once every expected batch key exists. Whichever
batch happens to finish last attempts the merge -- safe to attempt from multiple batches since
merge is idempotent (overwrites the same final key with the same result).

**Real-world snag during the mid-flight redesign:** the first 3 batches (offsets 0, 35, 70) had
already started/finished under the OLD sequential code before the new parallel code was deployed
(Lambda lets in-flight invocations finish on the version they started with -- deploying new code
doesn't kill or affect a running invocation). Those 3 wrote directly to the shared cycle key
(old behavior), not to individual `_batch<offset>.json` files, so the automatic merge (which
expects every offset's own file) could never find batch files for 0/35/70 and would have hung
forever waiting for files that would never exist. Manually merged around this one time: pulled
the old shared payload (already containing offsets 0/35/70's articles) plus the 4 new
`_batch105/140/175/210.json` files, combined and deduped by URL, uploaded as the final
`news/2026-08-15/22.json`. This only affected mid-deploy transition -- every future cycle
starts clean under the new code (all batches write per-batch files, all get merged
automatically), no code change needed for steady state.

**Verified result:** 228/228 markets processed, `cycle_complete: true`, 887 articles (0 URL
duplicates despite offset=105 running twice -- once from the old code's stale self-invoke that
fired after deploy, once from a manual dispatch -- dedup via `poly-rag-processed-urls` held).
Parallel batches completed in 294s-611s each vs. ~600-800s each when sequential -- real wall-clock
savings from not waiting on one batch to fully finish before the next starts, even though each
individual batch's own duration didn't change.

---

## Dynamic Domain Blocklist for News Extraction (implemented 2026-08-16)

**Issue:** during the production runs above, `egamersworld.com` was observed failing extraction
100% of the time across many distinct markets (confirmed via CloudWatch, same domain, same
failure pattern, no successes). Retrying it every cycle wastes ~8s (the `DOWNLOAD_TIMEOUT`) per
attempt for zero benefit -- worth skipping known-bad domains outright rather than re-discovering
the same failure repeatedly.

**Decision (chosen explicitly over a hardcoded list via AskUserQuestion):** dynamic,
DynamoDB-backed blocklist that adapts to real observed failures instead of a list someone has to
remember to update. New table `poly-rag-domain-failures` (hash key `domain`,
pay-per-request, same billing pattern as the other tables in this project): one item per domain,
`consecutive_failures` counter. `record_domain_result` resets the counter to 0 on any success and
increments on failure -- a streak counter, not a lifetime failure rate, so a domain that recovers
(a temporary block lifts, a paywall opens an article) isn't punished forever for past failures.
`is_domain_blocked` checks the counter against `BLOCKLIST_THRESHOLD = 5` before `extract_article`
is even attempted -- 5 gives a domain several independent chances (different articles, not
retries of the same URL) before being written off, since 1-2 failures could be one bad article
rather than a genuinely blocked site.

**Where it plugs in:** `process_market_news` checks the blocklist right after resolving the real
URL (via `get_domain`) and before calling `extract_article` -- skips the network request entirely
for blocked domains, same "discard, try the next result" flow already used for dedup/decode
failures, so a blocked domain doesn't cost a slot in `RESULTS_TARGET_PER_MARKET`, it's treated
like any other failed candidate.

**IAM:** `ingest_lambda_role` granted `GetItem`/`PutItem`/`UpdateItem` scoped to the new table's
ARN only, same least-privilege pattern as `processed_urls`.

**Revisit if:** a legitimately good outlet gets blocklisted due to a transient outage rather than
a real block (5-strike threshold with per-success reset should make this rare, but not
impossible) -- would need a manual DynamoDB item delete to un-block, no admin tooling built for
this yet since it hasn't been observed as a real problem.

---

## Comments Source Replaces Bluesky (deployed and verified 2026-08-16)

**Issue:** auditing real production data (492 markets, 689 posts, 0 HTTP failures) revealed a
quality problem the metrics couldn't see: manually inspecting the actual post text showed most
of the 437 sampled posts were bot accounts republishing Polymarket's own price feed verbatim
("Will the next diplomatic US-Iran meeting be in Qatar... YES 48% -> 74% (+26) in 5m. Polymarket
[link]") or news aggregator accounts reposting formal market questions -- not independent human
reaction. This is circular signal: correlating "Bluesky sentiment" against odds movement would
partly just be correlating Polymarket's odds against a mirror of themselves.

**Alternative found: Polymarket's own comment sections.** `gamma-api.polymarket.com/comments`
-- same domain already used for market data, public, no auth, officially documented at
docs.polymarket.com (`list-comments`, `get-comments-by-comment-id`). Response includes author
(wallet address + optional display name/pseudonym), full comment body, `createdAt`,
`parentCommentID` (threading), and `reactions`. Rate limit: 200 req/10s (tighter than the
general Gamma API's 4000 req/10s -- reflected in `MAX_RETRIES`/backoff in the handler). Real
sample (2026-08-16, La Liga opener market) showed genuine trader analysis ("first game of the
season and the away side's the favorite, always feels off for a home opener... had Sportstensor
model open on la liga all week and its got rayo too"), not bot noise. No ToS clause found
prohibiting AI/RAG use (unlike Reddit's explicit ban) -- Polymarket's own FAQ describes its
API/code as "open source and free to use"; only restriction found is geographic (US persons
blocked from trading, but data/read access is explicitly not geo-blocked).

**Comments hang off Event or Series, not the market itself -- and NOT always 1:1 with a market.**
Confirmed empirically via three separate checks before committing to a design:
1. A market's `events[0].commentCount` can be 0 while `events[0].series[0].commentCount` is in
   the thousands -- e.g. market 3448712 (Royals vs. Angels), event comment count 0, series (MLB)
   comment count 6705. Comments exist somewhere for nearly every market (19-market sample: 14/19
   had Event-level comments, the other 5 -- all sports/recurring markets -- had Series-level
   comments instead; ~100% combined coverage).
2. Series-level comments are NOT specific to the individual market -- verified by fetching real
   comments for two DIFFERENT MLB games (Royals/Angels and Brewers/Dodgers, both series_id=3) and
   confirming they returned the IDENTICAL comment thread, including comments about a THIRD,
   unrelated MLB game. Also confirmed visually in the Polymarket frontend by the user directly
   (screenshot matched the API response exactly). This ruled out treating Series comments as
   "lower confidence" -- they're not lower quality, they're structurally many-to-one, a different
   kind of fact than Event-level comments.
3. **Second bug found in the FIRST production test run (self-caught, not by the user this
   time):** the initial two-tier design (`direct` for any Event-level comment, `shared_series`
   for Series-level) turned out wrong too -- an Event can itself contain MULTIPLE markets (e.g. a
   tournament with one market per team, all sharing one comment section). Production data showed
   802/1698 comments tagged `direct` actually carried more than one `market_id`, breaking
   `direct`'s implicit 1:1 promise. Fixed same-day by splitting into three tiers instead of two:
   `direct` (Event with exactly one open market), `shared_event` (Event with several), and
   `shared_series` (Series-level, shared across a whole league/category). All three tag every
   applicable `market_id` rather than picking one arbitrarily.

**Design:** `ingest_polymarket` extracts `comment_entity_type`/`comment_entity_id`/
`comment_link_type` per market directly from the Gamma API's `events[]` field (no LLM --
`get_comment_link`), Event preferred, falling back to Series only if the Event has none. The new
`ingest_comments` Lambda groups open markets by `(entity_type, entity_id)` before fetching, so a
series shared by many markets is only queried once, then tags every comment with every applicable
`market_id` and the correct `link_type`.

**LLM search-text generation removed as a side effect.** Neither News (uses `question` verbatim
against Google News RSS) nor Comments (direct ID lookup) need LLM-generated search text anymore
-- `search_query`/`news_match_terms` were Bluesky's dependency specifically. Removed from the
verifiability-classification prompt in `ingest_polymarket` (back to 1500 max_tokens from 2500)
and from the registry schema. One-off backfill script run against the 329 pre-existing registry
items that predated this field (`comment_entity_type` etc. only populate for NEW markets by
default) -- 187 direct, 105 with a shared_series candidate, 37 with no comments at either level,
0 errors.

**Deployed via `terraform apply`:** `ingest_bluesky` (Lambda, EventBridge rule/target, Lambda
permission) destroyed; `ingest_comments` (same resource types) created. No new IAM permissions
needed -- Comments reuses the existing `dynamodb:Scan` on the registry already granted to the
shared ingestion role. `BLUESKY_HANDLE`/`BLUESKY_APP_PASSWORD` env vars and their
`lifecycle.ignore_changes` no longer apply to anything.

**Verified in production (2026-08-16, post-fix):** 295/329 registry markets have some form of
comment coverage (89%), 2506 comments fetched via 94 grouped API calls (not 329 individual
calls), 0 failures. Final `link_type` distribution: 896 `direct`, 802 `shared_event`, 808
`shared_series`.

**Revisit if:** the 200 req/10s comments-specific rate limit becomes a real constraint as the
registry grows (not hit yet at 94 calls/cycle), or `shared_event`/`shared_series` comments turn
out too noisy to be useful once retrieval/RAG work (Day 4) can actually measure signal quality
rather than just coverage.
