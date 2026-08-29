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
- Keep framework-specific code isolated so pivoting is lower-cost
- Document pros/cons of each approach in decision log

**Scheduled (2026-08-16):** this had never landed on a concrete task despite being called out in
CLAUDE.md/README since day zero -- everything built so far (the 4 ingestion Lambdas,
`synthesize_executive_summary` in send_digest) is plain boto3+Bedrock, no framework. Explicitly
added as gerdau/sprint_plan.md Day 5, block 2: decide LangChain/LlamaIndex vs. continuing with
boto3 directo specifically for the synthesis Lambda (retrieval + prompt-building + LLM call +
possible tool-calling), where that pattern is the actual use case those frameworks target --
informed by what's already working, not decided in the abstract before any code exists.

**Revisit if:** Framework maturity or API stability changes unexpectedly, or Day 5 work reveals
boto3 direct is sufficient and the framework question resolves itself by not mattering in practice.

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

**Update (2026-08-18): the CI/CD bullet above is no longer undifferentiated debt -- it has a
scheduled home and explicit deferral reasoning.** Evaluated at the start of Day 4 (user asked
directly whether to build it now) and moved into gerdau/sprint_plan.md as Day 5 block 6, with a
dependency-ordered sub-list: remote Terraform state (S3 + DynamoDB lock) -> first real Python
tests -> CI (lint/tests/`terraform validate`) -> CD (`terraform apply` via GitHub OIDC, no
long-lived keys). Deferred out of Day 4 for three measured reasons: (1) CI cannot apply against
a Terraform state file that lives in the Codespace and is gitignored, so remote state is a hard
prerequisite, not a nice-to-have; (2) there is not a single Python test in the repo today, and
CI without tests is `terraform validate` on a timer -- the repo's real failure mode has been
logic bugs caught only by invoking live Lambdas (`NameError` after a half-finished rename, a
nonexistent `article_count` field, an S3 key built from `now()` instead of `cycle_started_at`),
all of which are catchable by tests that never touch AWS; (3) Day 4's retrieval work is what
validates the project's thesis, and CI/CD improves deploy ergonomics without moving it. Note
the old Day 6 block 2b framing (`aws lambda update-function-code` on push) was replaced, not
just relocated -- it would have bypassed Terraform entirely and drifted state against IaC.
**Also folded into that same block:** the alerting gap deliberately deferred in "Strict
Ingestion Chaining" below (a cycle still stuck after watchdog retries notifies nobody) --
arguably higher value than CD itself for a pipeline running unattended every 12h.

A separate question raised the same day -- whether a dev/staging environment should exist so
in-progress work stops being tested against live deployed Lambdas -- was answered as environment
separation, NOT CI/CD (CI/CD automates how code moves; environments are where it lands, and one
does not provide the other). Not adopted: a parallel `poly-rag-dev-*` stack would double Bedrock
spend (~$8.85 -> ~$17.70/mo, since LLM calls dominate cost) and parameterize every Terraform
resource, while a dev environment with small/synthetic data would have caught almost none of the
bugs actually hit so far (the DynamoDB eventual-consistency `total_markets` bug, the 900s
offset=0 timeout on slow outlets, the `shared_event` linkage discovery all required real
registry scale and real network behavior). Cheaper alternatives preferred, in order: unit tests
on pure functions, continued scoped `terraform apply -target` with a confirmed plan, and a
`-dev` copy of only the single Lambda under active change. Full environment separation revisits
if the project becomes collaborative or if a bad deploy actually corrupts the odds time-series
(currently protected by S3 versioning + DynamoDB PITR, both enabled 2026-08-17).

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
- Build ingestion script (`scripts/build_corpus.sh`) to parse git history and extract doc snapshots -- lives in the repo-root `scripts/` (one-off tooling), not `.claude/hooks/` (hook handlers only, see infra_design.md)
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

---

**DEPRECATED (2026-08-18, start of Day 4) — the two-layer framing above no longer describes
this pipeline, and `retrieval/time_window.py` that implemented it has been deleted.** Kept
above as historical record of the reasoning, not as current design.

**Why it died — the premise was removed by a later redesign, not by a change of opinion.**
Layer 2 was designed when News came from 10 curated RSS feeds: articles arrived with NO market
association at all, so an ingestion timestamp was genuinely the only thing that could relate an
article to a market, and a time window was the only available retrieval path for unlinked
content. The News Source Redesign (see that entry above) replaced those feeds with one Google
News search PER OPEN MARKET, using the market's `question` as the query. That means every
article now enters the corpus already attached to exactly one `market_id` by construction.
**Verified against real production data (2026-08-18, re-measured across all 6 complete cycles
after the 12:00 UTC run: 3,315 articles, 100% carrying exactly one `market_id`, zero
unlinked** -- the first measurement covered 4 cycles/2,638 articles and the ratio held exactly
as the corpus grew). There is no ambient pool for Layer 2 to
retrieve from — the set it was invented to reach is empty, and the "Layer 1 vs Layer 2"
distinction collapses because everything is Layer 1.

**What replaces it: one retrieval path, not two — metadata filter + semantic rank.** Retrieval
is filtered by chunk metadata (`market_id`, timestamp/cycle, source) and ranked by semantic
similarity within that filtered set. Time is NOT deprecated as a concept — it remains
load-bearing, since "why did market X move between cycle 3 and cycle 4?" is inherently a
bounded-time question. What changes is its status: time goes from being an architectural LAYER
to being one field in the chunk metadata envelope, sitting alongside `market_id`, applied as a
filter rather than as a separate retrieval mechanism with its own confidence semantics.

**`retrieval/time_window.py` deleted (2026-08-18), not repaired.** Beyond the dead framing, it
had rotted against two intervening redesigns: it read the `bluesky/` S3 prefix (deleted
2026-08-17, so it silently returned 0 posts and reported that as a real result rather than
failing), and it never read `comments/` at all (the source that replaced Bluesky). Its entire
return shape was built around the `layer1_linked`/`layer2_ambient` split. This is a concept
that no longer exists, not a module with bugs — deleted rather than patched, same treatment
given to `ingest_bluesky`'s source when its source was retired. Full history preserved in git
(last touched in `ce64d34`) and in session_ledger.md.

**The underlying limitation this entry originally raised is still REAL and now formally
unaddressed — do not read this deprecation as "the problem went away."** The ambient/indirect
correlation gap stands, and per-market search arguably makes it sharper: an article about
"crypto market sentiment souring" that never names Bitcoin or a price threshold will never be
FETCHED in the first place, because no market's `question`-derived Google News query would
return it. Layer 2 was a bad answer (a time window over a pool that doesn't exist), but it was
pointing at a genuine gap.

**Successor path for that gap: cross-market semantic search over the accumulated corpus.**
Once chunks are embedded, an article fetched FOR market A can surface as relevant to market B
purely on semantic similarity, with no linkage between them ever having been recorded at
ingestion time. That recovers indirect/ambient correlation at RETRIEVAL time rather than trying
(and failing) to capture it at ingestion time. Note this only reaches content already in the
corpus for some other market's sake — genuinely uncovered topics (nothing in the whole top-500
would have surfaced them) stay out of reach, which is a real remaining boundary, not something
embeddings fix.

**Revisit if:** cross-market semantic retrieval, once measured against real questions (Day 4
Block 4), turns out not to recover meaningful ambient signal in practice — at which point the
gap needs a genuinely different answer (e.g. a broad topical news pull independent of any
specific market's question, deliberately reintroducing an unlinked pool), rather than
reinstating a time-window layer over content that is already 100% linked.

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

---

## Design Question: Should Comments Live Inside ingest_polymarket? (resolved 2026-08-16)

**Issue (raised by the user, same day Comments was deployed):** Comments and odds both hit
`gamma-api.polymarket.com`, no separate auth, no separate domain -- unlike News (a different
domain entirely) or the old Bluesky (a different platform with its own auth). This raised a fair
question: if the separation into independent Lambdas was originally justified as "one Lambda per
data source," doesn't Comments belong inside `ingest_polymarket` since it's the same source?

**Answer: the real criterion was always failure isolation, not "one Lambda per API domain" --
that distinction just wasn't made explicit until this question forced it.** The original Day 2
rationale (see "Data Source Dependency Risk" and the 3-Lambda pattern throughout this file) was
"a failure in one source doesn't block the others." Comments and odds sharing a domain doesn't
change their failure profiles: odds is the project's core differentiator (the time-series) and
must not be blocked by anything; Comments has its own tighter rate limit (200 req/10s vs. the
general Gamma API's 4000 req/10s) and inherently noisier data (shared_event/shared_series
comments). Merging them would save one Lambda but reintroduce exactly the coupling risk the
3-Lambda split existed to avoid -- a Comments timeout or rate-limit backoff could delay or block
that cycle's odds snapshot write.

**What already reflects the right boundary:** `ingest_polymarket` extracts
`comment_entity_type`/`comment_entity_id` from the SAME `/markets` response it already fetches
for odds (no extra API call, `events[]` is already in that payload) -- this part genuinely
belongs with odds, since it's free data riding along with a call already being made. But the
actual `/comments` fetch, with its own rate limit and its own risk of partial failure across up
to ~94 grouped calls, stays in `ingest_comments`. The split isn't "by domain," it's "by which
data must never be blocked by which other data's failure risk."

**Revisit if:** this reasoning stops holding -- e.g. if Comments becomes fast/reliable enough
that the isolation benefit is negligible, or if operational overhead of a 4th Lambda (cold
starts, EventBridge rules, IAM surface) becomes a real cost worth trading against.

---

## Bespoke Digest Redesign (deployed 2026-08-16)

**Issue:** the original `send_digest` was a flat text email concatenating each ingestion
Lambda's own `llm_summary` field -- three disconnected paragraphs, one per source, each written
blind to the other two. No structure beyond raw prose, and (separately) `SOURCES` still listed
`bluesky` after it was destroyed, with no `comments` entry at all.

**Redesign:** two real changes, not just a visual refresh.

1. **The digest is now a data artifact first, an email second.** A structured JSON
   (`digest/YYYY-MM-DD/HH.json`) is written to S3 BEFORE the email is sent -- this is the source
   of truth, since the digest is meant to be ingested into the RAG corpus later (Day 4/5) and a
   rendered HTML email is the wrong format to re-parse for that. The email HTML is generated FROM
   the JSON, not authored independently.
2. **Content is synthesized across sources, not concatenated.** New fields: `newly_tracked_markets`
   and `resolved_markets` (with real `market_id`/`question`/outcome, not just counts --
   `ingest_polymarket` was extended to carry these in its own S3 payload, since it already
   computes them but previously only wrote the count), `top_volatility` (reads each open market's
   last 2 odds snapshots from `odds/<market_id>.json` and ranks by price delta -- movement, not
   just current price), and verbatim `quotes` (real article titles / real trader comments, not
   LLM-paraphrased). One new Bedrock call (`synthesize_executive_summary`) sees all of this
   together and writes a 2-3 sentence narrative tying odds movement, news, and sentiment into one
   story -- verified in production to produce genuinely useful synthesis (e.g. correctly
   identifying that a cluster of near-1.0 sports odds moves were probably game completions, not
   predictive trading, and noting when news coverage had NO corresponding odds movement, which is
   itself a signal).

**IAM additions:** `send_digest_role` gained `dynamodb:GetItem`/`Scan` (registry reads for
volatility + market_id/question lookups), `bedrock:InvokeModel` (executive summary), and
`s3:PutObject` scoped to `digest/*` (previously read-only -- this role never wrote to S3 before).
Timeout raised 30s -> 60s to cover the registry scan + one S3 read per open market + one Bedrock
call, more work than the old flat concatenation of 3 pre-existing summaries.

**Bugs found and fixed during testing (same day):**
- First test hit `AccessDenied` on `s3:PutObject` -- the role had never needed to write before,
  fixed by adding the scoped statement above.
- `SOURCES` dict referenced count fields (`article_count`, `comment_count`) that don't actually
  exist in News/Comments' own payloads -- those Lambdas only ever wrote the raw `articles`/
  `comments` list, never a separate count field. Fixed by reading `len()` of the actual list field
  instead of a nonexistent count field (`SOURCE_LIST_FIELDS`).
- A leftover reference to the old variable name `resolved_count` (renamed to `resolved_markets`
  during the payload-enrichment edit) survived in the Lambda's own return-value body, causing a
  `NameError` on the very next real invocation after the rename -- caught by actually invoking the
  Lambda in production rather than trusting the syntax check alone.

**Verified in production:** real digest generated and emailed showing a genuinely new market
(`Will Jesus Christ return before 2027?`), correct per-source item counts (72 articles, 2506
comments), and a coherent executive summary connecting Bitcoin odds movement, a lack of political
market reaction to real polling news, and unrelated entertainment-focused trader comments.

**Revisit if:** the RAG ingestion work (Day 4/5) reveals the digest JSON schema needs different
fields than what's captured here -- this was designed for email readability + a reasonable first
guess at RAG-ingestible structure, not validated yet against actual retrieval needs.

---

## Digest Emails Land in Spam -- DKIM Not Configurable for a Third-Party Domain (found 2026-08-16)

**Issue:** digest emails from `poly-rag-send-digest` keep landing in Gmail spam even after being
manually marked "not spam" -- the misclassification recurs on every new send rather than being
remembered.

**Root cause, confirmed via `aws sesv2 get-account` and `aws ses get-identity-dkim-attributes`:**
`DkimEnabled: false`. Without a DKIM signature (and without SPF/DMARC alignment), Gmail cannot
verify the email was sent by an authorized party for the sender's domain, so it treats every send
as suspicious regardless of prior manual overrides.

**Why this can't be fixed by enabling DKIM in SES alone:** the sender is `bernardolw@gmail.com`
-- a Gmail address, not a domain the project controls. DKIM works by publishing a public key in
the SENDER DOMAIN's DNS (`gmail.com`), which only Google can modify. SES cannot sign mail as
`@gmail.com` no matter how it's configured, because that requires write access to Google's DNS,
not just AWS permissions. Same reasoning rules out SPF/DMARC alignment as a workaround -- Gmail's
own SPF record doesn't authorize SES's IP ranges to send as `gmail.com`.

**Real fix (not applied, explicit user choice 2026-08-16):** the only permanent solution is
sending from a domain the project actually controls (e.g. a cheap purchased domain, ~$10-12/yr
via Route53 or another registrar), configuring SES's Easy DKIM for that domain (3 CNAME records
in its own DNS), and changing `SES_SENDER` to an address on that domain. Also gets the project out
of SES sandbox mode's reputation drag as a side benefit. Not pursued today -- user explicitly
chose to leave it as-is and filter manually rather than add domain-purchase/DNS setup scope right
now.

**Revisit if:** spam misclassification becomes a real problem (e.g. missing a time-sensitive
digest), or a project domain gets purchased for other reasons anyway (at which point wiring DKIM
for the existing SES setup is cheap incremental work, not a new project).

---

## Strict Ingestion Chaining Replaces Independent 12h Timers (deployed 2026-08-16)

**Issue:** the original ingestion design ran all 3 (later 4) Lambdas on independent EventBridge
timers, all firing at the same instant (00:00/12:00 UTC), with `send_digest` on a fixed 5-minute
offset assumed to be "enough time" for the others to finish. Real production data showed this
assumption was false and, worse, silently wrong: a market that `ingest_polymarket` newly tracked
this cycle got zero News coverage, because News's timer fired at the same moment as Polymarket's
and read a registry snapshot that (by the time News actually ran its search) didn't yet reflect
that day's newly-tracked market in a way News's own logic could act on consistently. The digest
similarly picked up a stale News payload from the PRIOR cycle rather than the current one, because
News (with the parallel fan-out redesign) could take anywhere from ~10 min to well over an hour,
far past the 5-minute digest offset.

**Root architectural problem:** News and Comments were never actually independent of Polymarket --
both read the market registry Polymarket writes (open markets, questions, comment_entity_type).
Running them on separate timers was an illusion of isolation; in practice it meant they could run
against a registry that hadn't been updated yet for that cycle, silently producing incomplete or
wrong results rather than failing loudly.

**Fix: strict sequential chaining.** Only `ingest_polymarket` is triggered by EventBridge now
(00:00/12:00 UTC) -- it is the sole entry point. On completion, it invokes `ingest_news` directly
(`lambda.invoke`, `InvocationType=Event`) with `cycle_started_at` threaded through. News's fan-out
design (see "News Source Redesign" update) already tracks when its OWN cycle is complete (the
`cycle_complete: true` merge) -- that exact trigger point now also invokes `ingest_comments`
directly. Comments, which always completes in a single invocation (no fan-out), invokes
`send_digest` at the end of its own handler. The 3 downstream EventBridge rules (News, Comments,
digest) were deleted from Terraform entirely -- nothing fires them except the chain itself.

**Explicit trade-off accepted:** this reverses the "3 independent Lambdas, one failure doesn't
block the others" isolation principle from Day 2. If Polymarket fails, nothing else runs that
cycle. User's own reasoning for accepting this (2026-08-16): News/Comments were never truly
independent of Polymarket's registry write in the first place -- the old "isolation" just meant
they'd run anyway against stale data rather than not running at all, which is worse, not better.
Consistency of what each stage sees now takes priority over failure isolation.

**Real bug found and fixed during the first production test of the new chain (same day):**
`ingest_news`'s fan-out dispatcher (offset=0) computed `total_markets`/`all_offsets` from a fresh
`get_open_markets()` scan, and every subsequent batch (including whichever one attempts the final
merge) ALSO independently re-scanned the registry to recompute the same numbers. DynamoDB `Scan`
is only eventually consistent -- two scans minutes apart, from different Lambda invocations, are
not guaranteed to return the same count even with no real underlying data change. In the first
real test, this caused the last batch to compute a different `all_offsets` set than what was
actually dispatched, so `merge_batch_payloads` never found a complete matching set of
`_batch<offset>.json` files, the merge silently never happened, and the chain never advanced to
Comments/digest -- with no error, no log, nothing indicating anything was wrong. Root-caused via
direct evidence: the 5 markets that changed status (4 resolved, 5 newly tracked) all had the exact
same `first_seen`/`resolution_date` timestamp as the Polymarket run that kicked off the chain,
proving the registry was already stable BEFORE News started, ruling out "the registry kept
changing mid-cycle" as the explanation. **Fix:** `total_markets` is now computed ONCE by the
dispatcher and threaded through every fanned-out batch's invocation payload (`total_markets` key)
instead of being re-derived by each batch independently -- every batch in a cycle now agrees on
the same expected-offset set by construction, not by hoping two DynamoDB scans agree.

**Second real failure found in the same test, after the fix, on the RE-run:** even with the
`total_markets` bug fixed, one batch (offset=0, which also does the fan-out dispatch work itself)
genuinely hit Lambda's 900s hard timeout while extracting articles from slow-responding outlets --
a real timeout, not a bug in the counting logic. This exposed a second, distinct unattended-
failure mode: a single slow batch blocks the ENTIRE downstream chain (Comments, digest) with no
retry and no notification. User's reaction, verbatim: "esto me genera mucho escepticismo... que
pasara cuando se ejecute cada 12 horas... no estamos ni tu ni yo para depurar." Correctly
identified that a chain which only works when someone is watching and can manually re-invoke the
missing batch is not actually production-ready for a 12h unattended cadence.

**Fix: `poly-rag-watchdog-ingest-news`, a new Lambda on a 10-minute EventBridge schedule.**
Finds the most recent News cycle from S3 batch-file listings, checks whether that cycle's final
merged file already exists (nothing to do if so), and if not -- and at least
`RETRY_AFTER_MINUTES = 20` has passed since that cycle started (real margin over the 900s/15-min
Lambda timeout case, so a batch that's merely still running isn't mistaken for stuck) -- re-invokes
`ingest_news` directly for whichever expected offsets have no corresponding S3 file yet. Reuses
the SAME `total_markets` value the stuck cycle's batches already agreed on (read from an existing
batch file, not a fresh scan -- consistent with the fix above, not reintroducing the same class of
bug). Idempotent: re-invoking an offset that's actually still in-flight or already succeeded is
safe (News's own URL-dedup table prevents duplicate work; the batch file write is an overwrite,
not an append). IAM scoped to exactly `lambda:InvokeFunction` on `ingest_news` plus S3 read on the
data bucket -- nothing broader.

**Not implemented (explicitly deferred):** an alerting mechanism (e.g. CloudWatch Alarm + SNS/SES
email) if a cycle is still stuck even after watchdog retries, or if the watchdog itself fails.
Today, if the watchdog's retry ALSO times out repeatedly, the cycle stays stuck with no human
notification -- same silent-failure risk one level up. User was offered this as an option
(alongside the retry mechanism) and chose to prioritize the retry first; alerting remains
unaddressed.

**Revisit if:** the watchdog's own retries prove insufficient in practice (e.g. the same offset
keeps timing out repeatedly, suggesting a specific batch of markets or outlets is systematically
too slow rather than transiently slow), or if the lack of alerting on a truly stuck cycle (retries
exhausted, no human watching) causes a real missed cycle to go unnoticed for an extended period.

**Verified end-to-end (2026-08-16, after the total_markets fix and watchdog deploy):** a full
chain run completed automatically -- Polymarket -> News (cycle_complete: true, 73 articles) ->
Comments (2587 comments) -> digest, all self-triggered with no manual invocation of Comments or
digest. Real digest output: 4 newly-tracked markets, coherent cross-source executive summary
(correctly identified in-progress sports markets as the source of the biggest odds swings, not
predictive trading). This is the first confirmed working run of the strict chain end-to-end.

**Two issues found in this same run:**

1. **Comments/digest wrote to the WRONG hour partition -- FIXED same day, 2026-08-16.**
   `ingest_comments` and `send_digest` both built their S3 key from `datetime.now(timezone.utc)`
   (wall-clock time when they happened to run) instead of the `cycle_started_at` that travels
   through the rest of the chain. In the first verified run, News started at 01:56 UTC but
   Comments/digest didn't run until ~02:29 (the News stage took ~33 min due to the offset=0
   retries), so their output landed under the `02` hour prefix instead of `01`, misaligned with
   News's own `01.json`. **Fix:** `ingest_comments.invoke_next_stage` now takes and forwards
   `cycle_started_at` in its payload (it was previously invoking `send_digest` with an EMPTY
   payload -- the propagation bug started there, not just in the S3-key construction). Both
   `ingest_comments` and `send_digest` now read `event.get("cycle_started_at")` and use it (via
   `datetime.fromisoformat`) to build their own S3 key, falling back to `now()` only for a
   standalone/manual invocation with no upstream cycle context. `now()` is still correctly used
   for `ingested_at` and the email subject/heading (those should reflect real send time, not the
   cycle's logical start). **Verified:** manual invocation of `ingest_comments` with
   `cycle_started_at=2026-08-16T03:00:00...` produced `comments/2026-08-16/03.json` AND correctly
   triggered `send_digest`, which produced `digest/2026-08-16/03.json` -- both under the injected
   cycle hour, not the real wall-clock hour (~02:38) the Lambdas actually ran in.

2. **Double-trigger from concurrent offset=0 retries.** Because 3 offset=0 invocations were
   in-flight simultaneously during the first test (2 manual duplicates + 1 from the watchdog), 2 of
   them independently found the completed batch set and each triggered a merge + chain-forward --
   Comments and send_digest each ran twice. Harmless (idempotent operations, both send_digest
   invocations completed cleanly, verified via CloudWatch) but wasteful (extra Bedrock calls, extra
   S3 writes). Only possible because of the manual duplicate invocations sent during debugging that
   day -- a real production cycle only ever has ONE offset=0 dispatch under normal operation, so
   this specific double-trigger is not expected to recur outside of manual intervention scenarios.
   Not fixed -- no guard exists against the watchdog retrying an offset that's already in flight.

**Revisit if:** double-triggers start happening during normal (non-manual-intervention) operation,
which would indicate the watchdog's retry logic needs a lock/guard against retrying an offset
that's already mid-execution (e.g. checking whether time since cycle start minus expected batch
duration suggests a retry is premature, not just "still missing after 20 min").

---

## World Snapshot Added to Digest -- Belief, Not Movement (implemented and deployed 2026-08-16)

**Issue (raised by the user, same day):** the digest (`top_volatility`) only ever showed what
CHANGED this cycle -- biggest price deltas between two consecutive odds snapshots. There was no
view of the market's current STATE independent of movement: "what does the world currently think
is going to happen," as a single readable snapshot, not a log of the last 12h of price wiggling.
User's own framing: an image of what the market is betting FOR or AGAINST, filtered down to what's
actually important/relevant -- not all ~330 open registry markets.

**Design (agreed via AskUserQuestion before implementation):**
- New `world_snapshot` JSON key, additive -- coexists with `top_volatility`, doesn't replace it.
- Two groups of 5 over the same open-market pool, not one undifferentiated list:
  - `top_conviction`: highest `volume24hr` (from each market's latest odds snapshot) -- where the
    most real money is placed, i.e. what the market is most confident about.
  - `most_disputed`: current price within `SNAPSHOT_UNCERTAIN_LOW`/`HIGH` (0.40-0.60) of 50/50 --
    genuinely contested bets, sorted by closeness to exactly 0.5.
- Both derived from a SINGLE shared read pass (`_load_open_market_odds`, new helper in
  `lambdas/send_digest/handler.py`) that `compute_top_volatility` and `compute_world_snapshot` both
  consume -- avoids scanning the registry and re-reading S3 odds files twice per cycle for two
  overlapping views of the same ~330 open markets (cost discipline per CLAUDE.md).
- Fed into `synthesize_executive_summary`'s Bedrock prompt alongside movement data, with an
  explicit instruction to weave in current belief, not just what changed -- and rendered as two new
  sections in the HTML email (`Highest-Conviction Bets`, `Most Disputed Bets`).

**Deployed (2026-08-16):** `terraform apply -target=aws_lambda_function.send_digest
-target=data.archive_file.send_digest` -- scoped deploy touching ONLY the `send_digest` Lambda
(plan confirmed `0 to add, 1 to change, 0 to destroy` before applying), no other Lambda's code or
infra affected. Manually invoked `poly-rag-send-digest` directly afterward (not the full chain --
no Polymarket/News/Comments re-run) to verify without waiting for the next 12h EventBridge cycle.

**Verified in production (2026-08-16):** manual invocation produced `digest/2026-08-16/03.json`
with both groups populated (5/5 each) and a real, sensible split: `most_disputed` surfaced genuine
political toss-ups (2026 Senate balance of power at 0.49, Ohio Senate race at 0.48, Netanyahu-out
odds at 0.47) -- exactly the kind of signal that's invisible in `top_volatility`. `top_conviction`
mixed real conviction (Fed 50+bps cut unlikely at 0.00, Strait of Hormuz normalcy at 0.02) with
near-settled sports markets (price 1.00/0.00 from games already effectively over) -- expected
behavior, since `volume24hr` doesn't distinguish "the market is confident" from "the game just
ended." Email delivered and confirmed readable by the user, including the two new HTML sections.

**Revisit if:** real production data shows the 40-60% uncertainty band is too narrow/wide to
surface a meaningful `most_disputed` group (e.g. genuinely disputed markets cluster outside that
band in practice), `top_conviction` being dominated by near-resolved sports markets turns out to be
noise worth filtering (e.g. excluding markets within some horizon of resolution, mirroring the
existing 48h min-horizon filter), or `world_snapshot` needs to feed the RAG corpus (Day 4/5)
differently than the rest of the digest JSON once retrieval work can evaluate it.

---

## send_digest's Bedrock Call Was Invisible to the Cost Metrics Table (closed 2026-08-16)

**Issue:** ahead of letting the automated 12:00 UTC cycle run (to remeasure LLM-in-ingestion cost
with Comments in the mix instead of the stale Bluesky figures -- see "LLM Enrichment" in
architecture_canon.md, still pending that remeasurement), a readiness audit found `send_digest`'s
own Bedrock call (`synthesize_executive_summary` -- the 2-3 sentence narrative at the top of every
digest email, a real LLM call with its own tokens/cost, not just JSON formatting) was never written
to `poly-rag-architecture-metrics`. The other 3 ingestion Lambdas (Polymarket, News, Comments) each
call `write_metrics()` unconditionally every invocation; `send_digest` only folded its Bedrock
usage into the S3 digest JSON's `metadata` block, invisible to anyone querying the metrics table for
a full-cycle cost picture. Confirmed live via `aws lambda get-function-configuration`: `send_digest`
had no `METRICS_TABLE` env var and its IAM role had no `dynamodb:PutItem` on that table at all --
structurally incapable of writing there, not just skipped in code.

**Fix:** added `write_metrics`/`estimate_cost_usd` to `lambdas/send_digest/handler.py`, same shape
and pricing constants as the other 3 Lambdas' identical functions (source="send_digest",
`items_processed=1` since digest synthesizes one narrative per cycle, not a list of items). Added
`METRICS_TABLE` env var (`terraform/lambdas.tf`) and a scoped `dynamodb:PutItem` statement on
`poly-rag-architecture-metrics` to `send_digest_role` (`terraform/iam_send_digest.tf`) -- PutItem
only, this role never reads the metrics table.

**Deployed and verified (2026-08-16):** `terraform apply -target` scoped to exactly
`aws_iam_role_policy.send_digest_permissions` + `aws_lambda_function.send_digest` +
`data.archive_file.send_digest` (plan confirmed `0 to add, 2 to change, 0 to destroy` before
applying). Manual invocation immediately after confirmed a real row landed in
`poly-rag-architecture-metrics`: `source=send_digest`, 775 tokens in, 243 tokens out, latency 7251ms,
estimated cost $0.00597. All 4 Lambdas that call Bedrock now write to the same metrics table with
the same schema -- the upcoming 12:00 UTC cycle will produce a complete per-Lambda cost picture,
closing the "Pendiente: remedir con Comments en el mix" note in architecture_canon.md's LLM
Enrichment section.

**Revisit if:** the remeasured cost table (once pulled from real 12:00 UTC cycle data) shows
send_digest's cost is negligible enough that tracking it separately isn't worth the row, or if the
digest's executive-summary prompt grows enough (e.g. more sources, longer synthesis) that its cost
becomes a meaningful fraction of the per-cycle total rather than a rounding error.

---

## LLM-in-Ingestion Trial Closed as Canon -- RAG-Optimization Pass Deliberately Deferred (2026-08-16)

**Decision (explicit, by the user, 2026-08-16):** the LLM-in-ingestion trial (started 2026-08-13,
originally framed as optional/reversible under the strict $5/mo hard-cap regime) is now canon, not
an open question. Two things changed since the original framing: (1) the ingestion redesign
(verifiability filter + registry) made the LLM step the quality gate itself -- ingestion literally
does not work the same way without it, unlike the original "nice-to-have summary" framing; (2) with
News/Comments now live and the digest synthesizing across all three sources, LLM enrichment output
is a direct input to the future RAG corpus (Day 4/5), not just a convenience for the human-readable
digest email. Real measured cost from the first clean canonical cycle (2026-08-16, 12:00 UTC, all
4 Lambdas, Comments in the mix): $0.147/cycle, ~$8.85/month projected -- see architecture_canon.md,
"LLM Enrichment (Ingestion) -- Canon" for the full breakdown. Higher than the pre-redesign historical
estimate (~$1.10/mo) mainly because News now generates its own LLM summary per batch (up to 11 calls
in a full fan-out cycle) rather than once per run under the old 10-feeds-RSS design. Still well
within the $120 promotional-credit buffer and the "spend deliberately, not miserly" principle in
CLAUDE.md.

**Explicitly deferred, not forgotten:** the current LLM enrichment output (per-source `llm_summary`
fields, digest's `executive_summary`) was designed and tuned for human readability (the digest email)
-- it has never been evaluated or optimized for what actually makes good RAG retrieval input
(chunking granularity, what metadata to preserve alongside the summary, whether summarization loses
information a retriever would want raw, embedding-friendly structure, etc.). User's explicit framing:
this optimization pass happens later, when the RAG/retrieval layer itself gets designed (Day 4/5) --
not now, and not on a fixed timeline. Revisiting the ingestion LLM prompts/output shape before the
RAG design exists would mean optimizing for a consumer that doesn't have requirements yet.

**Revisit if:** RAG/retrieval design work (Day 4/5) begins and reveals the current LLM enrichment
output shape is a poor fit for retrieval (e.g. summaries are too lossy, chunking doesn't align with
how the retriever wants to split documents, key entities/timestamps aren't preserved in a queryable
way) -- at that point, redesign the enrichment prompts/output specifically for RAG consumption,
informed by real retrieval requirements rather than guessed upfront.

---

## Odds Snapshot Coverage Silently Coupled to Top-500 Candidate Discovery (fixed 2026-08-17)

**Issue (found via real data, 2026-08-17):** while auditing `eda_mio` (the user's own Databricks EDA
notebook) against the 4 clean complete cycles, found that markets already open and tracked in the
registry could have silent, unrecorded gaps in their odds time-series. Concrete example: market
1365861 had a snapshot in cycle 1, no snapshot in cycle 2, then snapshots again in cycles 3 and 4 --
with nothing in the data indicating why, or that a gap had even occurred.

**Root cause:** `ingest_polymarket` only ever appended an odds snapshot for markets present in that
cycle's top-500-by-volume24hr candidate pull (`already_tracked_open`, the old variable name). A
market still open and still being tracked in the registry, but whose `volume24hr` happened to rank
outside the top 500 that cycle, simply got skipped -- not because it resolved, not because of any
error, but purely because two unrelated concerns were coupled into one fetch: (1) discovering NEW
candidate markets to evaluate for the registry, and (2) deciding who gets an odds snapshot this
cycle. This directly contradicts the project's core thesis (CLAUDE.md): a complete, self-built
open-to-resolution time-series for every tracked market. A market that quietly drops in and out of
top-500 ranking would have a time-series full of unexplained holes.

**Fix (user-directed redesign, not a patch):** decoupled the two concerns entirely. Odds
snapshot + resolution-check now iterates over `open_registry_ids` -- literally every market with
`status == "open"` in the registry (plus markets newly tracked this same cycle) -- with no concept
of "dropped" markets at all. For each: if the market happens to already be in this cycle's top-500
batch (`candidates_by_id`), that data is reused for free; otherwise it's fetched individually via
`fetch_market_by_id` (the Gamma API's single-market endpoint), which also reveals whether it has
resolved (`closed`) in the same call. A market falling out of top-500 by volume is no longer a
special case needing a fix -- it's simply routed to individual fetch instead of the batch, same as
before, just no longer silently dropped from odds tracking.

**Zero added cost, confirmed:** the old code already called `fetch_market_by_id` once for every
market in what used to be called `dropped_ids` (purely to check resolution status, discarding the
price data if the market wasn't closed). The new code calls `fetch_market_by_id` for the exact same
set of markets (open registry markets not in this cycle's top-500 batch) -- it just uses the
response's price data instead of throwing it away. No new Gamma API calls, only better use of ones
already being paid for.

**First iteration considered and rejected:** an earlier version of this fix kept `dropped_ids` as a
named special case and added a `recovered_snapshots_written` counter to track how often the gap-fix
path fired. User rejected this as still architecturally wrong -- the existence of a "dropped ids
need special handling" concept was itself the bug, not something to patch around and measure. The
final design has no such counter because there's no such special case anymore: `snapshots_written`
counts everyone uniformly, regardless of which data source (batch or individual fetch) supplied it.

**Revisit if:** the number of markets requiring individual `fetch_market_by_id` calls grows large
enough (e.g. if the registry grows well beyond ~300-500 open markets while top-500-by-volume stays
fixed) that per-cycle latency or Gamma API rate limits become a real constraint -- not yet measured,
but structurally the same cost profile as the resolution-check that already existed before this fix.

---

## Databricks Notebooks Not Version-Controlled (closed 2026-08-18)

**Issue (raised 2026-08-17):** `poly_rag_exploration` and `eda_mio` (Day 3 work, Delta Lake + Unity
Catalog + the user's own EDA) only existed in the Databricks workspace -- never committed to this
repo. Every other piece of the project (Lambda code, Terraform, docs) is git-tracked; the notebooks
were the one exception, kept in sync only via ad-hoc `databricks workspace export`/`import` CLI
calls run manually during sessions, with no persistent copy in git history.

**Closed (2026-08-18):** Databricks Repos (now called Git Folders in the newer UI) connected to
`bernardowise/Poly-RAG`. Setup: a GitHub fine-grained PAT scoped to only this repo (Contents:
Read and write, Metadata: Read-only) registered as a Databricks git credential
(`databricks git-credentials create`), then `databricks repos create` cloned the repo into
`/Users/bernardolw@gmail.com/Poly-RAG` inside the workspace. `eda_mio`, `eda_mio_2`, `eda_mio_3`
moved into a new `databricks/` folder inside that git-connected copy (`databricks/eda_mio.py`,
etc.), mirroring how `lambdas/` and `terraform/` already organize the repo by domain. Committing
and pushing back to GitHub from there is a manual step done by the user via Databricks' Git panel
UI (not automated by Claude, consistent with this repo's git rules in CLAUDE.md -- commits are
never run on the user's behalf).

**Revisit if:** the GitHub fine-grained PAT registered for this integration needs rotating (it was
created with no expiration, GitHub's own recommendation is to set one) -- or if committing/pushing
from Databricks proves annoying enough in practice to warrant automating it further (e.g. a
scheduled sync job), though manual commits from the Databricks Git panel are the deliberate default
today, matching this repo's git rules.

---

## Future Consideration: Publish the Digest to LinkedIn as Personal Branding (raised 2026-08-18)

**Idea (user, 2026-08-18):** once the project is genuinely finished end-to-end, publish the
LLM-generated digest to the user's LinkedIn profile at a cadence of **2-3 times per week** (not
every cycle -- 14 posts/week from a 12h pipeline would read as automated spam, which is the
opposite of the intended effect), as a way to build a public track record as an AI engineer. The
content already exists and is already synthesized: `digest/YYYY-MM-DD/HH.json` carries
`executive_summary`, `top_volatility`, `world_snapshot` (`top_conviction`/`most_disputed`), and
verbatim `quotes` -- a post is a rendering problem, not a new-content problem, the same way the
digest email is already generated FROM that JSON rather than authored separately.

**Explicitly gated on the project being 100% done** -- user's own framing. Not a Day 4/5 task,
and deliberately not something to start while the pipeline is still changing shape underneath it.

**What would need deciding when this is picked up (none of it resolved now):**
- **Selection, not just cadence.** 2-3 posts/week over a 14-cycle week means most digests are
  never published -- so something has to CHOOSE. Candidates: highest `top_volatility` delta of
  the week, a market resolving against consensus, or manual pick. This is the real design
  question; the posting mechanics are the easy half.
- **Automated vs. human-in-the-loop.** Publishing to a personal profile is outward-facing and
  irreversible in reputation terms, unlike everything else in this pipeline (S3 writes, an email
  to oneself). Strong argument for draft-then-approve rather than direct auto-post, at least
  initially -- an LLM-written post about live prediction markets going out unreviewed under the
  user's own name carries a different risk profile than a wrong number in a private digest.
- **Accuracy/liability framing.** Posts would state market probabilities and implied forecasts
  publicly. Needs an explicit stance on hedging language and on not reading as investment advice
  -- a concern that does not exist for the private digest email.
- **LinkedIn API access.** Not yet investigated. Per this project's standing ToS-check discipline
  (established after the Reddit rejection and applied to Google News/GDELT/Brave), verify the
  developer terms permit automated posting BEFORE building anything -- LinkedIn's API has
  historically been restrictive about third-party posting, and the free-tier path may not exist
  for an individual developer. Possible fallback: generate the post text and copy-paste manually,
  which sidesteps the API question entirely and pairs naturally with human-in-the-loop review.

**Revisit if:** the project reaches a genuinely finished state (retrieval + synthesis agent
working end-to-end, per the Day 4/5 sprint items) -- at which point this becomes a small, well-
scoped addition on top of a digest artifact that already exists, rather than new scope competing
with core pipeline work.

---

## Odds History Backfill from Polymarket CLOB (implemented and verified 2026-08-18)

**Issue:** the odds time-series only started when a market first entered the registry
(first cycle 2026-08-16), even though Polymarket exposes each market's full price history
from creation, free, no auth, via `clob.polymarket.com/prices-history`. This directly
matters for Day 4 retrieval design -- see "Known Limitation: Explicit ID-Linkage", flag 3.2
(news published after a market was created but before we started tracking it): without odds
history for that window, such news is uncorrelatable by definition, not by choice.

**Decision, and an explicit scope boundary (user, 2026-08-18):** backfill odds only, as far
back as the CLOB API allows (market creation). Backfilling the NEWS side of that same
history (what was published during a market's pre-tracking life) is explicitly OUT of
scope -- Google News RSS has no arbitrary historical date-range search, so recovering it
would be a project of its own, not an extension of this one. User's framing: "this is the
furthest back I am willing to go, no more." If this gap needs revisiting later, it is a new
scoped decision, not an assumed continuation of the odds backfill.

**One-off script, not a Lambda, not part of the 12h cycle:** `scripts/backfill_odds_history.py`,
run manually like the registry bootstrap and the `comment_entity_type` backfill before it. A
market's pre-tracking history is immutable, so re-fetching it every cycle would be ~600
wasted API calls twice a day. The chained Lambdas are unmodified and keep appending forward
snapshots exactly as before.

**Provenance made explicit, not inferred (see the paired entry below on `source: cycle`
tagging):** every backfilled point carries `source: "clob_backfill"` and lacks
`volume`/`volume24hr`/`liquidity` (not returned by the CLOB endpoint) -- two independent,
structural signals separating it from a real cycle snapshot, so nothing needs to be
remembered as a convention.

**Hard safety guarantees, all verified against real S3 data after `--apply`:**
- A hard cutoff (`CYCLE_BOUNDARY = 2026-08-16T00:00:00Z`) refuses to write any point at or
  after the first real cycle -- confirmed 0 boundary violations across all 63,641 snapshots
  post-apply. The backfill can only ever touch the past.
- Merge-on-timestamp, existing (cycle) data always wins a collision -- idempotent, safe to
  re-run.
- Dry run is the script's default; nothing was written until a human reviewed the dry-run
  output twice (once after each bug fix below) and explicitly approved `--apply`.

**Two real bugs found and fixed via the dry run, before any write -- both are worth keeping
as a pattern, not just a changelog line, because both ran clean and looked like findings
about the data rather than defects in the code:**

1. **Cross-token timestamp alignment discarded ~75% of real history.** First version
   required every outcome token to report a price at the exact same timestamp before
   accepting a snapshot. Measured on market 559672: YES token had 371 points, NO had 395,
   but only 43 timestamps were shared between them -- each token trades independently on its
   own order book, so they are not sampled together. The script reported
   `snapshots added: 318` with zero errors; nothing signaled that ~943 real points had been
   silently thrown away except an absurd-looking `[+672 incomplete]` counter that prompted
   investigation. **Fix:** read only the first (YES) outcome's history and derive the
   complement (`NO = 1 - YES`) -- sound for binary markets by construction (complete-sets
   mechanism, see knowledge.md), and simpler besides (one fetch instead of two). Non-binary
   markets are skipped outright rather than approximated (0 encountered in this registry).
   Re-running after the fix recovered 62,273 real points from the same corpus that had
   produced 318.
2. **All 93 resolved markets would have been silently skipped.** The script queried
   `gamma-api.polymarket.com/markets?id={id}` -- a FILTERED LIST endpoint that returns an
   empty list for `closed: True` markets, not an error. This read as "no clobTokenIds" and
   skipped the market with a plausible-sounding message, `no clobTokenIds: 93` in the
   summary. This would have denied backfill to exactly the complete open-to-resolution
   arcs the README names as the project's differentiator, while looking like a genuine
   Polymarket data limitation rather than a wrong endpoint choice. **Fix:** switched to
   `gamma-api.polymarket.com/markets/{id}` -- the path endpoint, which `ingest_polymarket`
   already uses for its own resolution checks and returns closed markets correctly.
   Verified post-fix: `no clobTokenIds: 0` across all 595 markets, resolved markets like
   3514570 and 3448662 backfilled successfully (4 and 6 points respectively).

**Verified in production (2026-08-18, full run, all 595 registry markets):** 487 markets
backfilled, 108 had no pre-tracking history (created after 2026-08-16, correctly nothing to
backfill), 0 non-binary, 0 missing tokens, 0 errors. 62,273 snapshots added in 465s. Post-
apply S3 verification: 63,641 total snapshots (62,273 `clob_backfill` + 1,368 `cycle`,
arithmetic exact, zero collisions), 0 boundary violations, 0 cycle snapshots missing
volume, 0 backfill snapshots carrying volume, chronological order and a clean price handoff
confirmed on a spot-checked market (561980: 375 points spanning 2025-07-10 to today).

**Depth achieved:** markets created as early as 2025-07-03 now have up to 375 daily
snapshots (vs. the 1-6 cycle-only snapshots every market had before this ran) -- the
time-series depth for those markets went from ~2 days to over a year.

**Not yet decided (separate task, deliberately deferred until this backfill's output could
be inspected first):** whether/how `ingest_polymarket` should fetch history automatically
for markets newly entering the registry going forward, so they do not start with a
cycle-only time-series the way every currently-tracked market did before today.

**Revisit if:** a market later needs backfill and did not exist in the registry at the time
this ran (see the deferred task above), or if Polymarket's CLOB `prices-history` endpoint
changes shape/availability.

---

## Cycle Snapshots Explicitly Tagged source=cycle (implemented 2026-08-18)

**Issue:** the CLOB backfill above (`source: "clob_backfill"`) introduced the odds
time-series' first explicit provenance field. The ~1,368 pre-existing snapshots written by
`ingest_polymarket` carry no `source` field at all, since at write time no second kind of
snapshot existed.

**Rejected shortcut:** leave existing snapshots untagged and treat absence of `source` as
implicitly meaning cycle-origin. User explicitly rejected this, correctly, for the same
reason the two-tier comment `link_type` was replaced by three tiers (see "Comments Source
Replaces Bluesky" above) -- an implicit promise nobody writes down is one nobody can verify,
and this project has already been burned by exactly that pattern once. Concretely: (1) a
third provenance is already planned (history fetched by `ingest_polymarket` itself for
future new entrants -- see the deferred task in the entry above), at which point "no source
field" stops identifying anything specific; (2) it reads as null in Databricks, where
`WHERE source = 'cycle'` is a far better query than `WHERE source IS NULL`; (3) it cannot
distinguish "written by a cycle" from "written before the field existed," which are
identical today but need not stay identical.

**Fix:** `scripts/tag_cycle_snapshots.py`, run BEFORE the CLOB backfill so the two writes
stay independently verifiable. Strictly additive -- adds one key per snapshot, changes no
timestamp/price/volume/volume24hr/liquidity value, adds or removes no snapshot. Idempotent
(a snapshot already carrying `source` is left untouched, which also means it cannot
relabel a `clob_backfill` point as `cycle` if ever run out of order -- verified directly with
a mixed-source test case before running against real data).

**Verified in production:** dry run confirmed 595/595 files needing exactly 1,368 tags with
0 errors; a before/after diff on one file confirmed every original key/value preserved with
`source` as the only addition; `--apply` matched the dry run exactly. Post-apply S3 spot
check (40-file random sample): all snapshots carry `source: "cycle"`, 0 untagged, volume/
liquidity fields intact.

**Revisit if:** a further provenance type is added later (e.g. the deferred
`ingest_polymarket`-native history fetch above) -- confirm it also tags explicitly rather
than reintroducing an implicit default.

---

## News Temporal Tiers (3.1/3.2/3.3) -- Registry created_at Backfilled, Articles Tagged (2026-08-18)

**Issue:** classifying a News article's relationship to its market -- and therefore whether
odds data even exists to correlate it against -- requires comparing the article's `pubDate`
against TWO market dates: `created_at` (when Polymarket made the market) and `first_seen`
(when WE started tracking it). Only `first_seen` existed in the registry before today.
`created_at` was never stored, even though `ingest_polymarket` already reads it from the
same Gamma API response used for everything else.

**Tiers (decided by the user, 2026-08-18):**
- **3.1** -- `pubDate < created_at`. Published before the market existed. No odds ever
  existed for this window (nothing to correlate), but kept as legitimate market context
  rather than discarded -- capped at 1 year before `created_at` (not before ingestion date),
  beyond which an article is tagged `too_old` instead.
- **3.2** -- `created_at <= pubDate < first_seen`. Published after the market existed but
  before we tracked it. Rescued by the CLOB odds backfill (see the entry above) -- before
  that backfill this window had zero odds to correlate against; now it has daily-resolution
  price history.
- **3.3** -- `pubDate >= first_seen`. Published while actively tracking, backed by full
  cycle snapshots (volume/volume24hr/liquidity all present). Highest-confidence tier.

**Two backfills, in dependency order:**
1. `scripts/backfill_registry_created_at.py` -- one-off, additive (`UpdateItem` writing only
   `created_at`), uses the `/markets/{id}` path endpoint (not `?id=`, which silently returns
   an empty list for `closed=True` markets -- see the CLOB backfill entry above for the same
   bug found the same day in a different script). Verified: 595/595 registry items backfilled,
   0 errors, 0 missing `createdAt`, and a sanity check confirmed `created_at` never comes
   after `first_seen` for any item (a market cannot be tracked before it exists).
2. `scripts/tag_news_temporal_tier.py` -- one-off, additive (`temporal_tier` field added per
   article, no other field touched, no article added/removed), classifies retroactively
   rather than forward-only, consistent with "nothing gets deleted" from the odds-backfill
   decision above: an article ingested yesterday deserves the same classification as one
   ingested tomorrow, since the dates it depends on do not change after the fact. Classifies
   by the HIGHEST-confidence tier among an article's `market_ids` when more than one applies
   (3.3 > 3.2 > 3.1 > too_old), though the current News design links every article to exactly
   one market by construction (see "News Source Redesign"), so this only matters if that
   changes later.

**Not yet wired into `ingest_news` for new articles going forward -- deliberately deferred as
part of "F"** (a batched, single-deploy set of `ingest_polymarket`/`ingest_news` extensions
for new registry entrants, alongside teaching `ingest_polymarket` to fetch odds history for
newly-tracked markets). Reasoning: two Lambda changes discovered on the same day, for
different reasons, are batched into one deploy with one verified plan, rather than deploying
twice. `classify_temporal_tier()` in the tagging script is written standalone (no S3/DynamoDB
dependency, plain datetimes in/out) specifically so the eventual `ingest_news` change can
reuse it directly -- this repo has no shared `lib/` between Lambdas, so the intended path is a
direct copy, not an import.

**Verified in production (2026-08-18, full run, all 6 news cycles, 3,315 articles):**

| Tier | Count | % |
|---|---|---|
| 3.2 (correlatable via CLOB backfill) | 1,858 | 56% |
| 3.1 (pre-market, capped at 1yr) | 860 | 26% |
| unknown_market (see below) | 305 | 9% |
| 3.3 (full cycle-snapshot backing) | 167 | 5% |
| too_old (>1yr before created_at) | 125 | 4% |

**Real orphan-data finding surfaced by this run, deliberately NOT fixed yet (user's explicit
call, 2026-08-18): `unknown_market` -- 305 articles referencing a `market_id` no longer
present in the registry at all.** Root cause confirmed, not assumed: the 2026-08-17 registry
cleanup (see "Pendiente para completar el ciclo de ingestion" in architecture_canon.md) purged
329 legacy registry items tied to the pre-redesign Bluesky/keyword pipeline, but that cleanup
only ever touched the registry and `odds/` -- it never touched `news/` or `comments/`, so
articles referencing those now-deleted market_ids survived untouched in `news/` files older
than the cleanup. Confirmed via the actual per-cycle breakdown that this is fully historical,
not an ongoing leak: `unknown_market` rate was 85% (08-16 01:00) -> 21% -> 13% -> 12% -> **0%
starting 08-18 00:00**, exactly the day after the cleanup stabilized. Every orphan predates
2026-08-17; zero new ones are being created.

**User's decision: leave `unknown_market` articles exactly as tagged, do not delete, revisit
later -- possibly at query time through the RAG itself** rather than as another one-off
cleanup pass right now. Explicitly different from the earlier registry/odds cleanup
precedent (which deleted debug-run data from a design that no longer exists) -- these are
legitimately-ingested articles under the CURRENT pipeline design, just pointing at a market
id retired for an unrelated reason.

**On the 5% 3.3 figure -- validated reasoning, corrected on one point (user + assistant
discussion, 2026-08-18):** the low 3.3 share is expected and is NOT primarily a function of
how many markets existed before cycle 1 (most did). It is a function of CORPUS AGE -- News has
only run 6 cycles (3 days) at all, so no article can be 3.3 (`pubDate >= first_seen`) unless it
was published within roughly the last 3 days, regardless of how old or new its market is. The
initial framing ("new markets will always be 3.3") was corrected: a market's FIRST post-tracking
Google News pull mostly returns older, relevance-ranked coverage (see the News staleness
finding above -- only ~23% of any pull is <=1 day old), which lands in 3.1/3.2, not 3.3. What
is true: as a market stays tracked and cycles keep running, ITS SUBSEQUENT pulls increasingly
surface genuinely same-day news, which does land in 3.3. So 3.3 grows with PIPELINE OPERATING
TIME (calendar days the chain has been running), not with registry size -- an honest metric,
since it is measuring real-time correlation actually observed, which cannot be backfilled or
accelerated by any means already used elsewhere in this project (CLOB history, registry
scans), only by continuing to run.

**Revisit if:** `unknown_market` articles need a real resolution before Day 4/5 retrieval work
depends on clean market-scoped queries (candidate approaches: drop them, re-resolve their
market_id against Polymarket's per-id endpoint to see if it still exists under a different
registry status, or simply exclude `unknown_market` at query time and leave the raw data
alone forever); or once "F" is executed, confirming `ingest_news`'s new per-article
classification produces the same tier a human would expect for a market tracked from day one.

**CLOSED 2026-08-19 -- retroactive gap closed, and the deferred `ingest_news` connection now
live.** Two things happened, in order:

1. **Gap discovered:** while building a Day 4 chunking checklist, confirmed that
   `temporal_tier`/`market_status_at_publish` only existed on articles ingested through
   2026-08-18 -- the connection to `ingest_news` was explicitly deferred out of the F-lambdas
   deploy the same day, and nobody had come back for it. Measured precisely: the 2026-08-19T00
   and T12 cycles had **1,798 articles with neither field** (0/922, 0/876), growing by roughly
   900/cycle. The user asked directly whether this would have been caught by
   `runbook_verify_phase1_health.md` -- it would NOT have: that runbook's own Paso 5 explicitly
   said "skip this check until the tagging is connected," so a runbook run against either
   gapped cycle would have reported 4/4 green with a grey note, not a failure. Corrected the
   runbook the same day (see its own changelog) to assert on missing fields instead of skipping.
2. **Retrofill + native connection, in that order** (dry run confirmed both scripts correctly
   skipped the 6 already-tagged cycles and only touched the 2 gapped ones -- `already tagged: 6`,
   `articles tagged: 1798`, 0 errors -- then applied):
   `scripts/tag_news_temporal_tier.py --apply` and `scripts/tag_news_market_status.py --apply`
   closed the retroactive gap; all 8 cycle files now carry both fields on 100% of articles.
   First real evidence of the post-resolution capture design actually working: **144 articles
   tagged `closed`** in the two 08-19 cycles (0 in the entire corpus before F-lambdas) --
   confirms markets are genuinely being searched after resolving now.

**Then `classify_temporal_tier`/`classify_market_status` copied verbatim into
`lambdas/ingest_news/handler.py`**, exactly as both scripts' own docstrings said they were
written to allow. `get_open_markets`'s return shape changed from a 3-tuple to a dict (adding
`created_at`/`first_seen`/`resolution_date`, needed for classification) -- the one call site
(`process_market_news`) updated to match, now tags each article at fetch time instead of
needing a follow-up script ever again. Deployed via `terraform plan`/`apply -target` (plan
`1 to add, 1 to change, 1 to destroy` -- same `null_resource` dependency-rebuild pattern as
every prior handler-only deploy this project has done, no IAM or table changes needed since
this only adds fields to article JSON). Verified post-deploy: downloaded the actual deployed
zip from `Code.Location` and diffed `handler.py` against the repo -- byte-identical. No Lambda
invoked to confirm; the next automatic cycle is the first real test.

**Revisit if:** the next automatic cycle's `news/*.json` doesn't carry both fields on 100% of
its articles (per the corrected `runbook_verify_phase1_health.md`, Paso 5 -- that would now be a
real regression, not a known gap).

---

## Post-Resolution News Capture -- Design Closed, Data Tagged, Ingestion Extension Deferred (2026-08-18)

**Issue (raised by the user, 2026-08-18):** the project has no way to capture how news/reaction
looks in the days immediately after a market resolves -- `ingest_news` only ever searches for
markets with `status == "open"` (see "News Source Redesign"), so the moment a market resolves,
it stops being searched entirely. This blocks a real question the RAG should be able to answer:
"how did people react when X resolved."

**Decided (user, 2026-08-18):**
- **Window: 4 fixed cycles = 48h post-resolution**, not a variable window. The cycle in which a
  market resolves counts as post-resolution cycle #1 (so the window is resolution-cycle through
  resolution-cycle+3).
- **Trigger: `status == closed`.** `ingest_news` needs to widen its search to include markets
  that resolved within the last 4 cycles, not just currently-open ones -- otherwise the exact
  cycle a market resolves in is never searched at all (see the real corpus evidence below).
- **Same search mechanism** -- Google News RSS with the market's `question` verbatim, no change
  to how the search itself works.
- **No new temporal tier.** The first proposal was a 4th tier (alongside 3.1/3.2/3.3) for
  "post-resolution" articles -- rejected after discussion, correctly: an article published 30h
  after resolution already satisfies `pubDate >= first_seen` and lands in 3.3 without any new
  category. What actually differs is a SEPARATE axis -- whether the market could still move
  when the article was published -- which does not belong inside `temporal_tier` any more than
  `link_type` should have absorbed a second, unrelated fact (see "Comments Source Replaces
  Bluesky" for the prior instance of this exact mistake with the two-tier `link_type`).

**New field instead: `market_status_at_publish` (open/closed/unknown_market), computed from the
registry's `resolution_date`, not the market's CURRENT status** -- current status answers "where
is this market today," not "where was it when this specific article was published," and a
market resolved by now could easily have been open when an old article was published. The
retrieval-relevant combination is **`temporal_tier == "3.3" AND market_status_at_publish ==
"open"`** for "can this explain an odds movement" vs. **`"3.3" AND "closed"`** for "reaction to
an already-fixed outcome" -- two independent questions, two independent fields, queried
together rather than merged into one.

**Retroactive, same reasoning as every other tag this session:** `scripts/tag_news_market_status.py`,
additive only (`market_status_at_publish` added, no other field touched), classifies by the
MOST PERMISSIVE status among an article's `market_ids` when more than one applies (mirrors
`tag_news_temporal_tier.py`'s most-confident-tier logic) -- an article is genuine open-market
reporting for any linked market where that is true.

**Real finding confirmed by running this against the full corpus, not assumed: 0 of 3,315
articles are tagged `closed`.** This is the CORRECT result given today's ingestion design, not
a bug in the script -- verified directly: 339 articles ARE linked to a market that has since
resolved, but every single one was published BEFORE that market's `resolution_date` (Google
News returns older, relevance-ranked coverage from while the market was still open and
trending -- see the News staleness finding above). None were published after resolution,
because `ingest_news` never searches a market once it leaves `status == "open"`. This is live
proof, not just design reasoning, of the exact gap this feature exists to close.

**Implemented same day (F-lambdas, 2026-08-18) -- see the batched Lambda-extension entry below
for the other two changes deployed alongside this one.** Design changed slightly from the
original plan during implementation: rather than comparing dates on every run, the mechanism
is a COUNTER, not a live date comparison --
`mark_registry_resolved` (`lambdas/ingest_polymarket/handler.py`) sets
`post_resolution_cycles_remaining = 4` the exact cycle a market transitions open->resolved (the
transition itself, never re-armed later); `get_open_markets`
(`lambdas/ingest_news/handler.py`) now selects `status == open OR
post_resolution_cycles_remaining > 0`; `decrement_post_resolution_counter` counts it down by 1
each cycle a resolved market is included, guarded by a `ConditionExpression` so it can never go
negative (verified live: 2 -> 1 -> 0 -> 0, third decrement correctly a no-op). This is simpler
than date math and matches exactly how the user described it ("checa si estaba open y ahora
closed... y si ya esta closed entonces empieza el pull de 4 ciclos") -- a transition-triggered
counter, not a resolution_date comparison recomputed every cycle.

**One-time catch-up for the 93 markets resolved BEFORE this code existed, per the user's
explicit decision:** `scripts/start_legacy_post_resolution_windows.py`, a hardcoded list of the
93 `market_id`s frozen at the moment this was written (deliberately NOT a live "all resolved
markets" query -- see the script's docstring for why re-running that query later would wrongly
re-arm markets whose real window already happened and ended). Starts their counter at 4 as if
they had just resolved NOW, accepting the resulting bias (Google News reflects what's indexed
today, not each market's real historical 48h window) as acceptable given the pipeline's own
short lifetime (News has only run 6 cycles / 3 days total, so "old" here means at most 2-3
days). Dry run confirmed 93/93 with 0 errors before applying; DynamoDB verified post-apply: all
93 resolved markets show `post_resolution_cycles_remaining == 4`. No standalone News-fetching
script was written for this -- the regular `ingest_news` cycle picks these 93 up automatically
through the same widened `get_open_markets` query, so there is no duplicate search/decode/
extract/dedup logic to maintain.

**Update (2026-08-19): user decided to KEEP the script, do not delete it, even once the 93
counters reach 0.** Superseding the original "delete once consumed" plan below. Checked live:
counters are at 2/4 (armed by this script during incident cleanup on 2026-08-18, decremented
twice by the normal 2026-08-19T00 and T12 automatic cycles -- the mechanism is working exactly
as designed, no special-case code in ingest_news, it just sees counter>0 and decrements). Two
more automatic decrements remain (2026-08-20T00 and T12) before reaching 0. The script's job
(arming the counter) is already done regardless -- nothing will invoke it again -- but it stays
in the repo as a record of which 93 market_ids were manually caught up and why, rather than
being deleted once functionally inert.

Original reasoning (kept for context, no longer the decision): once its one-time job is done,
the file is dead code with no reuse path (its own docstring warns against re-running it with a
different market list), so the instinct was to remove it rather than leave it as a trap for a
future session. That reasoning was sound in isolation but the user's call overrides it --
keeping a well-documented record of a manual intervention has value independent of whether the
code is ever executed again.

**Revisit if:** the first real post-resolution cycles (starting with the 93 legacy markets)
produce meaningful `closed`-tagged articles -- confirm the 4-cycle window in practice actually
captures reaction rather than silence (Google News may have little to say about a market 12-48h
after resolution if the underlying event isn't newsworthy beyond the market resolving).

---

## F-lambdas: Batched Ingestion Extensions for Markets Going Forward (deployed 2026-08-18)

**Issue:** three extensions to `ingest_polymarket`/`ingest_news`, each discovered as a "not yet
implemented, deferred" note earlier the same day (see the CLOB odds backfill, News temporal
tiers, and post-resolution capture entries above) -- all three only ever mattered for markets
NEWLY entering the registry going forward, since the one-off scripts already covered every
market tracked before today. Deliberately batched into ONE deploy rather than three, per the
user's explicit call ("dejar la letra F para el final, un solo deploy") -- avoids touching the
same two Lambdas multiple times in one session for unrelated reasons.

**Three changes, `lambdas/ingest_polymarket/handler.py`:**
1. **`created_at` written on registry entry** (`upsert_registry_entry`) -- straight from the same
   Gamma API response already used for everything else, zero extra cost. Closes the gap the
   `created_at` backfill script had to fill retroactively for the 595 pre-existing markets.
2. **`backfill_odds_history_for_new_market`**, called immediately after a market is upserted --
   same CLOB endpoint, same YES-token-only + derived-complement logic as
   `scripts/backfill_odds_history.py` (see that entry above for the two bugs found there and why
   the complement derivation is sound for binary markets only). Fails silently on any error --
   odds history is a bonus, must never block registry entry itself. `append_odds_snapshot`
   (already called for every open market later in the same handler run) correctly read-modify-
   writes on top of whatever this function just wrote, so no merge logic was needed.
3. **Post-resolution counter started at the open->resolved transition** (`mark_registry_resolved`,
   now also setting `post_resolution_cycles_remaining = POST_RESOLUTION_CYCLES = 4`) -- the
   transition itself is the only moment this should ever be armed, never re-armed later.

**One change, `lambdas/ingest_news/handler.py`:**
4. **`get_open_markets` widened** from `status == open` to `status == open OR
   post_resolution_cycles_remaining > 0` -- the exact gap proven empirically in the entry above
   (0/3315 articles were `closed` because a resolved market stopped being searched entirely).
   Now returns `(market_id, question, status)` tuples (previously just `(market_id, question)`)
   so the handler knows which markets are in their post-resolution window.
5. **`decrement_post_resolution_counter`**, called once per resolved market actually processed
   this cycle -- guarded with a `ConditionExpression` so the counter can never go negative
   (verified live before deploy: 2 -> 1 -> 0 -> 0, third decrement correctly a no-op).

**Design note, mechanism differs slightly from the original plan:** rather than comparing
`resolution_date` against "now" on every run (which the user correctly identified as redundant
work once a counter exists), the final design is a pure counter armed once at the transition and
decremented by the consuming Lambda -- matches the user's own framing exactly ("checa si estaba
open y ahora closed... y si ya esta closed entonces empieza el pull de 4 ciclos").

**Verified before deploy (all against real data/API, nothing assumed):**
- `fetch_clob_price_history` (the ingest_polymarket copy) returned 371 real points for market
  559672, matching the one-off script's earlier result exactly.
- `decrement_post_resolution_counter` tested live against a throwaway registry item
  (`__test_market_do_not_use__`, deleted after): 2 -> 1 -> 0 -> 0, guard confirmed working.
- `get_open_markets` against the real registry returned exactly 502 open + 93 resolved = 595,
  matching the registry's real composition with no duplicates or omissions.
- Both handlers compile clean (`python3 -m py_compile`).

**Deployed via `terraform apply -target=aws_lambda_function.ingest_polymarket
-target=aws_lambda_function.ingest_news`** (plan reviewed first: `1 to add, 2 to change, 1 to
destroy` -- the add/destroy pair is `null_resource.ingest_news_deps` being replaced because its
`handler_hash` trigger changed, which is what correctly forces the dependency zip to rebuild
with the new handler code, not an unexpected resource). Applied cleanly: `1 added, 2 changed, 1
destroyed`, no other resource touched (Comments, send_digest, EventBridge, IAM, S3, DynamoDB all
untouched by this deploy).

**One-time catch-up for the 93 pre-existing resolved markets:** see the "Post-Resolution News
Capture" entry above for `scripts/start_legacy_post_resolution_windows.py` (already run,
93/93 counters set to 4) -- these 93 will be picked up by the newly-deployed `get_open_markets`
starting with the very next `ingest_news` cycle, through the same normal path as any market that
resolves from now on.

**Revisit if:** the next few live cycles show `backfill_odds_history_for_new_market` silently
failing more often than expected (its errors are swallowed by design -- worth spot-checking
CloudWatch logs after a few real new-market cycles to confirm it's actually firing, not just
failing invisibly every time), or if post-resolution News capture in practice needs the window
length or trigger condition adjusted once real `closed`-tagged articles start arriving.

**Bug found and fixed during post-deploy verification, same day:** `append_odds_snapshot`
(the function that writes every cycle's own odds snapshot, unchanged by this deploy) was never
updated to write `source: "cycle"` -- verified live by invoking `ingest_polymarket` twice against
production and inspecting the resulting S3 files directly. This would have reintroduced, for all
NEW snapshots going forward, exactly the implicit-absence problem the user explicitly rejected
earlier the same day for the 1,368 pre-existing snapshots (see "Cycle Snapshots Explicitly Tagged
source=cycle" above). Fixed by adding the field directly to `append_odds_snapshot`'s snapshot
dict -- redeployed with a second, separately verified `terraform apply -target` (plan confirmed
`0 to add, 1 to change, 0 to destroy` first). Verified against a real snapshot written by the
second live invocation: `{"source": "cycle", "timestamp": ..., ...}`, field present natively,
no follow-up tagging script needed for snapshots written from this point forward.

**Verified against two real, live invocations of `poly-rag-ingest-polymarket`** (not a
CloudWatch-only check): first invocation (before the source fix) surfaced 9 real new markets,
each landing in the registry with a genuine `created_at` (spanning 2025-11-11 to 2026-08-17) and
`post_resolution_cycles_remaining: 0` as expected; 4 of those markets' `odds/<id>.json` files
confirmed populated with `clob_backfill` history automatically, including one (677396, created
2025-11-11) with 255 backfilled snapshots -- nearly 9 months of price history recovered the
instant the market entered the registry, zero manual script involved. Second invocation (after
the source fix) confirmed newly-written cycle snapshots now carry `source: "cycle"` natively.

---

## Bug de Doble-Disparo en el Fan-Out de News (confirmado 2026-08-18, CERRADO 2026-08-19)

**Decision del usuario (2026-08-18): esto es lo PRIMERO que se arregla al retomar, antes de
seguir con el Dia 4 (chunking/embedding/retrieval).** No es negociable ni se pospone otra vez --
ya se pospuso una vez (ver "Strict Ingestion Chaining", nota de doble-disparo, donde se juzgo
"solo ocurre por intervencion manual de debugging, no en operacion normal") y esa evaluacion
resulto demasiado optimista.

**El bug:** `merge_batch_payloads` en `lambdas/ingest_news/handler.py` es idempotente para
ESCRIBIR el payload final (varios batches pueden hacer el merge y todos producen el mismo
archivo, sin race). Pero NO tiene ninguna guarda sobre el paso siguiente:
`invoke_next_stage(cycle_started_at)`. Cualquier batch que observe "ya existen todos los
archivos `_batch<offset>.json`" invoca a `ingest_comments` -- y con N batches corriendo en
paralelo, N pueden observar eso casi al mismo tiempo.

**Evidencia real medida (incidente 2026-08-18, ver
runbook_manual_invocation_cleanup.md):** 4 invocaciones de `ingest_polymarket` -> 80
invocaciones de `ingest_news` (fan-out normal, ~20 batches cada una) -> **26 invocaciones de
`ingest_comments`** -> **25 correos digest reales al usuario**. Factor de amplificacion x12.5
sobre las 2 invocaciones manuales originales. Sin este bug habrian sido 4 correos: molesto,
pero proporcional al error humano que lo origino.

**Por que la evaluacion previa ("solo pasa con intervencion manual") era incorrecta:** el
watchdog (`poly-rag-watchdog-ingest-news`, cron de 10 min) reinvoca offsets faltantes de forma
AUTOMATICA, sin humano de por medio. Si reintenta un offset que en realidad seguia en vuelo (no
hay guarda contra eso -- documentado como pendiente en "Strict Ingestion Chaining"), se produce
exactamente la misma condicion de carrera: dos invocaciones del mismo offset, ambas terminan,
ambas ven el set completo, ambas encadenan. El escenario no requiere que nadie invoque nada a
mano.

**Direcciones candidatas (ninguna evaluada aun -- decidir al implementar):**
- **Lock/claim atomico en DynamoDB:** antes de `invoke_next_stage`, hacer un `put_item` con
  `ConditionExpression="attribute_not_exists(pk)"` sobre una key tipo
  `cycle_chain_advanced#<cycle_started_at>`. Solo el primero en escribir gana y encadena; los
  demas reciben `ConditionalCheckFailedException` y no hacen nada. Es el mismo patron de guarda
  condicional ya usado en `decrement_post_resolution_counter`, asi que no introduce un concepto
  nuevo al proyecto.
- **Marcar el cycle payload final como "ya encadenado"** (un campo en el propio JSON de S3) y
  releerlo antes de invocar -- mas simple pero con race real entre leer y escribir, no es
  atomico como DynamoDB.
- **Que solo un offset designado encadene** (ej. el offset mas alto) -- simple, pero se rompe si
  ese batch especifico falla o hace timeout, que es justo lo que el watchdog existe para cubrir.

**Nota de alcance:** arreglar esto NO elimina la necesidad de la regla de CLAUDE.md ni del hook
`block_lambda_invoke.sh` -- son defensas independientes. Este bug hace que un error humano se
amplifique x12.5; la regla y el hook evitan el error humano en primer lugar. Ambos hacen falta.

**Revisit if:** al implementar el lock, aparece un caso donde encadenar dos veces sea realmente
inofensivo y la complejidad del lock no se justifique -- improbable dado que cada encadenamiento
cuesta un correo real via SES y una llamada Bedrock completa (executive summary) por ciclo.

**CERRADO 2026-08-19.** Implementada la primera direccion candidata (lock atomico en DynamoDB),
en el mismo deploy que otros dos bugs reales encontrados por dos auditorias independientes (ver
"News Temporal Tiers" arriba para el hallazgo de `first_seen`-reset que motivo el fix del
backfill nativo):

1. **Lock de encadenamiento** -- tabla nueva `poly-rag-cycle-chain-locks` (hash key `pk`,
   `terraform/dynamodb.tf`), un item por `cycle_started_at`. `claim_chain_advance()` en
   `lambdas/ingest_news/handler.py` hace `put_item` con
   `ConditionExpression="attribute_not_exists(pk)"` antes de `invoke_next_stage` -- solo el
   primer batch que reclame la key encadena a Comments, el resto recibe
   `ConditionalCheckFailedException` y no hace nada. `invoke_next_stage(cycle_started_at)` ahora
   vive detras de `if claim_chain_advance(cycle_started_at):`.
2. **Decrement antes del error, no despues** -- `decrement_post_resolution_counter` se movio
   antes de la llamada a `process_market_news` en el loop de `lambda_handler`, para que un
   fallo transitorio de busqueda consuma la ventana post-resolucion en vez de extenderla
   silenciosamente.
3. **Backfill nativo con merge, no overwrite** -- `backfill_odds_history_for_new_market` en
   `lambdas/ingest_polymarket/handler.py` ahora lee el archivo `odds/<id>.json` existente (si
   hay), mergea por timestamp con los puntos nuevos (existentes ganan colision, mismo patron que
   `merge_snapshots` en `scripts/backfill_odds_history.py`), y escribe el resultado combinado --
   antes hacia `put_object` directo, lo cual habria destruido la historia de ciclo completa de
   cualquier market que saliera y regresara al registry (ver el hallazgo de `first_seen`-reset).

**Desplegado via dos `terraform apply -target` separados, planes verificados en ambos** (`2 to
add, 3 to change, 1 to destroy` para la tabla+IAM+ambas Lambdas; `0 to add, 1 to change, 0 to
destroy` para un fix de seguimiento -- la variable de entorno `CYCLE_CHAIN_LOCKS_TABLE` no se
habia declarado en Terraform para `ingest_news`, funcionaba por el default hardcoded en el
codigo pero rompia la convencion del proyecto de declarar cada nombre de tabla explicitamente).
Ningun otro recurso tocado (send_digest, ingest_comments, EventBridge, tablas existentes). No se
invoco ninguna Lambda para verificar -- el ciclo automatico de las 00:00 UTC es la primera
prueba real.

**Origen de los dos bugs de #2 y #3:** encontrados de forma INDEPENDIENTE por dos auditorias
distintas (subagentes sin contexto compartido) el 2026-08-19, y coincidieron exacto en ambos --
señal fuerte de que eran reales y no ruido de auditoria (a diferencia de otros 3 hallazgos de la
primera auditoria que resultaron sobre-reportados al verificarlos a mano).

**Revisit if:** el ciclo automatico expone un caso no cubierto por el lock (ej. dos cycle_started_at
distintos colisionando, que no deberia pasar dado que cada ciclo real tiene un timestamp unico).

---

## Digest Fidelity Audit and Redesign Backlog (2026-08-19)

**Trigger:** the "Bespoke Digest Redesign" entry above explicitly flagged this as a revisit
condition -- "the RAG ingestion work (Day 4/5) reveals the digest JSON schema needs different
fields... not validated yet against actual retrieval needs." That's what happened here: while
designing Day 4 chunking strategy and a proposed "Capa 0" retrieval layer that would reuse
`send_digest`'s already-computed artifacts (`executive_summary`, `top_volatility`,
`world_snapshot`) as a cheap first lookup before falling back to deep odds/news/comments
retrieval, the user correctly insisted this couldn't be trusted without first measuring the
LLM output's own fidelity -- an error in send_digest's synthesis would otherwise propagate
into anything built on top of it.

**Fidelity audit method:** compared 3 real digest emails (2026-08-16 01:00 and 12:00 UTC, plus
one more) against the raw JSON in `digest/*.json`, section by section, not from memory or
assumption.

**Finding: 7 of 8 digest sections are 100% structured data, never touched by an LLM.**
`newly_tracked_markets`, `resolved_markets`, `top_volatility`, `world_snapshot`
(`top_conviction`/`most_disputed`), `quotes`, and the per-source item counts are all computed
deterministically and copied verbatim into the email -- zero hallucination risk, confirmed
number-for-number against the source JSON across all 3 digests checked. Only
`executive_summary` is genuinely LLM-authored prose; in the one instance reviewed in detail it
was numerically correct with one causal inference ("likely prompted by...") that was
appropriately hedged, not asserted as fact. **Practical implication for the Capa 0 design:**
the 5 structured fields can be used as a retrieval source immediately, with no LLM evaluation
required -- only `executive_summary` needs the kind of fidelity evaluation originally planned
for "the digest's LLM."

**Real bug found and confirmed in code, not just observed in emails:** the user had already
independently noticed that `News Highlights`/`Trader Comments` repeat identical content across
consecutive cycles (verified: 2026-08-16 01:00 and 12:00 UTC emails show the exact same 3
trader comments verbatim, despite the comment pool growing from 2,589 to 2,831 in between).
Root cause confirmed in `lambdas/send_digest/handler.py`, `extract_quotes`: it does
`payload.get(source, [])[:QUOTE_COUNT_PER_SOURCE]` -- a plain positional slice of the first 3
items, no ranking, no relevance criterion, no dedup against what a prior cycle already showed.
If the upstream array's ordering is stable (e.g. Comments grouped by entity in a consistent
order), the same leading items surface every cycle regardless of how much new content arrived.
User noted this self-corrects partially in later cycles (observed, not yet root-caused why),
but the underlying selection logic is still broken.

**Second real bug found in code: `world_snapshot`'s ranking criterion (`volume24hr`) is
computed and stored but never rendered.** `compute_world_snapshot` populates `volume24hr` on
every `top_conviction`/`most_disputed` entry (confirmed present in the real JSON), but the HTML
template (`lambdas/send_digest/handler.py` lines ~393-411) only reads `question` and `price`
when building the email -- the number that justifies the ranking is computed, stored, and then
silently discarded before reaching the reader. A `0%` price with no volume next to it can't
distinguish a real $400K-volume conviction bet from a $50 ghost market.

**Redesign backlog collected with the user, not yet implemented (design/mock only):**
1. Daily/Week/Month range tabs -- real constraint identified: SES/Gmail can't run JS, so an
   actual email must default to a static Daily view with a "View full trends" link to a hosted
   web page for Week/Month, not live in-email tab switching. Weekly/monthly aggregation logic
   (sum deltas vs. net movement) is explicitly unresolved, flagged as its own open question.
2. New Markets: show odds + `volume24hr` (bet size) per market, not just the question text.
3. Resolved: show the pre-resolution price/momentum, not just the binary `outcome_prices` --
   a coin-flip resolving is not news, a 95%-favorite losing is.
4. Biggest Moves -- reviewed, no changes needed.
5. Fix the quote-repetition bug above -- real selection criterion (e.g. engagement, dedup
   against recently-shown items) instead of positional slicing.
6. Top 5 newest markets by `volume24hr` get linked News context inline -- feasible TODAY using
   the existing 1:1 `market_ids` linkage (no retrieval/embedding dependency, unlike an earlier
   version of this idea that assumed it needed Day 4 retrieval to exist first).
7. New Markets section: only show top 10 by volume, collapse the rest (79 new markets in one
   real digest is unreadable) -- overlaps with #2/#6, likely merges into one section redesign
   at implementation time rather than three separate changes.
8. Render `volume24hr` in `world_snapshot` (fix the discarded-data bug above) AND add a
   one-line caption explaining each section's ranking criterion, since a bare percentage with
   no stated basis doesn't communicate why those 5 markets were chosen.

**Mock built and published** (HTML artifact, fake but domain-realistic data, `artifact-design`
skill loaded first since this is a utilitarian/memo treatment for a real transactional email,
not an editorial page) demonstrating all 8 items together -- cool "data terminal" palette
instead of the generic warm-cream default, tabular-nums monospace for all figures so columns
of prices/volumes align like a ledger, and an explicit "Synthesized narrative" label on the
LLM-authored summary paragraph to visually separate it from the surrounding structured (fully
fidelity-verified) sections -- the same distinction the fidelity audit itself established.

**Capa 0 design decision (for later, Day 4 block G):** if built, it must be an isolated,
independently-toggleable function (e.g. `query_digest_layer()` behind a `USE_DIGEST_LAYER` env
var, same pattern as the existing `USE_LLM_ENRICHMENT` flag) so an A/B test against going
straight to deep odds/news/comments retrieval doesn't require refactoring either path. Whether
it's actually worth the extra hop is explicitly deferred to Day 4 block H (real evaluation with
real metrics) -- not decided here, per this project's own "measure, don't guess" discipline.

**Implemented 2026-08-20:** items #2/#5/#6/#7/#8 (both real bugs plus New Markets and Resolved
redesign), verified by running the modified pure functions locally against real S3 data for the
2026-08-19 12:00 UTC cycle (not invoked as a Lambda -- see CLAUDE.md rule on manual invocation).
`schema_version` bumped to v2. Item #1 (Daily/Week/Month tabs) intentionally NOT implemented --
still blocked on the unresolved weekly/monthly aggregation question and the separate hosted-page
requirement. Deploy (terraform apply / zip) left for the next automatic cycle to pick up, not
verified live yet.

**Status 2026-08-20: _digest is canon, treated as the MVP retrieval/RAG artifact for now.**
Explicit user decision -- stop polishing this in isolation. The 4 implemented fixes above close
the confirmed defects and the most unreadable gaps (79 new markets with no context, discarded
ranking signal); further refinement is deferred until real usage at the end of the pipeline
(Day 4/5 retrieval, Capa 0 if built) surfaces what's actually missing, instead of guessing more
improvements now. If a real bug shows up in a future _digest email, it gets reported ad hoc and
fixed then -- this entry is closed as "good enough MVP," not reopened for polish on its own.

**Revisit if:** a real fidelity/rendering bug is reported in a live _digest email, or Day 4/5
retrieval work reveals the schema itself (not just the email rendering) needs different fields.

---

## Guardrails Against Unbounded Structured Queries (Day 5, not yet designed)

**Issue:** the odds retrieval design (Day 4 block E, see architecture_canon.md "Retrieval")
treats odds as deterministic filter/aggregation, not semantic search -- F1-F5 all assume a
selective filter (`market_id`, a bounded time range, `source`) that lets partitioning/indexing
keep the query cheap regardless of how large the underlying corpus grows. That guarantee holds
only if every query is actually selective. When Day 5's synthesis agent starts translating a
user's natural-language question into one of these structured queries (prompt/context
engineering, deciding which F-path and which filters to apply), nothing yet stops it from
generating the equivalent of a full scan -- e.g. no market_id and no date bound, effectively
SELECT * across every market's entire history.

**Debt:** at current corpus size (hundreds of markets, tens of thousands of snapshots) an
unbounded query is just slow/wasteful, not dangerous. But the whole point of designing odds as
partition-friendly (see the petabyte-scale discussion, 2026-08-20 -- worth archiving to
knowledge.md too) is that cost stays proportional to the answer, not the table. An LLM-generated
query path can silently defeat that if nothing enforces selectivity, and the failure mode gets
worse exactly as the corpus grows, not better.

**Mitigation (not yet designed, flagged for Day 5):** when prompt/context engineering for the
synthesis agent is built, it needs an explicit guardrail layer that rejects or rewrites
structured queries lacking a bounded filter -- e.g. require at least one of market_id or a
capped date range before a query is allowed to execute, same spirit as the project's existing
cost guardrails (AWS Budget Deny policy, USE_LLM_ENRICHMENT toggles) but applied to query shape
instead of spend. Not designed yet -- no schema, no enforcement point (agent-side prompt
constraint vs. a query-layer validator) decided. Explicitly out of scope for Day 4 (retrieval
design itself), scoped to Day 5 (synthesis agent) per user, 2026-08-20.

**Revisit when:** Day 5 synthesis agent design begins and the structured-query path (F1-F5) gets
an actual query-generation mechanism, not just the conceptual F1-F5 taxonomy that exists today.

---

## Day 6: A/B Tests as a Live, User-Facing Feature -- 4 Corpus Indices x Query-Time Capa 0 Toggle (revised 2026-08-20)

**Reframed 2026-08-20, changes the design, not just the backlog.** Originally scoped as a
deferred measurement pass ("decide a default now, A/B it later"). The user clarified the real
target audience for this portfolio project (Pienza's successor): not primarily the trader
end-user, but **technical recruiters/interviewers who will interact with the RAG directly** --
the demonstrated design/evaluation PROCESS is the actual product, not a single optimized answer
path. Explicit user framing: "el chiste no es demostrar el chatbot funcionando... sino que es mas
importante ver el proceso creativo," imagining the same question run across all 8 combinations
with visible per-combination metrics side by side. Budget stance, also explicit: "si este dinero
saliera de mi bolsa no lo haria... dado que estamos en el free tier de 160 usd, vamos probando" --
deliberately spending the promotional credit buffer on this, accepting that hitting a spend
guardrail is itself useful signal, not a failure to avoid.

**Consequence: this is no longer a deferred backlog -- it's a day-1 requirement for blocks F/G/H.**
Three independent binary toggles, full combinatorial (2^3 = 8), each user-selectable (frontend
toggle target, not just an internal eval script):

- **Axis 1 -- corpus variant, named after the one source that actually differs between them:**
  1A = full corpus (News chunked paragraph+neighbors, PLUS Comments, Digest, and the
  question/description registry index unchanged) vs. 1B = full corpus (News chunked as
  whole-article instead, everything else identical to 1A). Comments, Digest, and the registry
  semantic index each have exactly ONE chunking method (see architecture_canon.md) -- they don't
  branch inside axis 1, they just travel unchanged inside both 1A and 1B. Axis 1 is a corpus-level
  label, not a per-source toggle -- resolved explicitly 2026-08-20 after initial confusion trying
  to fold Comments/Digest into a chunking axis they don't have.
- **Axis 2 -- Capa 0, NOT part of the combinatorial below, applied at query time instead:**
  2A digest-as-retrieval-layer on vs. 2B off (straight to deep odds/news/comments retrieval).
  Behind `USE_DIGEST_LAYER`, same isolation pattern as `USE_LLM_ENRICHMENT`. Confirmed 2026-08-20:
  this toggle doesn't change what's stored in the vector store, only whether retrieval consults
  the Digest index first -- it applies on top of whichever of the 4 corpus indices (below) answers
  a query, not as a 5th build-time axis.
- **Axis 3 -- embedding model, widened 2026-08-20 from binary to 3-way:** 3A Titan v2 (shipped
  default, zero friction, already authorized), 3B Cohere Embed v4 (verified real pricing ~$0.12/M
  input tokens, ~6x Titan), 3C **Voyage AI** (`voyage-finance-2`, Anthropic's recommended
  embeddings partner, tuned specifically for finance/news -- a good domain fit for Poly-RAG's
  actual content). Voyage was originally excluded from the Day 4 block F comparison for requiring
  an external API key outside AWS IAM -- the user pointed out 2026-08-20 that this exclusion was
  an inferred pattern, not an actual project rule (CLAUDE.md has no stated IAM-only requirement;
  it emerged from every other credential in the project happening to be AWS-native, not from a
  deliberate decision to avoid external secrets). Re-added on that basis. Concrete new
  requirement this creates, not yet designed: Voyage needs an API key stored somewhere (Secrets
  Manager, or a Lambda env var) -- the project currently has zero live secrets (the old Bluesky
  credentials are dead), so this would be the first one, and needs its own storage/injection
  design before Voyage can actually be tested, not just listed as a candidate. Bifurcating this
  axis for real means storing THREE full parallel embedded copies of the corpus (Titan, Cohere,
  and Voyage-embedded), not just a code branch, since vector similarity only makes sense within
  one embedding space.

**Actual build-time combinatorial: 2x3 = 6 corpus indices (axis 1 x axis 3), not 2^3 = 8, not
2x2 = 4 anymore either.** Axis 2 (Capa 0) is a query-time flag applied on top of any of the 6,
not a build-time multiplier:

```
1A3A -- full corpus (News paragraph+neighbors + Comments + Digest + registry), Titan-embedded
1A3B -- same corpus,                                                            Cohere-embedded
1A3C -- same corpus,                                                            Voyage-embedded
1B3A -- full corpus (News whole-article  + Comments + Digest + registry),      Titan-embedded
1B3B -- same corpus,                                                            Cohere-embedded
1B3C -- same corpus,                                                            Voyage-embedded
```

Each of the 6 indices can then be queried with Capa 0 on or off (axis 2) -- giving 12 answerable
configurations at query time, but only 6 things to actually build/store in the vector store.

**Design implications this forces on blocks F/G/H (not deferred, needed for the 8-way comparison
to exist at all):**
- Block F (vector store): must hold both embedding spaces (Titan + Cohere) from day one, not add
  a second index later as an afterthought.
- Block G (retrieval function): must accept all 3 toggles as parameters from the start, so a
  single query can be run through all 8 configurations and return 8 comparable results, not one.
- Block H (evaluation): must produce metrics PER combination, not one aggregate score -- the
  side-by-side comparison view (same question, 8 answers, 8 metric sets) is itself part of the
  product surface, not an internal debugging artifact.

**Cost/latency must also be tracked PER SOURCE, not just per corpus index (2026-08-20).**
Explicit user requirement: even though the 4 corpus indices are each built from the full corpus
as one unit (registry + News + Comments + Digest all feeding the same Titan or Cohere embedding
pass), every individual embedding call must log which source it came from --
`source: "registry" | "news_paragraph" | "news_article" | "comments" | "digest"` -- alongside
`embedding_model` (titan_v2 / cohere_v4), `tokens_in`, `latency_ms`, and `estimated_cost_usd`.

**New table, not reused: `poly-rag-embedding-metrics` (decided 2026-08-20), separate from
`poly-rag-architecture-metrics`.** The existing metrics table has one row shape for the 4
ingestion Lambdas (Fase 1); forcing embedding rows (with their own `source`/`embedding_model`
fields, N rows per Fase 2 invocation instead of 1) into that same table would mix two different
row shapes and make the existing table harder to query cleanly. A dedicated table keeps Fase 2
as cleanly separated at the data layer as it already is at the code/trigger layer (see "Fase de
embedding, desacoplada de la cadena de ingesta" in architecture_canon.md). Same infra pattern as
every other project table: pay-per-request, PITR enabled, added via Terraform when block F is
built -- not created ad hoc.

Without this, there's no way to later answer "how much did embedding Comments cost with Cohere
vs. Titan" without re-deriving it from an aggregate that already mixed all 4 sources together.
This is a Block F requirement regardless of whether the project ends up shipping with only Titan
in production -- the per-source breakdown is what makes the Day 6 comparison legible, not an
optional nice-to-have metric.

**BUILT 2026-08-22.** Table created via Terraform (dynamodb.tf), populated by all 4 embed
Lambdas (embed_digest/embed_comments/embed_registry/embed_news_article) -- one row per Bedrock
request, carrying `source`, `embedding_model`, `tokens_in`, `latency_ms`, `estimated_cost_usd`
(real confirmed Cohere v4 pricing, $0.12/M input tokens). Went further than originally scoped
here: `embed_news_article`, as the true last stage of the full cycle (Fase 1 + Fase 2), also
reads back `poly-rag-architecture-metrics` (Fase 1's existing table) and sends a second cycle
report email via SES -- separate from send_digest's market-content email, sent at the END of
Fase 2 rather than Fase 1, deliberately so the two emails' timestamps let the user measure
Fase 2's real wall-clock duration per cycle just by diffing when each arrives. See
session_ledger.md 2026-08-21 (local date) for the mock reviewed before deploying.

**Experimental design note, resolved:** considered vary-one-axis-at-a-time (4 runs against a
baseline) as cheaper and sufficient absent suspected interaction between axes -- user does not
suspect real interaction, but wants the full cube anyway because the demonstration value (a
recruiter freely toggling any combination) matters more here than experimental minimalism. Not a
scientific-rigor decision -- a portfolio-product decision.

**Cost guardrail still applies unchanged:** the existing $10 Deny-policy guardrail (see CLAUDE.md)
remains the backstop if the 2x embedding storage + 8-way eval runs spend faster than expected --
explicitly accepted as "useful information," not a scenario to design around defensively.

**Explicitly not decided yet:** the evaluation metric(s) themselves (retrieval precision/recall,
answer quality via LLM-as-judge, latency, cost per query, some combination -- see "RAG Evaluation
Metrics Landscape" entry above for the Ragas/RAG-Triad option) -- that's still Day 4 block H's
job, which this reframing depends on more directly now, not less.

**Revisit when:** Day 4 block F (vector store) design begins -- the dual-embedding-space
requirement needs to be in the initial schema, not retrofitted.

---

## RAG Evaluation Metrics Landscape (for Day 5 block H, sourced from Chip Huyen's book, 2026-08-20)

**Issue:** Day 4 block H (real evaluation with real metrics) and the Day 6 A/B test backlog above
both assume some evaluation method exists, but nothing concrete has been chosen yet -- prior
entries only say "LLM-as-judge" generically. While reading the relevant chapter of Chip Huyen's
*AI Engineering* (this project's primary reference, see CLAUDE.md), the user surfaced a
chronological map of what actually superseded BERTScore (2019, embedding-similarity baseline) as
the field's evaluation approach, worth archiving here so Day 5 doesn't reinvent this research from
scratch. Not yet decided which of these Poly-RAG will actually use -- this entry is the landscape,
not a decision.

**2020-2022, models trained against human judgment (not just embedding similarity):**
- **BLEURT** (Google) -- BERT-based, pretrained on millions of artificially corrupted sentences
  (typos, deletions, negations) and calibrated directly against human ratings.
- **COMET** -- the de facto standard for machine translation quality, combines source text +
  model output + human reference.

**2022-2023, NLI-based models (moved from measuring similarity to measuring logical
contradiction):**
- **SummaC** (2022) -- NLI-based metric built specifically to catch hallucination in document
  summarization.
- **AlignScore** (2023) -- trained across 7 distinct logical-inference tasks to verify factual
  consistency (detects when a candidate asserts something not present in the source).

**2023-2025, LLM-as-judge:**
- **G-Eval** (2023) -- uses GPT-4 with chain-of-thought reasoning, weights token probabilities to
  assign a 1-5 score against a detailed rubric.
- **Prometheus 2** (2024) -- best-known open-source model (7B/8x7B) trained specifically to judge
  other LLMs' quality/format/accuracy.

**2024-2025, RAG-specific frameworks -- most directly relevant to Poly-RAG's Day 5 block:**
- **Ragas / DeepEval / TruLens** -- don't rely on one metric, evaluate the **RAG Triad**:
  - **Faithfulness (groundedness):** does the answer come 100% from the retrieved chunks, or did
    the model hallucinate beyond them?
  - **Answer relevance:** did it actually answer the user's question?
  - **Context precision/recall:** did the retriever pull the right chunks in the first place?

**Why this matters for Poly-RAG specifically:** the RAG Triad separates retrieval quality
(context precision/recall) from generation quality (faithfulness, answer relevance) -- directly
useful for the Day 6 A/B backlog above (paragraph-vs-whole-article chunking, Capa 0 on/off),
since those changes affect retrieval, not generation, and a metric that conflates the two would
hide which layer actually caused a quality change.

**Not decided:** whether Poly-RAG adopts one of these frameworks (Ragas is the most commonly
cited for production RAG) or builds a narrower custom LLM-as-judge pass using Bedrock (consistent
with the project's existing all-Bedrock-via-IAM pattern, avoiding a new external dependency).

**Revisit when:** Day 5 synthesis agent design begins and Day 4 block H (evaluation) needs an
actual method, not just this landscape.

---

## Embedding Model Choice (Day 4 block F, DECIDED 2026-08-20 -- Titan v2 shipped, 3-way A/B vs Cohere v4 and Voyage in Day 6)

**Issue:** chunking (block E) is closed for all 3 sources, but nothing has been chosen yet for
turning chunk text into vectors -- that choice is separate from the vector store (which only
stores/searches vectors already produced, see knowledge.md if archived). Four candidates on the
table, not yet verified against real AWS/Bedrock docs -- treat every pricing/quality claim below
as a starting point to confirm, not a decided fact:

1. **Cohere Embed v3 via Bedrock** (`cohere.embed-english-v3` or multilingual) -- claimed to
   outperform Titan on financial/political news retrieval, and to expose an explicit
   `input_type` param (`search_document` vs `search_query`) that could meaningfully improve hit
   rate. Same IAM/billing pattern as the rest of the project (no new API key) IF it's actually
   available in us-east-1 -- unverified as of 2026-08-20.
2. **Amazon Titan Embeddings v2** (`amazon.titan-embed-text-v2:0`) -- simplest, most consistent
   with the rest of the stack (same pattern as Claude Sonnet 4.5 via Bedrock), claimed cheapest
   (~$0.02/M tokens) and supports configurable output dimensions (256/512/1024) to trade
   precision for storage. Claimed weaker semantic precision on niche domains (crypto, fine-
   grained political probabilities) -- exactly Poly-RAG's actual content, so this tradeoff
   matters more here than in a generic use case, if the claim holds up.
3. **Self-hosted in Lambda via fastembed (ONNX/Rust, not PyTorch)**, e.g.
   `BAAI/bge-small-en-v1.5` -- claimed ~30MB package (fits Lambda's 250MB limit, unlike
   sentence-transformers+torch which reportedly exceeds it), sub-100ms cold start, zero
   marginal cost per call. Only self-hosted path considered viable given the project's Lambda-
   only compute constraint (CLAUDE.md explicitly avoids always-on compute).
4. **Voyage AI** (`voyage-3-lite` or `voyage-finance-2`, the latter finance-news-tuned) --
   Anthropic's recommended embeddings partner. Requires an external API key outside AWS, which
   breaks the project's established IAM-only/no-new-secrets pattern (same reasoning that already
   ruled out separate API keys for Claude itself) -- would need a strong quality justification to
   accept that tradeoff.

**What matters most for Poly-RAG specifically, given the project's own constraints:** (a) IAM-
consistent auth/billing is an explicit existing value, ruling out Voyage unless quality gain is
large; (b) real $5/month budget with ~5M accumulated News tokens and growing means per-token
embedding cost matters more here than it did for the one-off executive_summary call -- favors
Titan or self-hosted; (c) fastembed/ONNX is the only viable self-hosted path given Lambda-only
compute (plain sentence-transformers+torch would blow both the package-size and cold-start
budgets the project has already had to tune around, e.g. News's 900s timeout).

**Verified 2026-08-20, real AWS CLI queries against this account/region (not invoking any
model -- `list-foundation-models`, `get-foundation-model`,
`list-foundation-model-agreement-offers`, `get-foundation-model-availability`, all read-only):**

| Model | Region availability | Account authorization | Real input pricing (USE1) |
|---|---|---|---|
| `amazon.titan-embed-text-v2:0` | AVAILABLE | AUTHORIZED, no agreement required | not returned by API (no agreement gate at all) but known ~$0.02/M tokens |
| `cohere.embed-english-v3` | AVAILABLE | AUTHORIZED, but marketplace agreement `NOT_AVAILABLE` -- needs an extra acceptance step before it can be invoked | $0.10/M input tokens (confirmed real) |
| `cohere.embed-v4:0` | AVAILABLE (also supports IMAGE input, life-of-model since 2025-10-02, newer than v3) | not individually checked, same Cohere agreement gate expected | $0.12/M input tokens (confirmed real) |

The `input_type` (search_document/search_query) claim was NOT verifiable via CLI -- it lives in
the invocation payload format, not the model catalog, so it can only be confirmed by reading
Cohere's Bedrock invocation docs or a real test call. Still unconfirmed.

**Decision, 2026-08-20:** ship with **Titan Embeddings v2** now -- zero friction (already
authorized, no agreement to accept), consistent with the project's existing all-Bedrock-via-IAM
pattern, and cheap at current corpus size (~5M News tokens accumulated: Titan bootstrap cost is
roughly $0.10 total vs. Cohere v3's ~$0.50, not dramatic today but scales worse if the corpus
grows 10x). The claimed weaker semantic precision on niche domains (crypto, fine-grained
political odds) is accepted as an open risk, not resolved -- exactly what the Day 6 A/B test
below exists to measure instead of guessing.

**Day 6 backlog, new item added:** A/B test Titan v2 vs. **Cohere Embed v4** (not v3 -- v4 is
Cohere's current model, no reason to benchmark against their prior generation) once Day 4/5 have
a real query interface to compare against. Folded into the same Day 6 evaluation pass as the
paragraph-vs-whole-article chunking and Capa 0 on/off axes (see the Day 6 entry above) -- same
reasoning: measure real quality difference before paying the 5-6x cost premium, rather than
deciding off spec sheets.

**Revisit when:** Day 6 evaluation work begins and there's a real query interface to A/B Titan
against Cohere v4.

**Update 2026-08-21 -- Titan v2 DROPPED from the project entirely, not deferred.** During the
real embedding bootstrap (`scripts/bootstrap_embed_corpus.py --apply`), Titan hit AWS's real
account quota: "On-demand model inference requests per minute for Amazon Titan Text Embeddings
V2" = 600 (verified via `aws service-quotas list-service-quotas --service-code bedrock`), and
Titan's Bedrock API has no batch field at all (`{"inputText": "<one string>"}`, one request per
text, confirmed via `aws bedrock get-foundation-model` -- inferenceTypesSupported=[ON_DEMAND],
no BATCH, so async Batch Inference doesn't apply either). At ~120K chunks per corpus variant that
quota projects to ~3.5 hours just for Titan on the paragraph-chunked variant alone (confirmed
real via CloudWatch: a sustained ~600 invocations/min, i.e. already saturating the account
ceiling, not throttling from this script's own concurrency). Cohere v4 (96 texts/call) and Voyage
(128 texts/call) both have real multi-text batch APIs and finished the same corpus size in
minutes (Voyage: 38/62 checkpoints, ~76,000 vectors, in under 20 minutes, before Titan was
dropped). The key realization: this 600/min ceiling is a **permanent account quota**, not a
one-off bootstrap fluke -- it would be the bottleneck of every future incremental Phase 2 cycle
in production too, not just this bootstrap. Titan was the cheapest of the three in dollars, but
"free in dollars" turned out to cost hours of wall-clock time every cycle, which is the resource
this project actually budgets against day to day (see CLAUDE.md, budget discipline is about
spending deliberately, not just about dollars). **Decision: Cohere v4 + Voyage
(voyage-finance-2) are now the 2 embedding models compared, not 3 -- Titan is out of the
architecture, not paused for Day 6.** architecture_canon.md updated to match. The original
"Titan default, Cohere/Voyage A/B in Day 6" framing was already superseded earlier the same
session (2026-08-20/21) when the project moved to all comparison models running from the
bootstrap itself, not staggered into Day 6 -- this update removes Titan from that already-revised
3-way comparison, leaving a 2-way one.

**Update 2026-08-21 (later the same day) -- Voyage is OUT too; the project is Cohere-only
for now, and the comparison axis is deliberately suspended, not deleted.** Explicit user
decision ("Voyage is out of the question") after Voyage's single completed run consumed
**56.5% of its 50M free tier** in one pass, projecting exhaustion in ~4 days at current
ingestion rate -- with no spend guardrail built (see "Voyage AI Free Tier Spend Alert"
above, still open and now moot for Voyage specifically). Combined with Titan already being
dropped for its 600 req/min ceiling, **Cohere Embed v4 is currently the only embedding
model in the project.**

This does collapse axis 3 of the Day 6 recruiter-facing comparison to a single value, which
is a real loss against a documented day-1 requirement -- accepted deliberately and
provisionally. The user's stated sequencing: get ONE variant (`news_article`) fully
embedded and queryable end-to-end first, and only then consider reintroducing a second
model (Titan was named as the more likely candidate to revive than Voyage) and/or the second
chunking variant (`news_paragraph`). The vectors already produced are unaffected by that
later choice -- adding a model means embedding the same chunks again into a separate
namespace, not redoing any prior work.

**Revisit when:** the `news_article` + Cohere path is proven end-to-end through Phase 3
(vectors queryable in a real store), which is the user's stated precondition for widening
back to a multi-model comparison.

---

## Voyage AI Free Tier Spend Alert -- Pending, Not Designed (2026-08-20)

**Issue:** Voyage AI (now owned by MongoDB, confirmed 2026-08-20, still has a standalone API
separate from Atlas) offers a real free tier -- 200M tokens for general text models, 50M tokens
specifically for `voyage-finance-2` (the domain-tuned model this project cares about, more than
enough for the current ~5M-token News corpus). The user is adding a credit card to reach Usage
Tier 1, which raises rate limits (confirmed via Voyage's real rate-limit docs, 2026-08-20 --
Tier 1 limits are in the thousands of RPM / millions of TPM depending on model, not the
restrictive no-card default) -- rate limiting is a non-issue for this project's bootstrap volume
at Tier 1. But adding a card also means usage can silently start being billed once the 50M/200M
free token allocation runs out, with no guardrail today -- that's the real open item, not rate
limits.

**Not yet designed:** an alert or hard stop on the `embed_voyage` Lambda (see the 3-level Fase 2
Lambda architecture above) for when the Voyage free-tier token allocation is close to exhausted
or exceeded. Needs a mechanism to track cumulative Voyage token usage against the 50M/200M free
ceiling and either alert (email/CloudWatch) or hard-stop invocations before real spend starts --
same spirit as the project's existing $10 AWS Budget Deny-policy guardrail, but for a
non-AWS vendor with no equivalent automatic mechanism (Voyage's billing lives outside AWS
Budgets entirely, so the existing guardrail doesn't cover it).

**Explicitly deferred:** flagged by the user as a pending item to design later, not now -- do not
design the actual mechanism (CloudWatch alarm vs. DynamoDB counter vs. Voyage's own dashboard
alerts, if they have one) until Day 4 block F implementation actually begins.

**Revisit when:** `embed_voyage` is actually being built.

---

## Vector Store Choice (Day 4 block F, DECIDED 2026-08-21 -- LanceDB, see correction at the end)

> **STATUS 2026-08-21: DECIDED.** LanceDB, chosen on measured storage-growth cost and
> free-tier ceilings, not on the retrieval-quality reasoning explored below (that
> comparison -- filter-during vs filter-after search -- is deferred to Day 6, unchanged).
> The reasoning below remains useful background for the Day 6 comparison; read the
> correction at the bottom of this entry for why LanceDB won TODAY's decision specifically.

**Issue:** the embedding model is decided (Titan v2 default, Cohere v4 + Voyage as Day 6 A/B),
chunking is closed for all 4 sources, but nothing has been chosen for WHERE the resulting vectors
actually get stored and searched. OpenSearch Serverless was already ruled out long ago
(~$700/month, incompatible with the project's budget, see architecture_canon.md). Four real
candidates on the table as of 2026-08-20, none of the specific claims below verified against
real docs yet -- same discipline as the embedding-model landscape entry above, treat as a
starting point to confirm, not a decided fact:

1. **LanceDB, stored directly in S3** -- an embedded vector library (runs inside the Lambda
   process, no external service) that writes its index in the Lance columnar format, claimed to
   be append-only-friendly: new chunks land as new fragment files in S3 rather than triggering a
   full index rewrite. If the claim holds, this directly fixes the read-modify-write problem that
   has already bitten this project twice for real (odds snapshots, News batch files) -- growing
   the index would no longer mean reading the whole thing into Lambda memory, appending, and
   rewriting. Zero new accounts, zero new secrets, cost is S3 storage only. Open question, not yet
   verified: packaging `lancedb` into a Lambda (Layer or Docker image) given the project's
   already-tuned Lambda limits (e.g. News's 900s timeout, existing package-size constraints).
2. **Qdrant** (managed cloud, free tier: a permanent 1GB cluster, or self-hostable) -- Rust-based,
   claimed sub-10ms latency, and specifically optimized for inline metadata filtering during HNSW
   graph traversal rather than filtering results after the vector search runs. This matches
   Poly-RAG's actual retrieval pattern directly: every query already starts with a structured
   filter (`market_id`, `temporal_tier`, `link_type`) before any semantic search happens (see the
   F1-F5 odds design and the News/Comments chunk metadata above) -- a store built around
   filter-during-search, not filter-after, fits that shape better than a generic vector DB would.
   Requires an external account + API key (real but trivial setup cost, same as setting up SES or
   an S3 bucket the first time -- not the friction the project first assumed for Voyage AI either,
   see "Embedding Model Choice" entry above).
3. **Pinecone** (managed cloud, serverless free tier) -- the most widely recognized vector DB,
   simplest SDK, but its filtering is post-search rather than inline like Qdrant's, which fits
   this project's filter-first retrieval pattern less precisely. Same external-account/API-key
   setup cost as Qdrant, without Qdrant's filtering advantage for this specific use case.
4. **Databricks Vector Search** -- would connect Delta Lake/Unity Catalog (already built in Day 3,
   currently an isolated exploration layer, never invoked from the live pipeline) to the real
   retrieval path for the first time -- the strongest portfolio narrative of the four (same data,
   two cloud platforms, a real cross-platform design decision to defend in an interview). Also
   introduces real cross-cloud latency (AWS Lambda invoking Databricks) and a new AWS<->Databricks
   auth path that doesn't exist today (all Databricks access so far has been notebook-side,
   read-only exploration).
   **Verified 2026-08-20: Free Edition DOES support Vector Search** (branded Mosaic AI Vector
   Search / Databricks AI Search), but with a concrete hard limit -- **only 1 active vector search
   endpoint per account at a time**, plus small/shared compute under a fair-use policy, and no
   support for advanced online tables or commercial use. This is a likely disqualifying constraint
   for THIS project's specific Day 6 design: 6 corpus indices (2 chunking methods x 3 embedding
   models) need to be queryable side-by-side for the recruiter-facing comparison, and it's not
   confirmed whether Databricks allows multiple named indices under a single endpoint (which would
   route around the limit) or whether the limit caps the whole use case at 1 index total. Needs
   this specific question answered -- not just "does Free Edition support Vector Search" -- before
   Databricks can be seriously compared against the other three for Day 6's actual requirement.

**Databricks Vector Search ruled out, 2026-08-20.** Verified support exists in Free Edition, but
the 1-endpoint-per-account limit is a real blocker for this project's specific Day 6 shape (6
corpus indices need to be queryable side-by-side) -- not worth pursuing further given the other
three candidates have no equivalent hard cap.

**Initially considered rejecting a 3-way parallel build (Pinecone/Qdrant/LanceDB) as overkill,
then REVERSED, 2026-08-20 -- this IS the point, not scope creep.** First pass reasoning
(rejected): the vector store is invisible infrastructure, a recruiter never sees which store
answered a query, so tripling integration work multiplies engineering surface without
multiplying anything demonstrable. **User's correction, and the actual reasoning that stands:**
that argument silently assumed its own conclusion -- it assumed retrieval quality is invariant
across stores (Qdrant's inline metadata filtering vs. Pinecone's post-search filtering) without
ever measuring it. If quality genuinely doesn't differ across stores on this project's real
corpus, that itself is the finding -- it means the theoretical architecture argument (filter-
during-search vs. filter-after) was engineering noise, not a real quality driver, and that's
worth knowing rather than assuming. This is the same "measure, don't guess" discipline already
applied to LLM-in-ingestion, the odds backfill, and the embedding model choice -- a falsifiable
hypothesis about this project's own corpus, not a benchmark borrowed from someone else's.

**Decision: build all 3 (Pinecone, Qdrant, LanceDB) eventually, but SEQUENCED, not in parallel
from day one -- resolved 2026-08-20, real asymmetry with the chunking/embedding axes.** Ship with
ONE store now (get to Day 5/6 faster) and clone the already-computed vectors into the other 2
stores later, during Day 6 itself -- not a 4th combinatorial axis multiplying build cost like
paragraph-vs-whole-article chunking does. The two situations are NOT the same shape: chunking
happens BEFORE embedding, so changing it means re-processing raw text and re-running the
(expensive) embedding pass -- no shortcut, must run the whole pipeline again per variant. Vector
store is only the DESTINATION for vectors that already exist once the 6 corpus indices (2
chunking x 3 embedding models) are built -- moving/cloning them into a different backend is an
infra operation (read vectors+metadata, insert into another store), not a recompute. The
expensive work (embedding) is paid once and never repeated across stores.

**Practical sequencing:** pick one store now to unblock Day 4 block F and reach Day 5/6 sooner;
in Day 6, clone the 6 already-vectorized indices into the other 2 store backends to run the same
retrieval-quality comparison described above, without re-embedding anything.

**Revisit when:** a starting store is picked (see the LanceDB/Qdrant leaning discussion above) --
this sequencing note removes the earlier open question about whether the store axis explodes the
combinatorial count; it doesn't, because it's a post-hoc clone, not a build-time multiplier.

**Pinecone Starter plan limits, confirmed 2026-08-20 (account created, real pricing page):**
- **Up to 5 indexes** -- directly relevant, since the project already committed to 6 corpus
  indices (2 chunking methods x 3 embedding models, see the Day 6 combinatorial entry above).
  6 > 5 on the Starter plan. Likely mitigation, not yet designed: use **namespaces** instead of
  separate indexes (Starter allows up to 100 namespaces per index, free) -- i.e. 1 Pinecone
  index with 6 namespaces (one per corpus variant) rather than 6 separate indexes. Changes how
  the embedding Lambdas need to address Pinecone (namespace parameter per write/query, not a
  different index endpoint), not yet reflected in the Lambda architecture design above.
- **Write Units: up to 2M/month, Read Units: up to 1M/month.** Real formula confirmed 2026-08-20
  (Pinecone's own docs): 1 WU per 1 KB of the full upsert payload (vector + ID + metadata), min 5
  WU per request -- NOT per-vector or per-token, per KB of payload size. Rough estimate using
  this project's real corpus numbers (News: 3,315 articles, mediana ~4,000 chars -> ~5-8
  paragraphs each -> ~20-26K paragraph chunks vs. ~3,315 whole-article chunks; registry: ~767
  markets; Comments/Digest: low hundreds): ~7 KB/chunk (1536-dim Titan vector ~6KB + metadata) ->
  roughly 21K chunks x ~7 WU for each paragraph-chunked variant, ~4.3K chunks x ~7 WU for each
  whole-article variant, summed across all 6 corpus variants -> **~530K WU estimated total for
  the full bootstrap, well under the 2M/month ceiling (~4x headroom).** This is a rough estimate
  with rounded numbers, not a measurement -- before trusting it for the real production
  bootstrap, still worth an empirical dry run (a small real batch, check Pinecone's Console
  Metrics dashboard) to confirm actual payload size with this project's real metadata, same
  discipline already used for `scripts/backfill_odds_history.py`'s dry-run-before-write pattern.
- Storage capped at 2GB, 1 project, up to 2 users -- not a blocker for a solo project at current
  corpus size, noted for completeness only.

**Revisit before running the bootstrap:** run a small empirical batch to confirm the ~7KB/chunk
estimate against Pinecone's real Console Metrics before the full 6-variant bootstrap, and decide
namespace-vs-index addressing before the embedding Lambdas are actually coded (affects their
Pinecone client calls directly).

**Clarified 2026-08-20: the 5-index-limit workaround is per-store, not a universal pattern to
copy across Pinecone/Qdrant/LanceDB.** Each store organizes multi-tenancy differently -- this is
a decision that lives inside each of the 3 embedding Lambdas' own store-client calls, not a
shared architecture concept:
- **Pinecone:** namespaces (as above) -- 1 index, 6 namespaces (`1a_titan`, `1a_cohere`,
  `1a_voyage`, `1b_titan`, `1b_cohere`, `1b_voyage`).
- **Qdrant:** the conceptual equivalent is **collections**, not namespaces -- either 1 collection
  per variant, or 1 collection with a `variant` metadata field to filter. Whether Qdrant Cloud's
  free tier caps the number of collections has NOT been checked -- needs verifying when Qdrant
  integration is actually coded, not assumed to work like Pinecone's namespace model.
- **LanceDB:** no account-level limit applies (it's an embedded library writing to S3, not a
  multi-tenant cloud service) -- likely just 6 separate Lance tables/files in S3, constrained
  only by S3 cost at this scale, not a hard cap like Pinecone/Qdrant.

**Revisit when:** Qdrant and LanceDB integrations are actually coded (Day 6 store cloning) --
confirm Qdrant's collection limits at that point, don't assume the Pinecone namespace pattern
transfers as-is.

---

### CORRECTION 2026-08-21 -- decided on measured growth cost, not the filtering theory above

The reasoning above (inline-filter-during-search vs post-search filtering) is real and still
the right axis for a Day 6 retrieval-quality comparison, but it was NOT what decided today's
choice. The user asked the sharper question directly: "imagina que tan grande sera mi corpus
en 2-3 meses" -- forcing a real growth projection instead of reasoning from today's ~8K
vectors.

**Measured, not estimated (2026-08-21):** the `news_article` variant grows at ~700
articles/cycle x 2 cycles/day (real rate, all 12 cycles). At the real per-record size (20.8 KB
per vector record including metadata, 6.4 KB per chunk including text -- both measured from
actual S3 objects, not assumed):

| Horizon | Corpus size (one variant) |
|---|---|
| +1 month | 1.40 GB |
| +3 months | 3.74 GB |
| +12 months | 14.27 GB |

**Qdrant's free tier (1GB) is exhausted in under 2 months. Pinecone's (2GB) in about 3.** Both
would force either a paid tier (breaking the $5/month budget) or active pruning within the
project's stated near-term horizon. LanceDB has no free-tier ceiling to exhaust -- it is not a
managed service, it is a columnar file format written directly to the S3 bucket the project
already pays for. Real S3 cost at the same horizon: **$0.09/month at 3 months, $0.33/month at
12 months.** No new account, no new secret -- confirmed live, `lancedb.connect("s3://...")`
authenticates via the same AWS credentials already used for S3/DynamoDB/Bedrock.

**A 6-way tournament (article/paragraph x LanceDB/Pinecone/Qdrant) was considered for
TODAY and explicitly rejected**, after the user asked directly whether skipping it was
self-sabotage. Resolved: the tournament answers "which store retrieves better," a Day 6
question that needs a real query interface and eval metrics (still undecided, see "RAG
Evaluation Metrics Landscape" above) to produce a trustworthy number. Running it today would
have measured infrastructure with no yardstick. The store axis was already decided by a
different, harder constraint (will it still be free in 3 months) that doesn't need a
tournament to answer.

**Packaging risk verified before committing, not assumed:** `pip download lancedb
--platform manylinux2014_x86_64 --python-version 3.12`, fully unpacked and measured (not just
downloaded) = **339 MB unzipped**, over Lambda's 250MB zip/Layer limit (pyarrow 136MB +
lancedb's native binary 132MB + numpy 31MB dominate; no pandas dependency, which helped). This
rules out a plain zip deploy AND a Lambda Layer (Layers share the same 250MB cap as function
code) for the Fase 2/3 Lambdas due Monday. **Only a container-image Lambda (10GB limit) works.**
The user confirmed comfort with container images before this was locked in -- real added setup
cost for Monday (Dockerfile, ECR repo, build/push step), not a blocker.

**Fase 3 one-off (`scripts/write_to_lancedb.py`) built and verified same day** against the
Friday `news_article` slice -- table created at `s3://poly-rag-369970405415/lancedb/`, reopened
from a fresh connection (not memory), vector search and `market_id` metadata filtering both
confirmed semantically correct on real data. See session_ledger.md 2026-08-21 for the two real
bugs found while verifying it (`list_tables()` returning a response object rather than a plain
list, and `merge_insert` correctly rejecting the duplicate-chunk_id case documented in "Duplicate
Article URLs Within a Single Cycle" below).

**Revisit when:** Day 6 store-cloning work begins -- Qdrant and Pinecone remain the comparison
targets for retrieval quality (a small corpus sample fits either free tier fine for a
quality test; the growth-cost argument above only rules them out as the PERMANENT store, not
as a valid comparison target).

---

## Orphan Comments From the 2026-08-17 Registry Cleanup -- Found and Purged (2026-08-20)

**Issue:** while building the Day 4 block F chunking bootstrap (`scripts/bootstrap_chunk_corpus.py`),
found 3,080 comments across 4 cycle files (`comments/2026-08-16/01.json`, `2026-08-16/12.json`,
`2026-08-17/00.json`, `2026-08-17/12.json`) referencing `market_id`s that no longer exist in the
registry at all -- 22 distinct market_ids for `direct` comments, 38 for `shared_event`/
`shared_series`, 60 total. Root cause: these 4 cycles ran BEFORE the 2026-08-17 registry cleanup
(see "limpieza mayor del registry" in architecture_canon.md) that purged 329 pre-redesign markets --
that cleanup was explicitly scoped to registry + odds, and never touched `comments/*.json` (per its
own stated scope: "esos payloads son historial de CICLO, no datos de registry/odds ligados a un
market_id"). The comment data was correct for its own cycle at ingestion time; it only became
orphaned once the markets it references were purged days later. Same underlying pattern as the
already-documented `unknown_market` finding for News (see "Known Limitation: Explicit ID-Linkage"),
but a real gap in the FIRST investigation pass: checking only `comment_group_key`'s
`shared_event`/`shared_series` path found 1,337 orphan comments (38 markets) and initially missed
that `direct` comments can be orphaned too (`direct` groups by its own market_id without ever
checking the registry) -- a second, correct check (`is_orphan`, validating every link_type against
the real registry) found the true total of 3,080 (60 markets). Caught and corrected before deciding
anything, not after.

**Decision, different from `unknown_market`'s "leave it, revisit later":** the user explicitly
chose to purge these now, not defer -- unlike an orphaned News article (which still has real
standalone content even without a resolvable market), an orphan comment's only purpose in this
corpus is grouping by market/entity for the Comments chunking design (`chunk_comments` in
`bootstrap_chunk_corpus.py`), and it has none once its market_id is gone. It was actively getting
in the way of that design, not just inert legacy data.

**Purged via `scripts/purge_orphan_comments.py`** (new script, the only one in `scripts/` that
deletes data rather than only adding -- see `scripts/README.md`), dry-run verified before
`--apply`. Strictly scoped: only the 4 affected cycle files touched, only comments where EVERY
`market_id` fails to resolve removed (a comment with at least one still-valid market_id is kept
untouched), `comment_count` recomputed to match the real post-filter array length (avoiding the
count/array mismatch bug class already fixed once in `eda_mio_3`, see session_ledger.md
2026-08-17). Does NOT touch `poly-rag-processed-comments` (the dedup table) -- deliberately, so a
purged `comment_id` stays deduped forever rather than risking re-ingestion if it ever reappeared
from the live API. Result: 3,080 orphans removed, 8,858 legitimate comments preserved
byte-for-byte, verified 0 orphans remain across the ENTIRE comments corpus (all 10 cycle files,
not just the 4 affected) after the purge.

**Revisit if:** a future registry cleanup again purges markets without also checking Comments for
newly-orphaned references -- this was a one-time catch-up, not a recurring maintenance task with
its own schedule.

---

## Phase 2 Embedding Bootstrap BLOCKED -- No Model Has Completed a Corpus (2026-08-20/21, RESOLVED 2026-08-22 -- TWO unrelated root causes, see both corrections at the end)

> **STATUS 2026-08-22: FULLY UNBLOCKED, after TWO separate root causes on two different days.**
> (1) The daily quota blamed below does not exist in this account -- the account was never near
> its real daily ceiling; the actual cause was unbounded request size against the per-MINUTE
> limit. (2) A second, genuinely different daily-quota block hit the next session: the bare
> on-demand model id draws against its own separate undocumented daily ceiling, distinct from the
> "Global cross-region" quota this project had been checking. All 13 cycles of `news_article` are
> now fully embedded and verified (9,229/9,229 chunk_ids, 0 missing). The original text is
> preserved unedited for the record; read BOTH corrections at the bottom of this entry before
> acting on anything above it.

**Issue:** step 1 of the Phase 2 bootstrap (chunking) is done and verified in S3 -- 5 source files
under `chunks/`, refreshed to include the 2026-08-21 00:00 UTC cycle (registry 981, news_paragraph
120,568, news_article 7,429, comments 681, digest 11). Step 2 (embedding those chunks into vectors)
is **not done and currently blocked**: after a full session of attempts, `vectors/` holds exactly
one partial checkpoint (`news_article_variant/cohere/part_00000.json`, 2,000 of 9,102 vectors) and
zero finalized files. Every candidate model hit a real, verified account-level limit:

| Model | Status | Real blocker (verified, not assumed) |
|---|---|---|
| Titan v2 | **dropped from the project** | No batch API at all (`{"inputText": "<one string>"}`); `inferenceTypesSupported=[ON_DEMAND]`, no BATCH, so async Batch Inference doesn't apply either. Account quota 600 req/min (`aws service-quotas`), saturated immediately -- ~3.5h projected for the paragraph corpus, and that ceiling is permanent, so it would bottleneck every future incremental cycle too, not just this bootstrap. |
| Voyage (`voyage-finance-2`) | corpus completed, then **data lost** | Completed 122,241 vectors in 62 checkpoints in ~30 min with zero throttling -- the only model that actually worked. Its checkpoints were then deleted by mistake (see session_ledger 2026-08-20). Separately: real dashboard reading showed **28,263,931 tokens = 56.5% of the 50M free tier consumed by that single run**, projecting free-tier exhaustion in ~7 cycles (~4 days) at current ingestion rate. |
| Cohere v4 | **blocked, unresolved** | First `--apply` died at 39s with `ThrottlingException` ("Too many tokens"). Exponential backoff+jitter (`COHERE_MAX_RETRIES=8`) did not help; a fixed-pacing model (`COHERE_PACE_SECONDS=10`, sleep before each request rather than reacting to throttles) did not help either. CloudWatch measured **~15-20 throttles/min sustained against 1-2 successful invocations/min** under both schemes. |

**Leading hypothesis for the Cohere block, NOT yet verified:** a **daily** quota, not the
per-minute one. `Model invocation max tokens per day for Cohere Embed V4` = **8,100,000 tokens/day**
(`aws service-quotas`), and `InputTokenCount` on CloudWatch showed **5,261,503 tokens already
consumed that same UTC day (65%)** before the last attempt. The aggressive throttling may be AWS
tightening as the account approaches its daily ceiling, which would explain why neither backoff nor
pacing (both of which only address the 150K tokens/min limit) changed anything. **First thing to
check next session:** the daily counter resets at 00:00 UTC -- re-run then and watch whether the
throttle rate collapses. If it does, the fix is scheduling/pacing against the DAILY budget, not the
per-minute one.

**Real lateral finding, already applied to the script:** running multiple models concurrently inside
one process made everything worse -- a single module-level `boto3.client("bedrock-runtime")` shared
across ~44 threads saturates urllib3's default ~10-connection pool, so models queue behind each
other. Cohere measured ~1.13s/request when run alone in an isolated test vs. ~1.67 invocations/min
when sharing the process with Titan and Voyage. **Run one model at a time, sequentially** -- this is
also more consistent with the failure-isolation principle already used across the Phase 1 Lambdas.

**Mitigation already built (keep it):** `scripts/bootstrap_embed_corpus.py` writes a checkpoint to
`vectors/_checkpoints/<variant>/<model>/part_NNNNN.json` every `CHECKPOINT_SIZE=2000` chunks and
resumes by skipping parts already present in S3. This was added after two runs died having written
nothing at all, and it is the only reason the partial Cohere progress survived at all. Do not remove
it when reworking the embedding path.

**Open decision, not made:** the user raised **Vertex AI (GCP, `text-embedding-004`)** as an
alternative -- real pricing confirmed at $0.10/M tokens, which works out to ~$3.04 for a one-time
full-corpus bootstrap but ~$15.88/month in steady-state production, breaking the project's $5/month
budget (CLAUDE.md). The user also argued persuasively that using GCP here would NOT violate
CLAUDE.md's "Out of Scope: GCP" rule in spirit: that rule exists to stop the project from retreating
to a familiar cloud instead of learning AWS, whereas this would be adopting a specific tool *because*
real AWS limits were discovered by doing the work in AWS. That reframing is accepted as reasonable;
the cost question is what remains unresolved.

**Also still open (pre-existing, now more urgent):** no spend/usage guardrail exists for any non-AWS
embedding vendor. See "Voyage AI Free Tier Spend Alert" above -- the same gap applies to whatever
model is chosen next, and today's session demonstrated concretely how fast a free tier can be
consumed without one.

**Revisit:** next session, starting with the daily-quota reset check described above.

---

### SECOND CORRECTION 2026-08-22 -- a genuinely different daily-quota block hit hours later

The "UNBLOCKED" banner at the top of this entry was accurate for its own root cause (unbounded
request size against the 300K TPM per-minute limit) but was written before a SECOND, unrelated
daily-quota block appeared during an aggressive same-day bootstrap of the full 13-cycle corpus
(registry + comments + digest + news_article, see session_ledger.md 2026-08-22 UTC / 2026-08-21
local). Named separately here because the mechanism is genuinely different, not a recurrence of
the first bug:

**What happened:** `news_article` embedding died again at 60.2% (6,000+ of 9,235 chunks) with
`ThrottlingException: Too many tokens per day, please wait before trying again` -- exhausting all
6 retries. CloudWatch showed only 38% of the documented 16.2M daily quota consumed, not remotely
close to a ceiling. A minimal 4-token test call against the bare model id confirmed the block was
TOTAL and unconditional (not a batch-size issue): even the smallest possible request failed with
the identical message.

**Root cause: the 16.2M quota this project has been checking is titled "Global CROSS-REGION model
inference tokens per day for Cohere Embed V4" -- it never applied to the bare on-demand model id
(`cohere.embed-v4:0`, no inference-profile prefix) that `embed_corpus_slice.py` had been using all
along.** On-demand calls apparently draw against a separate, lower, UNDOCUMENTED daily ceiling that
`aws service-quotas list-service-quotas` does not surface under any name matching "Cohere" or
"Embed". This is the same class of mistake as the first block in this entry (trusting a quota name
without confirming it actually governs the calls being made), but a different specific quota.

**Fix, found by testing rather than guessing:** switched `MODEL_ID` to the cross-region inference
profile `us.cohere.embed-v4:0` -- worked immediately on a live test call. Verified byte-identical
embeddings against the bare model id first (cosine similarity 1.0, max absolute difference 0.0
across all 1536 dimensions on the same input text) before trusting the switch -- same underlying
model, different routing path only, safe to mix vectors already written under one model id with new
ones under another.

**Second surprise: `us.cohere.embed-v4:0` ALSO hit the identical error minutes later.** At the exact
moment it failed, `global.cohere.embed-v4:0` was tested and succeeded. This proves the three routing
paths -- bare on-demand, `us.` cross-region, `global.` cross-region -- draw against INDEPENDENT
daily counters, not one shared account-level Cohere Embed V4 limit as first assumed after the
`us.` fix appeared to work. Switched to `global.cohere.embed-v4:0`, which completed the remaining
2,989 chunks cleanly: 0 throttles, 35.2 minutes, 115K tokens/min effective rate.

**Practical guidance left in `embed_corpus_slice.py` itself** (not just here): if this recurs, the
other two model ids are the fallback, in whichever order still has headroom -- check with a single
4-token test call before relaunching the real batch rather than assuming which route is free. No
`Retry-After` header is ever returned by any of the three, so there is no way to know a ceiling has
lifted except by testing.

**Not investigated:** what the actual on-demand/per-route daily ceiling number is, whether it
resets on the same 00:00 UTC boundary as the documented 16.2M cross-region quota, or whether AWS
Support has an official name for it. `list-service-quotas` does not expose it under any queried
term. Worth asking AWS Support directly if this blocks a future bootstrap again, rather than
re-diagnosing empirically each time.

**Revisit if:** a future embedding run hits an unexplained daily-quota-style throttle again --
check whether all three Cohere Embed v4 routing paths (bare, `us.`, `global.`) are exhausted before
assuming the whole model is blocked, and budget time for empirical testing since no quota name or
Retry-After header will confirm it directly.

---

### CORRECTION 2026-08-21 -- the diagnosis above was wrong, and the bootstrap is unblocked

The daily-quota hypothesis was checked first thing, as the entry above instructed, and it
**did not survive contact with the real account**:

- **The quota named above does not exist.** `Model invocation max tokens per day for
  Cohere Embed V4 = 8,100,000` is not in this account's quota list (verified by
  enumerating every `per day` quota via `aws service-quotas list-service-quotas
  --service-code bedrock`). The only Cohere Embed V4 daily quota is **`Global
  cross-region model inference tokens per day` = 16,200,000** -- exactly double the
  figure that was cited.
- **Consumption was therefore misread by 2x.** The 5,261,503 tokens measured that day
  were **37% of the real 16.2M ceiling, not 65%** of an 8.1M one. There was ~10M of
  daily headroom left at the moment the runs were being throttled to death.

**The real cause: unbounded request size against the per-MINUTE limit.** The binding
quota is `On-demand model inference tokens per minute for Cohere Embed English` =
**300,000 TPM (not adjustable)**. The old script batched by COUNT (96 texts/request,
Cohere's documented max) over a corpus whose article chunks range from ~200 chars to
240,387 chars -- a ~1,200x spread. A single 96-text request of large articles can carry
millions of tokens and exhaust a whole minute's budget in one call. This is why neither
exponential backoff nor `COHERE_PACE_SECONDS=10` ever helped: **both control request
RATE, and the limit being violated was tokens per minute.** No amount of waiting between
oversized requests makes an oversized request fit.

**Fixes, in the order they mattered:**
1. **Token-aware batching** -- fill each request to a token budget
   (`MAX_TOKENS_PER_REQUEST = 40,000`), never to a text count. Count is now only the
   secondary cap (Cohere's own 96-text API limit).
2. **Overflow-split oversized chunks** (`MAX_ARTICLE_CHARS = 32,000` in
   `bootstrap_chunk_corpus.py`) -- articles over the cap split into linked parts rather
   than being truncated, so no single chunk can dominate a request. Only 184/8,201
   articles (2.24%) are affected; the whole-article variant stays whole for 97.76% of
   the corpus.
3. **A sliding-window rate governor** that tracks tokens actually sent in the trailing
   60s and sleeps before breaching the target -- necessary because Bedrock's embed
   response carries **no `meta.billed_units`** field (verified against a real call), so
   there is no server-side token count to read back and pacing must be computed locally.

**Empirically-set pacing, measured not guessed:**

| TPM target | Throttles | Effective rate | Result |
|---|---|---|---|
| 210,000 (0.70 of limit) | 93 | ~168K/min | usable but wasteful; crashed on an unrelated bug |
| **150,000 (0.50 of limit)** | **0** | 119K/min | clean |

The reason 0.70 still throttles despite real consumption never exceeding ~215K/min
against a 300K ceiling: **Bedrock enforces over a window shorter than 60 seconds**, so a
governor that is correct on a 60-second average can still burst past the real limiter.
The chars/4 token estimate was independently confirmed accurate (CloudWatch counted
663,197 real tokens against ~700,000 estimated, ratio 0.95 -- it slightly OVER-counts,
so it is not the source of the throttles). **Use 0.50 for future slices; do not exceed
0.70.**

**Two of my own bugs, both found against real data rather than in review:**
- **A transient Bedrock HTTP 500 killed a run at 52.5%** because retry logic only
  matched throttling codes. Now retries any 5xx/429, matching on HTTP status as well as
  error code -- Bedrock returned the literal string `"500"` as the code, with no
  symbolic name to match on.
- **Resume-by-position was silently unsafe.** It skipped chunks by index, which only
  works if batch boundaries are identical between runs -- and they are not, since
  batching is token-driven and `MAX_TOKENS_PER_REQUEST` changed mid-effort (90K -> 40K),
  re-cutting every boundary. Rewritten to resume by `chunk_id` identity, which is
  immune to batch size, chunk order, and part count.

**Checkpointing earned its keep again** -- the 500-crash cost only the un-checkpointed
tail (~2.27M tokens survived in 3 parts). The instruction above to keep it stands.

**Result:** `news_article` cycles 1-5 fully embedded and verified -- 2,746 vectors, 6
checkpoint parts, ~4.06M tokens, 0 missing/extra ids, 1536-dim uniform, L2 norms exactly
1.0000, no zero/NaN/Inf vectors, and a real semantic sanity check (nearest neighbours of
random chunks are genuinely same-topic). Titan and Voyage remain out of the project for
the reasons already documented above -- neither was revived to reach this result.

**Superseded by:** the 4-day sliced bootstrap plan (see session_ledger.md 2026-08-21).
The remaining open question is no longer "can any model embed this corpus" -- it can --
but the Phase 3 vector store choice, still undecided (see "Vector Store Choice" above).

---

## Duplicate Article URLs Within a Single Cycle (2026-08-21, OPEN, low severity)

**Issue:** two article URLs appear **twice within the same News cycle payload**, each
tagged with a different `market_id` but carrying byte-identical text:

| URL | Cycle | market_ids | chars |
|---|---|---|---|
| `stealthex.io/blog/when-will-bitcoin-recover/` | 3 (`news/2026-08-17/00.json`) | 3257332, 3257338 | 14,319 |
| `federalnewsnetwork.com/prediction-markets/2026/02/democratic-nominee-odds-after-state-of-the-union/` | 5 (`news/2026-08-18/00.json`) | 559657, 559675 | 8,001 |

This contradicts the invariant stated in architecture_canon.md that News dedupes by exact
URL via `poly-rag-processed-urls`. The dedup table is not wrong -- it just cannot prevent
this specific race: `ingest_news` fans out ~20 batches that run **concurrently**, and two
batches searching two different markets can both fetch the same URL before either one
writes its dedup marker. The markets in each pair have adjacent ids (3257332/3257338,
559657/559675), i.e. sibling markets of the same event, which is exactly the case most
likely to return identical Google News results.

**Why it matters, concretely:** the chunking design uses the article `url` as
`article_id` and, for the whole-article variant, as `chunk_id` (decided 2026-08-20
precisely because the URL "ya es unica por diseno"). It is not unique in practice. On
write to a vector store keyed by id, the second record **silently overwrites** the first,
and the surviving vector keeps only ONE of the two `market_id`s -- so one market loses
its link to an article that genuinely belongs to it. Silent, not an error.

**Scale:** 2 of 2,746 chunks in cycles 1-5 (0.07%). Real but not urgent.

**Deliberately NOT fixed 2026-08-21** -- found mid-run while verifying the embedding
resume logic, and fixing it would have meant re-chunking and re-embedding a slice that
was already half-paid-for. Recorded instead of patched under time pressure.

**Two candidate fixes, not chosen:**
1. **Upstream (preferred):** make `ingest_news` collapse same-URL articles within a
   cycle merge, unioning their `market_ids` into one record -- which is what the schema
   already supports (`market_ids` is a list) and would make the corpus honest about an
   article belonging to two markets.
2. **Downstream (weaker):** qualify `chunk_id` with `market_id` for the article variant,
   which stops the overwrite but duplicates the vector and re-embeds identical text.

**Revisit when:** `ingest_news` is next touched, or when Phase 3 store-writing is built
(whichever comes first) -- the overwrite only becomes real damage at store-write time,
so Phase 3 is the deadline.

**Update 2026-08-21 (same day, hours later) -- hit for real in Phase 3, and the predicted
failure mode was WRONG in one detail.** `scripts/write_to_lancedb.py` reached this exact
case on its first `merge_insert` and did NOT silently overwrite as predicted above --
LanceDB raised a hard error instead: "Ambiguous merge inserts are prohibited: multiple
source rows match the same target row." The prediction of silent data loss was right in
spirit (one market does lose its link either way) but wrong on mechanism -- this specific
store fails loudly on the ambiguity rather than picking a winner quietly. Worked around
with an explicit, logged dedup-by-chunk_id step in the write script (keeps the last
occurrence, reports the count) as a stopgap -- the upstream fix in `ingest_news` above is
still the real fix and is still not done. Whether Pinecone or Qdrant would also reject the
ambiguous write outright, or silently overwrite as originally assumed, is unverified and
worth checking when either is coded in Day 6 -- do not assume LanceDB's fail-loud
behavior generalizes to the other two stores.

---

## Phase 2's First Real Production Cycle Surfaced Two Real Bugs (2026-08-22, both CLOSED same day)

**Context:** cycle 14 (2026-08-22 12:00 UTC) was the first time the full Phase 2 chain
(`embed_orchestrator` -> 4 chunking Lambdas in parallel -> 4 embedding Lambdas in
sequence) ran automatically in production, with no manual invocation. It failed in two
independent, silent ways, verified entirely via CloudWatch Logs / S3 / DynamoDB reads --
no Lambda was manually invoked to diagnose or fix either one.

**Bug 1 -- IAM region scope too narrow for the `global.` cross-region profile.**
`embed_digest` threw `AccessDeniedException` on its first real `bedrock:InvokeModel`
call. The policy (`terraform/iam_embed_lambda.tf`) granted the `foundation-model/
cohere.embed-v4:0` resource ARN in exactly 3 US regions (us-east-1/2, us-west-2) --
enough for the `us.` cross-region profile, but the `global.` profile (chosen precisely
because it survived two earlier daily-quota outages, see "Phase 2 Embedding Bootstrap"
above) can route outside those 3 regions. Since `embed_digest` is the first Lambda in
the strict sequential chain, its failure meant `embed_comments`/`embed_registry`/
`embed_news_article` never ran at all -- confirmed via `aws logs describe-log-groups`
showing zero log streams ever for those three. **Fix:** region-wildcarded the
`foundation-model` resource ARN (`arn:aws:bedrock:*::foundation-model/
cohere.embed-v4:0` -- safe because these ARNs never carry an account id). Verified
live by importing `embed_digest/handler.py` and calling Bedrock directly against a real
cycle 14 chunk, no Lambda invoked.

**Bug 2 -- `chunk_registry`'s comparison used `>` when the data model guarantees
equality, not strict inequality.** The user asked directly why `chunk_registry`
reported 0 new markets for cycle 14 when `send_digest`'s own digest already reported
`newly_tracked_markets: 25` for the same cycle -- a real cross-check against a
number Phase 1 had already computed independently, not a guess. Root cause:
`ingest_polymarket` computes a single `now_iso` once at the top of its handler and
reuses that exact same value both as `first_seen` on every market it upserts that
cycle AND as `cycle_started_at` threaded through the entire chain (`invoke_next_stage
(now_iso)`). So `first_seen` is never strictly greater than `cycle_started_at` for a
market that entered this cycle -- it is exactly equal. `chunk_registry`'s filter
(`first_seen > cycle_started_at`) silently returned 0 markets **every cycle since the
Lambda was written**, not just on cycle 14 -- this was not a cycle-14-specific
regression, it was latent from the design correction earlier the same day (see
"registry sin eje de ciclo real" in architecture_canon.md). **Fix:** `>=` instead of
`>` in `scan_new_registry_items` (`lambdas/chunk_registry/handler.py`), both
occurrences. Verified with a dry run against the real cycle 14 `cycle_started_at`
(25 markets found, matching `send_digest`'s count exactly), then re-run for real
(`chunk_registry` + `embed_registry`, `skip_chain: true`, no Lambda invoked) to close
cycle 14's registry gap -- 25 chunks, 25 vectors, verified zero duplicates in S3.

**Mitigation, both bugs:** fixed same day, deployed via `terraform apply` (user-run,
per the auto-mode classifier blocking `terraform apply` from this environment). A new
runbook, `.claude/claude_docs/runbook_verify_phase2_health.md`, was written the same
day specifically encoding checks that would have caught both bugs without depending on
"did the report email arrive" -- Paso 1 cross-checks `chunk_registry` output against
`send_digest`'s `newly_tracked_markets` (bug 2), Paso 3 sweeps CloudWatch for errors
across all 8 Phase 2 Lambdas (bug 1).

**Also found, not fixed by choice (user declined the anotation, recorded here only
because it is a distinct, real finding, not swept under the registry fix above):** the
first attempt at the cycle 14 one-off was interrupted by a tool permission prompt
after it had already done real embedding work against Bedrock (roughly 313 of
news_article's 603 chunks) but before it reached its first checkpoint (every 500
chunks). That work was never persisted, so the final successful run re-embedded those
same chunks from scratch -- confirmed via duplicate `chunk_count` sequences in
`poly-rag-embedding-metrics` (the exact same batch-size sequence appearing twice back
to back). Final data is correct (603 unique vectors, zero duplicates in the S3
checkpoint), but real Bedrock cost was paid twice for that slice. Not a code bug --
an interaction between interrupted-tool-call handling and the 500-chunk checkpoint
interval. No action taken per explicit user request.

**Revisit if:** a future cycle's Phase 2 healthcheck (once `runbook_verify_
phase2_health.md` gets its own first real run) finds either bug's symptom again --
would indicate a regression, not a known gap.

---

## Phase 3 (write_to_lancedb.py) Closed for All 4 Sources, Full 14-Cycle Corpus (2026-08-22)

**Context:** same day as the two Phase 2 bugs above, `write_to_lancedb.py` was run for
the first time against the real full corpus, not just the Friday 5-cycle news_article
slice it had been verified against before. Scope was all 4 embedded sources (registry,
comments, digest, news_article), each in 2 slices (the multi-cycle bootstrap/
cycles_01-13 file, then cycle 14's own per-cycle file) -- 8 dry runs, all clean, then 8
real writes.

**Three real bugs found and fixed in the script itself, none in production Lambdas:**

1. `tbl.create_index(metric="cosine")` used LanceDB's default
   `vector_column_name="vector"`, but this project's column is `embedding` --
   every index build failed with `Schema Error: Field path 'vector' not found`
   right after a successful data write (data was never lost, only the index step
   failed). Fixed with `vector_column_name="embedding"` explicit.
2. `_lineage` (added earlier the same day to `chunk_*` Lambda output, see the entry
   above) rides into the vector checkpoint record via `embed_*`'s
   `{k: v for k, v in chunk.items() if k != "text"}`, so any chunk written after
   that fix carries it into the vector too. Writing cycle 14's registry vectors
   into a table first created from the pre-fix bootstrap slice (no `_lineage`
   column) failed: `ValueError: Field '_lineage' not found in target schema`.
3. Same failure mode, different field: the older `bootstrap_chunk_corpus.py`
   tagged comment chunks with `cycle_key`/`cycle_number`, while the newer
   `chunk_comments` Lambda uses `cycle_started_at` instead. Writing cycle 14's
   comments into the bootstrap-created `comments_cohere` table failed:
   `ValueError: Field 'cycle_started_at' not found in target schema`.

**Root cause, all three:** LanceDB's `merge_insert` tolerates a batch with FEWER
fields than the table's existing schema (missing fields get padded null), but
hard-fails on a batch introducing a field the schema doesn't already have. Since the
table's schema is locked in by whichever slice created it first, and this project's
chunk format evolved mid-corpus (bootstrap script vs. the newer per-cycle Lambdas),
any newer slice written after an older one risks introducing a field the older
slice never had.

**Fix:** drop `_lineage`, `cycle_key`, `cycle_number`, and `cycle_started_at` from
every row before writing, uniformly, regardless of which are actually present.
None of the four are part of the documented retrieval filter set (`market_id`/
`temporal_tier`/`market_status_at_publish`/`link_type`/`comment_entity_id`, see
architecture_canon.md), so dropping them costs nothing at query time and closes
this whole class of schema drift instead of patching it field-by-field as new
mismatches surface.

**Result, verified via `count_rows()` on each table after writing:** `registry_cohere`
1,115 rows, `comments_cohere` 786, `digest_cohere` 14, `news_article_cohere` 9,832 --
11,747 vectors total, all 4 embedded sources, all 14 cycles. Phase 3 is now closed for
all 4 sources (previously only `news_article`'s Friday 5-cycle slice, 2,744 rows, had
ever been written).

**Revisit if:** a future chunk format change (a 5th field renamed or added) breaks a
merge again -- the fix above is a fixed exclusion list, not a general schema
reconciliation, so it will need extending by hand each time the chunk schema drifts.

---

## Second LLM Pass ("Query the Cycle") Is the Day 5 Synthesis Agent, Not a New Idea (raised 2026-08-22)

**Context:** right after closing Fase 3 (write_lancedb), the user proposed a Lambda at
the very end of the automatic pipeline (after write_lancedb) that would be "the second
LLM pass" -- the first being the verifiability classifier at ingestion
(`ingest_polymarket`) -- this time letting the LLM answer questions about the just-
ingested cycle, similar to how retrieval will eventually work. The user was explicit
that the idea wasn't fully formed yet and asked directly whether RAG (Bloque G /
Day 4 retrieval) needs to exist first.

**Answer, and why it matters:** the scope of the question determines the dependency.
- **Cycle-scoped questions** ("what moved most this cycle") need nothing new --
  `send_digest`'s `executive_summary` already does this today, one LLM call over that
  cycle's own structured digest JSON, no retrieval involved.
- **Cross-cycle/cross-source questions** ("how has this market moved since it entered",
  "what did coverage say about X over the last 3 cycles") are exactly this project's
  real differentiator (the self-built historical time-series, see README "Why this
  exists") and genuinely require retrieval -- there is no way to assemble that context
  without a metadata+semantic index over the accumulated corpus.

**Conclusion:** this is not a new Lambda to design -- it is the Day 5 synthesis agent,
already tracked (see "Guardrails Against Unbounded Structured Queries", "RAG Evaluation
Metrics Landscape", and the LLM-as-internet-search-substitute entries above, all under
Day 5). Its dependency on Day 4 (RAG retrieval, Bloque G) was already the documented
order in README's Pending/TODO before this conversation -- this entry just records that
the user independently arrived at the same conclusion from a different angle (proposing
the feature, not reading the roadmap), which is a useful confirmation that the
sequencing is right, not a new decision.

**Revisit when:** Bloque G (retrieval) is built and Day 5 synthesis agent design
actually starts -- fold this framing (cycle-scoped vs. cross-cycle questions) into
that design instead of treating it as a separate feature.

---

## write_lancedb Timeout on Its First Real Automatic Cycle, Fixed Same Day (2026-08-23)

**Context:** cycle `2026-08-23T00:00 UTC` was the first automatic EventBridge cycle to
run the complete new chain (embed_digest -> ... -> embed_news_article ->
digest_metrics -> write_lancedb) end to end, after all of 2026-08-22's fixes were
deployed. Phase 1 and Phase 2 both ran clean (verified via both healthcheck runbooks,
0 errors across 8 Phase 2 log groups, chunk_registry's 66 new markets cross-checked
exact against send_digest's `newly_tracked_markets`). Phase 3 (`write_lancedb`) is
where a real, previously-undetected bug surfaced.

**Bug:** `write_lancedb` timed out at its 120s limit on all 3 attempts (the original
invocation plus Lambda's automatic 2 retries for async/Event invocations, all sharing
the same `RequestId` in CloudWatch -- confirmed via `REPORT ... Status: timeout` in
each attempt's log line). This produced a real, silent-looking failure: the healthcheck's
CloudWatch error sweep (`?ERROR ?Exception ?Traceback`) reported 0 errors for
`write_lancedb`, because a platform-level timeout doesn't match any of those patterns --
a real gap in `runbook_verify_phase2_health.md`'s Paso 3 pattern, found by separately
checking invocation counts (3, not the expected 1) rather than trusting "0 errors" alone.

**Root cause:** `load_vectors(source)` read every checkpoint part ever written for a
source (paginating the whole `vectors/_checkpoints/<source>/cohere/` prefix), not just
this cycle's -- fine for a manual one-off run once, but this Lambda runs every 12h
forever, and by this first real cycle `news_article` already had ~20 checkpoint files
and ~9,800 records. The read + join + `merge_insert` cost of the WHOLE history blew
through the timeout specifically on `news_article` (the largest and last source in the
loop) -- `registry`/`comments`/`digest` (much smaller) wrote successfully in the same
invocation before it hit the wall.

**Fix:** `load_vectors` now filters checkpoint files by S3 `LastModified >=
cycle_started_at` instead of reading everything. Safe by construction: checkpoints are
always written by an embed Lambda that runs strictly after `cycle_started_at` (same
threading as the rest of the chain), so this can never miss a checkpoint that belongs
to the current cycle. Read cost is now bounded by one cycle's worth of new chunks
(~1-2 checkpoint files at `CHECKPOINT_SIZE=500`), not by total corpus size -- stays
flat as the corpus grows, instead of degrading further every cycle.

**Verified before redeploying:** ran the fixed image locally via the Lambda RIE against
the real stuck cycle -- completed in 35s (vs. the 120s limit), and closed the day's
actual data gap in the same run (`news_article_cohere` 9,832 -> 10,580 rows, the exact
748 that never landed; `registry`/`comments`/`digest` re-merged idempotently, `before ==
after` confirming no duplication). Deployed via `docker buildx build --provenance=false
--sbom=false` (same manifest-format fix as the original build) + `aws lambda
update-function-code` -- not `terraform apply`, since the Lambda's `image_uri` is a
`:latest` tag in Terraform state and doesn't diff when the tag's underlying digest
changes; Terraform stays unaware of image content changes by design here.

**Also built the same day, prompted by this incident:** a third checkpoint email,
sent by `write_lancedb` itself (per-source status/before/after/written/missing table),
completing the "one email per phase" pattern (`send_digest` for Phase 1,
`digest_metrics` for Phase 1+2 cost, `write_lancedb` for Phase 3) -- lets a human tell
which phase succeeded or failed from the inbox alone, which is precisely what would
have made this specific incident visible without needing CloudWatch at all.

**Revisit if:** `runbook_verify_phase2_health.md` Paso 3 gets extended to catch
`Status: timeout` explicitly (it currently only catches `ERROR`/`Exception`/`Traceback`
patterns, which is what let this incident slip past a "0 errors" reading) -- not yet
done, tracked here.

---

## Vector Search Metric Mismatch Across LanceDB Tables (found and fixed 2026-08-29)

**Context:** building `retrieval/query.py` for Bloque G (Dia 4, G1 -- retrieval
puro), the first real cross-table search surfaced a suspicious pattern: distances
returned from `news_article_cohere` were consistently ~0.46-0.49, while the other 3
tables (`registry_cohere`, `comments_cohere`, `digest_cohere`) returned ~1.1-1.3 for
the same query. Initially misread as "news_article matches better semantically" --
the user caught this and asked for verification instead of accepting the
explanation at face value.

**Root cause:** the 4 LanceDB tables do NOT share a search metric. Per
`MIN_ROWS_FOR_INDEX = 5_000` (decided in `scripts/write_to_lancedb.py`, Fase 3,
2026-08-22), only `news_article_cohere` (the fastest-growing source, ~700
articles/cycle) has ever crossed that row threshold -- it has an IVF-PQ index built
with `metric="cosine"` explicit. The other 3 tables are all still under 5,000 rows
(1,838 / 1,405 / 27 respectively as of 2026-08-29), so they have no index and
LanceDB falls back to brute-force search using its DEFAULT metric, which is L2
(Euclidean), not cosine. `retrieval/query.py`'s `search()` never specified a metric
on any of the 4 `.search()` calls, so each table silently used whatever it had
available -- comparing 0.46 (cosine) against 1.2 (L2) was never a valid comparison,
regardless of what the numbers seemed to suggest about relevance.

**This is NOT a bug in the `MIN_ROWS_FOR_INDEX` threshold itself** -- that design
(index only above 5,000 rows, since IVF-PQ needs enough rows to form meaningful
partitions, brute-force is already exact and fast below that) is sound and was a
deliberate Fase 3 decision. The bug is specifically in code that CONSUMES these
tables without accounting for the asymmetry it creates: any cross-table search
that doesn't force a metric ends up comparing incomparable numbers.

**Fix:** `search_source()` in `retrieval/query.py` now calls
`.metric("cosine")` explicitly on every `.search()`, regardless of whether the
target table has an index -- LanceDB computes exact cosine in brute-force mode
too, just slower (acceptable at these row counts, same reasoning as
`MIN_ROWS_FOR_INDEX` itself). Verified: after the fix, all 4 tables returned
distances in the same 0-2 cosine range for the same test query (0.46-0.65),
`news_article` still closest but now a real comparison, not a metric artifact.

**Also found while implementing the `market_id` metadata filter (same session):**
the 4 tables do not share a `market_id` schema either -- `registry_cohere` and
`news_article_cohere` have a scalar `market_id` column (direct `=` filter works);
`digest_cohere` has `market_ids_mentioned`, a LIST column (needs
`list_contains(...)`, `=` would silently match 0 rows forever); `comments_cohere`
has no `market_id` column at all (links via `comment_entity_id`, a separate
lookup through the registry -- see architecture_canon.md, "Comments" section).
`search_source()` now branches per-source instead of applying one filter
uniformly, and explicitly returns `[]` (not an error, not silently ignoring the
filter) for `comments` when a `market_id` filter is requested, since that source
genuinely cannot answer that filter with its current schema.

**Also flagged, initially assumed unfixable without a full rebuild -- CORRECTED
2026-08-28:** `news_article_cohere`'s index was built once at 9,229 rows
(2026-08-22) and never touched again -- the table has since grown to 19,918 rows
via `merge_insert`, so 10,689 rows (>50%) are unindexed. Doesn't break correctness
(LanceDB searches indexed + unindexed together), but the index's speed benefit
erodes every cycle as the unindexed fraction grows -- more and more of each query
falls back to brute-force. The initial read (2026-08-28, in conversation, not yet
written anywhere at the time) was that IVF-PQ has no incremental option and
"reindex" always means a full from-scratch rebuild over the whole table, so
avoiding a per-cycle reindex was framed as the only real choice available.

**That framing was wrong -- verified directly against the installed LanceDB
Python API (`help(lancedb.table.LanceTable.optimize)`), not assumed:**
`tbl.optimize()` exists specifically for this, modeled after PostgreSQL's
`VACUUM`. Its docstring: "Index: Optimizes the indices, adding new data to
existing indices" -- it incorporates unindexed rows into the existing index
WITHOUT a full rebuild, plus compacts small files and prunes old dataset
versions. LanceDB's own guidance: run `optimize()` after ~100,000 added/modified
records or ~20+ data-modification operations. `write_lancedb` calls
`merge_insert` every cycle (a data-modification operation by that definition) but
never calls `optimize()` -- the growing unindexed fraction is a missing
maintenance step in this project's code, not an inherent limitation of IVF-PQ or
a necessary consequence of avoiding full reindexes.

**Real fix, not yet implemented:** add a periodic `tbl.optimize()` call --
either inside `write_lancedb` on some cadence (e.g. every N cycles, or when
`index_stats()`/`list_indices()` shows unindexed rows crossing a threshold), or
as a separate low-cadence job outside the per-cycle chain. Cost is bounded by
`optimize()`'s own incremental design (not the whole-table rebuild cost a full
`create_index()` would carry), so this does not reintroduce the "reindex cost
scales with table size" problem the original no-reindex-per-cycle decision was
avoiding.

**Revisit when:** any new Fase 4/Bloque G/Dia 5 code searches these tables --
confirm it also forces `.metric("cosine")` and respects the per-source
`market_id` schema differences documented above. Also revisit to actually
implement the `optimize()` cadence above -- currently just diagnosed, not fixed.
