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

**Cleanup reminder: delete `scripts/start_legacy_post_resolution_windows.py` on 2026-08-19**
(tomorrow, Mexico City date), once the next 1-2 real ingest_news cycles have run and consumed
all 93 legacy windows (4 cycles = 48h, so by the 2026-08-20 12:00 UTC cycle every one of the 93
counters will have reached 0 through normal operation). The script's own docstring already warns
against re-running it later with an updated/live market list -- once its one-time job is done,
the file itself is dead code, not a reusable tool, and should be removed rather than left as a
trap for a future session that might reach for it again against a different set of resolved
markets.

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

## PRIORIDAD 1 SIGUIENTE SESION: Bug de Doble-Disparo en el Fan-Out de News (confirmado 2026-08-18)

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
