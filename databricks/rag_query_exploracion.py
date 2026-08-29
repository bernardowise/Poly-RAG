# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # rag_query_exploracion (2026-08-29)
# MAGIC
# MAGIC Bloque G, item 5 of the day's agenda: interactive retrieval exploration from
# MAGIC Databricks -- same logic as `retrieval/query.py` (already tested locally in
# MAGIC the Codespace), ported here to confirm Databricks can call Bedrock + LanceDB
# MAGIC directly, without assuming it.
# MAGIC
# MAGIC No conversational LLM synthesis here (that's Day 5, see tech_debt.md "Second
# MAGIC LLM Pass") -- pure retrieval plus query rewriting (an LLM call used as a
# MAGIC structured filter extractor, same pattern already used by
# MAGIC `ingest_polymarket`'s verifiability filter -- not conversational synthesis).
# MAGIC
# MAGIC **How to use it:** run the setup cells, then change `PREGUNTA`/`QUESTION` in
# MAGIC the later sections and re-run those cells as many times as you want.

# COMMAND ----------

import boto3
import json

aws_access_key_id = dbutils.secrets.get(scope="poly-rag", key="aws_access_key_id")
aws_secret_access_key = dbutils.secrets.get(scope="poly-rag", key="aws_secret_access_key")

boto_session = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name="us-east-1",
)
bedrock = boto_session.client("bedrock-runtime")

S3_BUCKET = "poly-rag-369970405415"
LANCEDB_URI = f"s3://{S3_BUCKET}/lancedb/"
MODEL_LABEL = "cohere"
MODEL_ID = "global.cohere.embed-v4:0"
SOURCES = ["registry", "comments", "digest", "news_article"]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Connect to LanceDB (on S3) from Databricks
# MAGIC
# MAGIC lancedb doesn't come preinstalled in the runtime -- install below if it
# MAGIC throws ModuleNotFoundError. The boto3 credentials configured above are used
# MAGIC automatically for the S3 access lancedb needs.

# COMMAND ----------

# MAGIC %pip install lancedb==0.37.1 -q

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import boto3
import json
import lancedb

aws_access_key_id = dbutils.secrets.get(scope="poly-rag", key="aws_access_key_id")
aws_secret_access_key = dbutils.secrets.get(scope="poly-rag", key="aws_secret_access_key")
import os
os.environ["AWS_ACCESS_KEY_ID"] = aws_access_key_id
os.environ["AWS_SECRET_ACCESS_KEY"] = aws_secret_access_key
os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

boto_session = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name="us-east-1",
)
bedrock = boto_session.client("bedrock-runtime")

S3_BUCKET = "poly-rag-369970405415"
LANCEDB_URI = f"s3://{S3_BUCKET}/lancedb/"
MODEL_LABEL = "cohere"
MODEL_ID = "global.cohere.embed-v4:0"
SOURCES = ["registry", "comments", "digest", "news_article"]

db = lancedb.connect(LANCEDB_URI)
print("available tables:", db.list_tables().tables)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Retrieval functions -- same design as retrieval/query.py
# MAGIC
# MAGIC Important notes (already learned from real bugs, see tech_debt.md):
# MAGIC - `input_type=search_query` (not `search_document`, that's what the corpus
# MAGIC   itself uses) -- Cohere's asymmetric mode.
# MAGIC - `.metric("cosine")` explicit ALWAYS -- without this, tables with an index
# MAGIC   (cosine) and without one (default L2) return distances on incomparable
# MAGIC   scales.
# MAGIC - the `market_id` filter respects each source's real schema: `registry`/
# MAGIC   `news_article` have a scalar column, `digest` is a LIST
# MAGIC   (`market_ids_mentioned`, needs `list_contains`), `comments` doesn't have
# MAGIC   the column at all -- filter unsupported there, returns empty explicitly.

# COMMAND ----------

SCALAR_MARKET_ID_SOURCES = {"registry", "news_article"}
LIST_MARKET_IDS_SOURCES = {"digest": "market_ids_mentioned"}
NO_MARKET_ID_FILTER_SOURCES = {"comments"}


def embed_query(text):
    body = json.dumps({
        "texts": [text],
        "input_type": "search_query",
        "embedding_types": ["float"],
    })
    response = bedrock.invoke_model(modelId=MODEL_ID, body=body)
    payload = json.loads(response["body"].read())
    return payload["embeddings"]["float"][0]


# status ("open"/"resolved") only exists as a column on registry_cohere --
# chunk_registry_item has written it since 2026-08-20, but retrieval never
# consumed it until now. See tech_debt.md, "Retrieval Ignores Structured
# Registry Metadata." Other 3 tables don't carry it -- no-op there, not an
# error, same non-uniform-schema pattern as market_id above.
STATUS_FILTER_SOURCE = "registry"


def search_source(source, query_vector, market_id=None, status=None, limit=None):
    """No .limit() call by default is NOT the same as "no limit" -- LanceDB
    applies its own hidden default (observed: 10) when .limit() is never
    called at all. That silently reintroduces the exact pre-filter/post-filter
    bug this was meant to fix, for any source with no .where() clause to rely
    on (e.g. registry's semantic-only resolve step, which has no market_id/
    status filter acting as a real cutoff). `limit` must be passed explicitly
    by the caller in that case -- see resolve_market_ids below for the
    "size of the whole table, capped" pattern used there."""
    table_name = f"{source}_{MODEL_LABEL}"
    if table_name not in db.list_tables().tables:
        return []
    tbl = db.open_table(table_name)
    q = tbl.search(query_vector).metric("cosine")
    if limit is not None:
        q = q.limit(limit)

    clauses = []
    if market_id is not None:
        if source in SCALAR_MARKET_ID_SOURCES:
            clauses.append(f"market_id = '{market_id}'")
        elif source in LIST_MARKET_IDS_SOURCES:
            col = LIST_MARKET_IDS_SOURCES[source]
            clauses.append(f"list_contains({col}, '{market_id}')")
        elif source in NO_MARKET_ID_FILTER_SOURCES:
            return []

    if status is not None and source == STATUS_FILTER_SOURCE:
        clauses.append(f"status = '{status}'")

    if clauses:
        q = q.where(" AND ".join(clauses))

    rows = q.to_list()
    for r in rows:
        r["_source"] = source
        r.pop("embedding", None)
    return rows


def search(question, k=5, market_id=None, status=None, sources=None):
    """Plain search (no query rewriting, no date filter) -- top-k cut here,
    as the last step, after search_source's .where() filters already ran."""
    query_vector = embed_query(question)
    targets = sources or SOURCES
    return {s: search_source(s, query_vector, market_id=market_id, status=status)[:k] for s in targets}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Try a question
# MAGIC
# MAGIC Change `QUESTION` and re-run this cell as many times as you want.

# COMMAND ----------

QUESTION = "what happened with Bitcoin markets this week"

results = search(QUESTION, k=5)
for source, rows in results.items():
    print(f"\n=== {source} ({len(rows)} results) ===")
    for r in rows:
        preview = (r.get("text") or "")[:160].replace("\n", " ")
        print(f"  [{r.get('_distance', '?'):.4f}] {r.get('chunk_id', '?')}: {preview}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Free space -- try your own questions
# MAGIC
# MAGIC E.g.: `search("markets recently resolved about elections", k=3)`,
# MAGIC or with a filter: `search("bitcoin", market_id="3653974")`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query rewriting: LLM as a structured filter extractor, not conversational
# MAGIC ## synthesis
# MAGIC
# MAGIC Same category of LLM use already present in `ingest_polymarket` (the
# MAGIC verifiability filter) -- a structured verdict/JSON, not a natural-language
# MAGIC answer for the user. This is still part of retrieval (Bloque G), NOT Day 5's
# MAGIC synthesis.
# MAGIC
# MAGIC **Real finding from implementing this -- the date filter:** `pubDate` in
# MAGIC `news_article_cohere` is stored as an RFC 2822 string ("Sun, 02 Aug 2026
# MAGIC 07:00:00 GMT") -- NOT directly comparable as a range in a LanceDB `where()`
# MAGIC (the day-of-week prefix breaks lexical ordering). The date filter for
# MAGIC news_article is applied client-side in Python, after fetching results. For
# MAGIC `digest`, the `digest_s3_key` prefix (`digest/YYYY-MM-DD/...`) IS comparable
# MAGIC as a string, so that one filters directly. `registry`/`comments` have no
# MAGIC reliable per-chunk date field -- the date filter simply doesn't apply there,
# MAGIC not simulated.

# COMMAND ----------

from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

QUERY_REWRITE_MODEL_ID = "us.anthropic.claude-sonnet-4-5-20250929-v1:0"

FILTER_SCHEMA_DOC = """Available sources and their supported filters:
- registry: 1 vector per market (question+description). Filters: market_id (exact) and status ("open" or "resolved", exact). No reliable date filter.
- news_article: full news article chunks. Filter: market_id (exact) and pubDate (real publish date).
- digest: 1 narrative chunk per cycle (every 12h). Filter: market_id (searches inside a list, not exact match) and cycle date.
- comments: trader comments grouped by entity. Does NOT support filtering by individual market_id nor a reliable date field.
- odds: price/volume time-series snapshots per market (S3, not a LanceDB vector table -- no
  semantic search, exact structured lookup only). Filter: market_id (exact) and snapshot
  timestamp (real event time, ISO 8601, directly range-comparable). Use this source when the
  question is about how odds/price/probability MOVED over time, not about news coverage or
  discussion.
"""


def _extract_json(text):
    """Claude often wraps structured output in a markdown code fence
    (```json ... ```) even when explicitly told to return JSON only --
    strip that before parsing, or json.loads fails with 'Expecting value:
    line 1 column 1' on the leading backtick."""
    text = text.strip()
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
        text = text.rsplit("```", 1)[0] if text.endswith("```") else text
    return json.loads(text.strip())


def rewrite_query(question):
    today_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    system_prompt = f"""You prepare queries for a semantic retrieval system over a
corpus of Polymarket prediction markets, news articles, trader comments, and cycle
digests.

Today's real date (UTC): {today_utc}

{FILTER_SCHEMA_DOC}

Given a natural-language question, decide:
1. search_text: the text to embed for semantic search (can be the same as the
   original question, or rewritten if it helps).
2. market_id: if the question mentions a specific market by a KNOWN exact ID,
   otherwise null. Do not invent an ID you don't know.
3. date_from / date_to: if the question implies a relative time range ("this
   week", "last month", "today"), compute EXACT dates in YYYY-MM-DD format using
   today's date above. null if not applicable.
4. sources: list of relevant sources from ["registry","news_article","digest","comments","odds"]
   if the question clearly applies to only some of them, null if it applies to all EXCEPT
   odds -- odds is structured time-series data, not general-purpose text, so only include it
   when the question is specifically about price/odds/probability movement over time (e.g.
   "how did the odds move", "what happened to the price"), never include it by default.
5. status: "open" or "resolved" if the question implies a registry market status
   (e.g. "still open", "already resolved", "settled"), otherwise null. Only
   meaningful for the registry source -- see filter schema above.
6. keywords: a list of 1-3 literal, exact terms (proper nouns, names, specific
   entities) from the question that should be used for a keyword/substring match
   against market questions, IN ADDITION to semantic search -- e.g. for "Trump"
   markets, keywords=["trump"]. Only include terms that are exact, unambiguous
   identifiers likely to appear literally in a market's question text -- not
   generic concepts (don't include e.g. "markets", "events", "developments").
   Empty list if the question has no clear literal keyword (e.g. a purely
   conceptual question like "what markets are close to 50/50").

Respond with ONLY valid JSON, no extra text, in exactly this shape:
{{"search_text": "...", "market_id": null, "date_from": null, "date_to": null, "sources": null, "status": null, "keywords": []}}"""

    body = json.dumps({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 500,
        "system": system_prompt,
        "messages": [{"role": "user", "content": question}],
    })
    response = bedrock.invoke_model(modelId=QUERY_REWRITE_MODEL_ID, body=body)
    payload = json.loads(response["body"].read())
    text = payload["content"][0]["text"]
    try:
        return _extract_json(text)
    except json.JSONDecodeError:
        print("RAW MODEL OUTPUT (failed to parse as JSON):", repr(text))
        raise


def filter_news_article_by_date(rows, date_from, date_to):
    if not date_from and not date_to:
        return rows
    kept = []
    for r in rows:
        try:
            pub = parsedate_to_datetime(r["pubDate"]).date()
        except Exception:
            continue
        if date_from and pub < datetime.strptime(date_from, "%Y-%m-%d").date():
            continue
        if date_to and pub > datetime.strptime(date_to, "%Y-%m-%d").date():
            continue
        kept.append(r)
    return kept


def filter_digest_by_date(rows, date_from, date_to):
    if not date_from and not date_to:
        return rows
    kept = []
    for r in rows:
        key_date = r.get("digest_s3_key", "").split("/")[1] if "/" in r.get("digest_s3_key", "") else None
        if not key_date:
            continue
        if date_from and key_date < date_from:
            continue
        if date_to and key_date > date_to:
            continue
        kept.append(r)
    return kept


def search_with_rewrite(question, k=5):
    rewritten = rewrite_query(question)
    print("query rewriting:", json.dumps(rewritten, indent=2))

    query_vector = embed_query(rewritten["search_text"])
    targets = rewritten.get("sources") or SOURCES
    market_id = rewritten.get("market_id")
    status = rewritten.get("status")
    date_from, date_to = rewritten.get("date_from"), rewritten.get("date_to")

    # Top-k is cut LAST, after every filter -- .where() filters inside
    # search_source AND the date post-filter below. Never truncate before a
    # filter has had a chance to act (see search_source's docstring).
    results = {}
    for source in targets:
        rows = search_source(source, query_vector, market_id=market_id, status=status)
        if source == "news_article":
            rows = filter_news_article_by_date(rows, date_from, date_to)
        elif source == "digest":
            rows = filter_digest_by_date(rows, date_from, date_to)
        results[source] = rows[:k]
    return results, rewritten

# COMMAND ----------

# MAGIC %md
# MAGIC ## Try query rewriting

# COMMAND ----------

results, rewritten = search_with_rewrite("what happened with Bitcoin markets this week", k=5)
for source, rows in results.items():
    print(f"\n=== {source} ({len(rows)} results) ===")
    for r in rows:
        preview = (r.get("text") or "")[:160].replace("\n", " ")
        print(f"  [{r.get('_distance', '?'):.4f}] {r.get('chunk_id', '?')}: {preview}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Registry-first cascade -- the correct architecture (2026-08-29)
# MAGIC
# MAGIC `search_with_rewrite` above runs all 4 (now potentially 5) sources as
# MAGIC INDEPENDENT parallel searches, each against the same raw/rewritten question
# MAGIC text. That's architecturally wrong for questions like "what happened with
# MAGIC Bitcoin markets this week": News shouldn't semantically search "bitcoin this
# MAGIC week" against article text in isolation -- it should look up articles
# MAGIC belonging to the EXACT market_ids that Registry already resolved "bitcoin" to,
# MAGIC then filter those by date. Same idea for odds.
# MAGIC
# MAGIC **The cascade, as designed with the user:**
# MAGIC 1. Registry resolves free text -> exact market_ids, via cosine similarity over
# MAGIC    question+description (+ status filter if implied). NO temporal filter here
# MAGIC    -- Registry has no "content date", only lifecycle dates (first_seen,
# MAGIC    end_date, resolution_date), which don't mean "this week" the way pubDate or
# MAGIC    an odds snapshot timestamp does.
# MAGIC 2. Those market_ids become the EXACT filter for every other source that needs
# MAGIC    them (news_article, odds) -- instead of each one re-running semantic search
# MAGIC    against the raw question independently.
# MAGIC 3. News: lookup by those market_ids (`.where("market_id IN (...)")`), then
# MAGIC    the existing date post-filter (pubDate's RFC 2822 format still can't go in
# MAGIC    `.where()` -- unchanged, accepted as Python post-filtering per the user).
# MAGIC 4. Odds: NEW source, not in LanceDB at all -- direct S3 lookup per market_id
# MAGIC    (`odds/<market_id>.json`), snapshots filtered by `timestamp` (already ISO
# MAGIC    8601, real range filter, no post-filter-format problem like pubDate has).
# MAGIC 5. Digest: stays independent, does NOT depend on Registry's market_ids (it's
# MAGIC    cycle-scoped narrative, not market-scoped) -- own semantic search + cycle
# MAGIC    date filter, same as before.
# MAGIC 6. No top-k inside the cascade at any intermediate step -- same "filter
# MAGIC    everything first, cut to k last" principle as the fixes above. Registry's
# MAGIC    own similarity step DOES still need a cutoff (it's the one place doing
# MAGIC    real semantic ranking against free text), but it's a similarity threshold
# MAGIC    decision, not an arbitrary top-k -- left as a wide k for now (registry is
# MAGIC    small, ~1,800 rows), revisit if it ever needs real tuning.
# MAGIC
# MAGIC Comments has no market_id column at all (links via comment_entity_id, see
# MAGIC architecture_canon.md) -- deliberately left OUT of the cascade, same
# MAGIC exclusion `search_source` already applies elsewhere.

# COMMAND ----------

import boto3 as _boto3_odds  # reuse the same session's credentials, s3 client only

s3 = boto_session.client("s3")


# Reverted to a plain top-N count (2026-08-29) after a distance-threshold
# attempt (0.70) and a Cohere Rerank cross-encoder pass both failed to
# produce a clean cutoff for a real test query ("trump") -- see "Tuning
# levers" below, item 10, for the full analysis (the 0.51-0.70 distance
# range has no real gap/elbow, and rerank scored "Bills vs. Browns" (no
# relation) above a real Trump-cabinet market). This TOP_N is explicitly
# accepted as a known-imperfect placeholder, not a solved design -- kept
# here, not deleted, because it's simpler than either failed alternative
# and produced results the user judged reasonable by inspection.
REGISTRY_SEMANTIC_TOP_N = 30


def resolve_market_ids(search_text, keywords=None, status=None):
    """Step 1 of the cascade: Registry resolves free text -> exact market_ids.

    HYBRID, not semantic-only (changed 2026-08-29 after finding real gaps --
    see tech_debt.md). Two branches, unioned and deduped by market_id:

    - Keyword branch: exact substring match against the market's QUESTION
      only, not `description` -- `text` is `question\n\ndescription`
      combined (registry_cohere has no separate question column), so
      `text ILIKE` would also match markets whose long resolution-criteria
      description happens to mention the keyword while the actual question
      doesn't (found live 2026-08-29: this inflated 21 real "Trump"
      question-matches to 37 by including description-only mentions).
      `.where("text ILIKE ...")` is used only as a cheap pre-filter to avoid
      a full table scan in Python; the real check re-splits `text` on the
      first `\n\n` and matches the keyword against that QUESTION part only,
      case-insensitive. No cutoff at all beyond that -- an exact literal
      match in the question is either right or it isn't, no similarity
      gradient to rank/cut.
    - Semantic branch: cosine similarity over the full question+description
      embedding, cut to REGISTRY_SEMANTIC_TOP_N (a real, generous top-k,
      not LanceDB's hidden default-10 -- see search_source's docstring for
      why calling .search() with no .limit() at all is NOT "no limit").
      Catches paraphrases keyword matching can't (e.g. "the sitting
      president" without the name) -- this is the whole reason registry has
      a semantic layer at all (see architecture_canon.md, "Capa semantica
      de Polymarket"). A distance-threshold cutoff was tried instead of a
      count and explicitly reverted -- see item 10 in "Tuning levers" below.

    status filter still applies to both branches (registry's only reliable
    structured column). No temporal filter here -- registry has no content
    date, see the cascade markdown above."""
    tbl = db.open_table(f"registry_{MODEL_LABEL}")

    # Question-only match, same exact criterion as the "Sanity check" cell
    # below (text.split("\n")[0], case-insensitive) -- this MUST stay
    # identical to that cell's check, or the two are comparing against
    # different definitions of "matches" and the false-negative/false-
    # positive counts stop meaning anything.
    keyword_rows = []
    for kw in (keywords or []):
        # .where() ILIKE against the full combined text is a cheap
        # pre-filter only (avoids a full table scan) -- text also contains
        # description, so ILIKE alone over-matches (found live 2026-08-29:
        # 37 text-level matches vs 21 real question-level ones for "trump").
        kw_clauses = [f"text ILIKE '%{kw}%'"]
        if status is not None:
            kw_clauses.append(f"status = '{status}'")
        candidates = tbl.search().where(" AND ".join(kw_clauses)).to_list()
        rows = [
            r for r in candidates
            if kw.lower() in (r.get("text", "").split("\n")[0] or "").lower()
        ]
        for r in rows:
            r["_source"] = "registry"
            r["_match_type"] = "keyword"
            r.pop("embedding", None)
        keyword_rows.extend(rows)

    query_vector = embed_query(search_text)
    semantic_rows = search_source("registry", query_vector, status=status, limit=REGISTRY_SEMANTIC_TOP_N)
    for r in semantic_rows:
        r["_match_type"] = "semantic"

    seen = set()
    merged = []
    for r in keyword_rows + semantic_rows:
        mid = r["market_id"]
        if mid in seen:
            continue
        seen.add(mid)
        merged.append(r)

    return [r["market_id"] for r in merged], merged


def search_news_by_market_ids(market_ids, date_from=None, date_to=None):
    """Step 3: News as a lookup by Registry's resolved market_ids, not an
    independent semantic search. No embedding/similarity involved here at
    all -- pure structured filter, then the existing date post-filter."""
    if not market_ids:
        return []
    table_name = "news_article_cohere"
    if table_name not in db.list_tables().tables:
        return []
    tbl = db.open_table(table_name)
    id_list = ", ".join(f"'{m}'" for m in market_ids)
    rows = tbl.search().where(f"market_id IN ({id_list})").to_list()
    for r in rows:
        r["_source"] = "news_article"
        r.pop("embedding", None)
    return filter_news_article_by_date(rows, date_from, date_to)


def search_odds_by_market_ids(market_ids, date_from=None, date_to=None):
    """Step 4: odds is not a LanceDB table -- direct S3 lookup per
    market_id, snapshots filtered by timestamp (already ISO 8601, real
    range comparison, no post-filter-format problem like pubDate). New
    source, previously absent from retrieval entirely."""
    if not market_ids:
        return {}
    results = {}
    for mid in market_ids:
        key = f"odds/{mid}.json"
        try:
            obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        except s3.exceptions.NoSuchKey:
            continue
        data = json.loads(obj["Body"].read())
        snapshots = data.get("snapshots", [])
        if date_from or date_to:
            kept = []
            for snap in snapshots:
                ts = snap.get("timestamp", "")[:10]  # YYYY-MM-DD prefix, ISO 8601
                if date_from and ts < date_from:
                    continue
                if date_to and ts > date_to:
                    continue
                kept.append(snap)
            snapshots = kept
        if snapshots:
            results[mid] = snapshots
    return results


def search_cascade(question, digest_k=5):
    """New entry point: Registry-first cascade, replaces the parallel/
    independent design in search()/search_with_rewrite() for questions that
    need News and/or odds. Digest stays independent (cycle-scoped, not
    market-scoped). Comments is out of scope (no market_id column)."""
    rewritten = rewrite_query(question)
    print("query rewriting:", json.dumps(rewritten, indent=2))

    targets = rewritten.get("sources") or ["registry", "news_article", "digest"]
    status = rewritten.get("status")
    date_from, date_to = rewritten.get("date_from"), rewritten.get("date_to")
    market_id = rewritten.get("market_id")

    results = {}

    # Step 1: Registry resolves market_ids (unless a specific market_id was
    # already given directly, in which case skip the semantic resolve step).
    market_ids = [market_id] if market_id else []
    registry_rows = []
    if "registry" in targets or "news_article" in targets or "odds" in targets:
        resolved_ids, registry_rows = resolve_market_ids(
            rewritten["search_text"], keywords=rewritten.get("keywords"), status=status
        )
        if not market_id:
            market_ids = resolved_ids
    # Always expose registry_rows when computed, even if the rewrite only
    # asked for "odds"/"news_article" -- it's the market_id -> question text
    # lookup every other source needs for readable output, not just an
    # internal step gated behind the user explicitly asking for "registry".
    if registry_rows:
        results["registry"] = registry_rows

    # Step 3: News, lookup by market_ids + date, not independent search.
    if "news_article" in targets:
        results["news_article"] = search_news_by_market_ids(market_ids, date_from, date_to)

    # Step 4: odds, S3 lookup by market_ids + timestamp, not a LanceDB table.
    if "odds" in targets:
        results["odds"] = search_odds_by_market_ids(market_ids, date_from, date_to)

    # Step 5: Digest, independent of Registry's market_ids -- own semantic
    # search + cycle date filter.
    if "digest" in targets:
        query_vector = embed_query(rewritten["search_text"])
        digest_rows = search_source("digest", query_vector, market_id=market_id, status=None)
        results["digest"] = filter_digest_by_date(digest_rows, date_from, date_to)[:digest_k]

    return results, rewritten, market_ids

# COMMAND ----------

# MAGIC %md
# MAGIC ## Try the cascade

# COMMAND ----------

results, rewritten, market_ids = search_cascade("what happened with Bitcoin markets this week")
print("resolved market_ids:", market_ids)
for source, rows in results.items():
    if source == "odds":
        print(f"\n=== odds ({len(rows)} markets with snapshots in range) ===")
        for mid, snaps in rows.items():
            print(f"  {mid}: {len(snaps)} snapshots")
        continue
    print(f"\n=== {source} ({len(rows)} results) ===")
    for r in rows:
        preview = (r.get("text") or "")[:160].replace("\n", " ")
        print(f"  [{r.get('_distance', '?')}] {r.get('chunk_id', '?')}: {preview}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Odds question: "how did the odds of Trump markets move in the last 48hrs"
# MAGIC
# MAGIC Exercises the `odds` source end to end -- Registry resolves "Trump" markets,
# MAGIC then odds snapshots are looked up directly from S3 (not LanceDB) and filtered
# MAGIC by timestamp, same cascade as the Bitcoin/News example above but through the
# MAGIC odds branch instead of news_article.

# COMMAND ----------

results, rewritten, market_ids = search_cascade(
    "how did the odds of the markets that talk about trump move in the last 48hrs"
)
print("resolved market_ids:", market_ids)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Readable odds output -- date + price per snapshot, not just a count

# COMMAND ----------

for source, rows in results.items():
    if source == "registry":
        print(f"\n=== registry ({len(rows)} matched markets) ===")
        for r in rows:
            preview = (r.get("text") or "")[:100].replace("\n", " ")
            print(f"  {r.get('market_id')}: {preview}")
        continue
    if source != "odds":
        continue
    print(f"\n=== odds ({len(rows)} markets with snapshots in range) ===")
    for mid, snaps in rows.items():
        # find the market's question from the registry results above, if resolved
        question = next((r.get("text", "").split("\n")[0] for r in results.get("registry", []) if r.get("market_id") == mid), mid)
        print(f"\n  {mid} -- {question}")
        for snap in snaps:
            try:
                prices = json.loads(snap.get("outcomePrices", "[]"))
            except (json.JSONDecodeError, TypeError):
                prices = snap.get("outcomePrices")
            print(f"    {snap.get('timestamp')}  source={snap.get('source')}  outcomePrices={prices}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sanity check: reverse-verify the semantic resolve against the full registry
# MAGIC
# MAGIC `resolve_market_ids` above used cosine similarity, not a literal keyword match
# MAGIC -- worth verifying it didn't miss obvious matches or include obvious
# MAGIC non-matches. Scans the ENTIRE registry (not just the resolved 10) with a plain
# MAGIC substring check on `question` (case-insensitive, "trump" in question text),
# MAGIC and cross-references against `market_ids` from the cascade above.
# MAGIC
# MAGIC - **False negatives:** markets whose question literally contains "trump" but
# MAGIC   were NOT in the resolved set -- the semantic search missed something obvious.
# MAGIC - **False positives:** markets in the resolved set whose question does NOT
# MAGIC   contain "trump" -- not necessarily wrong (could be a real semantic match,
# MAGIC   e.g. "the President" without naming him), but worth eyeballing.

# COMMAND ----------

tbl = db.open_table("registry_cohere")
all_rows = tbl.search().to_list()  # no vector query -- full scan, order doesn't matter here
print(f"total registry rows scanned: {len(all_rows)}")

literal_trump_ids = {
    r["market_id"] for r in all_rows
    if "trump" in (r.get("text", "").split("\n")[0] or "").lower()
}
resolved_ids = set(market_ids)

false_negatives = literal_trump_ids - resolved_ids
false_positives = resolved_ids - literal_trump_ids

print(f"\nliteral 'trump' matches in full registry: {len(literal_trump_ids)}")
print(f"resolved by cascade (registry step): {len(resolved_ids)}")

print(f"\n=== FALSE NEGATIVES ({len(false_negatives)}) -- contain 'trump', missed by resolve ===")
for mid in false_negatives:
    q = next((r.get("text", "").split("\n")[0] for r in all_rows if r["market_id"] == mid), mid)
    print(f"  {mid}: {q}")

print(f"\n=== FALSE POSITIVES ({len(false_positives)}) -- resolved, but no literal 'trump' ===")
for mid in false_positives:
    q = next((r.get("text", "").split("\n")[0] for r in all_rows if r["market_id"] == mid), mid)
    print(f"  {mid}: {q}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tuning levers -- running list, updated as we find things
# MAGIC
# MAGIC Not exhaustive, not all implemented -- this is a working list to come back
# MAGIC to, not a finished design.
# MAGIC
# MAGIC 1. **Top-k -- SUPERSEDED by the cascade's per-step design (2026-08-29).**
# MAGIC    No longer a single uniform `k` across sources. `search()` (the older,
# MAGIC    parallel/independent design) still takes one `k` applied per source.
# MAGIC    `search_cascade()` (current design, see below) has no single `k` at
# MAGIC    all: News and odds return everything that survives their market_id +
# MAGIC    date filters (no cutoff), Registry's semantic branch has its own
# MAGIC    cutoff (`REGISTRY_SEMANTIC_TOP_N`, see item 10), and only Digest still
# MAGIC    takes an explicit `digest_k` (default 5). "Should k vary by source" is
# MAGIC    resolved differently than originally framed -- most sources don't use
# MAGIC    a k at all anymore, they use a real filter instead.
# MAGIC
# MAGIC 2. **Reranking -- tried for Registry specifically, not adopted (see item
# MAGIC    10).** Cohere Rerank via Bedrock was tested as a cross-encoder second
# MAGIC    pass and didn't produce a clean result on a real query. Still
# MAGIC    unexplored for News/Digest, where the corpus is larger and a
# MAGIC    cross-encoder might behave differently than it did on Registry's short
# MAGIC    question-only text.
# MAGIC
# MAGIC 3. **Hybrid search -- PARTIALLY implemented, for Registry only.**
# MAGIC    `resolve_market_ids` combines a keyword branch (`text ILIKE`, exact
# MAGIC    substring against the question) with the semantic branch, unioned and
# MAGIC    deduped -- see item 10 and the cascade markdown above. This is real
# MAGIC    hybrid search, just scoped to solving Registry's resolve step
# MAGIC    specifically, not a general-purpose hybrid search across all 4
# MAGIC    sources. News/Digest/Comments still have no keyword/full-text
# MAGIC    component (LanceDB supports it natively, unused elsewhere) -- would
# MAGIC    still help distinguish semantically-similar-but-factually-different
# MAGIC    questions, e.g. "Bitcoin dip to $58,000" vs "$62,000" vs "$57,500".
# MAGIC
# MAGIC 4. **Query rewriting** -- implemented (`rewrite_query`). Extracts
# MAGIC    `search_text`, `market_id`, `date_from`/`date_to`, `sources` (now
# MAGIC    including `"odds"`), `status`, and `keywords` (added 2026-08-29 to
# MAGIC    feed the hybrid keyword branch above). LLM used as a structured filter
# MAGIC    extractor, same category of LLM use as `ingest_polymarket`'s
# MAGIC    verifiability filter -- not conversational synthesis, still Bloque G
# MAGIC    scope, not Day 5. Feeds both `search_with_rewrite()` (older parallel
# MAGIC    design) and `search_cascade()` (current design, see below).
# MAGIC
# MAGIC 5. **Date filtering -- FIXED (2026-08-29).** `search_source` no longer
# MAGIC    applies `.limit()` at all -- it returns every row matching its
# MAGIC    `.where()` clauses (market_id/status), ordered by distance, uncut.
# MAGIC    `search_with_rewrite` then applies `filter_news_article_by_date`/
# MAGIC    `filter_digest_by_date` over that FULL filtered set, and only THEN
# MAGIC    slices `[:k]` -- top-k is now strictly the last step, after every
# MAGIC    filter (`.where()` and post-filter alike) has already run. This was a
# MAGIC    classic pre-filter vs. post-filter bug: the previous version cut to
# MAGIC    top-k by pure semantic similarity BEFORE the date filter ever ran, so
# MAGIC    a real match ranked just outside the top-k never got a chance to be
# MAGIC    considered. Reproduced live pre-fix: "what happened with Bitcoin
# MAGIC    markets this week" (date_from 2026-08-23, date_to 2026-08-29) returned
# MAGIC    0 news_article results even though real matches existed. Note this
# MAGIC    doesn't fetch an arbitrary "bigger pool" (k=50-100) as an earlier draft
# MAGIC    of this fix proposed -- that's still a top-k cut before filtering, just
# MAGIC    a more generous one. The actual fix removes the pre-filter cut
# MAGIC    entirely: filter first, against everything, cut to k last.
# MAGIC
# MAGIC 6. **Cross-source fusion -- SUPERSEDED, no longer top-k-per-source
# MAGIC    everywhere (2026-08-29).** Originally: every source ranked
# MAGIC    independently, no unified ranking across tables (still true for
# MAGIC    `search()`/`search_with_rewrite()`, the older parallel design -- see
# MAGIC    tech_debt.md for the cosine/L2 metric mismatch bug this avoided).
# MAGIC    `search_cascade()` changes this for News/odds specifically: they're no
# MAGIC    longer independently-ranked searches at all, they're lookups gated by
# MAGIC    Registry's resolved market_ids (see item 11). Digest remains
# MAGIC    independent (cycle-scoped, not market-scoped). Metric-mismatch concern
# MAGIC    doesn't apply to the cascade the same way, since News/odds aren't
# MAGIC    ranking by distance against each other or against Registry at all.
# MAGIC
# MAGIC 7. **Metric consistency** -- already fixed project-wide: `.metric("cosine")`
# MAGIC    forced explicitly on every search, regardless of whether the target table
# MAGIC    has an index or not (see tech_debt.md, "Vector Search Metric Mismatch").
# MAGIC
# MAGIC 8. **Registry status filter -- FIXED (2026-08-29).** `registry_cohere` has
# MAGIC    carried `status`/`end_date`/`resolution_date` as metadata columns since
# MAGIC    2026-08-20 (`chunk_registry_item`), but retrieval never used them --
# MAGIC    same root cause as item 5 (a real filter never pushed into `.where()`),
# MAGIC    just a different symptom (never filtered at all, vs. filtered too late).
# MAGIC    `search_source`/`search`/`rewrite_query` now support an optional `status`
# MAGIC    ("open"/"resolved"), pushed into LanceDB's `.where()` BEFORE `.limit()` --
# MAGIC    same treatment `market_id` already got, no post-filtering involved. Only
# MAGIC    applies to `registry` (the only table with the column); no-op elsewhere.
# MAGIC    `end_date`/`resolution_date` filters not added yet -- no clear use case
# MAGIC    distinct from the existing `date_from`/`date_to` (news_article/digest).
# MAGIC
# MAGIC 9. **Top-k should not always apply when a structured filter already narrows
# MAGIC    the candidate set -- OPEN, not decided (2026-08-29).** If `.where()`
# MAGIC    (market_id, status, etc.) already reduces a table to a handful of rows,
# MAGIC    ranking that small set by cosine similarity and cutting to `k` doesn't
# MAGIC    save anything and risks dropping valid rows for no reason -- e.g. 8 open
# MAGIC    Bitcoin markets matching a status+text filter should probably all come
# MAGIC    back, not get cut to k=5 by an arbitrary similarity ranking. Proposed
# MAGIC    mechanism (not decided): check the filtered row count first
# MAGIC    (`.where(...).count_rows()`), skip a real rank-and-cut `.limit()` (or use
# MAGIC    a generous safety cap instead) when that count is small, apply top-k
# MAGIC    normally otherwise. Two open questions: (1) is the threshold a fixed row
# MAGIC    count or relative to the requested `k`? (2) does this apply to all 4
# MAGIC    sources or only where a structured filter can exist at all (comments never
# MAGIC    has one). See tech_debt.md, "Retrieval Ignores Structured Registry
# MAGIC    Metadata," for the full writeup.
# MAGIC
# MAGIC 10. **Registry semantic branch cutoff -- TRIED AND REVERTED, still an open
# MAGIC     problem (2026-08-29).** `resolve_market_ids`'s semantic branch uses
# MAGIC     `REGISTRY_SEMANTIC_TOP_N = 30`, an arbitrary count with no real
# MAGIC     justification -- acknowledged as such, not defended as correct. Two
# MAGIC     real alternatives were tried against a live test query ("markets about
# MAGIC     trump", 21 real question-level matches known via keyword ground truth)
# MAGIC     and both failed to produce something clean enough to adopt:
# MAGIC     - **Distance threshold instead of a count:** ranked all 1,863 registry
# MAGIC       rows by cosine distance -- found NO real gap/elbow between "trump"
# MAGIC       matches and unrelated markets. The 21 literal matches end at distance
# MAGIC       0.6449; genuinely unrelated markets ("Bills vs. Browns", other 2028
# MAGIC       candidates unconnected to Trump) start appearing around 0.69-0.70; in
# MAGIC       between (0.65-0.69) is a mix of real Trump-administration context
# MAGIC       (Bannon, Blanche, CAATSA/Turkey under his foreign policy) and
# MAGIC       unrelated noise, with no clean line separating them. Any fixed
# MAGIC       threshold either drops real context or admits noise -- confirmed by
# MAGIC       testing 0.70 specifically: it pulled in 50 rows including clear
# MAGIC       non-matches (Doug Burgum, Ron DeSantis 2028 bids -- no real Trump
# MAGIC       connection).
# MAGIC     - **Cohere Rerank (cross-encoder) via Bedrock, tried as a second pass
# MAGIC       on top of the threshold:** `bedrock-agent-runtime.rerank()` with
# MAGIC       `cohere.rerank-v3-5:0` works mechanically (confirmed real API call,
# MAGIC       correct ARN `arn:aws:bedrock:us-east-1::foundation-model/cohere.rerank-v3-5:0`),
# MAGIC       but scored "Bills vs. Browns" (an NFL market, zero relation to
# MAGIC       Trump) at rank 21 of 50 -- ABOVE "Todd Blanche nomination withdrawn"
# MAGIC       (a real market about a Trump cabinet pick). Scores were also tightly
# MAGIC       clustered (0.052 to 0.016, no clear separation). Only the bare
# MAGIC       `question` text was passed as the rerank document (not the full
# MAGIC       `question+description` text) -- untested whether passing the fuller
# MAGIC       text would help; this was not investigated further before reverting.
# MAGIC     - **Reverted to TOP_N=30** as an explicitly-imperfect placeholder --
# MAGIC       simpler than either failed alternative, and the resulting 30 results
# MAGIC       for the "trump" test query were judged reasonable by manual
# MAGIC       inspection (21 literal + 9 genuinely related: Bannon, Blanche,
# MAGIC       CAATSA/Turkey, Thunberg, White House Press Secretary, etc.).
# MAGIC     - **Open questions for whoever picks this up:** does a fixed TOP_N=30
# MAGIC       generalize to queries with very different candidate density (a topic
# MAGIC       with only 1-2 real matches in the whole registry vs. one with
# MAGIC       hundreds)? Would passing full `question+description` text to Cohere
# MAGIC       Rerank (instead of just `question`) produce a cleaner separation?
# MAGIC       Is there a cheaper reranking approach (e.g. a smaller local
# MAGIC       cross-encoder, BM25-style keyword scoring blended with cosine
# MAGIC       distance) worth trying before committing to a specific fix?
# MAGIC
# MAGIC 11. **Registry-first cascade -- IMPLEMENTED (2026-08-29), the main
# MAGIC     architectural change of this session.** `search_cascade()` replaces
# MAGIC     independent-parallel search with a real pipeline for questions that
# MAGIC     need News and/or odds: Registry resolves free text -> exact
# MAGIC     market_ids first (hybrid keyword+semantic, item 3/10), then News
# MAGIC     (`search_news_by_market_ids`) and odds (`search_odds_by_market_ids`,
# MAGIC     a NEW source -- direct S3 lookup on `odds/<market_id>.json`, not a
# MAGIC     LanceDB table, no embedding involved) look up by those exact ids
# MAGIC     instead of re-running semantic search independently. Digest stays
# MAGIC     independent (cycle-scoped, not market-scoped). Comments is out of
# MAGIC     scope (no market_id column). `search()`/`search_with_rewrite()` (the
# MAGIC     older design) are kept in this notebook for comparison, not deleted --
# MAGIC     they're superseded for market-scoped questions, not removed.

