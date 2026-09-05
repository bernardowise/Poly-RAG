"""
Poly-RAG retrieval, Bloque G / Dia 4.

Embeds a free-text question with the same model used for the corpus (Cohere
Embed v4, global.cohere.embed-v4:0 -- see lambdas/embed_registry/handler.py
for the reference implementation this mirrors) and searches the LanceDB
tables written by write_lancedb (registry_cohere, comments_cohere,
digest_cohere, news_article_cohere), plus odds (S3, not LanceDB).

No synthesis here -- returns raw chunks + metadata. Sintesis LLM (segunda
pasada) is explicitly Dia 5 scope, not this file (see tech_debt.md, "Second
LLM Pass").

Two designs, both kept (ported from the rag_query_exploracion Databricks
notebook 2026-08-29, where this was designed and tested against the real
corpus before landing here):

- search()/search_with_rewrite(): older, simpler design -- all sources
  searched independently and in parallel against the same question text.
  Top-k PER SOURCE, not one global ranked list (cosine scores aren't
  guaranteed comparable across tables with different embedding
  distributions -- see architecture_canon.md, Retrieval section).
- search_cascade(): current, correct design for market-scoped questions
  (see its docstring below) -- Registry resolves free text to exact
  market_ids FIRST, then News/odds/Comments look up by those ids instead of
  re-searching independently. Digest stays independent (cycle-scoped, not
  market-scoped).

Comments (2026-08-30): comments_cohere has no scalar market_id column (it
groups by comment_entity_id, an Event/Series id shared by up to 49 markets
-- see architecture_canon.md, "Comments" section), but it DOES carry
market_ids_mentioned, a LIST column added 2026-08-30 (chunk_comments,
backfilled for all pre-existing rows -- see tech_debt.md, "Comments Not in
Retrieval Cascade") that captures exactly which markets each comment
thread applies to. This makes Comments filterable by market_id via
list_contains(), same mechanism Digest's market_ids_mentioned already used
-- Comments is no longer out of scope for either design.
"""

import json
import os
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import boto3
import lancedb
from botocore.config import Config

S3_BUCKET = os.environ.get("S3_BUCKET", "poly-rag-369970405415")
LANCEDB_URI = f"s3://{S3_BUCKET}/lancedb/"
MODEL_LABEL = "cohere"
MODEL_ID = "global.cohere.embed-v4:0"
QUERY_REWRITE_MODEL_ID = "us.anthropic.claude-sonnet-4-5-20250929-v1:0"

SOURCES = ["registry", "comments", "digest", "news_article"]

bedrock = boto3.client(
    "bedrock-runtime",
    region_name="us-east-1",
    config=Config(retries={"max_attempts": 3}, max_pool_connections=10),
)
bedrock_agent_runtime = boto3.client("bedrock-agent-runtime", region_name="us-east-1")
s3 = boto3.client("s3")
_db = None


def get_db():
    global _db
    if _db is None:
        _db = lancedb.connect(LANCEDB_URI)
    return _db


def embed_query(text):
    """input_type=search_query, not search_document -- Cohere's asymmetric
    mode expects the query side tagged differently from the corpus side that
    was embedded by the embed_* Lambdas."""
    body = json.dumps({
        "texts": [text],
        "input_type": "search_query",
        "embedding_types": ["float"],
    })
    response = bedrock.invoke_model(modelId=MODEL_ID, body=body)
    payload = json.loads(response["body"].read())
    return payload["embeddings"]["float"][0]


# Schema is NOT uniform across the 4 tables -- market_id filtering has to
# know each source's real column, or it either errors (column doesn't
# exist) or silently returns 0 rows (comparing = against a list column).
# registry/news_article: scalar market_id, direct equality.
# digest/comments: market_ids_mentioned is a LIST column -- needs
# list_contains, not =. Comments gained this column 2026-08-30 (previously
# had no market_id linkage at all in LanceDB -- see module docstring).
SCALAR_MARKET_ID_SOURCES = {"registry", "news_article"}
LIST_MARKET_IDS_SOURCES = {"digest": "market_ids_mentioned", "comments": "market_ids_mentioned"}
NO_MARKET_ID_FILTER_SOURCES = set()

# status ("open"/"resolved") only exists as a column on registry_cohere --
# the other 3 tables don't carry it, so a status filter is a no-op there,
# not an error (same non-uniform-schema pattern as market_id above).
STATUS_FILTER_SOURCE = "registry"


def search_source(db, source, query_vector, market_id=None, status=None, limit=None):
    """No .limit() call by default -- but that is NOT the same as "no
    limit": LanceDB applies its own hidden default (observed: 10) when
    .limit() is never called at all. Top-k must happen AFTER every filter
    (.where() clauses here, plus any post-filter the caller applies, e.g.
    date filtering below), never before -- cutting before filtering can
    silently drop real matches a filter would have kept. See tech_debt.md,
    "Retrieval Ignores Structured Registry Metadata" and the pre-filter/
    post-filter date bug entry above it. Callers that need the true full
    ranked list (e.g. resolve_market_ids below) must pass an explicit
    `limit` large enough to cover it -- this was found the hard way."""
    table_name = f"{source}_{MODEL_LABEL}"
    if table_name not in db.list_tables().tables:
        return []
    tbl = db.open_table(table_name)
    search = tbl.search(query_vector).metric("cosine")
    if limit is not None:
        search = search.limit(limit)

    clauses = []
    if market_id is not None:
        if source in SCALAR_MARKET_ID_SOURCES:
            clauses.append(f"market_id = '{market_id}'")
        elif source in LIST_MARKET_IDS_SOURCES:
            col = LIST_MARKET_IDS_SOURCES[source]
            clauses.append(f"list_contains({col}, '{market_id}')")
        elif source in NO_MARKET_ID_FILTER_SOURCES:
            return []  # explicit no-op, not a silent 0-row false negative

    # status is a real column only on registry_cohere (open/resolved) --
    # written by chunk_registry_item since 2026-08-20, never consumed by
    # retrieval until now. See tech_debt.md, "Retrieval Ignores Structured
    # Registry Metadata."
    if status is not None and source == STATUS_FILTER_SOURCE:
        clauses.append(f"status = '{status}'")

    if clauses:
        search = search.where(" AND ".join(clauses))

    rows = search.to_list()
    for r in rows:
        r["_source"] = source
        r.pop("embedding", None)  # not useful past this point, keeps output small
    return rows


def search(question, k=5, market_id=None, status=None, sources=None):
    """Returns {source: [chunk, ...]} -- one ranked list per source, not a
    single merged ranking (see module docstring).

    Top-k is cut HERE, as the very last step, after every filter has already
    been applied inside search_source -- never before. LanceDB's .search()
    already returns rows ordered by distance, so slicing [:k] after
    filtering is still the correct top-k, it just no longer risks cutting
    before a filter had a chance to act."""
    query_vector = embed_query(question)
    db = get_db()
    targets = sources or SOURCES
    return {
        source: search_source(db, source, query_vector, market_id=market_id, status=status)[:k]
        for source in targets
    }


# --- Query rewriting: LLM as a structured filter extractor -----------------
# Same category of LLM use already present in ingest_polymarket (the
# verifiability filter) -- a structured verdict/JSON, not a natural-language
# answer for the user. Still retrieval (Bloque G), NOT Day 5's synthesis.

FILTER_SCHEMA_DOC = """Available sources and their supported filters:
- registry: 1 vector per market (question+description). Filters: market_id (exact) and status ("open" or "resolved", exact). No reliable date filter.
- news_article: full news article chunks. Filter: market_id (exact) and pubDate (real publish date).
- digest: 1 narrative chunk per cycle (every 12h). Filter: market_id (searches inside a list, not exact match) and cycle date.
- comments: trader comments grouped by entity. Filter: market_id (searches inside a list, not exact match, one comment thread can apply to many markets). No reliable date field.
- odds: price/volume time-series snapshots per market (S3, not a LanceDB vector table -- no
  semantic search, exact structured lookup only). Filter: market_id (exact) and snapshot
  timestamp (real event time, ISO 8601, directly range-comparable). Use this source when the
  question is about how odds/price/probability MOVED over time, not about news coverage or
  discussion.
"""


def _extract_json(text):
    """Claude often wraps structured output in a markdown code fence
    (```json ... ```) even when explicitly told not to -- strip that before
    parsing, or json.loads fails with 'Expecting value: line 1 column 1' on
    the leading backtick."""
    text = text.strip()
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
        text = text.rsplit("```", 1)[0] if text.endswith("```") else text
    return json.loads(text.strip())


def rewrite_query(question, history_text=None):
    """history_text (added 2026-08-30, for Gradio's conversational mode):
    plain text of prior turns ("User: ...\\nAssistant: ..."), used so a
    context-dependent follow-up ("what about last month?") can be resolved
    into a self-contained search_text/date_from/date_to in THIS one call,
    instead of needing a separate LLM call to "contextualize" the question
    first (cheaper -- one Bedrock call per turn for rewriting, not two).
    None/omitted keeps this exactly the single-turn behavior it always had."""
    today_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    history_block = f"\nConversation so far, for resolving context-dependent references only (the LATEST user message below is what you're actually rewriting):\n{history_text}\n" if history_text else ""
    system_prompt = f"""You prepare queries for a semantic retrieval system over a
corpus of Polymarket prediction markets, news articles, trader comments, and cycle
digests.

Today's real date (UTC): {today_utc}

{FILTER_SCHEMA_DOC}
{history_block}
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
7. needs_sql: true if answering the question requires AGGREGATION, RANKING,
   COUNTING, or a computed comparison over many markets or many odds snapshots
   -- things semantic search cannot do. Examples that need SQL: "top 10 markets
   by volume last week", "how many markets resolved YES in September", "which
   market had the biggest price swing between two cycles", "average liquidity of
   open Bitcoin markets". Examples that do NOT need SQL (plain retrieval is
   enough): "how did the Bitcoin market move last week" (one/few named markets),
   "what news was there about the Fed", "what are traders saying about X". When
   in doubt, false.

Respond with ONLY valid JSON, no extra text, in exactly this shape:
{{"search_text": "...", "market_id": null, "date_from": null, "date_to": null, "sources": null, "status": null, "keywords": [], "needs_sql": false}}"""

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
        parsed = _extract_json(text)
    except json.JSONDecodeError:
        print("RAW MODEL OUTPUT (failed to parse as JSON):", repr(text))
        raise
    parsed.setdefault("needs_sql", False)  # older prompt / model omission
    return parsed


# --------------------------------------------------------------------------
# text-to-SQL route (2026-09-05). For AGGREGATION / RANKING / COUNTING
# questions that the semantic cascade structurally cannot answer -- see
# tech_debt.md "Phase 4 SQL Layer". Reads the Parquet that build_sql_parquet
# (Phase 4 Lambda) writes to s3://<bucket>/sql/ via DuckDB, in-process, no
# server. Guardrails are NON-DESTRUCTIVE by design (DuckDB reads Parquet
# read-only anyway): the point is to keep a generated query from doing
# something surprising, not to sandbox a hostile one.
# --------------------------------------------------------------------------
SQL_MARKETS_URI = f"read_parquet('s3://{S3_BUCKET}/sql/markets.parquet')"
SQL_ODDS_URI = f"read_parquet('s3://{S3_BUCKET}/sql/odds_snapshots/*.parquet')"
SQL_ROW_CAP = 200          # hard LIMIT injected if a generated query has none

SQL_SCHEMA_DOC = f"""Two tables, both read from Parquet on S3 (DuckDB):

markets  -- one row per prediction market (from the registry)
  market_id                         VARCHAR   (join key)
  question                          VARCHAR   (the market's title)
  description                       VARCHAR
  status                            VARCHAR   ('open' | 'resolved')
  resolution_source                 VARCHAR
  created_at, first_seen, last_updated, end_date, resolution_date  VARCHAR (ISO 8601)
  final_outcome                     VARCHAR   (JSON string, e.g. '["1", "0"]' when resolved, NULL otherwise)
  comment_entity_type, comment_entity_id, comment_link_type  VARCHAR
  post_resolution_cycles_remaining  BIGINT

odds_snapshots  -- one row per (market_id, snapshot). ~200k rows, all monthly
                   partitions unioned by the '*' in the path.
  market_id           VARCHAR   (join key -> markets.market_id)
  timestamp           VARCHAR   (ISO 8601, directly range-comparable as text)
  source              VARCHAR   ('cycle' = a 12h tracked snapshot with volume;
                                 'clob_backfill' = pre-tracking price history,
                                  volume/volume24hr/liquidity are NULL there)
  yes_price, no_price DOUBLE    (0..1, the two binary outcome prices)
  outcome_prices_raw  VARCHAR   (original JSON string, for multi-outcome markets)
  volume, volume24hr, liquidity  DOUBLE  (USD; only on source='cycle' rows.
                                          volume is CUMULATIVE lifetime volume,
                                          not per-cycle -- for "volume over a
                                          time window" use MAX(volume) within
                                          the window, or MAX minus MIN for the
                                          delta, NEVER SUM.)

Reference these tables in FROM as:
  {SQL_MARKETS_URI} AS m
  {SQL_ODDS_URI} AS o

Notes:
- For "by volume" questions filter `o.source = 'cycle'` (clob_backfill has no volume).
- timestamp is text; a date range is `o.timestamp >= '2026-09-01' AND o.timestamp < '2026-10-01'`.
- Always LEFT JOIN markets onto odds (some odds market_ids predate the current
  registry) and return m.question so the answer is readable.
"""

_SQL_FORBIDDEN = (
    "insert", "update", "delete", "drop", "create", "alter", "attach", "detach",
    "copy", "install", "load", "pragma", "set ", "export", "import", "call",
    "vacuum", "checkpoint",
)


def _guard_sql(sql):
    """Non-destructive guardrails on a generated query. Returns a cleaned SQL
    string or raises ValueError. Not a security sandbox -- DuckDB opens the
    Parquet read-only -- just a stop against a generated statement doing
    something other than a single read."""
    s = sql.strip().rstrip(";").strip()
    low = s.lower()
    if not (low.startswith("select") or low.startswith("with")):
        raise ValueError(f"generated SQL is not a SELECT/WITH: {s[:120]!r}")
    if ";" in s:
        raise ValueError("generated SQL has multiple statements")
    for kw in _SQL_FORBIDDEN:
        # word-ish boundary check so 'created_at' doesn't trip 'create'
        idx = low.find(kw)
        while idx != -1:
            before = low[idx - 1] if idx > 0 else " "
            after = low[idx + len(kw)] if idx + len(kw) < len(low) else " "
            if not before.isalnum() and before != "_" and not after.isalnum() and after != "_":
                raise ValueError(f"generated SQL contains forbidden keyword {kw!r}")
            idx = low.find(kw, idx + 1)
    import re
    if not re.search(r"\blimit\b", low):
        s = f"{s}\nLIMIT {SQL_ROW_CAP}"
    return s


def text_to_sql(question, history_text=None):
    """One Claude call: natural-language question -> a single DuckDB SELECT
    against the markets / odds_snapshots Parquet. Returns the SQL string
    (already guard-checked)."""
    today_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    history_block = f"\nConversation so far (for resolving references like 'those markets' only):\n{history_text}\n" if history_text else ""
    system_prompt = f"""You translate a natural-language question into ONE DuckDB SQL
query over the schema below. Today's real date (UTC): {today_utc}.

{SQL_SCHEMA_DOC}
{history_block}
Rules:
- Output ONE statement, a SELECT (or WITH ... SELECT). No semicolons, no DDL,
  no PRAGMA/SET/COPY/INSTALL -- read only.
- Always include an explicit LIMIT (<= {SQL_ROW_CAP}).
- Return m.question alongside any market_id so the result is human-readable.
- Compute date ranges yourself from today's date for relative phrases.

Respond with ONLY the SQL, no prose, no code fence."""
    body = json.dumps({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 600,
        "system": system_prompt,
        "messages": [{"role": "user", "content": question}],
    })
    response = bedrock.invoke_model(modelId=QUERY_REWRITE_MODEL_ID, body=body)
    text = json.loads(response["body"].read())["content"][0]["text"].strip()
    if text.startswith("```"):
        text = text.split("```", 2)[1]
        if text.lstrip().lower().startswith("sql"):
            text = text.lstrip()[3:]
        text = text.split("```", 1)[0]
    return _guard_sql(text)


_sql_con = None


def _get_sql_con():
    global _sql_con
    if _sql_con is None:
        import duckdb
        con = duckdb.connect()
        con.execute("INSTALL httpfs; LOAD httpfs;")
        con.execute("SET s3_region='us-east-1';")
        cr = boto3.Session().get_credentials().get_frozen_credentials()
        con.execute(f"SET s3_access_key_id='{cr.access_key}';")
        con.execute(f"SET s3_secret_access_key='{cr.secret_key}';")
        if cr.token:
            con.execute(f"SET s3_session_token='{cr.token}';")
        _sql_con = con
    return _sql_con


def run_sql(sql):
    """Execute a guard-checked SELECT against the Phase 4 Parquet. Returns
    {"sql": str, "columns": [...], "rows": [ {...}, ... ], "error": str|None}.
    Never raises -- a failure is reported in the return value so the cascade
    and the synthesis layer can surface it instead of 500-ing."""
    try:
        con = _get_sql_con()
        # DuckDB has no per-statement timeout knob (statement_timeout is a
        # Postgres-ism); the Parquet is ~2MB total and the Lambda/Space both
        # cap wall time upstream, so an unbounded query here is not a real risk.
        cur = con.execute(sql)
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        return {"sql": sql, "columns": cols, "rows": rows, "error": None}
    except Exception as exc:  # noqa: BLE001 -- report, never raise into retrieval
        return {"sql": sql, "columns": [], "rows": [], "error": f"{type(exc).__name__}: {exc}"}


def filter_news_article_by_date(rows, date_from, date_to):
    """pubDate on news_article_cohere is stored as an RFC 2822 string --
    not directly comparable as a range in a LanceDB where() clause (the
    day-of-week prefix breaks lexical ordering), so this stays a Python
    post-filter by design (accepted tradeoff, not revisited -- see
    tech_debt.md)."""
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
    """Older parallel/independent design + query rewriting. Superseded by
    search_cascade() for market-scoped questions (News/odds) -- kept for
    comparison, not deleted. Top-k is cut LAST, after every filter -- both
    the .where() filters inside search_source and the date post-filter
    below, never before."""
    rewritten = rewrite_query(question)
    db = get_db()
    query_vector = embed_query(rewritten["search_text"])
    targets = rewritten.get("sources") or SOURCES
    market_id = rewritten.get("market_id")
    status = rewritten.get("status")
    date_from, date_to = rewritten.get("date_from"), rewritten.get("date_to")

    results = {}
    for source in targets:
        rows = search_source(db, source, query_vector, market_id=market_id, status=status)
        if source == "news_article":
            rows = filter_news_article_by_date(rows, date_from, date_to)
        elif source == "digest":
            rows = filter_digest_by_date(rows, date_from, date_to)
        results[source] = rows[:k]
    return results, rewritten


# --- Registry-first cascade --------------------------------------------
# The correct architecture for market-scoped questions (designed 2026-08-29,
# see tech_debt.md and the rag_query_exploracion notebook for the full
# design discussion): Registry resolves free text -> exact market_ids
# FIRST, then News/odds look up by those exact ids instead of independently
# re-running semantic search against the raw question. No top-k inside the
# cascade at any intermediate step -- filter everything first, cut to k
# last (same principle as the date-filter/status-filter fixes above).

# Reverted to a plain top-N count (2026-08-29) after a distance-threshold
# attempt (0.70) and a Cohere Rerank cross-encoder pass both failed to
# produce a clean cutoff for a real test query ("trump") -- see tech_debt.md,
# "Registry semantic branch cutoff" for the full analysis (the 0.51-0.70
# distance range has no real gap/elbow, and rerank scored an unrelated NFL
# market above a real Trump-cabinet market). This TOP_N is explicitly
# accepted as a known-imperfect placeholder, not a solved design.
REGISTRY_SEMANTIC_TOP_N = 30


def resolve_market_ids(search_text, keywords=None, status=None):
    """Step 1 of the cascade: Registry resolves free text -> exact market_ids.

    HYBRID, not semantic-only. Two branches, unioned and deduped by market_id:

    - Keyword branch: exact substring match against the market's QUESTION
      only, not `description` -- `text` is `question\\n\\ndescription`
      combined (registry_cohere has no separate question column), so
      `text ILIKE` would also match markets whose long resolution-criteria
      description happens to mention the keyword while the actual question
      doesn't. `.where("text ILIKE ...")` is used only as a cheap pre-filter
      to avoid a full table scan in Python; the real check re-splits `text`
      on the first newline and matches the keyword against that QUESTION
      part only, case-insensitive. No cutoff at all beyond that -- an exact
      literal match in the question is either right or it isn't, no
      similarity gradient to rank/cut.
    - Semantic branch: cosine similarity over the full question+description
      embedding, cut to REGISTRY_SEMANTIC_TOP_N. Catches paraphrases keyword
      matching can't (e.g. "the sitting president" without the name) -- this
      is the whole reason registry has a semantic layer at all (see
      architecture_canon.md, "Capa semantica de Polymarket").

    status filter applies to both branches (registry's only reliable
    structured column). No temporal filter here -- registry has no content
    date (only lifecycle dates: first_seen, end_date, resolution_date)."""
    db = get_db()
    tbl = db.open_table(f"registry_{MODEL_LABEL}")

    keyword_rows = []
    for kw in (keywords or []):
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
    semantic_rows = search_source(db, "registry", query_vector, status=status, limit=REGISTRY_SEMANTIC_TOP_N)
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


def search_registry_by_ids(market_ids):
    """Plain .where() lookup of registry rows by exact market_id -- used
    when the ids came from the text-to-SQL branch or a direct market_id
    (not from resolve_market_ids' semantic pass), so downstream sources
    still get market_id -> question for readable output. Returns the same
    row shape resolve_market_ids' semantic branch does (minus the
    embedding), preserving the input id order."""
    if not market_ids:
        return []
    db = get_db()
    table_name = f"registry_{MODEL_LABEL}"
    if table_name not in db.list_tables().tables:
        return []
    tbl = db.open_table(table_name)
    id_list = ", ".join(f"'{m}'" for m in market_ids)
    rows = tbl.search().where(f"market_id IN ({id_list})").to_list()
    by_id = {}
    for r in rows:
        r["_source"] = "registry"
        r["_match_type"] = "id_lookup"
        r.pop("embedding", None)
        by_id[str(r["market_id"])] = r
    return [by_id[m] for m in market_ids if m in by_id]


def search_news_by_market_ids(market_ids, date_from=None, date_to=None):
    """Step of the cascade: News as a lookup by Registry's resolved
    market_ids, not an independent semantic search. No embedding/similarity
    involved here at all -- pure structured filter, then the existing date
    post-filter (pubDate's RFC 2822 format still can't go in .where())."""
    if not market_ids:
        return []
    db = get_db()
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


def search_comments_by_market_ids(market_ids):
    """Step of the cascade: Comments as a lookup by Registry's resolved
    market_ids, via market_ids_mentioned (LIST column, added 2026-08-30 --
    see module docstring). One comment thread (shared_event/shared_series)
    can apply to many markets at once, so this is an OR of list_contains()
    across all resolved ids, not a single equality/IN check like News'
    scalar market_id. No date filter -- comments has no reliable per-chunk
    date field (see FILTER_SCHEMA_DOC)."""
    if not market_ids:
        return []
    db = get_db()
    table_name = "comments_cohere"
    if table_name not in db.list_tables().tables:
        return []
    tbl = db.open_table(table_name)
    clauses = " OR ".join(f"list_contains(market_ids_mentioned, '{m}')" for m in market_ids)
    rows = tbl.search().where(clauses).to_list()
    for r in rows:
        r["_source"] = "comments"
        r.pop("embedding", None)
    return rows


def search_odds_by_market_ids(market_ids, date_from=None, date_to=None):
    """Step of the cascade: odds is not a LanceDB table -- direct S3 lookup
    per market_id, snapshots filtered by timestamp (already ISO 8601, real
    range comparison, no post-filter-format problem like pubDate has). A
    source previously absent from retrieval entirely."""
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


def search_cascade(question, digest_k=5, history_text=None):
    """New entry point: Registry-first cascade, the correct architecture
    for market-scoped questions -- replaces the parallel/independent design
    in search()/search_with_rewrite(). Digest stays independent (cycle-
    scoped, not market-scoped). history_text: see rewrite_query's docstring
    -- optional, only used for conversational follow-up resolution."""
    rewritten = rewrite_query(question, history_text=history_text)

    targets = rewritten.get("sources") or ["registry", "news_article", "digest", "comments"]
    status = rewritten.get("status")
    date_from, date_to = rewritten.get("date_from"), rewritten.get("date_to")
    market_id = rewritten.get("market_id")

    results = {}

    # Step 0: text-to-SQL branch (2026-09-05). If the rewrite flagged the
    # question as aggregation/ranking/counting, run one generated SELECT
    # against the Phase 4 Parquet first. Its rows go into results["sql"];
    # any market_id column in the output seeds the semantic lookups below,
    # so "top 10 markets by volume last week -- and what's the news"
    # cross-references. The semantic sources still run (unless the rewrite
    # narrowed `sources`) -- SQL augments the cascade, it doesn't replace it.
    market_ids = [market_id] if market_id else []
    if rewritten.get("needs_sql"):
        sql = text_to_sql(question, history_text=history_text)
        sql_result = run_sql(sql)
        results["sql"] = sql_result
        if not sql_result["error"]:
            sql_ids = [
                str(row["market_id"]) for row in sql_result["rows"]
                if row.get("market_id") is not None
            ]
            if sql_ids and not market_id:
                market_ids = list(dict.fromkeys(sql_ids))  # dedup, keep order

    # Step 1: Registry resolves market_ids semantically -- SKIPPED when a
    # market_id was given directly or Step 0's SQL already produced the set.
    registry_rows = []
    needs_downstream = (
        "registry" in targets or "news_article" in targets
        or "odds" in targets or "comments" in targets
    )
    if not market_ids and needs_downstream:
        resolved_ids, registry_rows = resolve_market_ids(
            rewritten["search_text"], keywords=rewritten.get("keywords"), status=status
        )
        market_ids = resolved_ids
    elif market_ids and needs_downstream:
        # ids came from SQL / a direct market_id -- still fetch their
        # registry rows so downstream sources have market_id -> question
        # for readable output (search_registry_by_ids is a plain .where()).
        registry_rows = search_registry_by_ids(market_ids)

    # Always expose registry_rows when computed, even if the rewrite only
    # asked for "odds"/"news_article"/"comments" -- it's the market_id ->
    # question text lookup every other source needs for readable output.
    if registry_rows:
        results["registry"] = registry_rows

    # Step: News, lookup by market_ids + date, not independent search.
    if "news_article" in targets:
        results["news_article"] = search_news_by_market_ids(market_ids, date_from, date_to)

    # Step: Comments, lookup by market_ids via market_ids_mentioned -- not
    # independent semantic search either (comments text is short/noisy,
    # see architecture_canon.md, "Chunking de Comments" -- an exact
    # market_id link is more reliable than similarity here).
    if "comments" in targets:
        results["comments"] = search_comments_by_market_ids(market_ids)

    # Step: odds, S3 lookup by market_ids + timestamp, not a LanceDB table.
    if "odds" in targets:
        results["odds"] = search_odds_by_market_ids(market_ids, date_from, date_to)

    # Step: Digest, independent of Registry's market_ids -- own semantic
    # search + cycle date filter.
    if "digest" in targets:
        db = get_db()
        query_vector = embed_query(rewritten["search_text"])
        digest_rows = search_source(db, "digest", query_vector, market_id=market_id, status=None)
        results["digest"] = filter_digest_by_date(digest_rows, date_from, date_to)[:digest_k]

    return results, rewritten, market_ids


if __name__ == "__main__":
    import sys
    question = sys.argv[1] if len(sys.argv) > 1 else "what happened with Bitcoin markets this week"
    results, rewritten, market_ids = search_cascade(question)
    print("query rewriting:", json.dumps(rewritten, indent=2))
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
