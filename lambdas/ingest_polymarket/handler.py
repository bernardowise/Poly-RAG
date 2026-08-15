"""
Poly-RAG ingestion Lambda: Polymarket Gamma API.

Redesigned 2026-08-15 (see tech_debt.md, "Ingestion Redesign" entry). Replaces
static keyword-vertical tagging with an LLM verifiability filter: a market is
ingested only if its outcome resolves against a citable public record, not
human judgment over ambiguous evidence. The same LLM call also produces a
single combined search_query per market (not isolated keywords -- terms that
only carry meaning together, e.g. a name plus a date, must stay combined),
which News/Bluesky will read from the registry to drive their own searches
(no more static VERTICAL_KEYWORDS dict).

Pulls the top 500 active markets by volume24hr each cycle, diffs against the
market registry (DynamoDB) to find genuinely new ids, and only runs the LLM
pass on those -- steady-state cost scales with new-id arrival rate, not with
the candidate pool size. Ids previously tracked as open that no longer appear
in the pull are checked individually against the Gamma API for resolution.

Minimum horizon filter (added 2026-08-15, same day, after observing markets
resolve within 11 minutes of being tracked -- e.g. live sports/esports
matches): a market whose endDate is less than MIN_HORIZON_HOURS away is
skipped before the LLM pass entirely, not just filtered post-hoc. This isn't
only a "not enough time to track it" problem -- it's that the project's core
thesis (news/sentiment can influence a market's odds while it's open) does
not apply to a live match already being decided on the field, so including
these markets pollutes the dataset with cases that can't demonstrate the
thing the project is trying to measure.
"""

import json
import os
import time
import uuid
from datetime import datetime, timedelta, timezone

import boto3
import urllib.request

S3_BUCKET = os.environ.get("S3_BUCKET", "poly-rag-369970405415")
METRICS_TABLE = os.environ.get("METRICS_TABLE", "poly-rag-architecture-metrics")
REGISTRY_TABLE = os.environ.get("REGISTRY_TABLE", "poly-rag-market-registry")
BEDROCK_MODEL_ID = os.environ.get("BEDROCK_MODEL_ID", "us.anthropic.claude-sonnet-4-5-20250929-v1:0")
USE_LLM_ENRICHMENT = os.environ.get("USE_LLM_ENRICHMENT", "true").lower() == "true"

GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"
CANDIDATE_POOL_SIZE = 500
PAGE_SIZE = 100
# 48h = 4 full 12h ingestion cycles of runway before resolution -- enough to
# capture several odds snapshots and give news/sentiment correlation a real
# window, per CLAUDE.md's "measure, don't guess" discipline this is a starting
# point, not a permanent constant; revisit if it excludes too much or too little.
MIN_HORIZON_HOURS = 48

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
bedrock = boto3.client("bedrock-runtime")


def fetch_top_markets_by_volume24hr(limit=CANDIDATE_POOL_SIZE):
    """Paginates the Gamma API (hard 100-per-page cap) to build the top-N
    candidate pool ordered by volume24hr, deduping by id across pages."""
    markets = {}
    offset = 0
    while len(markets) < limit:
        url = f"{GAMMA_MARKETS_URL}?limit={PAGE_SIZE}&offset={offset}&active=true&order=volume24hr&ascending=false"
        req = urllib.request.Request(url, headers={"User-Agent": "poly-rag-ingestion/1.0"})
        with urllib.request.urlopen(req, timeout=10) as response:
            page = json.loads(response.read().decode())
        if not page:
            break
        for m in page:
            markets[m.get("id")] = m
        if len(page) < PAGE_SIZE:
            break
        offset += PAGE_SIZE
    return list(markets.values())[:limit]


def fetch_market_by_id(market_id):
    url = f"{GAMMA_MARKETS_URL}/{market_id}"
    req = urllib.request.Request(url, headers={"User-Agent": "poly-rag-ingestion/1.0"})
    with urllib.request.urlopen(req, timeout=10) as response:
        return json.loads(response.read().decode())


def has_minimum_horizon(market, now):
    """Excludes markets resolving too soon to be trackable (see module
    docstring) -- checked before the LLM pass, not after, so no Bedrock cost
    is spent on markets we'd discard anyway."""
    end_date_str = market.get("endDate")
    if not end_date_str:
        return True  # no endDate to check against -- don't exclude on missing data
    try:
        end_date = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
    except ValueError:
        return True  # unparseable date -- don't exclude on malformed data
    return (end_date - now) >= timedelta(hours=MIN_HORIZON_HOURS)


def get_known_ids(table):
    """Scans the registry for id -> status. Table is small enough for a full
    scan at this stage; revisit if the registry grows large enough to matter."""
    known = {}
    resp = table.scan(ProjectionExpression="market_id, #s", ExpressionAttributeNames={"#s": "status"})
    for item in resp.get("Items", []):
        known[item["market_id"]] = item["status"]
    while "LastEvaluatedKey" in resp:
        resp = table.scan(
            ProjectionExpression="market_id, #s",
            ExpressionAttributeNames={"#s": "status"},
            ExclusiveStartKey=resp["LastEvaluatedKey"],
        )
        for item in resp.get("Items", []):
            known[item["market_id"]] = item["status"]
    return known


BATCH_SIZE = 20


def _classify_batch(batch):
    listing = "\n".join(
        f'{i+1}. id={m["id"]} question="{m.get("question", "")}"'
        for i, m in enumerate(batch)
    )
    prompt = (
        "For each numbered prediction market below, judge whether its outcome "
        "will be objectively verifiable against a citable public record (e.g. an "
        "official statement, certified vote count, regulatory filing, market price) "
        "versus resolved by human/oracle judgment over ambiguous social evidence "
        "(e.g. celebrity rumors, disputed claims with no official record).\n\n"
        "Also produce TWO search representations, because downstream consumers match "
        "text differently:\n"
        "1. search_query: a single combined free-text search string, for a search API "
        "that accepts natural-language queries (e.g. social media search). Keep terms "
        "that only make sense together combined in one string. Good: \"Elon Musk "
        "Twitter posting frequency August 2026\".\n"
        "2. news_match_terms: a short list of 1-3 DISTINCTIVE terms/phrases, for "
        "matching against already-downloaded article text via AND logic (every term "
        "must appear in the SAME article to count as a match -- this is substring "
        "matching, not a search engine, so terms must be specific enough that their "
        "co-occurrence is meaningful, not generic words that appear everywhere). "
        "Prefer multi-word phrases over single common words. Example for the Fed "
        "rate market: [\"Federal Reserve\", \"September 2026\"] -- NOT [\"Fed\", "
        "\"rate\", \"2026\"], which would false-match unrelated articles.\n\n"
        f"{listing}\n\n"
        "Respond with ONLY a JSON array, one object per market, no other text:\n"
        '[{"id": "...", "is_verifiable": true|false, "search_query": "...", '
        '"news_match_terms": ["...", "..."]}]'
    )

    print(f"[classify_batch] prompt sent ({len(batch)} markets):\n{prompt}")

    start = time.time()
    response = bedrock.invoke_model(
        modelId=BEDROCK_MODEL_ID,
        body=json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            # 2500 (up from 1500): output now carries two text fields per
            # market (search_query + news_match_terms) instead of one, and a
            # truncated response mid-array was observed at the old limit.
            "max_tokens": 2500,
            "messages": [{"role": "user", "content": prompt}],
        }),
    )
    latency_ms = int((time.time() - start) * 1000)

    body = json.loads(response["body"].read())
    raw_text = body["content"][0]["text"].strip()
    usage = body.get("usage", {})

    print(f"[classify_batch] raw response:\n{raw_text}")

    try:
        classifications = json.loads(raw_text)
    except json.JSONDecodeError:
        try:
            start_idx = raw_text.find("[")
            end_idx = raw_text.rfind("]")
            classifications = json.loads(raw_text[start_idx:end_idx + 1])
        except (json.JSONDecodeError, ValueError):
            # Malformed/truncated JSON (e.g. the model ran out of max_tokens
            # mid-array). One bad batch shouldn't fail the whole Lambda run --
            # these markets are simply skipped this cycle and get re-classified
            # on the next one, since they're still "new" (never entered the
            # registry).
            print(f"[classify_batch] FAILED to parse response, skipping this batch of {len(batch)} markets")
            classifications = []

    return classifications, usage, latency_ms, raw_text


def classify_new_markets(candidates):
    """Batched Bedrock calls (BATCH_SIZE markets per call): for each candidate
    market, judge whether its outcome is objectively verifiable against a
    citable public record (vs. resolved by human judgment over ambiguous
    evidence), and produce a combined search_query to drive News/Bluesky
    searches. Steady-state cycles only see a handful of new ids (one call);
    the bootstrap cycle, seeing hundreds of new ids at once, needs multiple
    batches to cover all of them.
    """
    if not candidates:
        return {}, {"tokens_in": 0, "tokens_out": 0, "latency_ms": 0, "raw_responses": []}

    results = {}
    total_tokens_in, total_tokens_out, total_latency_ms = 0, 0, 0
    raw_responses = []

    for i in range(0, len(candidates), BATCH_SIZE):
        batch = candidates[i:i + BATCH_SIZE]
        classifications, usage, latency_ms, raw_text = _classify_batch(batch)
        for c in classifications:
            if "id" in c:
                results[c["id"]] = c
        total_tokens_in += usage.get("input_tokens", 0)
        total_tokens_out += usage.get("output_tokens", 0)
        total_latency_ms += latency_ms
        raw_responses.append(raw_text)

    llm_meta = {
        "tokens_in": total_tokens_in,
        "tokens_out": total_tokens_out,
        "latency_ms": total_latency_ms,
        "raw_responses": raw_responses,
    }
    return results, llm_meta


def estimate_cost_usd(tokens_in, tokens_out):
    # Claude Sonnet Bedrock pricing (approx, per 1K tokens): $0.003 in / $0.015 out
    return round((tokens_in / 1000) * 0.003 + (tokens_out / 1000) * 0.015, 6)


def write_metrics(source, llm_used, tokens_in, tokens_out, latency_ms, items_processed):
    table = dynamodb.Table(METRICS_TABLE)
    now = datetime.now(timezone.utc).isoformat()
    table.put_item(Item={
        "pk": f"{source}#{now}#{uuid.uuid4().hex[:8]}",
        "source": source,
        "llm_used": llm_used,
        "tokens_in": tokens_in,
        "tokens_out": tokens_out,
        "latency_ms": latency_ms,
        "estimated_cost_usd": str(estimate_cost_usd(tokens_in, tokens_out)),
        "items_processed": items_processed,
        "timestamp": now,
    })


def upsert_registry_entry(table, market, search_query, news_match_terms, now_iso):
    table.put_item(Item={
        "market_id": market["id"],
        "question": market.get("question", ""),
        "description": market.get("description", ""),
        "end_date": market.get("endDate"),
        "resolution_source": market.get("resolutionSource", ""),
        "status": "open",
        # Two derived search representations, because News and Bluesky match text
        # differently -- see the redesign entry in tech_debt.md for the reasoning:
        # - search_query: single combined free-text string, for Bluesky's
        #   search-API-style searchPosts (accepts natural-language queries).
        # - news_match_terms: short AND-matched term list, for substring matching
        #   against already-downloaded RSS article text (News has no search API --
        #   it greps its own downloaded text, so isolated common words would
        #   false-match; every term must co-occur in the same article).
        "search_query": search_query,
        "news_match_terms": news_match_terms,
        "first_seen": now_iso,
        "last_updated": now_iso,
        "resolution_date": None,
        "final_outcome": None,
    })


def mark_registry_resolved(table, market_id, final_outcome, now_iso):
    table.update_item(
        Key={"market_id": market_id},
        UpdateExpression="SET #s = :resolved, final_outcome = :outcome, resolution_date = :now, last_updated = :now",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={
            ":resolved": "resolved",
            ":outcome": final_outcome,
            ":now": now_iso,
        },
    )


def append_odds_snapshot(market, now_iso):
    """Read-modify-write the per-market odds time-series file in S3. One file
    per market_id, one appended snapshot per cycle -- never overwritten."""
    key = f"odds/{market['id']}.json"
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        history = json.loads(obj["Body"].read())
    except s3.exceptions.ClientError as e:
        # Missing key -- normally NoSuchKey (404), but S3 can also report a
        # generic AccessDenied (403) for a nonexistent key when the caller
        # lacks bucket-level ListBucket, so both are treated as "not found yet".
        error_code = e.response.get("Error", {}).get("Code", "")
        if error_code not in ("NoSuchKey", "AccessDenied", "404", "403"):
            raise
        history = {"market_id": market["id"], "snapshots": []}

    history["snapshots"].append({
        "timestamp": now_iso,
        "outcomePrices": market.get("outcomePrices"),
        "volume": market.get("volumeNum"),
        "volume24hr": market.get("volume24hr"),
        "liquidity": market.get("liquidityNum"),
    })

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(history),
        ContentType="application/json",
    )


def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()
    registry_table = dynamodb.Table(REGISTRY_TABLE)

    fetch_start = time.time()
    candidates = fetch_top_markets_by_volume24hr()
    fetch_latency_ms = int((time.time() - fetch_start) * 1000)

    known_ids = get_known_ids(registry_table)
    candidate_ids = {m["id"] for m in candidates}
    new_candidates = [
        m for m in candidates
        if m["id"] not in known_ids and has_minimum_horizon(m, now)
    ]

    tokens_in, tokens_out, llm_latency_ms = 0, 0, 0
    llm_raw_responses = []
    tracked_markets = []
    if USE_LLM_ENRICHMENT and new_candidates:
        classifications, llm_meta = classify_new_markets(new_candidates)
        tokens_in = llm_meta["tokens_in"]
        tokens_out = llm_meta["tokens_out"]
        llm_latency_ms = llm_meta["latency_ms"]
        llm_raw_responses = llm_meta["raw_responses"]

        for m in new_candidates:
            verdict = classifications.get(m["id"])
            if not verdict or not verdict.get("is_verifiable"):
                continue  # not objectively verifiable -- discard, never enters the registry
            search_query = verdict.get("search_query", "")
            news_match_terms = verdict.get("news_match_terms", [])
            upsert_registry_entry(registry_table, m, search_query, news_match_terms, now_iso)
            tracked_markets.append(m)

    # Every candidate id already known and still open gets a fresh odds snapshot,
    # regardless of whether it was newly classified this cycle.
    already_tracked_open = [
        m for m in candidates
        if m["id"] in known_ids and known_ids[m["id"]] == "open"
    ]
    for m in already_tracked_open + tracked_markets:
        append_odds_snapshot(m, now_iso)

    # Ids that were open in the registry but no longer appear in today's top-N pull:
    # query the Gamma API directly per id to check for resolution.
    open_known_ids = {mid for mid, status in known_ids.items() if status == "open"}
    dropped_ids = open_known_ids - candidate_ids
    resolved_count = 0
    for market_id in dropped_ids:
        try:
            detail = fetch_market_by_id(market_id)
        except Exception:
            continue  # transient API error -- leave status as-is, retry next cycle
        if detail.get("closed"):
            outcome_prices = detail.get("outcomePrices")
            mark_registry_resolved(registry_table, market_id, outcome_prices, now_iso)
            resolved_count += 1

    total_latency_ms = fetch_latency_ms + llm_latency_ms
    payload = {
        "source": "polymarket",
        "ingested_at": now_iso,
        "candidate_count": len(candidates),
        "new_ids_this_cycle": len(new_candidates),
        "newly_tracked_count": len(tracked_markets),
        "odds_snapshots_written": len(already_tracked_open) + len(tracked_markets),
        "resolved_this_cycle": resolved_count,
        "metadata": {
            "schema_version": "v2",
            "lambda_name": context.function_name,
            "lambda_request_id": context.aws_request_id,
            "llm_used": USE_LLM_ENRICHMENT,
            "llm_model_id": BEDROCK_MODEL_ID if USE_LLM_ENRICHMENT else None,
            "latency_ms": total_latency_ms,
            "estimated_cost_usd": estimate_cost_usd(tokens_in, tokens_out),
            # Bronze-layer audit trail: raw, unparsed LLM responses for the
            # verifiability+keywords classification pass, one string per batch
            # call. Lets us re-inspect exactly what the model said if the
            # parsed registry entries ever look wrong.
            "llm_raw_responses": llm_raw_responses,
        },
    }

    s3_key = f"polymarket/{now.strftime('%Y-%m-%d')}/{now.strftime('%H')}.json"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=json.dumps(payload),
        ContentType="application/json",
    )

    write_metrics(
        source="ingest_polymarket",
        llm_used=USE_LLM_ENRICHMENT,
        tokens_in=tokens_in,
        tokens_out=tokens_out,
        latency_ms=total_latency_ms,
        items_processed=len(candidates),
    )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "s3_key": s3_key,
            "candidate_count": len(candidates),
            "newly_tracked_count": len(tracked_markets),
            "resolved_this_cycle": resolved_count,
            "llm_used": USE_LLM_ENRICHMENT,
        }),
    }
