"""
Poly-RAG ingestion Lambda: Polymarket Gamma API.

Redesigned 2026-08-15 (see tech_debt.md, "Ingestion Redesign" entry). Replaces
static keyword-vertical tagging with an LLM verifiability filter: a market is
ingested only if its outcome resolves against a citable public record, not
human judgment over ambiguous evidence.

Simplified 2026-08-16 (see tech_debt.md, "Comments Source Replaces Bluesky"):
the LLM call no longer generates search_query/news_match_terms. Neither is
needed anymore -- News searches Google News using each market's `question`
verbatim (see ingest_news), and the new Comments source (replacing Bluesky)
looks up sentiment by direct event_id/series_id lookup, not text search.
This Lambda now also extracts comment_entity_type/comment_entity_id/
comment_link_type per market straight from the Gamma API's `events[]` field
(no LLM involved) -- see get_comment_link.

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
# Strict chaining (2026-08-16, see tech_debt.md "Strict Ingestion Chaining"):
# News/Comments read the registry this Lambda writes -- they were never
# truly independent, just running in parallel against a registry that might
# not reflect this cycle yet. Chaining Polymarket -> News -> Comments ->
# digest removes that race entirely: each stage only starts once the one
# before it has fully written its data.
NEXT_LAMBDA_NAME = os.environ.get("NEXT_LAMBDA_NAME", "poly-rag-ingest-news")

GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"
CANDIDATE_POOL_SIZE = 500
PAGE_SIZE = 100
# 48h = 4 full 12h ingestion cycles of runway before resolution -- enough to
# capture several odds snapshots and give news/sentiment correlation a real
# window, per CLAUDE.md's "measure, don't guess" discipline this is a starting
# point, not a permanent constant; revisit if it excludes too much or too little.
MIN_HORIZON_HOURS = 48

# Post-resolution News capture (see tech_debt.md, "Post-Resolution News
# Capture"): 4 cycles = 48h of continued News search after a market
# resolves, to capture reaction to the now-fixed outcome. The cycle a
# market resolves in counts as post-resolution cycle #1 -- this constant
# is what that first cycle's counter gets set to (ingest_news decrements
# it by 1 each cycle it includes the market, stopping at 0).
POST_RESOLUTION_CYCLES = 4

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
bedrock = boto3.client("bedrock-runtime")
lambda_client = boto3.client("lambda")


def invoke_next_stage(cycle_started_at):
    lambda_client.invoke(
        FunctionName=NEXT_LAMBDA_NAME,
        InvocationType="Event",
        Payload=json.dumps({"cycle_started_at": cycle_started_at}),
    )


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
        f"{listing}\n\n"
        "Respond with ONLY a JSON array, one object per market, no other text:\n"
        '[{"id": "...", "is_verifiable": true|false}]'
    )

    print(f"[classify_batch] prompt sent ({len(batch)} markets):\n{prompt}")

    start = time.time()
    response = bedrock.invoke_model(
        modelId=BEDROCK_MODEL_ID,
        body=json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            # 1500: reverted from 2500 now that the response is just
            # is_verifiable per market (search_query/news_match_terms
            # removed 2026-08-16 -- see tech_debt.md, "Comments Source
            # Replaces Bluesky": neither News nor the new Comments source
            # need LLM-generated search text, News uses `question` verbatim
            # and Comments looks up by event_id/series_id directly).
            "max_tokens": 1500,
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


def get_comment_link(market):
    """Extracts where this market's Polymarket comments live -- Event level
    if that event has any comments, else Series level (comments shared
    across every market in that series/league, confirmed 2026-08-16 by
    inspecting real MLB comments: the same feed appeared verbatim under two
    different games). Returns (entity_type, entity_id, link_type) or
    (None, None, None) if neither has comments. link_type flags whether a
    comment is genuinely about this specific market ("direct") or shared
    across a whole series ("shared_series") -- not a confidence judgment,
    the shared comments are just as real, only their attribution is
    many-to-one instead of one-to-one. See tech_debt.md, "Comments Source
    Replaces Bluesky"."""
    events = market.get("events") or []
    if not events:
        return None, None, None
    event = events[0]
    if event.get("commentCount", 0) > 0:
        return "Event", event.get("id"), "direct"
    series_list = event.get("series") or []
    if series_list and series_list[0].get("commentCount", 0) > 0:
        return "Series", series_list[0].get("id"), "shared_series"
    return None, None, None


def upsert_registry_entry(table, market, now_iso):
    entity_type, entity_id, link_type = get_comment_link(market)
    table.put_item(Item={
        "market_id": market["id"],
        "question": market.get("question", ""),
        "description": market.get("description", ""),
        "end_date": market.get("endDate"),
        "resolution_source": market.get("resolutionSource", ""),
        "status": "open",
        # Where to look up this market's Polymarket comments -- see
        # get_comment_link. None/None/None if this market has no comments
        # at either level.
        "comment_entity_type": entity_type,
        "comment_entity_id": entity_id,
        "comment_link_type": link_type,
        "first_seen": now_iso,
        # The market's REAL creation date (from the same Gamma response,
        # zero extra API cost), distinct from first_seen (when WE started
        # tracking it) -- required to classify News articles into temporal
        # tiers (see scripts/tag_news_temporal_tier.py and tech_debt.md,
        # "News Temporal Tiers"). Backfilled for the 595 pre-existing
        # registry items via scripts/backfill_registry_created_at.py; new
        # entries get it directly going forward as of this change.
        "created_at": market.get("createdAt"),
        "last_updated": now_iso,
        "resolution_date": None,
        "final_outcome": None,
        # Post-resolution News capture (see tech_debt.md, "Post-Resolution
        # News Capture"): 0 here means "not resolved yet, nothing to
        # count." Set to POST_RESOLUTION_CYCLES when this market transitions
        # to resolved (see mark_registry_resolved) and decremented by
        # ingest_news each cycle it's included in the search.
        "post_resolution_cycles_remaining": 0,
    })


def mark_registry_resolved(table, market_id, final_outcome, now_iso):
    """Marks a market resolved AND starts its post-resolution News capture
    window (see tech_debt.md, "Post-Resolution News Capture") -- this is the
    open->closed transition itself, the only moment this counter should ever
    be initialized. ingest_news reads post_resolution_cycles_remaining each
    cycle to decide whether to keep searching a resolved market, and
    decrements it; it never re-initializes it, so a market only ever gets
    this one 4-cycle window, starting exactly at its real resolution."""
    table.update_item(
        Key={"market_id": market_id},
        UpdateExpression=(
            "SET #s = :resolved, final_outcome = :outcome, resolution_date = :now, "
            "last_updated = :now, post_resolution_cycles_remaining = :cycles"
        ),
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={
            ":resolved": "resolved",
            ":outcome": final_outcome,
            ":now": now_iso,
            ":cycles": POST_RESOLUTION_CYCLES,
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
        # Explicit provenance (see tech_debt.md, "Cycle Snapshots Explicitly
        # Tagged source=cycle") -- written directly here as of 2026-08-18 so
        # every NEW snapshot is self-describing from day one, rather than
        # needing another one-off tagging pass later the way the 1,368
        # pre-existing snapshots did.
        "source": "cycle",
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


# Odds history backfill for newly-tracked markets (see tech_debt.md, "Odds
# History Backfill from Polymarket CLOB", "Not yet decided" note). Same
# CLOB endpoint, same YES-token-only + derived-complement approach as
# scripts/backfill_odds_history.py -- see that script's docstring for the
# full reasoning (the cross-token timestamp misalignment bug that discarded
# ~75% of real history, and why deriving NO=1-YES is sound for binary
# markets). This is the forward-going counterpart: instead of a one-off
# script backfilling 595 already-tracked markets, this runs automatically
# the moment a market FIRST enters the registry, so no market starts with
# a cycle-only time-series the way every market did before 2026-08-18.
CLOB_PRICES_HISTORY_URL = "https://clob.polymarket.com/prices-history"


def fetch_clob_price_history(token_id):
    url = f"{CLOB_PRICES_HISTORY_URL}?market={token_id}&interval=max&fidelity=1440"
    req = urllib.request.Request(url, headers={"User-Agent": "poly-rag-ingestion/1.0"})
    with urllib.request.urlopen(req, timeout=10) as response:
        return json.loads(response.read().decode()).get("history", []) or []


def backfill_odds_history_for_new_market(market, now):
    """Fetches and writes pre-tracking odds history for a market the moment
    it enters the registry. Binary markets only (2 clobTokenIds) -- markets
    with more outcomes are skipped, same as the one-off script, since the
    complement-derivation trick only holds for two-outcome markets. Silently
    does nothing on any fetch error or missing/malformed clobTokenIds --
    this must never block registry entry itself, odds history is a bonus,
    not a prerequisite."""
    try:
        token_ids = json.loads(market.get("clobTokenIds") or "[]")
    except json.JSONDecodeError:
        return
    if len(token_ids) != 2:
        return

    try:
        history = fetch_clob_price_history(token_ids[0])
    except Exception:
        return  # transient API error -- market still gets cycle snapshots going forward

    snapshots = []
    for point in sorted(history, key=lambda p: p["t"]):
        moment = datetime.fromtimestamp(point["t"], timezone.utc)
        if moment >= now:
            continue  # never write into or past the current cycle's own snapshot
        yes_price = float(point["p"])
        snapshots.append({
            "source": "clob_backfill",
            "timestamp": moment.isoformat(),
            "outcomePrices": json.dumps([f"{yes_price:.4f}", f"{1 - yes_price:.4f}"]),
            "backfilled_at": now.isoformat(),
        })

    if not snapshots:
        return

    key = f"odds/{market['id']}.json"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps({"market_id": market["id"], "snapshots": snapshots}),
        ContentType="application/json",
    )
    # append_odds_snapshot (called later in lambda_handler for every open
    # registry market, including this one) does its own read-modify-write
    # and will correctly read this file back and append today's cycle
    # snapshot on top -- no merge logic needed here, this only ever runs
    # once, before that first cycle snapshot exists.


def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()
    registry_table = dynamodb.Table(REGISTRY_TABLE)

    fetch_start = time.time()
    candidates = fetch_top_markets_by_volume24hr()
    fetch_latency_ms = int((time.time() - fetch_start) * 1000)

    known_ids = get_known_ids(registry_table)
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
            upsert_registry_entry(registry_table, m, now_iso)
            backfill_odds_history_for_new_market(m, now)
            tracked_markets.append(m)

    # Odds snapshot + resolution check -- decoupled from candidate discovery
    # entirely (redesigned 2026-08-17, see tech_debt.md). Every market
    # currently open in the registry is processed here, regardless of
    # whether it happened to appear in today's top-500 pull. The top-500
    # batch (candidates_by_id) is reused as a free data source when a
    # market happens to be in it -- but it is no longer the determining
    # factor for who gets a snapshot. A market that falls out of top-500 by
    # volume24hr is not "dropped": it is simply fetched individually
    # instead, same as before, just no longer framed as a special edge case.
    # Closes a real gap found the same day: market 1365861 had a silent,
    # unrecorded hole in its odds history (present cycle 1, missing cycle 2,
    # back for cycles 3-4) purely because it briefly fell out of the top-500
    # ranking -- contradicting the project's core thesis of a complete
    # open-to-resolution time-series for every tracked market.
    candidates_by_id = {m["id"]: m for m in candidates}
    open_registry_ids = {mid for mid, status in known_ids.items() if status == "open"}
    open_registry_ids |= {m["id"] for m in tracked_markets}

    resolved_markets = []
    snapshots_written = 0
    for market_id in open_registry_ids:
        market_data = candidates_by_id.get(market_id)
        if market_data is None:
            try:
                market_data = fetch_market_by_id(market_id)
            except Exception:
                continue  # transient API error -- leave status as-is, retry next cycle
            market_data["id"] = market_id  # markets/{id} response isn't guaranteed to echo id back
        if market_data.get("closed"):
            outcome_prices = market_data.get("outcomePrices")
            mark_registry_resolved(registry_table, market_id, outcome_prices, now_iso)
            resolved_markets.append({
                "market_id": market_id,
                "question": market_data.get("question", ""),
                "outcome_prices": outcome_prices,
            })
            continue
        append_odds_snapshot(market_data, now_iso)
        snapshots_written += 1

    total_latency_ms = fetch_latency_ms + llm_latency_ms
    payload = {
        "source": "polymarket",
        "ingested_at": now_iso,
        "candidate_count": len(candidates),
        "new_ids_this_cycle": len(new_candidates),
        "newly_tracked_count": len(tracked_markets),
        # market_id + question for each newly-tracked/resolved market, not
        # just a count -- send_digest (see tech_debt.md, "Bespoke Digest
        # Redesign") needs the actual identity of what changed this cycle,
        # not only how many changed, since the digest gets ingested into the
        # RAG corpus later and a bare count carries no retrievable signal.
        "newly_tracked_markets": [
            {"market_id": m["id"], "question": m.get("question", "")}
            for m in tracked_markets
        ],
        "odds_snapshots_written": snapshots_written,
        "resolved_this_cycle": len(resolved_markets),
        "resolved_markets": resolved_markets,
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

    # Chain into News only after the registry is fully written for this
    # cycle -- News/Comments depend on it (open markets, questions,
    # comment_entity_type) and running them before this point risks reading
    # a stale registry from the prior cycle. cycle_started_at travels
    # through the whole chain so every stage's S3 output lands under the
    # same hour partition.
    invoke_next_stage(now_iso)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "s3_key": s3_key,
            "candidate_count": len(candidates),
            "newly_tracked_count": len(tracked_markets),
            "resolved_this_cycle": len(resolved_markets),
            "llm_used": USE_LLM_ENRICHMENT,
        }),
    }
