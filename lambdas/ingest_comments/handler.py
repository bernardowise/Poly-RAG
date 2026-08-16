"""
Poly-RAG ingestion Lambda: Polymarket Comments (sentiment source).

Replaces Bluesky (2026-08-16, see tech_debt.md "Comments Source Replaces
Bluesky") as the project's sentiment source. Root cause of the switch:
Bluesky's searchPosts results were dominated by bot accounts republishing
Polymarket's own odds feed ("YES 19% -> 49%... Polymarket") rather than
genuine human sentiment -- circular signal, not independent reaction.
Polymarket's own comment sections (`gamma-api.polymarket.com/comments`,
public, no auth, documented at docs.polymarket.com) are real trader
discussion instead.

Comments hang off an Event or a Series, not the market itself. Each market
in the registry carries comment_entity_type/comment_entity_id (set by
ingest_polymarket, see get_comment_link there) pointing at whichever level
actually has comments -- Event preferred (comments are specific to that one
market), falling back to Series only if the Event has none.

Three link_type levels, not two (revised 2026-08-16 same day after finding
the first version's bug in production -- see tech_debt.md):
- "direct": Event-level comments AND exactly one open market behind that
  event. Genuinely 1:1 -- the comment is reachable only from this one
  market's page and about nothing else.
- "shared_event": Event-level comments but the event has SEVERAL open
  markets (e.g. a tournament with one market per team, all sharing one
  comment section). Found empirically: 802/1698 Event-linked comments in
  the first production run had multiple market_ids, which broke "direct"'s
  1:1 promise. Comment applies to the whole event, not attributable to any
  single market within it.
- "shared_series": comments hang off a Series, shared across EVERY market
  in that series, often unrelated events entirely (verified directly: two
  different MLB games' pages showed the identical comment thread, including
  comments about a THIRD unrelated game).
All three tag every applicable market_id rather than picking one arbitrarily
-- collapsing shared_event/shared_series down to a single market_id would
overstate the linkage's precision.
"""

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections import defaultdict
from datetime import datetime, timezone

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "poly-rag-369970405415")
METRICS_TABLE = os.environ.get("METRICS_TABLE", "poly-rag-architecture-metrics")
REGISTRY_TABLE = os.environ.get("REGISTRY_TABLE", "poly-rag-market-registry")
BEDROCK_MODEL_ID = os.environ.get("BEDROCK_MODEL_ID", "us.anthropic.claude-sonnet-4-5-20250929-v1:0")
USE_LLM_ENRICHMENT = os.environ.get("USE_LLM_ENRICHMENT", "true").lower() == "true"
# Strict chaining (2026-08-16, see tech_debt.md "Strict Ingestion
# Chaining"): the final stage before the digest -- Comments always
# completes in one invocation (no fan-out like News), so unlike ingest_news
# this can simply invoke at the end of lambda_handler, no "am I the last
# batch" check needed.
NEXT_LAMBDA_NAME = os.environ.get("NEXT_LAMBDA_NAME", "poly-rag-send-digest")

GAMMA_COMMENTS_URL = "https://gamma-api.polymarket.com/comments"
COMMENTS_PER_ENTITY = 20
MAX_RETRIES = 3
BASE_BACKOFF_SECONDS = 2

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
bedrock = boto3.client("bedrock-runtime")
lambda_client = boto3.client("lambda")


def invoke_next_stage():
    lambda_client.invoke(
        FunctionName=NEXT_LAMBDA_NAME,
        InvocationType="Event",
        Payload=json.dumps({}),
    )


def get_open_markets_with_comment_links(table):
    """Scans the registry for open markets carrying a comment_entity_type/id
    (set by ingest_polymarket -- markets with neither Event nor Series
    comments are skipped, comment_entity_type is None for those)."""
    markets = []
    resp = table.scan(
        FilterExpression="#s = :open",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={":open": "open"},
        ProjectionExpression="market_id, comment_entity_type, comment_entity_id, comment_link_type",
    )
    markets.extend(resp.get("Items", []))
    while "LastEvaluatedKey" in resp:
        resp = table.scan(
            FilterExpression="#s = :open",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":open": "open"},
            ProjectionExpression="market_id, comment_entity_type, comment_entity_id, comment_link_type",
            ExclusiveStartKey=resp["LastEvaluatedKey"],
        )
        markets.extend(resp.get("Items", []))
    return [
        m for m in markets
        if m.get("comment_entity_type") and m.get("comment_entity_id")
    ]


def group_by_entity(markets):
    """Groups markets by (entity_type, entity_id) so a shared Series is
    fetched from the API once, not once per market that shares it."""
    groups = defaultdict(list)
    for m in markets:
        key = (m["comment_entity_type"], m["comment_entity_id"])
        groups[key].append(m["market_id"])
    return groups


def fetch_comments(entity_type, entity_id, limit=COMMENTS_PER_ENTITY):
    """Calls the public, unauthenticated Gamma API comments endpoint, with
    retry/backoff on 429 -- documented rate limit for this endpoint
    specifically is 200 req/10s (tighter than the general Gamma API's 4000
    req/10s), so backoff matters more here than it did for market/odds
    calls."""
    params = urllib.parse.urlencode({
        "parent_entity_type": entity_type,
        "parent_entity_id": entity_id,
        "limit": limit,
        "order": "createdAt",
        "ascending": "false",
    })
    url = f"{GAMMA_COMMENTS_URL}?{params}"
    req = urllib.request.Request(url, headers={"User-Agent": "poly-rag-ingestion/1.0"})

    for attempt in range(MAX_RETRIES + 1):
        try:
            with urllib.request.urlopen(req, timeout=10) as response:
                return json.loads(response.read().decode())
        except urllib.error.HTTPError as e:
            if e.code == 429 and attempt < MAX_RETRIES:
                backoff = BASE_BACKOFF_SECONDS * (2 ** attempt)
                print(f"[fetch_comments] rate limited (429), retrying in {backoff}s (attempt {attempt + 1}/{MAX_RETRIES})")
                time.sleep(backoff)
                continue
            raise


def extract_comments(raw_comments, market_ids, link_type):
    comments = []
    for c in raw_comments:
        body = c.get("body", "")
        if not body:
            continue
        profile = c.get("profile") or {}
        comments.append({
            "comment_id": c.get("id"),
            "author": profile.get("name") or c.get("userAddress"),
            "text": body,
            "created_at": c.get("createdAt"),
            "reaction_count": c.get("reactionCount", 0),
            "market_ids": list(market_ids),
            "link_type": link_type,
        })
    return comments


def enrich_with_bedrock(comments_batch):
    """Single Bedrock call summarizing up to 20 comments -- keeps token cost
    bounded regardless of total comment count fetched."""
    sample = "\n".join(f"- {c['text'][:200]}" for c in comments_batch[:20])
    prompt = (
        "Below are trader comments from Polymarket prediction market pages. "
        "In 3-5 bullet points, summarize the dominant sentiment and themes "
        "relevant to prediction market research.\n\n"
        + sample
    )

    start = time.time()
    response = bedrock.invoke_model(
        modelId=BEDROCK_MODEL_ID,
        body=json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 400,
            "messages": [{"role": "user", "content": prompt}],
        }),
    )
    latency_ms = int((time.time() - start) * 1000)

    body = json.loads(response["body"].read())
    summary_text = body["content"][0]["text"]
    usage = body.get("usage", {})

    return {
        "summary": summary_text,
        "tokens_in": usage.get("input_tokens", 0),
        "tokens_out": usage.get("output_tokens", 0),
        "latency_ms": latency_ms,
    }


def estimate_cost_usd(tokens_in, tokens_out):
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


def lambda_handler(event, context):
    registry_table = dynamodb.Table(REGISTRY_TABLE)
    markets = get_open_markets_with_comment_links(registry_table)
    entity_groups = group_by_entity(markets)

    fetch_start = time.time()
    all_comments = []
    entities_failed = []

    for (entity_type, entity_id), market_ids in entity_groups.items():
        # "direct" requires BOTH Event-level AND exactly one market behind
        # that event -- an Event with several markets (e.g. a tournament
        # with one market per team) means the comment is visible from, and
        # presumably about, the whole event, not any single market inside
        # it. Confirmed in production (2026-08-16): 802/1698 Event-linked
        # comments actually had multiple market_ids, which "direct" was
        # supposed to rule out by definition. See tech_debt.md.
        if entity_type == "Event" and len(market_ids) == 1:
            link_type = "direct"
        elif entity_type == "Event":
            link_type = "shared_event"
        else:
            link_type = "shared_series"
        try:
            raw = fetch_comments(entity_type, entity_id)
            all_comments.extend(extract_comments(raw, market_ids, link_type))
        except Exception as e:
            # One entity's fetch failing must not sink the rest.
            entities_failed.append({"entity_type": entity_type, "entity_id": entity_id, "error": str(e)})

    fetch_latency_ms = int((time.time() - fetch_start) * 1000)

    llm_enrichment = None
    tokens_in, tokens_out, llm_latency_ms = 0, 0, 0
    if USE_LLM_ENRICHMENT and all_comments:
        llm_enrichment = enrich_with_bedrock(all_comments)
        tokens_in = llm_enrichment["tokens_in"]
        tokens_out = llm_enrichment["tokens_out"]
        llm_latency_ms = llm_enrichment["latency_ms"]

    now = datetime.now(timezone.utc)
    total_latency_ms = fetch_latency_ms + llm_latency_ms
    payload = {
        "source": "comments",
        "ingested_at": now.isoformat(),
        "comment_count": len(all_comments),
        "markets_with_comments": len(markets),
        "entities_queried": len(entity_groups),
        "entities_failed": entities_failed,
        "comments": all_comments,
        "llm_summary": llm_enrichment["summary"] if llm_enrichment else None,
        "metadata": {
            "schema_version": "v1",
            "lambda_name": context.function_name,
            "lambda_request_id": context.aws_request_id,
            "llm_used": USE_LLM_ENRICHMENT,
            "llm_model_id": BEDROCK_MODEL_ID if USE_LLM_ENRICHMENT else None,
            "latency_ms": total_latency_ms,
            "estimated_cost_usd": estimate_cost_usd(tokens_in, tokens_out),
        },
    }

    s3_key = f"comments/{now.strftime('%Y-%m-%d')}/{now.strftime('%H')}.json"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=json.dumps(payload),
        ContentType="application/json",
    )

    write_metrics(
        source="ingest_comments",
        llm_used=USE_LLM_ENRICHMENT,
        tokens_in=tokens_in,
        tokens_out=tokens_out,
        latency_ms=total_latency_ms,
        items_processed=len(all_comments),
    )

    # Last stage before the digest -- see tech_debt.md "Strict Ingestion
    # Chaining".
    invoke_next_stage()

    return {
        "statusCode": 200,
        "body": json.dumps({
            "s3_key": s3_key,
            "comment_count": len(all_comments),
            "markets_with_comments": len(markets),
            "entities_queried": len(entity_groups),
            "entities_failed": entities_failed,
            "llm_used": USE_LLM_ENRICHMENT,
        }),
    }
