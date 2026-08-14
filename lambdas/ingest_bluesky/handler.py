"""
Poly-RAG ingestion Lambda: Bluesky (AT Protocol).

Queries app.bsky.feed.searchPosts once per vertical using a representative keyword,
tags results by vertical, optionally enriches with a Bedrock/Claude summary (Opcion 1
trial per CLAUDE.md Development Conventions), writes a single batched JSON to S3,
and records cost/latency metrics to DynamoDB.

NOTE: searchPosts requires authentication as of 2026-08-13 (confirmed by direct
reproduction -- returns 403 unauthenticated even though other read endpoints like
getProfile remain open). Authenticates via com.atproto.server.createSession using
an app password (not the account password) at the start of each invocation --
simpler than persisting/refreshing short-lived JWTs for a 12h-cadence job. Still
uses the REST search endpoint, NOT the firehose/Jetstream, which requires a
persistent WebSocket connection incompatible with a short-lived Lambda. See
tech_debt.md for why Reddit/X/Truth Social were rejected in favor of Bluesky.
"""

import json
import os
import re
import time
import uuid
from datetime import datetime, timezone

import boto3
import urllib.request
import urllib.parse
import urllib.error

S3_BUCKET = os.environ.get("S3_BUCKET", "poly-rag-369970405415")
METRICS_TABLE = os.environ.get("METRICS_TABLE", "poly-rag-architecture-metrics")
BEDROCK_MODEL_ID = os.environ.get("BEDROCK_MODEL_ID", "us.anthropic.claude-sonnet-4-5-20250929-v1:0")
USE_LLM_ENRICHMENT = os.environ.get("USE_LLM_ENRICHMENT", "true").lower() == "true"
BLUESKY_HANDLE = os.environ.get("BLUESKY_HANDLE")
BLUESKY_APP_PASSWORD = os.environ.get("BLUESKY_APP_PASSWORD")

BLUESKY_PDS_URL = "https://bsky.social"
# NOTE: searchPosts must be called against bsky.social (the PDS), not
# public.api.bsky.app -- the public read-mirror returns 403 on this specific
# endpoint even with a valid Bearer token (confirmed 2026-08-14).
BLUESKY_SEARCH_URL = "https://bsky.social/xrpc/app.bsky.feed.searchPosts"

# One representative search query per vertical (searchPosts is a single-keyword-per-call
# API, not a multi-keyword OR filter like our regex approach for Polymarket/News) --
# so verticals here are queried directly rather than tagged post-hoc.
VERTICAL_QUERIES = {
    "macro": "Federal Reserve inflation",
    "geopolitics": "Ukraine election Trump",
    "regulatory_tech": "FDA antitrust AI regulation",
}

RESULTS_PER_QUERY = 25

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
bedrock = boto3.client("bedrock-runtime")


def create_session():
    """
    Authenticate with an app password (not the account password) to get a
    short-lived access JWT. Re-run once per Lambda invocation rather than
    persisting/refreshing tokens -- simplest correct approach at a 12h cadence.
    """
    url = f"{BLUESKY_PDS_URL}/xrpc/com.atproto.server.createSession"
    body = json.dumps({
        "identifier": BLUESKY_HANDLE,
        "password": BLUESKY_APP_PASSWORD,
    }).encode()
    req = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json", "User-Agent": "poly-rag-ingestion/1.0"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as response:
        session = json.loads(response.read().decode())
    return session["accessJwt"]


def search_posts(query, access_jwt, limit=RESULTS_PER_QUERY):
    params = urllib.parse.urlencode({"q": query, "limit": limit, "sort": "top"})
    url = f"{BLUESKY_SEARCH_URL}?{params}"
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "poly-rag-ingestion/1.0",
            "Authorization": f"Bearer {access_jwt}",
        },
    )
    with urllib.request.urlopen(req, timeout=10) as response:
        return json.loads(response.read().decode())


def extract_posts(search_response, vertical):
    posts = []
    for item in search_response.get("posts", []):
        record = item.get("record", {})
        text = record.get("text", "")
        if not text:
            continue
        posts.append({
            "uri": item.get("uri"),
            "author_handle": item.get("author", {}).get("handle"),
            "text": text,
            "created_at": record.get("createdAt"),
            "like_count": item.get("likeCount", 0),
            "repost_count": item.get("repostCount", 0),
            "verticals": [vertical],
        })
    return posts


def enrich_with_bedrock(posts_batch):
    """
    Single Bedrock call summarizing up to 20 posts across all verticals --
    keeps token cost bounded regardless of total post count fetched.
    """
    sample = "\n".join(f"- [{p['verticals'][0]}] {p['text'][:200]}" for p in posts_batch[:20])
    prompt = (
        "Below are Bluesky posts tagged by vertical (macro/central-bank, "
        "geopolitics, regulatory/tech). In 3-5 bullet points, summarize the "
        "dominant sentiment and themes relevant to prediction market research.\n\n"
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
    fetch_start = time.time()
    all_posts = []
    queries_failed = []

    try:
        access_jwt = create_session()
    except Exception as e:
        # Auth failure blocks all 3 verticals -- report clearly instead of a
        # confusing per-vertical 401 for each query.
        return {
            "statusCode": 500,
            "body": json.dumps({"error": f"Bluesky authentication failed: {e}"}),
        }

    for vertical, query in VERTICAL_QUERIES.items():
        try:
            resp = search_posts(query, access_jwt)
            all_posts.extend(extract_posts(resp, vertical))
        except Exception as e:
            # One vertical's query failing must not sink the other two.
            queries_failed.append({"vertical": vertical, "error": str(e)})

    fetch_latency_ms = int((time.time() - fetch_start) * 1000)

    llm_enrichment = None
    tokens_in, tokens_out, llm_latency_ms = 0, 0, 0
    if USE_LLM_ENRICHMENT and all_posts:
        llm_enrichment = enrich_with_bedrock(all_posts)
        tokens_in = llm_enrichment["tokens_in"]
        tokens_out = llm_enrichment["tokens_out"]
        llm_latency_ms = llm_enrichment["latency_ms"]

    now = datetime.now(timezone.utc)
    total_latency_ms = fetch_latency_ms + llm_latency_ms
    payload = {
        "source": "bluesky",
        "ingested_at": now.isoformat(),
        "post_count": len(all_posts),
        "queries_failed": queries_failed,
        "posts": all_posts,
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

    s3_key = f"bluesky/{now.strftime('%Y-%m-%d')}/{now.strftime('%H')}.json"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=json.dumps(payload),
        ContentType="application/json",
    )

    write_metrics(
        source="ingest_bluesky",
        llm_used=USE_LLM_ENRICHMENT,
        tokens_in=tokens_in,
        tokens_out=tokens_out,
        latency_ms=total_latency_ms,
        items_processed=len(all_posts),
    )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "s3_key": s3_key,
            "post_count": len(all_posts),
            "queries_failed": queries_failed,
            "llm_used": USE_LLM_ENRICHMENT,
        }),
    }
