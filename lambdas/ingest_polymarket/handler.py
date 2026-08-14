"""
Poly-RAG ingestion Lambda: Polymarket Gamma API.

Fetches active markets, tags them by vertical (Macro/Geopolitics/Regulatory-Tech)
via keyword match, optionally enriches with a Bedrock/Claude summary (Opcion 1
trial per CLAUDE.md Development Conventions), writes a single batched JSON to S3,
and records cost/latency metrics to DynamoDB for the architecture-decisions trial.
"""

import json
import os
import re
import time
import uuid
from datetime import datetime, timezone

import boto3
import urllib.request

S3_BUCKET = os.environ.get("S3_BUCKET", "poly-rag-369970405415")
METRICS_TABLE = os.environ.get("METRICS_TABLE", "poly-rag-architecture-metrics")
BEDROCK_MODEL_ID = os.environ.get("BEDROCK_MODEL_ID", "us.anthropic.claude-sonnet-4-5-20250929-v1:0")
USE_LLM_ENRICHMENT = os.environ.get("USE_LLM_ENRICHMENT", "true").lower() == "true"

POLYMARKET_API_URL = "https://gamma-api.polymarket.com/markets?limit=100&active=true&order=volume&ascending=false"

VERTICAL_KEYWORDS = {
    "macro": [
        "fed", "federal reserve", "interest rate", "inflation", "cpi",
        "unemployment", "recession", "gdp", "trump", "truth social",
    ],
    "geopolitics": [
        "election", "president", "war", "ceasefire", "sanctions", "tariff",
        "nato", "invasion", "trump", "putin", "xi", "ukraine", "taiwan", "china",
    ],
    "regulatory_tech": [
        "fda", "antitrust", "lawsuit", "sec", "regulation", "approval", "ban",
        "ai regulation", "google", "apple", "meta", "openai", "anthropic", "spacex",
    ],
}

# Sports markets dominate Polymarket by volume and generate false-vertical-positive
# matches (e.g. "SEC" matching mid-word inside a soccer resolution-rules paragraph,
# or team names accidentally containing a country/company keyword). Excluded outright
# before vertical tagging runs, regardless of any keyword match.
SPORTS_EXCLUSION_PATTERNS = [
    r"\bvs\.?\b", r"\bfc\b", r"\bnfl\b", r"\bnba\b", r"\bnhl\b", r"\bmlb\b",
    r"\bmls\b", r"\bo/u\b", r"\bover/under\b", r"\b\d+(st|nd|rd|th) half\b",
    r"\bexact score\b", r"\btotal corners\b", r"\bboth teams to score\b",
    r"\bcomeback player\b", r"\bmvp\b", r"\bbo\d\b", r"\bregular season\b",
]
_SPORTS_RE = re.compile("|".join(SPORTS_EXCLUSION_PATTERNS), re.IGNORECASE)

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
bedrock = boto3.client("bedrock-runtime")


def fetch_markets():
    req = urllib.request.Request(
        POLYMARKET_API_URL,
        headers={"User-Agent": "poly-rag-ingestion/1.0"},
    )
    with urllib.request.urlopen(req, timeout=10) as response:
        return json.loads(response.read().decode())


def is_sports_market(question):
    # Checked against the question only, not description -- descriptions are long
    # rules paragraphs prone to incidental word matches (e.g. team names, "second half").
    return bool(_SPORTS_RE.search(question))


def tag_verticals(question, description):
    if is_sports_market(question):
        return []

    text = f"{question} {description}".lower()
    matched = []
    for vertical, keywords in VERTICAL_KEYWORDS.items():
        for kw in keywords:
            # Word-boundary match: "sec" must not match inside "second".
            pattern = r"\b" + re.escape(kw) + r"\b"
            if re.search(pattern, text):
                matched.append(vertical)
                break
    return matched


def enrich_with_bedrock(markets_batch):
    """
    Single Bedrock call summarizing the whole batch (not one call per market) --
    keeps token cost bounded regardless of how many markets came back.
    """
    questions = "\n".join(f"- {m['question']}" for m in markets_batch[:20])
    prompt = (
        "Below are prediction market questions from Polymarket. In 3-5 bullet "
        "points, summarize the dominant themes and any notable probability "
        "extremes (very high or very low confidence outcomes) worth flagging "
        "for a research analyst.\n\n" + questions
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


def lambda_handler(event, context):
    fetch_start = time.time()
    raw_markets = fetch_markets()
    fetch_latency_ms = int((time.time() - fetch_start) * 1000)

    tagged_markets = []
    for m in raw_markets:
        verticals = tag_verticals(m.get("question", ""), m.get("description", ""))
        if not verticals:
            continue  # out of scope: no vertical match, drop (pop-culture/noise filter)
        tagged_markets.append({
            "id": m.get("id"),
            "question": m.get("question"),
            "outcomes": m.get("outcomes"),
            "outcomePrices": m.get("outcomePrices"),
            "volume": m.get("volumeNum"),
            "volume24hr": m.get("volume24hr"),
            "liquidity": m.get("liquidityNum"),
            "endDate": m.get("endDate"),
            "updatedAt": m.get("updatedAt"),
            "verticals": verticals,
        })

    llm_enrichment = None
    tokens_in, tokens_out, llm_latency_ms = 0, 0, 0
    if USE_LLM_ENRICHMENT and tagged_markets:
        llm_enrichment = enrich_with_bedrock(tagged_markets)
        tokens_in = llm_enrichment["tokens_in"]
        tokens_out = llm_enrichment["tokens_out"]
        llm_latency_ms = llm_enrichment["latency_ms"]

    now = datetime.now(timezone.utc)
    total_latency_ms = fetch_latency_ms + llm_latency_ms
    payload = {
        "source": "polymarket",
        "ingested_at": now.isoformat(),
        "market_count": len(tagged_markets),
        "markets": tagged_markets,
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
        items_processed=len(tagged_markets),
    )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "s3_key": s3_key,
            "market_count": len(tagged_markets),
            "llm_used": USE_LLM_ENRICHMENT,
        }),
    }
