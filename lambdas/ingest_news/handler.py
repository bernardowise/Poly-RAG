"""
Poly-RAG ingestion Lambda: News (10 curated RSS feeds).

Fetches all 10 feeds, tags each article by vertical (Macro/Geopolitics/Regulatory-Tech)
via keyword match, optionally enriches with a Bedrock/Claude summary (Opcion 1 trial
per CLAUDE.md Development Conventions), writes a single batched JSON to S3, and
records cost/latency metrics to DynamoDB for the architecture-decisions trial.
"""

import json
import os
import re
import time
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

import boto3
import urllib.request

S3_BUCKET = os.environ.get("S3_BUCKET", "poly-rag-369970405415")
METRICS_TABLE = os.environ.get("METRICS_TABLE", "poly-rag-architecture-metrics")
BEDROCK_MODEL_ID = os.environ.get("BEDROCK_MODEL_ID", "us.anthropic.claude-sonnet-4-5-20250929-v1:0")
USE_LLM_ENRICHMENT = os.environ.get("USE_LLM_ENRICHMENT", "true").lower() == "true"

# See infra_design.md for the full feed/vertical taxonomy and why each feed was picked.
RSS_FEEDS = [
    ("bbc_world", "https://feeds.bbci.co.uk/news/world/rss.xml"),
    ("bbc_business", "https://feeds.bbci.co.uk/news/business/rss.xml"),
    ("cbc_business", "https://www.cbc.ca/webfeed/rss/rss-business"),
    ("cbc_topstories", "https://www.cbc.ca/webfeed/rss/rss-topstories"),
    ("nyt_world", "https://rss.nytimes.com/services/xml/rss/nyt/World.xml"),
    ("nyt_opinion", "https://rss.nytimes.com/services/xml/rss/nyt/Opinion.xml"),
    ("nyt_technology", "https://rss.nytimes.com/services/xml/rss/nyt/Technology.xml"),
    ("cnn_topstories", "http://rss.cnn.com/rss/cnn_topstories.rss"),
    ("cnn_world", "http://rss.cnn.com/rss/cnn_world.rss"),
    ("france24_english", "https://www.france24.com/en/rss"),
]

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

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
bedrock = boto3.client("bedrock-runtime")


def fetch_feed(url):
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
    )
    with urllib.request.urlopen(req, timeout=10) as response:
        return response.read()


def parse_rss(xml_bytes, feed_name):
    articles = []
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return articles  # malformed feed this cycle -- skip, don't crash the whole Lambda

    for item in root.findall(".//item"):
        title = item.findtext("title", default="").strip()
        description = item.findtext("description", default="").strip()
        link = item.findtext("link", default="").strip()
        pub_date = item.findtext("pubDate", default="").strip()
        if not title:
            continue
        articles.append({
            "feed": feed_name,
            "title": title,
            "description": description,
            "link": link,
            "pubDate": pub_date,
        })
    return articles


def tag_verticals(title, description):
    text = f"{title} {description}".lower()
    matched = []
    for vertical, keywords in VERTICAL_KEYWORDS.items():
        for kw in keywords:
            pattern = r"\b" + re.escape(kw) + r"\b"
            if re.search(pattern, text):
                matched.append(vertical)
                break
    return matched


def enrich_with_bedrock(articles_batch):
    """
    Single Bedrock call summarizing up to 20 headlines across all feeds --
    keeps token cost bounded regardless of total article count fetched.
    """
    headlines = "\n".join(f"- [{a['feed']}] {a['title']}" for a in articles_batch[:20])
    prompt = (
        "Below are news headlines tagged by their source feed. In 3-5 bullet "
        "points, summarize the dominant themes across macro/central-bank, "
        "geopolitics, and regulatory/tech news relevant to prediction markets.\n\n"
        + headlines
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
    all_articles = []
    feeds_failed = []

    for feed_name, url in RSS_FEEDS:
        try:
            xml_bytes = fetch_feed(url)
            all_articles.extend(parse_rss(xml_bytes, feed_name))
        except Exception as e:
            # One feed failing (timeout, 403, etc.) must not sink the other 9.
            feeds_failed.append({"feed": feed_name, "error": str(e)})

    fetch_latency_ms = int((time.time() - fetch_start) * 1000)

    tagged_articles = []
    for a in all_articles:
        verticals = tag_verticals(a["title"], a["description"])
        if not verticals:
            continue
        a["verticals"] = verticals
        tagged_articles.append(a)

    llm_enrichment = None
    tokens_in, tokens_out, llm_latency_ms = 0, 0, 0
    if USE_LLM_ENRICHMENT and tagged_articles:
        llm_enrichment = enrich_with_bedrock(tagged_articles)
        tokens_in = llm_enrichment["tokens_in"]
        tokens_out = llm_enrichment["tokens_out"]
        llm_latency_ms = llm_enrichment["latency_ms"]

    now = datetime.now(timezone.utc)
    payload = {
        "source": "news",
        "ingested_at": now.isoformat(),
        "article_count": len(tagged_articles),
        "feeds_failed": feeds_failed,
        "articles": tagged_articles,
        "llm_summary": llm_enrichment["summary"] if llm_enrichment else None,
    }

    s3_key = f"news/{now.strftime('%Y-%m-%d')}/{now.strftime('%H')}.json"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=json.dumps(payload),
        ContentType="application/json",
    )

    write_metrics(
        source="ingest_news",
        llm_used=USE_LLM_ENRICHMENT,
        tokens_in=tokens_in,
        tokens_out=tokens_out,
        latency_ms=fetch_latency_ms + llm_latency_ms,
        items_processed=len(tagged_articles),
    )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "s3_key": s3_key,
            "article_count": len(tagged_articles),
            "feeds_failed": feeds_failed,
            "llm_used": USE_LLM_ENRICHMENT,
        }),
    }
