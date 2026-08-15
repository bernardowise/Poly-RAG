"""
Poly-RAG retrieval: time-window query (Layer 2 -- contextual/temporal).

See tech_debt.md, "Known Limitation: Explicit ID-Linkage Only Captures Direct
Correlation" for the design reasoning. This is complementary to, not a
replacement for, the high-confidence explicit market_id linkage that
ingest_news/ingest_bluesky already write (Layer 1 -- see those handlers).

Layer 1 answers: "what do we know FOR CERTAIN is about this market?"
Layer 2 (this module) answers: "what else happened in the world while this
market's odds were moving?" -- broader, noisier, no linkage required, since
every News/Bluesky item already carries an ingestion timestamp regardless of
whether it matched any market's search terms.

Usage (standalone script, not a Lambda -- this is a query utility for
exploration/debugging today; the Day 5 synthesis agent will call the same
logic, likely refactored into a shared module at that point):

    python retrieval/time_window.py --market-id 567688 --hours 12
"""

import argparse
import json
from datetime import datetime, timedelta, timezone

import boto3

S3_BUCKET = "poly-rag-369970405415"

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")


def get_market_odds_window(market_id, start, end):
    """Reads the market's odds time-series file and returns snapshots whose
    timestamp falls within [start, end]."""
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=f"odds/{market_id}.json")
        history = json.loads(obj["Body"].read())
    except s3.exceptions.NoSuchKey:
        return []

    snapshots = []
    for snap in history.get("snapshots", []):
        ts = datetime.fromisoformat(snap["timestamp"])
        if start <= ts <= end:
            snapshots.append(snap)
    return snapshots


def _list_source_objects_in_window(source, start, end):
    """Lists S3 objects under <source>/YYYY-MM-DD/ for each date the window
    spans, since the partition scheme is by date/hour, not a flat prefix."""
    keys = []
    current_date = start.date()
    while current_date <= end.date():
        prefix = f"{source}/{current_date.isoformat()}/"
        resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
        for obj in resp.get("Contents", []):
            keys.append(obj["Key"])
        current_date += timedelta(days=1)
    return keys


def get_items_in_window(source, item_field, start, end):
    """Fetches all News/Bluesky items (articles/posts) ingested within the
    time window, regardless of whether they carry a market_ids link -- this
    is the point: Layer 2 doesn't filter by linkage, only by time."""
    items = []
    for key in _list_source_objects_in_window(source, start, end):
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        payload = json.loads(obj["Body"].read())
        ingested_at = datetime.fromisoformat(payload["ingested_at"])
        if not (start <= ingested_at <= end):
            continue
        for item in payload.get(item_field, []):
            item["_source_key"] = key
            item["_ingested_at"] = payload["ingested_at"]
            items.append(item)
    return items


def retrieve(market_id, hours):
    """Layer 1 + Layer 2 combined result for a market's recent time window:
    - odds trajectory (from the market's own append-only S3 file)
    - Layer 1: News/Bluesky items explicitly linked to this market_id
    - Layer 2: every other News/Bluesky item ingested in the same window,
      ranked by nothing (no semantic similarity yet -- that's Day 4 work),
      just returned as raw candidates for the caller to reason over.
    """
    end = datetime.now(timezone.utc)
    start = end - timedelta(hours=hours)

    odds = get_market_odds_window(market_id, start, end)
    articles = get_items_in_window("news", "articles", start, end)
    posts = get_items_in_window("bluesky", "posts", start, end)

    linked_articles = [a for a in articles if market_id in a.get("market_ids", [])]
    linked_posts = [p for p in posts if market_id in p.get("market_ids", [])]
    ambient_articles = [a for a in articles if market_id not in a.get("market_ids", [])]
    ambient_posts = [p for p in posts if market_id not in p.get("market_ids", [])]

    return {
        "market_id": market_id,
        "window_start": start.isoformat(),
        "window_end": end.isoformat(),
        "odds_snapshots": odds,
        "layer1_linked": {
            "articles": linked_articles,
            "posts": linked_posts,
        },
        "layer2_ambient": {
            "article_count": len(ambient_articles),
            "post_count": len(ambient_posts),
            "articles": ambient_articles,
            "posts": ambient_posts,
        },
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--market-id", required=True)
    parser.add_argument("--hours", type=int, default=12)
    args = parser.parse_args()

    result = retrieve(args.market_id, args.hours)
    print(f"Market {result['market_id']} -- window {result['window_start']} to {result['window_end']}")
    print(f"Odds snapshots: {len(result['odds_snapshots'])}")
    print(f"Layer 1 (linked): {len(result['layer1_linked']['articles'])} articles, {len(result['layer1_linked']['posts'])} posts")
    print(f"Layer 2 (ambient, same window, not linked): {result['layer2_ambient']['article_count']} articles, {result['layer2_ambient']['post_count']} posts")
