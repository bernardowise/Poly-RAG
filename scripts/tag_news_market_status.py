"""
One-off: tag every existing News article with market_status_at_publish
(open/closed), the market's status AS OF that article's pubDate.

WHY THIS EXISTS
---------------
Part of Day 4's post-resolution capture design (block D, 2026-08-18): the idea
was to capture what news/reaction looks like in the 4 cycles (48h) after a
market resolves. Early on the temptation was to invent a 4th temporal tier
(alongside 3.1/3.2/3.3) for "post-resolution" articles. Rejected deliberately --
an article published 30h after resolution still satisfies `pubDate >=
first_seen`, so it already lands in tier 3.3 (see tag_news_temporal_tier.py)
without any new tier. What actually differs is a SEPARATE axis: whether the
market could still move when the article was published. `temporal_tier` answers
"when, relative to the market's lifecycle" -- this field answers "was the
outcome still uncertain, or already fixed." Conflating the two into one field
would mean one attribute carrying two independent questions, which this project
has already been burned by once (see tech_debt.md, "Comments Source Replaces
Bluesky" -- the two-tier link_type that turned out to conflate two different
facts).

This distinction has real retrieval value: a 3.3 article with
market_status_at_publish="open" can genuinely explain an odds movement; a 3.3
article with market_status_at_publish="closed" cannot (there is no more
movement to explain) and instead represents reaction to a now-fixed outcome --
useful for a different kind of question ("how did people react when X
resolved") than "why did the price move."

RETROACTIVE, SAME REASONING AS EVERY OTHER TAG THIS SESSION
-------------------------------------------------------------
Existing articles get the same tag a future one would, since the underlying
facts (pubDate, resolution_date) do not change after the fact. Nothing deleted,
consistent with every other decision this session.

HOW STATUS IS DETERMINED
--------------------------
Uses the registry's `resolution_date` (when a market's status flipped from open
to resolved), not the current `status` field -- the CURRENT status describes
"where is this market today," not "where was it when this specific article was
published." A market resolved by now could easily have been open when an old
article was published.

  - No resolution_date on record (market currently open, or resolved but this
    field was never populated) -> "open" for every article, since the market
    was never seen to close.
  - pubDate < resolution_date -> "open" (article predates the outcome becoming
    fixed).
  - pubDate >= resolution_date -> "closed" (article postdates resolution).

NOT PART OF THE 12H CYCLE (for now)
------------------------------------
Same as temporal_tier: teaching `ingest_news` to compute this for NEW articles,
and to actually WIDEN its search to include recently-resolved markets for their
4-cycle post-resolution window (today it only searches status="open" markets --
a market that resolves stops being searched at all, so the very reaction this
field is meant to distinguish is not even being fetched yet), is deferred to
"F-lambdas" -- the batched set of ingest_polymarket/ingest_news changes for
markets going forward, alongside the odds-history and created_at extensions.
This script only retro-tags what ingest_news has already fetched under today's
open-only search.

USAGE
-----
    python scripts/tag_news_market_status.py              # dry run
    python scripts/tag_news_market_status.py --limit 1    # dry run, first cycle file
    python scripts/tag_news_market_status.py --apply      # write to S3
"""

import argparse
import json
import time
from collections import Counter
from email.utils import parsedate_to_datetime

import boto3

S3_BUCKET = "poly-rag-369970405415"
REGISTRY_TABLE = "poly-rag-market-registry"
NEWS_PREFIX = "news/"

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")


def get_registry_resolution_dates():
    """market_id -> resolution_date datetime, or None if never resolved."""
    table = dynamodb.Table(REGISTRY_TABLE)
    from datetime import datetime

    dates = {}
    response = table.scan(ProjectionExpression="market_id, resolution_date")
    items = response["Items"]
    while "LastEvaluatedKey" in response:
        response = table.scan(
            ProjectionExpression="market_id, resolution_date",
            ExclusiveStartKey=response["LastEvaluatedKey"],
        )
        items.extend(response["Items"])

    for item in items:
        resolution_date = item.get("resolution_date")
        dates[item["market_id"]] = (
            datetime.fromisoformat(resolution_date) if resolution_date else None
        )
    return dates


def classify_market_status(pub_date, resolution_date):
    """"open" or "closed" as of pub_date. Standalone, same reuse intent as
    classify_temporal_tier in tag_news_temporal_tier.py -- copy into
    ingest_news directly when F-lambdas is executed, this repo has no shared
    lib/ between Lambdas.
    """
    if resolution_date is None:
        return "open"
    return "closed" if pub_date >= resolution_date else "open"


def list_news_keys():
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=NEWS_PREFIX):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json") and "_batch" not in obj["Key"]:
                keys.append(obj["Key"])
    return keys


def tag_articles(articles, resolution_dates):
    """Classify by the market that puts the article in the MOST PERMISSIVE
    (open) state when more than one market_id applies -- an article is
    genuinely "open-market" reporting for any market where that is true,
    regardless of another linked market's status. Mirrors the
    most-permissive-tier logic in tag_news_temporal_tier.py's tag_articles.
    """
    tagged = []
    changed = 0
    status_counts = Counter()

    for article in articles:
        if "market_status_at_publish" in article:
            tagged.append(article)
            continue

        try:
            pub_date = parsedate_to_datetime(article["pubDate"])
        except (KeyError, TypeError, ValueError):
            tagged.append({**article, "market_status_at_publish": "unparseable_date"})
            changed += 1
            status_counts["unparseable_date"] += 1
            continue

        statuses = []
        for market_id in article.get("market_ids", []):
            if market_id not in resolution_dates:
                statuses.append("unknown_market")
            else:
                statuses.append(
                    classify_market_status(pub_date, resolution_dates[market_id])
                )

        # "open" wins if ANY linked market was still open at publish time.
        if "open" in statuses:
            best = "open"
        elif "closed" in statuses:
            best = "closed"
        else:
            best = "unknown_market"

        tagged.append({**article, "market_status_at_publish": best})
        changed += 1
        status_counts[best] += 1

    return tagged, changed, status_counts


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="actually write to S3 (default is a dry run that writes nothing)",
    )
    parser.add_argument("--limit", type=int, help="only process the first N cycle files")
    args = parser.parse_args()

    resolution_dates = get_registry_resolution_dates()
    keys = list_news_keys()
    if args.limit:
        keys = keys[: args.limit]

    mode = "APPLY (writing to S3)" if args.apply else "DRY RUN (writing nothing)"
    print(f"=== Tag News articles with market_status_at_publish -- {mode} ===")
    print(f"Registry markets loaded: {len(resolution_dates)}")
    print(f"  (with a resolution_date on record: "
          f"{sum(1 for v in resolution_dates.values() if v)})")
    print(f"News cycle files to process: {len(keys)}")
    print()

    stats = Counter()
    overall_status = Counter()
    total_tagged = 0
    started = time.time()

    for position, key in enumerate(keys, start=1):
        try:
            payload = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
        except Exception as exc:
            print(f"  [{position}/{len(keys)}] {key}: READ ERROR {type(exc).__name__}")
            stats["error"] += 1
            continue

        articles = payload.get("articles", [])
        tagged, changed, status_counts = tag_articles(articles, resolution_dates)
        overall_status.update(status_counts)

        if changed == 0:
            stats["already_tagged"] += 1
            continue

        total_tagged += changed
        stats["files_updated"] += 1
        print(
            f"  [{position}/{len(keys)}] {key}: {changed}/{len(articles)} tagged -- "
            + ", ".join(f"{status}={count}" for status, count in status_counts.most_common())
        )

        if args.apply:
            payload["articles"] = tagged
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=key,
                Body=json.dumps(payload),
                ContentType="application/json",
            )

    elapsed = time.time() - started
    print()
    print("=== Summary ===")
    print(f"  files updated:    {stats['files_updated']}")
    print(f"  already tagged:   {stats['already_tagged']}")
    print(f"  errors:           {stats['error']}")
    print(f"  articles tagged:  {total_tagged}")
    print(f"  status distribution: {dict(overall_status.most_common())}")
    print(f"  elapsed:          {elapsed:.1f}s")
    if not args.apply:
        print()
        print("DRY RUN -- nothing was written. Re-run with --apply to write.")


if __name__ == "__main__":
    main()
