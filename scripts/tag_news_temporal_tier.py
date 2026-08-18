"""
One-off: classify every existing News article into a temporal tier (3.1/3.2/3.3)
relative to the market(s) it is linked to, and write it back into the article.

WHY THIS EXISTS
---------------
An article's `pubDate` alone does not say whether it can be correlated against
odds movement -- that depends on which of the market's three life stages it falls
in, per the design decided 2026-08-18 (see tech_debt.md, "Known Limitation:
Explicit ID-Linkage"):

  3.1  pubDate < market's created_at
       Published before the market existed. No odds ever existed for this
       period -- capped at 1 year before created_at (see CAP_DAYS below), but
       never discarded past that: it is legitimate background context even
       though it cannot explain any price movement.
  3.2  created_at <= pubDate < first_seen
       Published after the market existed, but before WE started tracking it.
       Odds now exist for this window via the CLOB backfill
       (scripts/backfill_odds_history.py, source="clob_backfill") -- this tier
       is what that backfill rescues: before it, this window had no odds to
       correlate against at all.
  3.3  pubDate >= first_seen
       Published while we were actively tracking. Backed by full cycle
       snapshots (source="cycle": volume, volume24hr, liquidity all present),
       the highest-confidence tier.

This requires `created_at` on every registry item, which did not exist before
today -- see scripts/backfill_registry_created_at.py, run immediately before this
script as its hard prerequisite.

WHY THIS IS RETROACTIVE, NOT FORWARD-ONLY
------------------------------------------
Per the user's decision on odds backfill scope (2026-08-18): nothing gets
deleted, old content is legitimate context. Consistent with that, existing
articles are retro-tagged rather than left tier-less -- an article ingested
yesterday deserves the same classification as one ingested tomorrow, since the
underlying dates it depends on (pubDate, created_at, first_seen) do not change
after the fact.

NOT PART OF THE 12H CYCLE (for now)
------------------------------------
One-off script, same shape as every other backfill this session. Teaching
`ingest_news` to compute this tier at ingestion time for NEW articles going
forward is deliberately deferred as part of "F" (batched Lambda changes for new
registry entrants, alongside the odds-history-for-new-markets extension) --
mixing this into ingest_news today would mean touching that Lambda twice in one
session for two different reasons. THE CLASSIFICATION LOGIC BELOW
(`classify_temporal_tier`) is written standalone specifically so that future
change can reuse it verbatim rather than reinventing it -- this repo has no
shared lib/ module between Lambdas, so the intended path is a direct copy, not
an import.

SAFETY
------
Strictly additive: writes one new field (`temporal_tier`) per article, changes
no other article field, adds/removes no article, changes no news cycle file's
article COUNT. Idempotent -- an article that already carries `temporal_tier` is
left untouched. Dry run is the default.

CAP_DAYS
--------
Tier 3.1 has no lower bound in principle (an article could be published years
before a market existed) but the user capped relevance at 1 YEAR before
created_at, not before ingestion date -- see tech_debt.md. Articles older than
that are tagged `"too_old"` rather than `"3.1"`, so retrieval can trivially
exclude them without re-deriving the cutoff, while their body text is still kept
(nothing gets deleted per the same decision).

USAGE
-----
    python scripts/tag_news_temporal_tier.py              # dry run
    python scripts/tag_news_temporal_tier.py --limit 1     # dry run, first cycle file
    python scripts/tag_news_temporal_tier.py --apply       # write to S3
"""

import argparse
import json
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

import boto3

S3_BUCKET = "poly-rag-369970405415"
REGISTRY_TABLE = "poly-rag-market-registry"
NEWS_PREFIX = "news/"

CAP_DAYS = 365

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")


def get_registry_dates():
    """market_id -> (created_at, first_seen), both as timezone-aware datetimes."""
    table = dynamodb.Table(REGISTRY_TABLE)
    dates = {}
    response = table.scan(ProjectionExpression="market_id, created_at, first_seen")
    items = response["Items"]
    while "LastEvaluatedKey" in response:
        response = table.scan(
            ProjectionExpression="market_id, created_at, first_seen",
            ExclusiveStartKey=response["LastEvaluatedKey"],
        )
        items.extend(response["Items"])

    for item in items:
        if "created_at" not in item or "first_seen" not in item:
            continue
        dates[item["market_id"]] = (
            datetime.fromisoformat(item["created_at"].replace("Z", "+00:00")),
            datetime.fromisoformat(item["first_seen"]),
        )
    return dates


def classify_temporal_tier(pub_date, created_at, first_seen):
    """Return one of "too_old", "3.1", "3.2", "3.3" for a single market_id.

    Standalone on purpose -- see the module docstring's "F" note. Takes plain
    datetimes so it has no S3/DynamoDB dependency and can be copied into
    ingest_news later without dragging this script's I/O along with it.
    """
    cap = created_at - timedelta(days=CAP_DAYS)
    if pub_date < cap:
        return "too_old"
    if pub_date < created_at:
        return "3.1"
    if pub_date < first_seen:
        return "3.2"
    return "3.3"


def list_news_keys():
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=NEWS_PREFIX):
        for obj in page.get("Contents", []):
            # Skip intermediate fan-out batch files -- only final cycle payloads
            # carry the complete, de-duplicated article list.
            if obj["Key"].endswith(".json") and "_batch" not in obj["Key"]:
                keys.append(obj["Key"])
    return keys


def tag_articles(articles, registry_dates):
    """Classify each article by the EARLIEST-relevant of its linked markets.

    An article can carry more than one market_id in principle (the schema
    supports a list, even though the per-market Google News redesign makes
    every current article 1:1 -- see tech_debt.md). When more than one applies,
    the article is tagged against whichever market makes it MOST reachable
    (3.3 > 3.2 > 3.1 > too_old), since the article is genuinely relevant to that
    market regardless of how it classifies against another.
    """
    tier_rank = {"3.3": 3, "3.2": 2, "3.1": 1, "too_old": 0, "unknown_market": -1}
    tagged = []
    changed = 0
    tier_counts = Counter()

    for article in articles:
        if "temporal_tier" in article:
            tagged.append(article)
            continue

        try:
            pub_date = parsedate_to_datetime(article["pubDate"])
        except (KeyError, TypeError, ValueError):
            tagged.append({**article, "temporal_tier": "unparseable_date"})
            changed += 1
            tier_counts["unparseable_date"] += 1
            continue

        best_tier = None
        for market_id in article.get("market_ids", []):
            if market_id not in registry_dates:
                candidate = "unknown_market"
            else:
                created_at, first_seen = registry_dates[market_id]
                candidate = classify_temporal_tier(pub_date, created_at, first_seen)
            if best_tier is None or tier_rank[candidate] > tier_rank[best_tier]:
                best_tier = candidate

        best_tier = best_tier or "unknown_market"
        tagged.append({**article, "temporal_tier": best_tier})
        changed += 1
        tier_counts[best_tier] += 1

    return tagged, changed, tier_counts


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="actually write to S3 (default is a dry run that writes nothing)",
    )
    parser.add_argument("--limit", type=int, help="only process the first N cycle files")
    args = parser.parse_args()

    registry_dates = get_registry_dates()
    keys = list_news_keys()
    if args.limit:
        keys = keys[: args.limit]

    mode = "APPLY (writing to S3)" if args.apply else "DRY RUN (writing nothing)"
    print(f"=== Tag News articles with temporal_tier -- {mode} ===")
    print(f"Registry markets with created_at+first_seen: {len(registry_dates)}")
    print(f"News cycle files to process: {len(keys)}")
    print()

    stats = Counter()
    overall_tiers = Counter()
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
        tagged, changed, tier_counts = tag_articles(articles, registry_dates)
        overall_tiers.update(tier_counts)

        if changed == 0:
            stats["already_tagged"] += 1
            continue

        total_tagged += changed
        stats["files_updated"] += 1
        print(
            f"  [{position}/{len(keys)}] {key}: {changed}/{len(articles)} tagged -- "
            + ", ".join(f"{tier}={count}" for tier, count in tier_counts.most_common())
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
    print(f"  tier distribution: {dict(overall_tiers.most_common())}")
    print(f"  elapsed:          {elapsed:.1f}s")
    if not args.apply:
        print()
        print("DRY RUN -- nothing was written. Re-run with --apply to write.")


if __name__ == "__main__":
    main()
