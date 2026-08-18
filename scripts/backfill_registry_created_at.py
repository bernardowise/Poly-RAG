"""
One-off: backfill `created_at` (the market's real creation date) into every
registry item.

WHY THIS EXISTS
---------------
The registry's `first_seen` field records when a market entered OUR tracking (its
first appearance in a top-500-by-volume24hr pull), not when Polymarket actually
created it. A market can trend into our top-500 long after it was created -- the
Gamma API's own `createdAt` field is the only source of truth for that, and
`ingest_polymarket` already reads the same API response for everything else but
has never stored this particular field.

This is a real prerequisite, not a nice-to-have: the News temporal tiers (see
tech_debt.md, "Known Limitation: Explicit ID-Linkage", flags 3.1/3.2/3.3) compare
an article's `pubDate` against the market's `createdAt`, not against `first_seen`.
Without `created_at` stored on every registry item, tagging an article's tier
would require a live Gamma API call per market at ingestion time -- this backfill
makes it a plain DynamoDB read instead.

NOT PART OF THE 12H CYCLE
--------------------------
One-off, like the odds CLOB backfill and the comment_entity_type backfill before
it. A market's creation date never changes once set, so there is nothing to
re-fetch on a schedule. `ingest_polymarket` is extended SEPARATELY (a distinct,
smaller change) to write `created_at` for markets newly entering the registry
going forward -- this script only closes the gap for the 595 markets already
tracked before that extension exists.

SAFETY
------
Strictly additive: writes ONE new attribute (`created_at`) via DynamoDB
UpdateItem, touches no other field, adds/removes no registry item. Idempotent --
an item that already has `created_at` is left untouched, so re-running is a
no-op. Dry run is the default.

USAGE
-----
    python scripts/backfill_registry_created_at.py              # dry run
    python scripts/backfill_registry_created_at.py --limit 20   # dry run, first 20
    python scripts/backfill_registry_created_at.py --apply      # write to DynamoDB
"""

import argparse
import json
import time
import urllib.error
import urllib.request
from collections import Counter

import boto3

REGISTRY_TABLE = "poly-rag-market-registry"
GAMMA_MARKET_URL = "https://gamma-api.polymarket.com/markets/{market_id}"

# Path endpoint, not ?id= -- see scripts/backfill_odds_history.py for why: the
# ?id= filter query silently returns an empty list for closed=True markets,
# which would deny created_at to every resolved market in the registry.

USER_AGENT = "Mozilla/5.0 (compatible; poly-rag-backfill/1.0)"
REQUEST_TIMEOUT = 20
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 2

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(REGISTRY_TABLE)


def http_get_json(url):
    last_error = None
    for attempt in range(MAX_RETRIES):
        try:
            request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
            with urllib.request.urlopen(request, timeout=REQUEST_TIMEOUT) as response:
                return json.loads(response.read())
        except (urllib.error.URLError, json.JSONDecodeError, TimeoutError) as exc:
            last_error = exc
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_BACKOFF_SECONDS * (attempt + 1))
    raise last_error


def get_registry_items():
    items = []
    response = table.scan(ProjectionExpression="market_id, created_at")
    items.extend(response["Items"])
    while "LastEvaluatedKey" in response:
        response = table.scan(
            ProjectionExpression="market_id, created_at",
            ExclusiveStartKey=response["LastEvaluatedKey"],
        )
        items.extend(response["Items"])
    return items


def fetch_created_at(market_id):
    payload = http_get_json(GAMMA_MARKET_URL.format(market_id=market_id))
    market = payload[0] if isinstance(payload, list) else payload
    return market.get("createdAt")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="actually write to DynamoDB (default is a dry run that writes nothing)",
    )
    parser.add_argument("--limit", type=int, help="only process the first N items")
    args = parser.parse_args()

    items = get_registry_items()
    if args.limit:
        items = items[: args.limit]

    mode = "APPLY (writing to DynamoDB)" if args.apply else "DRY RUN (writing nothing)"
    print(f"=== Backfill registry created_at -- {mode} ===")
    print(f"Registry items to process: {len(items)}")
    print()

    stats = Counter()
    started = time.time()

    for position, item in enumerate(items, start=1):
        market_id = item["market_id"]

        if "created_at" in item:
            stats["already_tagged"] += 1
            continue

        try:
            created_at = fetch_created_at(market_id)
        except Exception as exc:
            print(f"  [{position}/{len(items)}] {market_id}: FETCH ERROR {type(exc).__name__}")
            stats["error"] += 1
            continue

        if not created_at:
            print(f"  [{position}/{len(items)}] {market_id}: no createdAt in Gamma response")
            stats["no_created_at"] += 1
            continue

        stats["backfilled"] += 1
        if position <= 5 or position % 100 == 0:
            print(f"  [{position}/{len(items)}] {market_id}: created_at = {created_at}")

        if args.apply:
            table.update_item(
                Key={"market_id": market_id},
                UpdateExpression="SET created_at = :created_at",
                ExpressionAttributeValues={":created_at": created_at},
            )

    elapsed = time.time() - started
    print()
    print("=== Summary ===")
    print(f"  backfilled:      {stats['backfilled']}")
    print(f"  already tagged:  {stats['already_tagged']}")
    print(f"  no createdAt:    {stats['no_created_at']}")
    print(f"  errors:          {stats['error']}")
    print(f"  elapsed:         {elapsed:.1f}s")
    if not args.apply:
        print()
        print("DRY RUN -- nothing was written. Re-run with --apply to write.")


if __name__ == "__main__":
    main()
