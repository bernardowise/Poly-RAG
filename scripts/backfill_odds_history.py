"""
One-off backfill of pre-tracking odds history from Polymarket's CLOB API.

WHY THIS EXISTS
---------------
Poly-RAG's odds time-series only starts when a market first entered our registry
(first cycle: 2026-08-16). But Polymarket exposes each market's FULL price history
from creation, free and without auth, via clob.polymarket.com/prices-history.
Backfilling it deepens the project's core differentiator: the time-series stops
being "since we started watching" and becomes "since the market was created."

NOT PART OF THE 12H CYCLE, DELIBERATELY
---------------------------------------
A market's pre-tracking history is immutable, so re-fetching it every cycle would
burn ~600 API calls twice a day for data that cannot change. This runs manually,
once, like the registry bootstrap and the comment_entity_type backfill before it.
The chained Lambdas keep appending forward snapshots exactly as they do today;
this script only ever writes to the past.

WHY IT DOES NOT CORRUPT THE "COMPLETE CYCLE" INVARIANT
------------------------------------------------------
On 2026-08-17 the registry and odds files were purged of anything that did not
enter via one of the real complete cycles -- the problem being that debug-run
snapshots were INDISTINGUISHABLE from cycle snapshots. Backfilled points are
made impossible to confuse, by construction:

  1. Every backfilled point carries source="clob_backfill".
     Cycle snapshots have no "source" key at all (absence == cycle origin), so
     existing analysis keeps working untouched and `source != 'clob_backfill'`
     isolates the real cycle dataset exactly.
  2. Backfilled points carry NO volume/volume24hr/liquidity -- the CLOB history
     endpoint does not return them. They are structurally thinner on sight.
  3. A HARD CUTOFF refuses to write any point at or after the first real cycle
     (CYCLE_BOUNDARY). The backfill can only ever touch the past.
  4. Every point carries backfilled_at, so a later run can be told apart from
     this one, and all backfilled data can be stripped back out by that tag.

Backfilled points are daily (fidelity=1440); cycle snapshots are 12-hourly. The
CLOB endpoint only serves hourly fidelity for the last ~30 days, whereas daily
reaches back to market creation -- and our own cycle data already covers the
recent window at higher resolution, so daily is the right choice here.

RELATIONSHIP TO THE NEWS TEMPORAL FLAGS
---------------------------------------
This is what rescues flag 3.2 (news published after a market was created but
before we started tracking it). Before this backfill there were no odds to
correlate that news against; after it there are, at daily resolution. Note the
snapshot `source` field and the news tier flag are DIFFERENT axes on different
objects: the tier says which era an article falls in, `source` says what quality
of odds evidence backs it.

USAGE
-----
    python scripts/backfill_odds_history.py                  # dry run, writes nothing
    python scripts/backfill_odds_history.py --limit 20       # dry run, first 20 markets
    python scripts/backfill_odds_history.py --apply          # actually write to S3

Dry run is the DEFAULT and prints exactly what would be added per market.
"""

import argparse
import json
import time
import urllib.error
import urllib.request
from collections import Counter
from datetime import datetime, timezone

import boto3

S3_BUCKET = "poly-rag-369970405415"
REGISTRY_TABLE = "poly-rag-market-registry"

# Path endpoint, NOT the ?id= filter query. Measured 2026-08-18: `?id=` returns an
# empty list for markets with closed=True (e.g. 3619197, 3625104 -- same-day tennis
# matches that resolved almost immediately), which silently looked like "this market
# has no clobTokenIds" and would have denied history to every resolved market. The
# path endpoint returns them correctly, and is the same one ingest_polymarket already
# uses for its resolution checks.
GAMMA_MARKET_URL = "https://gamma-api.polymarket.com/markets/{market_id}"
CLOB_HISTORY_URL = (
    "https://clob.polymarket.com/prices-history?market={token}&interval=max&fidelity=1440"
)

# Hard safety boundary: the first real complete cycle. Nothing at or after this
# instant may be written by this script -- that window belongs to the cycles.
CYCLE_BOUNDARY = datetime(2026, 8, 16, 0, 0, 0, tzinfo=timezone.utc)

# Public endpoints reject the default urllib agent.
USER_AGENT = "Mozilla/5.0 (compatible; poly-rag-backfill/1.0)"
REQUEST_TIMEOUT = 20
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 2
POLITE_DELAY_SECONDS = 0.1

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")


def http_get_json(url):
    """GET with retry/backoff. Public no-auth endpoints, but they rate-limit."""
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


def get_registry_markets():
    """Every market in the registry, open and resolved alike.

    Resolved markets are included on purpose: a complete open-to-resolution arc
    is exactly the dataset the project claims as its differentiator, and those
    arcs are only reconstructable from backfilled history.
    """
    table = dynamodb.Table(REGISTRY_TABLE)
    markets = []
    response = table.scan(
        ProjectionExpression="market_id, question, #s",
        ExpressionAttributeNames={"#s": "status"},
    )
    markets.extend(response["Items"])
    while "LastEvaluatedKey" in response:
        response = table.scan(
            ProjectionExpression="market_id, question, #s",
            ExpressionAttributeNames={"#s": "status"},
            ExclusiveStartKey=response["LastEvaluatedKey"],
        )
        markets.extend(response["Items"])
    return markets


def fetch_clob_token_ids(market_id):
    """Resolve a market_id to its CLOB token ids, ordered to match `outcomes`.

    Verified 2026-08-18: clobTokenIds[i] tracks outcomes[i] -- token[0]'s last
    price matched outcomePrices[0] exactly on a real market.
    """
    payload = http_get_json(GAMMA_MARKET_URL.format(market_id=market_id))
    if not payload:
        return None, None
    market = payload[0] if isinstance(payload, list) else payload
    raw_token_ids = market.get("clobTokenIds")
    if not raw_token_ids:
        return None, market.get("createdAt")
    return json.loads(raw_token_ids), market.get("createdAt")


def fetch_price_history(token_id):
    """Daily price history for one outcome token, oldest first."""
    payload = http_get_json(CLOB_HISTORY_URL.format(token=token_id))
    return payload.get("history", []) or []


def build_backfill_snapshots(token_ids, backfilled_at):
    """Turn CLOB history into snapshots shaped like our own.

    Only the FIRST outcome token is read, and the remaining outcomes are derived
    as its complement. This is not a shortcut -- it is forced by how the data
    actually behaves. Measured 2026-08-18 on market 559672: the YES token had 371
    points and the NO token 395, but only 43 timestamps were shared between them.
    Each token trades independently, so they are sampled at different instants.
    An earlier version of this function required every outcome to report at the
    same timestamp and consequently discarded ~90% of real history as
    "incomplete."

    Deriving the complement is sound for binary markets, where the outcomes are
    two sides of one probability (YES + NO == 1 by construction -- see the
    complete-sets mechanism in knowledge.md). Markets with more than two
    outcomes cannot be reconstructed this way and are skipped rather than
    approximated.
    """
    if len(token_ids) != 2:
        return [], 0, 0

    history = fetch_price_history(token_ids[0])
    time.sleep(POLITE_DELAY_SECONDS)

    snapshots = []
    skipped_after_boundary = 0
    for point in sorted(history, key=lambda p: p["t"]):
        moment = datetime.fromtimestamp(point["t"], timezone.utc)

        # Hard cutoff -- the tracked window belongs to the cycles alone.
        if moment >= CYCLE_BOUNDARY:
            skipped_after_boundary += 1
            continue

        yes_price = float(point["p"])
        snapshots.append(
            {
                "timestamp": moment.isoformat(),
                "outcomePrices": json.dumps(
                    [f"{yes_price:.4f}", f"{1 - yes_price:.4f}"]
                ),
                "source": "clob_backfill",
                "backfilled_at": backfilled_at,
            }
        )
    return snapshots, skipped_after_boundary, 0


def load_existing_odds(market_id):
    key = f"odds/{market_id}.json"
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except s3.exceptions.NoSuchKey:
        return None


def merge_snapshots(existing_snapshots, new_snapshots):
    """Merge on timestamp, never duplicating, never overwriting cycle data.

    Idempotent by construction: re-running adds nothing that is already present.
    Existing snapshots always win a timestamp collision -- a real cycle snapshot
    (with volume/liquidity) must never be replaced by a thinner backfilled one.
    """
    by_timestamp = {snapshot["timestamp"]: snapshot for snapshot in new_snapshots}
    for snapshot in existing_snapshots:
        by_timestamp[snapshot["timestamp"]] = snapshot
    return [by_timestamp[ts] for ts in sorted(by_timestamp)]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="actually write to S3 (default is a dry run that writes nothing)",
    )
    parser.add_argument("--limit", type=int, help="only process the first N markets")
    args = parser.parse_args()

    backfilled_at = datetime.now(timezone.utc).isoformat()
    markets = get_registry_markets()
    if args.limit:
        markets = markets[: args.limit]

    mode = "APPLY (writing to S3)" if args.apply else "DRY RUN (writing nothing)"
    print(f"=== Odds history backfill -- {mode} ===")
    print(f"Markets to process: {len(markets)}")
    print(f"Hard cutoff: nothing written at or after {CYCLE_BOUNDARY.isoformat()}")
    print()

    stats = Counter()
    total_added = 0
    started = time.time()

    for position, market in enumerate(markets, start=1):
        market_id = market["market_id"]
        status = market.get("status", "?")

        try:
            token_ids, created_at = fetch_clob_token_ids(market_id)
        except Exception as exc:
            print(f"  [{position}/{len(markets)}] {market_id}: FETCH ERROR {type(exc).__name__}")
            stats["error"] += 1
            continue

        if not token_ids:
            print(f"  [{position}/{len(markets)}] {market_id}: no clobTokenIds, skipped")
            stats["no_tokens"] += 1
            continue

        try:
            new_snapshots, skipped_boundary, skipped_incomplete = build_backfill_snapshots(
                token_ids, backfilled_at
            )
        except Exception as exc:
            print(f"  [{position}/{len(markets)}] {market_id}: HISTORY ERROR {type(exc).__name__}")
            stats["error"] += 1
            continue

        if not new_snapshots:
            if len(token_ids) != 2:
                reason = f"non-binary market ({len(token_ids)} outcomes), skipped"
                stats["non_binary"] += 1
            else:
                reason = "no pre-tracking history (created after we started tracking)"
                stats["no_history"] += 1
            print(f"  [{position}/{len(markets)}] {market_id}: {reason}")
            continue

        existing = load_existing_odds(market_id)
        existing_snapshots = existing["snapshots"] if existing else []
        merged = merge_snapshots(existing_snapshots, new_snapshots)
        added = len(merged) - len(existing_snapshots)
        total_added += added
        stats["backfilled"] += 1

        oldest = new_snapshots[0]["timestamp"][:10]
        note = f" [+{skipped_boundary} after cutoff]" if skipped_boundary else ""
        note += f" [+{skipped_incomplete} incomplete]" if skipped_incomplete else ""
        print(
            f"  [{position}/{len(markets)}] {market_id} ({status}): "
            f"+{added} pts from {oldest}, created {str(created_at)[:10]}, "
            f"total {len(existing_snapshots)} -> {len(merged)}{note}"
        )

        if args.apply:
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=f"odds/{market_id}.json",
                Body=json.dumps({"market_id": market_id, "snapshots": merged}),
                ContentType="application/json",
            )

    elapsed = time.time() - started
    print()
    print("=== Summary ===")
    print(f"  backfilled:      {stats['backfilled']}")
    print(f"  no history:      {stats['no_history']}")
    print(f"  non-binary:      {stats['non_binary']}")
    print(f"  no clobTokenIds: {stats['no_tokens']}")
    print(f"  errors:          {stats['error']}")
    print(f"  snapshots added: {total_added}")
    print(f"  elapsed:         {elapsed:.1f}s")
    if not args.apply:
        print()
        print("DRY RUN -- nothing was written. Re-run with --apply to write.")


if __name__ == "__main__":
    main()
