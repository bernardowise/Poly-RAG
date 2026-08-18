"""
One-off, run ONCE: start the post-resolution News capture window for markets
that resolved BEFORE this feature existed.

WHY THIS EXISTS
---------------
ingest_polymarket now starts a 4-cycle post-resolution counter
(post_resolution_cycles_remaining) the moment a market transitions from
open to resolved (see mark_registry_resolved in
lambdas/ingest_polymarket/handler.py). The 93 markets already resolved
before this code was deployed never had that transition observed by the new
logic -- their counter is 0, so ingest_news would never search them, leaving
them permanently orphaned from post-resolution coverage.

USER'S EXPLICIT DECISION (2026-08-18): treat these 93 as a one-time,
hardcoded exception -- start their counter as if they had JUST resolved,
accepting the real but small bias this introduces (Google News returns
whatever it has indexed NOW, not what existed in each market's actual 48h
post-resolution window, which for some of these 93 was up to a few days
ago). Explicitly judged acceptable given the pipeline's own short lifetime
(News has only run 6 cycles / 3 days total) -- "muy muy antiguo" here still
means at most 2-3 days, not months. No attempt is made to reconstruct the
real historical window, unlike the CLOB odds backfill (which has genuine
historical data available); Google News search only ever reflects the
present. This is a deliberately simple, deliberately biased one-time catch-up,
not a rigorous reconstruction.

Rejected alternative: a standalone script that fetches News directly,
bypassing ingest_news. Rejected because it would duplicate that Lambda's
entire search/decode/extract/dedup pipeline for a one-time case. Simpler:
just start the counter here, and let the NEXT regular ingest_news cycle
pick these markets up through its normal path (get_open_markets already
includes any resolved market with post_resolution_cycles_remaining > 0) --
no duplicate logic, and poly-rag-processed-urls' existing dedup means this
is safe even if a market happens to get touched more than once.

SAFETY
------
Hardcoded market_id list, frozen at the moment this was written
(2026-08-18) -- deliberately NOT a live registry scan for "all resolved
markets," since running this script again later would wrongly re-arm the
counter for markets that already had their real post-resolution window
happen and legitimately ended. Idempotent by construction: only markets
whose counter is currently 0 get set to POST_RESOLUTION_CYCLES; a market
already mid-window (counter > 0) is left untouched. Dry run is the default.

USAGE
-----
    python scripts/start_legacy_post_resolution_windows.py            # dry run
    python scripts/start_legacy_post_resolution_windows.py --apply    # write
"""

import argparse
import time
from collections import Counter

import boto3

REGISTRY_TABLE = "poly-rag-market-registry"
POST_RESOLUTION_CYCLES = 4  # must match lambdas/ingest_polymarket/handler.py

# Frozen snapshot of every market with status="resolved" as of 2026-08-18,
# taken BEFORE this feature existed -- see module docstring for why this is
# a fixed list, not a live query.
LEGACY_RESOLVED_MARKET_IDS = [
    "3625104", "3619299", "3638263", "3596129", "3514572", "3593562", "3514560",
    "3514574", "3625126", "3448665", "3586260", "3514562", "3484852", "3649977",
    "3402192", "3607958", "3598446", "2292417", "3607946", "3656069", "3586261",
    "3514567", "1321136", "3598440", "3619295", "3596121", "3638280", "3639697",
    "3484946", "3484304", "2736060", "897241", "3484777", "3625148", "3596133",
    "3514564", "3619275", "3593554", "3618042", "3625152", "3593560", "3619321",
    "3619253", "3622890", "3639618", "3619588", "3656077", "3448670", "3593544",
    "3575707", "3619279", "3598969", "3619255", "3638282", "3598453", "3596123",
    "3638269", "3593546", "3514568", "3638275", "3619325", "3648671", "3625106",
    "3619257", "3618044", "3631532", "3619213", "3619277", "3484297", "3514566",
    "3565575", "3593568", "3593566", "3513920", "3607945", "3622894", "3484837",
    "3598975", "3596127", "3596131", "3606840", "3514558", "3619227", "3619211",
    "3639634", "3619273", "3638278", "3639702", "3514570", "3607959", "3656065",
    "3448662", "3619271",
]

dynamodb = boto3.resource("dynamodb")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="actually write to DynamoDB (default is a dry run that writes nothing)",
    )
    args = parser.parse_args()

    table = dynamodb.Table(REGISTRY_TABLE)

    mode = "APPLY (writing to DynamoDB)" if args.apply else "DRY RUN (writing nothing)"
    print(f"=== Start legacy post-resolution windows -- {mode} ===")
    print(f"Markets to process: {len(LEGACY_RESOLVED_MARKET_IDS)}")
    print()

    stats = Counter()
    started = time.time()

    for market_id in LEGACY_RESOLVED_MARKET_IDS:
        item = table.get_item(Key={"market_id": market_id}).get("Item")
        if item is None:
            print(f"  {market_id}: NOT FOUND in registry, skipped")
            stats["not_found"] += 1
            continue
        if item.get("status") != "resolved":
            print(f"  {market_id}: status is {item.get('status')!r}, not resolved -- skipped")
            stats["not_resolved"] += 1
            continue
        current = item.get("post_resolution_cycles_remaining", 0)
        if current and current > 0:
            print(f"  {market_id}: already mid-window ({current} cycles left), left untouched")
            stats["already_active"] += 1
            continue

        stats["started"] += 1
        if args.apply:
            table.update_item(
                Key={"market_id": market_id},
                UpdateExpression="SET post_resolution_cycles_remaining = :cycles",
                ExpressionAttributeValues={":cycles": POST_RESOLUTION_CYCLES},
            )

    elapsed = time.time() - started
    print()
    print("=== Summary ===")
    print(f"  windows started:  {stats['started']}")
    print(f"  already active:   {stats['already_active']}")
    print(f"  not resolved:     {stats['not_resolved']}")
    print(f"  not found:        {stats['not_found']}")
    print(f"  elapsed:          {elapsed:.1f}s")
    if not args.apply:
        print()
        print("DRY RUN -- nothing was written. Re-run with --apply to write.")


if __name__ == "__main__":
    main()
