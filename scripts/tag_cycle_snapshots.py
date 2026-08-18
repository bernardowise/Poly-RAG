"""
One-off: tag every existing odds snapshot with an explicit source="cycle".

WHY THIS EXISTS
---------------
Odds snapshots written by `ingest_polymarket` carry no provenance field -- at the
time they were the only kind of snapshot that existed, so none was needed. The
CLOB history backfill (scripts/backfill_odds_history.py) introduces a second
kind, tagged source="clob_backfill".

The tempting shortcut was to leave existing snapshots untagged and treat absence
as meaning cycle-origin. Rejected deliberately, for the same reason the two-tier
comment link_type was replaced by three tiers (see tech_debt.md, "Comments Source
Replaces Bluesky"): an implicit promise nobody wrote down is one nobody can
verify, and it broke in production last time. Concretely, implicit-absence fails
three ways:

  1. A THIRD provenance breaks it. The next planned task is teaching
     `ingest_polymarket` to fetch history for newly-registered markets -- at that
     point "no source field" stops identifying anything in particular.
  2. It reads as null in Databricks. `WHERE source = 'cycle'` is a far better
     thing to write in eda_mio than `WHERE source IS NULL`.
  3. It cannot distinguish "written by a cycle" from "written before the field
     existed." Identical today, not identical forever.

WHAT THIS TOUCHES
-----------------
Strictly additive: it adds one key to each snapshot and changes NOTHING else --
no timestamp, price, volume, volume24hr or liquidity value is modified, and no
snapshot is added or removed. It is still a write to canonical data (the odds
time-series purged and verified on 2026-08-17), so it runs as its own script with
its own dry run, separate from the backfill, and S3 versioning (enabled
2026-08-17) makes it reversible.

Idempotent: a snapshot that already carries a `source` is left exactly as-is, so
re-running is a no-op and this can safely be run after a partial failure.

USAGE
-----
    python scripts/tag_cycle_snapshots.py              # dry run, writes nothing
    python scripts/tag_cycle_snapshots.py --limit 20   # dry run, first 20 files
    python scripts/tag_cycle_snapshots.py --apply      # actually write to S3
"""

import argparse
import json
import time
from collections import Counter

import boto3

S3_BUCKET = "poly-rag-369970405415"
ODDS_PREFIX = "odds/"
CYCLE_SOURCE = "cycle"

s3 = boto3.client("s3")


def list_odds_keys():
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=ODDS_PREFIX):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json"):
                keys.append(obj["Key"])
    return keys


def tag_snapshots(snapshots):
    """Add source="cycle" to snapshots lacking any source. Returns (new, count).

    Existing `source` values are preserved untouched -- this must never relabel a
    clob_backfill point as cycle-origin, which would destroy exactly the
    distinction the field exists to make.
    """
    tagged = []
    changed = 0
    for snapshot in snapshots:
        if "source" in snapshot:
            tagged.append(snapshot)
            continue
        # Rebuild with source first so the field is visible when eyeballing JSON.
        tagged.append({"source": CYCLE_SOURCE, **snapshot})
        changed += 1
    return tagged, changed


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="actually write to S3 (default is a dry run that writes nothing)",
    )
    parser.add_argument("--limit", type=int, help="only process the first N files")
    args = parser.parse_args()

    keys = list_odds_keys()
    if args.limit:
        keys = keys[: args.limit]

    mode = "APPLY (writing to S3)" if args.apply else "DRY RUN (writing nothing)"
    print(f"=== Tag existing odds snapshots with source={CYCLE_SOURCE} -- {mode} ===")
    print(f"Odds files to process: {len(keys)}")
    print()

    stats = Counter()
    total_tagged = 0
    started = time.time()

    for position, key in enumerate(keys, start=1):
        try:
            payload = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
        except Exception as exc:
            print(f"  [{position}/{len(keys)}] {key}: READ ERROR {type(exc).__name__}")
            stats["error"] += 1
            continue

        snapshots = payload.get("snapshots", [])
        tagged, changed = tag_snapshots(snapshots)

        if changed == 0:
            stats["already_tagged"] += 1
            continue

        total_tagged += changed
        stats["files_updated"] += 1

        if args.apply:
            payload["snapshots"] = tagged
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=key,
                Body=json.dumps(payload),
                ContentType="application/json",
            )

        if position <= 5 or position % 100 == 0:
            print(
                f"  [{position}/{len(keys)}] {key}: "
                f"{changed}/{len(snapshots)} snapshots tagged"
            )

    elapsed = time.time() - started
    print()
    print("=== Summary ===")
    print(f"  files updated:     {stats['files_updated']}")
    print(f"  already tagged:    {stats['already_tagged']}")
    print(f"  errors:            {stats['error']}")
    print(f"  snapshots tagged:  {total_tagged}")
    print(f"  elapsed:           {elapsed:.1f}s")
    if not args.apply:
        print()
        print("DRY RUN -- nothing was written. Re-run with --apply to write.")


if __name__ == "__main__":
    main()
