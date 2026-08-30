"""
One-off: backfill `market_ids_mentioned` into every existing comments_cohere
row (LanceDB), so the whole historical corpus becomes filterable by
market_id via list_contains(), not just chunks written from 2026-08-30
onward.

WHY THIS EXISTS
---------------
chunk_comments (lambdas/chunk_comments/handler.py) was extended 2026-08-30
to attach market_ids_mentioned to every chunk it writes -- the union of
market_ids folded into that entity/link_type group, same mechanism
digest_cohere's market_ids_mentioned already uses (see retrieval/query.py,
LIST_MARKET_IDS_SOURCES). That closes the gap for chunks written from the
next automatic cycle onward, but does nothing for the ~1,524 rows already
in comments_cohere -- this script backfills those.

comments_cohere's schema was already evolved via tbl.add_columns() (a
separate, already-applied step) to add market_ids_mentioned as a nullable
list<string> column -- this script only fills in VALUES for existing rows,
it does not touch the schema.

HOW MARKET_IDS ARE RECOVERED PER ROW
-------------------------------------
Existing chunks don't carry the original market_ids anywhere retrievable
from the LanceDB row itself (only comment_entity_id and link_type survive
into the chunk) -- but comment_entity_id -> market_ids can be reconstructed
from the SAME registry scan chunk_comments itself already does every cycle
(market_id -> comment_entity_id, in poly-rag-market-registry), reversed:

- link_type == "direct": comment_entity_id IS the market_id itself (see
  comment_group_key in chunk_comments/handler.py -- for direct, entity_key
  = market_ids[0]), so market_ids_mentioned = [comment_entity_id].
- link_type in ("shared_event", "shared_series"): market_ids_mentioned =
  every market_id in the registry whose comment_entity_id matches this
  chunk's comment_entity_id (the reverse of the same map).

KNOWN IMPRECISION, ACCEPTED
----------------------------
The registry is live and grows over time -- a shared_event/shared_series
entity's set of associated markets at backfill time may include markets
that joined that entity AFTER a given historical chunk was originally
written (e.g. a new market added to an existing Series). This is the same
characteristic the live chunk_comments Lambda already has (it recomputes
from a fresh registry scan every cycle too, reflecting current state, not
the state at original write time) -- not a new inconsistency this backfill
introduces.

SAFETY
------
Strictly additive: writes ONE field (market_ids_mentioned) via LanceDB
tbl.update(), touches no other column, adds/removes no row, never touches
the embedding vector. Idempotent -- a row that already has a non-null
market_ids_mentioned is skipped, so re-running is a no-op for already-
backfilled rows. Dry run is the default.

VERIFIED BEFORE BUILDING THIS SCRIPT (2026-08-30, see tech_debt.md):
tbl.update(where=..., values_sql={"col": "['a','b']"}) confirmed to work
against a real comments_cohere row with a None value, verified end-to-end
including a revert back to None, before this script was written.

USAGE
-----
    python scripts/backfill_comments_market_ids.py              # dry run
    python scripts/backfill_comments_market_ids.py --limit 20   # dry run, first 20
    python scripts/backfill_comments_market_ids.py --apply      # write to LanceDB
"""

import argparse
import os
import time
from collections import Counter, defaultdict

import boto3
import lancedb

S3_BUCKET = os.environ.get("S3_BUCKET", "poly-rag-369970405415")
LANCEDB_URI = f"s3://{S3_BUCKET}/lancedb/"
REGISTRY_TABLE = os.environ.get("REGISTRY_TABLE", "poly-rag-market-registry")

dynamodb = boto3.resource("dynamodb", region_name="us-east-1")


def scan_reverse_entity_map():
    """comment_entity_id -> sorted list of market_ids -- the reverse of the
    market_id -> comment_entity_id map chunk_comments already builds every
    cycle (scan_registry_entity_map in lambdas/chunk_comments/handler.py)."""
    table = dynamodb.Table(REGISTRY_TABLE)
    reverse_map = defaultdict(set)

    def process_items(items):
        for item in items:
            entity_id = item.get("comment_entity_id")
            market_id = item.get("market_id")
            if entity_id and market_id:
                reverse_map[entity_id].add(market_id)

    response = table.scan(ProjectionExpression="market_id, comment_entity_id")
    process_items(response["Items"])
    while "LastEvaluatedKey" in response:
        response = table.scan(
            ProjectionExpression="market_id, comment_entity_id",
            ExclusiveStartKey=response["LastEvaluatedKey"],
        )
        process_items(response["Items"])

    return {entity_id: sorted(mids) for entity_id, mids in reverse_map.items()}


def compute_market_ids_mentioned(link_type, comment_entity_id, reverse_map):
    if comment_entity_id in ("unknown_entity", "unknown_market"):
        return []
    if link_type == "direct":
        return [comment_entity_id]
    return reverse_map.get(comment_entity_id, [])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="actually write to LanceDB (default is a dry run that writes nothing)",
    )
    parser.add_argument("--limit", type=int, help="only process the first N rows")
    args = parser.parse_args()

    db = lancedb.connect(LANCEDB_URI)
    tbl = db.open_table("comments_cohere")

    print("Scanning registry for market_id -> comment_entity_id map...")
    reverse_map = scan_reverse_entity_map()
    print(f"Reverse map built: {len(reverse_map)} distinct comment entities")
    print()

    rows = tbl.search().select(["chunk_id", "link_type", "comment_entity_id", "market_ids_mentioned"]).to_list()
    if args.limit:
        rows = rows[: args.limit]

    mode = "APPLY (writing to LanceDB)" if args.apply else "DRY RUN (writing nothing)"
    print(f"=== Backfill comments_cohere market_ids_mentioned -- {mode} ===")
    print(f"Rows to process: {len(rows)}")
    print()

    stats = Counter()
    started = time.time()

    for position, row in enumerate(rows, start=1):
        chunk_id = row["chunk_id"]

        if row.get("market_ids_mentioned"):
            stats["already_backfilled"] += 1
            continue

        market_ids = compute_market_ids_mentioned(
            row["link_type"], row["comment_entity_id"], reverse_map
        )

        if not market_ids:
            stats["unresolvable"] += 1
            if position <= 5 or position % 200 == 0:
                print(f"  [{position}/{len(rows)}] {chunk_id}: no market_ids resolved "
                      f"(link_type={row['link_type']}, entity={row['comment_entity_id']})")
            continue

        stats["backfilled"] += 1
        if position <= 5 or position % 200 == 0:
            print(f"  [{position}/{len(rows)}] {chunk_id}: market_ids_mentioned = {market_ids}")

        if args.apply:
            sql_literal = "[" + ", ".join(f"'{m}'" for m in market_ids) + "]"
            tbl.update(where=f"chunk_id = '{chunk_id}'", values_sql={"market_ids_mentioned": sql_literal})

    elapsed = time.time() - started
    print()
    print("=== Summary ===")
    print(f"  backfilled:        {stats['backfilled']}")
    print(f"  already backfilled:{stats['already_backfilled']}")
    print(f"  unresolvable:      {stats['unresolvable']}")
    print(f"  elapsed:           {elapsed:.1f}s")
    if not args.apply:
        print()
        print("DRY RUN -- nothing was written. Re-run with --apply to write.")


if __name__ == "__main__":
    main()
