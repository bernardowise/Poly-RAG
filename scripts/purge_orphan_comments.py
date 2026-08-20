"""
One-off: remove orphan Comments that reference only purged market_ids.

WHY THIS EXISTS
---------------
Discovered 2026-08-20 while building the chunking bootstrap
(scripts/bootstrap_chunk_corpus.py): 1,337 comments across 4 cycle files
(comments/2026-08-16/01.json, 2026-08-16/12.json, 2026-08-17/00.json,
2026-08-17/12.json) reference market_ids that no longer exist in the
registry at all (38 distinct market_ids). Root cause: these 4 cycles ran
BEFORE the 2026-08-17 registry cleanup (see architecture_canon.md, "limpieza
mayor del registry") that purged 329 pre-redesign markets -- that cleanup was
explicitly scoped to registry + odds, never touched comments/*.json (see
architecture_canon.md: "esos payloads son historial de CICLO, no datos de
registry/odds ligados a un market_id"). The comment data was correct for its
own cycle at ingestion time; it only became orphaned once the markets it
references were purged days later.

This is the SAME pattern as the already-documented `unknown_market` finding
for News (tech_debt.md, "Known Limitation: Explicit ID-Linkage") -- but that
one was deliberately left untouched by explicit user decision ("dont delete
yet, will handle later perhaps querying through the RAG"). Comments are
different: the user's explicit call here (2026-08-20) is to purge them now,
not defer -- unlike a News article (which still has real standalone content
even without a resolvable market), an orphan comment's ONLY purpose in this
corpus is grouping by market/entity, and it has none. They just get in the
way of the comment chunking design (see chunk_comments in
bootstrap_chunk_corpus.py), which depends on every comment resolving to a
real entity or market.

SCOPE, DELIBERATELY NARROW
---------------------------
Only touches the 4 cycle files where orphans were actually found. The other
6 comment cycle files (2026-08-18 onward) already have zero orphans --
confirmed via bootstrap_chunk_corpus.py's per-cycle orphan count -- and are
never read or written by this script.

WHAT COUNTS AS AN ORPHAN
--------------------------
A comment where EVERY entry in market_ids is absent from the registry (a
comment can carry multiple market_ids for shared_event/shared_series -- if
even one of them still resolves, the comment is NOT an orphan and is kept
untouched). direct comments always resolve via their own single market_id,
so this can only ever affect shared_event/shared_series comments in
practice (verified empirically, 2026-08-20).

SAFETY
------
- Strictly a filter: removes only the orphan comment objects, changes
  nothing about the comments that remain (byte-for-byte identical dicts).
- Recomputes `comment_count` to match the real post-filter array length --
  avoids reintroducing the count/array mismatch bug class already fixed
  once in this project (see eda_mio_3 in session_ledger.md, 2026-08-17).
- Does NOT touch poly-rag-processed-comments (the dedup table) -- leaving a
  purged comment_id there is safe and intentional: if that comment_id ever
  reappeared from the live API, it would still be correctly deduped, not
  re-ingested. We do not want a purged orphan to "resurrect".
- Does NOT touch any other comments/*.json cycle file, any News/Polymarket/
  Digest data, or the registry itself.
- Dry run is the default. Prints the exact before/after comment_count per
  file and total removed before anything is written.

USAGE
-----
    python scripts/purge_orphan_comments.py            # dry run
    python scripts/purge_orphan_comments.py --apply     # write to S3
"""

import argparse
import json

import boto3

S3_BUCKET = "poly-rag-369970405415"
REGISTRY_TABLE = "poly-rag-market-registry"

AFFECTED_KEYS = [
    "comments/2026-08-16/01.json",
    "comments/2026-08-16/12.json",
    "comments/2026-08-17/00.json",
    "comments/2026-08-17/12.json",
]

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")


def get_known_market_ids():
    table = dynamodb.Table(REGISTRY_TABLE)
    ids = set()
    response = table.scan(ProjectionExpression="market_id")
    ids.update(item["market_id"] for item in response["Items"])
    while "LastEvaluatedKey" in response:
        response = table.scan(
            ProjectionExpression="market_id",
            ExclusiveStartKey=response["LastEvaluatedKey"],
        )
        ids.update(item["market_id"] for item in response["Items"])
    return ids


def is_orphan(comment, known_market_ids):
    market_ids = comment.get("market_ids") or []
    if not market_ids:
        return False  # nothing to check against -- leave untouched, not our call to make
    return not any(mid in known_market_ids for mid in market_ids)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="actually write to S3 (default is a dry run that writes nothing)",
    )
    args = parser.parse_args()

    mode = "APPLY (writing to S3)" if args.apply else "DRY RUN (writing nothing)"
    print(f"=== Purge orphan Comments -- {mode} ===")

    known_market_ids = get_known_market_ids()
    print(f"Registry market_ids: {len(known_market_ids)}")
    print(f"Files to check: {len(AFFECTED_KEYS)}")
    print()

    total_before = 0
    total_removed = 0

    for key in AFFECTED_KEYS:
        payload = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
        comments = payload.get("comments", [])
        before = len(comments)
        kept = [c for c in comments if not is_orphan(c, known_market_ids)]
        removed = before - len(kept)

        total_before += before
        total_removed += removed

        print(f"  {key}: {before} -> {len(kept)} comments "
              f"({removed} orphans removed, comment_count {payload.get('comment_count')} -> {len(kept)})")

        if args.apply:
            payload["comments"] = kept
            payload["comment_count"] = len(kept)
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=key,
                Body=json.dumps(payload),
                ContentType="application/json",
            )

    print()
    print("=== Summary ===")
    print(f"  total comments before: {total_before}")
    print(f"  total orphans removed: {total_removed}")
    print(f"  total comments after:  {total_before - total_removed}")
    if not args.apply:
        print()
        print("DRY RUN -- nothing was written. Re-run with --apply to write.")


if __name__ == "__main__":
    main()
