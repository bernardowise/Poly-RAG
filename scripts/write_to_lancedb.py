"""
One-off: write embedded vectors + their source text into a LanceDB table in
S3. This is Fase 3 (store write) -- deliberately decoupled from Fase 2
(embedding), per the canon agreed 2026-08-21: Fase 1 ingestion, Fase 2
embedding, Fase 3 write-to-vector-store, each independently runnable and each
able to fail without corrupting the stage before it.

WHY LANCEDB, AND WHY TEXT LIVES IN THE TABLE
---------------------------------------------
Chosen over Pinecone/Qdrant/Databricks on a measured constraint, not a
retrieval-quality benchmark (that comparison is deferred to Day 6, see
tech_debt.md): this project's real growth rate (measured 2026-08-21, ~700
news_article chunks/day) produces ~3.7 GB by month 3 and ~14 GB by month 12 for
this one variant -- comfortably past Qdrant's 1GB and Pinecone's 2GB free
tiers, and comfortably inside LanceDB's real cost, since it is not a managed
service but a columnar file format written directly to S3 (~$0.09-0.33/month
in S3 storage at those sizes, measured against this project's real per-record
size). No new secret either -- it authenticates via the same AWS credentials
already used for S3/DynamoDB/Bedrock.

Text is stored alongside the vector (not left to a separate S3 join) per the
Fase 3 design decision on 2026-08-21: this project's retrieval is filter-first
(market_id/temporal_tier/market_status_at_publish) then semantic-ranked within
that scope, so a single table that carries both the filter fields and the text
answers a query in one round trip. The corpus is small enough (see the growth
projection above) that denormalizing costs pennies, not dollars, and it avoids
a join key (chunk_id) that has already changed shape twice this session
(overflow-split #part suffixes, comment @cN qualifiers).

WHAT THIS SCRIPT DOES
----------------------
1. Reads a chunk file (source text) and its matching vector checkpoint parts
   (embeddings) from S3.
2. Joins them by chunk_id.
3. Upserts into (or creates) a LanceDB table at
   s3://<bucket>/lancedb/<variant>_<model>, using chunk_id as the merge key so
   re-running this script for the same slice is idempotent -- required for the
   4-day plan, where each day's slice gets written independently and Monday's
   incremental Lambda will do the same merge pattern for every new cycle.
4. Creates a vector index only once the table is large enough for LanceDB to
   build one (IVF-PQ needs a minimum row count) -- brute-force search is used
   below that, which is exact and fine at this corpus size.

SAFETY
------
Read-only against `chunks/` and `vectors/`. Writes only under the NEW
`lancedb/` S3 prefix, and only with --apply. Dry run (default) reports the
join coverage and row count without writing anything.

USAGE
-----
    python scripts/write_to_lancedb.py --variant news_article --slice cycles_01-05
    python scripts/write_to_lancedb.py --variant news_article --slice cycles_01-05 --apply
"""

import argparse
import json
import time

import boto3

S3_BUCKET = "poly-rag-369970405415"
LANCEDB_URI = f"s3://{S3_BUCKET}/lancedb/"
MODEL_LABEL = "cohere"

# IVF-PQ needs enough rows to form meaningful partitions; below this, an index
# would be misleading (empty or degenerate partitions) and brute-force search
# is already fast at this scale.
MIN_ROWS_FOR_INDEX = 5_000

s3 = boto3.client("s3")


def read_json(key):
    return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())


def load_chunks(variant, slice_label):
    return read_json(f"chunks/{variant}/{slice_label}.json")


def load_vectors(variant, model_label=MODEL_LABEL):
    """All checkpoint parts for a variant/model, concatenated. Reads every
    part currently in S3 for this variant -- if prior slices were already
    embedded (Saturday's run on top of Friday's, for example), their vectors
    are included too, since the join below is keyed by chunk_id and this
    script is meant to be safe to re-run as the corpus grows."""
    prefix = f"vectors/_checkpoints/{variant}/{model_label}/"
    records = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json"):
                records.extend(read_json(obj["Key"]))
    return records


def join_text(vector_records, chunks):
    """vector records (no text) + chunk records (text, no embedding) -> one
    row per chunk_id with both. A vector record with no matching chunk (should
    not happen, but chunk files can theoretically be re-cut) is skipped and
    counted, not silently dropped.

    Also deduplicates by chunk_id, keeping the LAST record seen. This is not
    hypothetical: found live 2026-08-21 (see tech_debt.md, "Duplicate Article
    URLs Within a Single Cycle") -- two article URLs were fetched by two
    different News fan-out batches in the same cycle under two different
    market_ids, producing two chunks with an identical chunk_id (the URL).
    LanceDB's merge_insert correctly REFUSES an ambiguous match rather than
    picking one silently ("multiple source rows match the same target row"),
    which is the right behavior for a merge but means this script must resolve
    the ambiguity before it ever reaches LanceDB. Deduping here is a stopgap,
    not the fix -- the real fix is upstream in ingest_news (see tech_debt.md
    for the two candidate designs), and doing it here means one of the two
    markets loses its link to this article, same tradeoff already documented
    as accepted at 0.07% of the corpus."""
    text_by_id = {c["chunk_id"]: c["text"] for c in chunks}
    rows_by_id, missing = {}, []
    for record in vector_records:
        chunk_id = record["chunk_id"]
        text = text_by_id.get(chunk_id)
        if text is None:
            missing.append(chunk_id)
            continue
        rows_by_id[chunk_id] = {**record, "text": text}
    return list(rows_by_id.values()), missing


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--variant", required=True,
                        help="source variant, e.g. news_article")
    parser.add_argument("--slice", required=True,
                        help="chunk file label under chunks/<variant>/, e.g. cycles_01-05")
    parser.add_argument("--model", default=MODEL_LABEL,
                        help=f"embedding model label under vectors/_checkpoints/<variant>/ (default: {MODEL_LABEL})")
    parser.add_argument("--table",
                        help="LanceDB table name (default: <variant>_<model>)")
    parser.add_argument("--apply", action="store_true",
                        help="actually write to LanceDB (default: dry run, writes nothing)")
    args = parser.parse_args()

    table_name = args.table or f"{args.variant}_{args.model}"

    print(f"=== Write to LanceDB -- {'APPLY' if args.apply else 'DRY RUN (writes nothing)'} ===")
    print(f"chunks : s3://{S3_BUCKET}/chunks/{args.variant}/{args.slice}.json")
    print(f"vectors: s3://{S3_BUCKET}/vectors/_checkpoints/{args.variant}/{args.model}/*.json")
    print(f"table  : {LANCEDB_URI}{table_name}")
    print()

    chunks = load_chunks(args.variant, args.slice)
    vectors = load_vectors(args.variant, args.model)
    print(f"chunks read   : {len(chunks):,}")
    print(f"vectors read  : {len(vectors):,}  (all checkpoint parts currently in S3 for this variant/model)")

    rows, missing = join_text(vectors, chunks)
    # _lineage (added 2026-08-22 to chunk_* Lambda output for provenance
    # tracing, see tech_debt.md) rides along into the vector checkpoint
    # record via embed_*'s `{k: v for k, v in chunk.items() if k != "text"}`,
    # but older slices written before that fix don't have it. Similarly, the
    # older one-off bootstrap script tagged chunks with cycle_key/cycle_number,
    # while the newer per-cycle Lambdas use cycle_started_at instead (comments
    # specifically -- verified 2026-08-22: bootstrap and cycle 14 chunk records
    # otherwise match). LanceDB's merge_insert tolerates a batch with FEWER
    # fields than the table's existing schema (padded with null) but hard-fails
    # on a batch with a field the schema doesn't already have -- so any of
    # these cycle-bookkeeping fields can break a later write depending on
    # which slice created the table first. None of the 4 are part of the
    # documented retrieval filter set (market_id/temporal_tier/
    # market_status_at_publish/link_type/comment_entity_id, see
    # architecture_canon.md), so drop all of them uniformly rather than
    # patch per-source schema drift as it's discovered.
    for r in rows:
        for stale_field in ("_lineage", "cycle_key", "cycle_number", "cycle_started_at"):
            r.pop(stale_field, None)
    dupes = len(vectors) - len(rows) - len(missing)
    print(f"joined rows   : {len(rows):,}")
    if dupes > 0:
        print(f"  NOTE: {dupes} duplicate chunk_id(s) collapsed to their last occurrence "
              f"(see tech_debt.md, 'Duplicate Article URLs Within a Single Cycle')")
    if missing:
        print(f"  WARNING: {len(missing)} vector record(s) had no matching chunk_id in "
              f"this slice's chunk file -- skipped, not written. First few: {missing[:5]}")

    if not rows:
        print("\nNothing to write.")
        return

    dims = {r["embedding_dim"] for r in rows}
    if len(dims) > 1:
        print(f"\n  ERROR: mixed embedding dimensions in this batch: {dims} -- "
              f"refusing to write a table with inconsistent vector size.")
        return

    if not args.apply:
        print(f"\nDRY RUN -- would write {len(rows):,} rows, dim={dims.pop()}.")
        print("Re-run with --apply to write.")
        return

    import lancedb  # imported late: only needed for --apply, dry runs stay light

    print("\n=== Writing ===")
    started = time.time()
    db = lancedb.connect(LANCEDB_URI)
    # list_tables() returns a ListTablesResponse, NOT a plain list -- verified
    # 2026-08-21 the hard way: `table_name in existing` silently evaluated
    # False against the response object every time, so every re-run tried
    # create_table() and crashed on "Table already exists" instead of
    # upserting. The actual list of names is the .tables attribute.
    existing = db.list_tables().tables

    if table_name in existing:
        tbl = db.open_table(table_name)
        before = tbl.count_rows()
        # merge_insert upserts by chunk_id: re-running this script for a slice
        # already written updates nothing (same id, same data) rather than
        # duplicating rows, and running it for a NEW slice on the same variant
        # adds only the new rows -- this is what makes the script safe to call
        # once per day across the 4-day plan without tracking what was already
        # sent.
        (tbl.merge_insert("chunk_id")
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute(rows))
        after = tbl.count_rows()
        print(f"  merged into existing table '{table_name}': "
              f"{before:,} -> {after:,} rows ({after - before:+,})")
    else:
        tbl = db.create_table(table_name, data=rows)
        after = tbl.count_rows()
        print(f"  created table '{table_name}': {after:,} rows")

    if after >= MIN_ROWS_FOR_INDEX:
        print(f"  building vector index ({after:,} >= {MIN_ROWS_FOR_INDEX:,} row threshold)...")
        tbl.create_index(vector_column_name="embedding", metric="cosine")
    else:
        print(f"  skipping vector index ({after:,} < {MIN_ROWS_FOR_INDEX:,}) -- "
              f"brute-force search is exact and fast at this size")

    elapsed = time.time() - started
    print(f"\n=== Done in {elapsed:.1f}s ===")
    print(f"  table: {LANCEDB_URI}{table_name}")
    print(f"  rows : {tbl.count_rows():,}")


if __name__ == "__main__":
    main()
