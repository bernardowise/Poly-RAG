"""
Poly-RAG write_lancedb Lambda: Fase 3, per-cycle write of the 4 embedded
sources (registry, comments, digest, news_article) into their LanceDB
tables. Terminal stage of the whole cycle -- invokes nothing further.

WHY THIS EXISTS (2026-08-22)
-----------------------------
Ports the logic already proven manually the same day (scripts/
write_to_lancedb.py, run against the full 14-cycle corpus, 3 real
schema-drift bugs found and fixed there -- see tech_debt.md) into a Lambda
that runs automatically every cycle instead of needing a one-off each time.
By the time this Lambda runs, all 4 sources are guaranteed already embedded:
it is invoked by digest_metrics, which is invoked by embed_news_article, the
last and heaviest stage of the strictly sequential embed chain (embed_digest
-> embed_comments -> embed_registry -> embed_news_article).

WHY A CONTAINER IMAGE, NOT A ZIP
----------------------------------
LanceDB's real dependency footprint is 339MB unzipped, over Lambda's 250MB
zip/Layer limit (measured 2026-08-21, see tech_debt.md "Vector Store
Choice"). Deployed via ECR + Lambda's Image package type, not
archive_file/zip like every other Lambda in this project.

WHAT THIS DOES, PER CYCLE (deliberately unlike the one-off script)
---------------------------------------------------------------------
Unlike scripts/write_to_lancedb.py (which takes --variant/--slice and can
point at a multi-cycle bootstrap file), this Lambda ALWAYS reads exactly one
cycle's per-source chunk file (chunks/<source>/<date>/<hour>.json, derived
from cycle_started_at) for all 4 sources in one invocation -- no slice
argument, no manual scope decision. A source with zero new chunks this cycle
(e.g. registry on a cycle with no new markets) is skipped, not an error.

INDEX REBUILD -- DELIBERATELY NOT DONE HERE (decided 2026-08-22)
---------------------------------------------------------------------
Every cycle only merge_inserts the new rows -- it never calls
tbl.create_index(). Rebuilding IVF-PQ scales with the TABLE's total size, not
with how many rows are new, so rebuilding every 12h would mean re-indexing
the whole table (already ~10K rows for news_article and growing ~700/cycle)
to add a few hundred rows each time -- cost that grows without bound while
providing zero benefit under LanceDB's real behavior at this scale
(brute-force search is already exact and fast below ~5,000 rows, see
MIN_ROWS_FOR_INDEX in the one-off script). Index maintenance is a separate,
lower-cadence concern (manual or a periodic job), not this Lambda's job.

SCHEMA DRIFT -- SAME FIX AS THE ONE-OFF SCRIPT
---------------------------------------------------
LanceDB's merge_insert tolerates a batch with FEWER fields than the table's
existing schema (padded null) but hard-fails on a batch introducing a field
the schema doesn't already have. `_lineage`/`cycle_key`/`cycle_number`/
`cycle_started_at` are all cycle-bookkeeping fields, none part of the
documented retrieval filter set -- dropped uniformly before every write,
same as the one-off script.
"""

import json
import os

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "poly-rag-369970405415")
LANCEDB_URI = f"s3://{S3_BUCKET}/lancedb/"
MODEL_LABEL = "cohere"

SOURCES = ["registry", "comments", "digest", "news_article"]

STALE_FIELDS = ("_lineage", "cycle_key", "cycle_number", "cycle_started_at")

s3 = boto3.client("s3")


def read_json(key):
    return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())


def chunk_input_key(source, cycle_started_at):
    from datetime import datetime
    dt = datetime.fromisoformat(cycle_started_at)
    return f"chunks/{source}/{dt.strftime('%Y-%m-%d')}/{dt.strftime('%H')}.json"


def load_chunks(source, cycle_started_at):
    key = chunk_input_key(source, cycle_started_at)
    try:
        return read_json(key)
    except s3.exceptions.NoSuchKey:
        return None


def load_vectors(source, model_label=MODEL_LABEL):
    """All checkpoint parts for a source/model currently in S3 -- the join
    below (keyed by chunk_id, scoped to this cycle's chunk file) naturally
    picks out only this cycle's new rows, same mechanism already verified
    working in the one-off script."""
    prefix = f"vectors/_checkpoints/{source}/{model_label}/"
    records = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json"):
                records.extend(read_json(obj["Key"]))
    return records


def join_text(vector_records, chunks):
    """Same as the one-off script's join_text -- see its docstring for the
    duplicate chunk_id story (tech_debt.md, 'Duplicate Article URLs Within a
    Single Cycle'). Deduplicates by chunk_id, keeping the last record seen."""
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


def write_source(db, source, cycle_started_at):
    chunks = load_chunks(source, cycle_started_at)
    if chunks is None:
        return {"source": source, "status": "no_chunk_file"}
    if not chunks:
        return {"source": source, "status": "empty_chunk_file"}

    vectors = load_vectors(source)
    rows, missing = join_text(vectors, chunks)
    for r in rows:
        for stale_field in STALE_FIELDS:
            r.pop(stale_field, None)

    if not rows:
        return {"source": source, "status": "no_matching_vectors", "missing": len(missing)}

    dims = {r["embedding_dim"] for r in rows}
    if len(dims) > 1:
        return {"source": source, "status": "error_mixed_dims", "dims": list(dims)}

    table_name = f"{source}_{MODEL_LABEL}"
    existing = db.list_tables().tables

    if table_name in existing:
        tbl = db.open_table(table_name)
        before = tbl.count_rows()
        (tbl.merge_insert("chunk_id")
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute(rows))
        after = tbl.count_rows()
        return {"source": source, "status": "merged", "before": before, "after": after,
                "written": len(rows), "missing": len(missing)}
    else:
        tbl = db.create_table(table_name, data=rows)
        after = tbl.count_rows()
        return {"source": source, "status": "created", "after": after,
                "written": len(rows), "missing": len(missing)}


def lambda_handler(event, context):
    import lancedb  # imported inside the handler: only needed once invoked,
    # keeps cold-start import time honest about what this Lambda actually
    # needs before the connection is opened.

    cycle_started_at = event["cycle_started_at"]  # no fallback -- Fase 3
    # only makes sense chained from Fase 2, never invoked standalone without
    # a specific cycle to write.

    db = lancedb.connect(LANCEDB_URI)
    results = [write_source(db, source, cycle_started_at) for source in SOURCES]

    return {
        "cycle_started_at": cycle_started_at,
        "results": results,
    }
