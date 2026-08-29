"""
Poly-RAG write_lancedb Lambda: Fase 3, per-cycle write of the 4 embedded
sources (registry, comments, digest, news_article) into their LanceDB
tables. Terminal stage of the whole cycle -- invokes nothing further, but
sends the cycle's THIRD checkpoint email (2026-08-22, see send_report below)
after send_digest's market-content digest (Fase 1) and digest_metrics' cost
report (Fase 1+2) -- one email per phase, so a human can tell which phase
succeeded/failed from the inbox alone without opening CloudWatch.

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

INDEX MAINTENANCE -- optimize() every cycle, NEVER create_index() (revised 2026-08-29)
---------------------------------------------------------------------------------------
Every cycle still never calls tbl.create_index() -- rebuilding IVF-PQ from
scratch scales with the TABLE's total size, not how many rows are new, so
doing that every 12h would mean re-indexing the whole table (already ~20K
rows for news_article) to add a few hundred rows each time -- cost that
grows without bound.

But every cycle DOES now call tbl.optimize() (see run_optimize) after each
source's write -- this is NOT the same operation. optimize() folds newly
merge_inserted rows into the EXISTING index incrementally (LanceDB's own
words: "adding new data to existing indices"), plus compacts small files
and prunes old versions -- it does not rebuild anything from scratch.
Originally left out of this Lambda (2026-08-22) on the assumption that any
index maintenance would carry the same table-size-scaling cost as
create_index() -- that assumption was never verified and turned out wrong.
Measured directly 2026-08-29 (one-off run, see tech_debt.md): 2.4-13.3s
across all 4 tables, including news_article with a 7-day backlog of ~10,700
unindexed rows -- trivial next to this Lambda's 120s timeout. Wrapped in
try/except (see run_optimize): a maintenance failure should never block the
data write that already succeeded above it. Tables still under
MIN_ROWS_FOR_INDEX (no index built yet, see the one-off script) simply get
the compaction/pruning part of optimize() -- harmless, not skipped.

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
import time

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "poly-rag-369970405415")
LANCEDB_URI = f"s3://{S3_BUCKET}/lancedb/"
MODEL_LABEL = "cohere"
SES_SENDER = os.environ.get("SES_SENDER", "bernardolw@gmail.com")
SES_RECIPIENT = os.environ.get("SES_RECIPIENT", "bernardolw@gmail.com")

SOURCES = ["registry", "comments", "digest", "news_article"]

ses = boto3.client("ses")

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


def load_vectors(source, cycle_started_at, model_label=MODEL_LABEL):
    """Checkpoint parts written SINCE this cycle started -- not the whole
    history. Fixed 2026-08-23 after a real timeout: the original version read
    every checkpoint part ever written for a source (mirroring the one-off
    script, which is fine for a single manual run but not for something that
    runs every 12h forever) -- news_article alone was already ~20 checkpoint
    files/~9,800 records on this Lambda's first real automatic cycle, and the
    read+join+merge_insert cost of the WHOLE history blew through the 120s
    timeout on write_lancedb's very first production run (registry/comments/
    digest, all much smaller, wrote fine in the same invocation before it hit
    the wall on news_article). This filters by S3 LastModified >=
    cycle_started_at instead -- checkpoints are always written by an embed_*
    Lambda that runs strictly after cycle_started_at (see the whole chain's
    threading), so this can never miss a checkpoint that belongs to the
    current cycle, and the read cost stays flat (bounded by one cycle's worth
    of new chunks, ~1-2 checkpoint files at CHECKPOINT_SIZE=500) instead of
    growing without bound as the corpus grows."""
    from datetime import datetime
    cutoff = datetime.fromisoformat(cycle_started_at)

    prefix = f"vectors/_checkpoints/{source}/{model_label}/"
    records = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json") and obj["LastModified"] >= cutoff:
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


def run_optimize(tbl):
    """Folds any unindexed rows into the table's existing vector index
    (or no-ops if the table has no index yet, still under
    MIN_ROWS_FOR_INDEX -- see scripts/write_to_lancedb.py) plus compacts
    small files and prunes old versions. Measured 2026-08-29 (one-off,
    all 4 tables): 2.4-13.3s even against a table with a 7-day backlog of
    ~10,700 unindexed rows -- well under this Lambda's 120s timeout, so
    called every cycle rather than on some N-cycle cadence (see
    tech_debt.md, "Vector Search Metric Mismatch..." entry for the full
    reasoning and the LanceDB guidance this measurement revises).
    Wrapped in try/except: optimize() is maintenance, not correctness --
    a failure here should never block the actual data write that already
    succeeded above it."""
    started = time.time()
    try:
        tbl.optimize()
    except Exception:
        return None
    return int((time.time() - started) * 1000)


def write_source(db, source, cycle_started_at):
    chunks = load_chunks(source, cycle_started_at)
    if chunks is None:
        return {"source": source, "status": "no_chunk_file"}
    if not chunks:
        return {"source": source, "status": "empty_chunk_file"}

    vectors = load_vectors(source, cycle_started_at)
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
        optimize_ms = run_optimize(tbl)
        return {"source": source, "status": "merged", "before": before, "after": after,
                "written": len(rows), "missing": len(missing), "optimize_ms": optimize_ms}
    else:
        tbl = db.create_table(table_name, data=rows)
        after = tbl.count_rows()
        optimize_ms = run_optimize(tbl)
        return {"source": source, "status": "created", "after": after,
                "written": len(rows), "missing": len(missing), "optimize_ms": optimize_ms}


def build_report_html(cycle_started_at, results, elapsed_s):
    def optimize_cell(r):
        ms = r.get("optimize_ms")
        if ms is None:
            return "-" if "optimize_ms" not in r else "failed"
        return f"{ms:,}ms"

    rows_html = "".join(
        f"<tr><td>{r['source']}</td><td>{r['status']}</td>"
        f"<td>{r.get('before', '-')}</td><td>{r.get('after', '-')}</td>"
        f"<td>{r.get('written', 0):,}</td><td>{r.get('missing', 0):,}</td>"
        f"<td>{optimize_cell(r)}</td></tr>"
        for r in results
    )
    return f"""
    <html><body style="font-family: monospace; font-size: 13px;">
    <h2>Poly-RAG Fase 3 (write_lancedb) -- {cycle_started_at}</h2>
    <table border="1" cellpadding="4" cellspacing="0">
      <tr><th>source</th><th>status</th><th>rows before</th><th>rows after</th>
          <th>written</th><th>missing</th><th>optimize()</th></tr>
      {rows_html}
    </table>
    <h3>Total time: {elapsed_s:.1f}s</h3>
    </body></html>
    """


def send_report(cycle_started_at, results, elapsed_s):
    html_body = build_report_html(cycle_started_at, results, elapsed_s)
    ses.send_email(
        Source=SES_SENDER,
        Destination={"ToAddresses": [SES_RECIPIENT]},
        Message={
            "Subject": {"Data": f"Poly-RAG Fase 3 (LanceDB) -- {cycle_started_at}"},
            "Body": {"Html": {"Data": html_body}},
        },
    )


def lambda_handler(event, context):
    import lancedb  # imported inside the handler: only needed once invoked,
    # keeps cold-start import time honest about what this Lambda actually
    # needs before the connection is opened.

    cycle_started_at = event["cycle_started_at"]  # no fallback -- Fase 3
    # only makes sense chained from Fase 2, never invoked standalone without
    # a specific cycle to write.

    started = time.time()
    db = lancedb.connect(LANCEDB_URI)
    results = [write_source(db, source, cycle_started_at) for source in SOURCES]
    elapsed_s = time.time() - started

    # Third checkpoint email of the cycle (2026-08-22), separate from
    # send_digest's market-content digest and digest_metrics' Fase 1+2 cost
    # report -- lands genuinely after both, so Fase 3's own wall-clock
    # duration is measurable from the gap between all three timestamps, same
    # reasoning as the Fase 1/Fase 2 split.
    send_report(cycle_started_at, results, elapsed_s)

    return {
        "cycle_started_at": cycle_started_at,
        "results": results,
        "report_sent": True,
    }
