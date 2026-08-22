"""
Poly-RAG chunk_registry Lambda: chunks only the market_ids that entered the
registry THIS cycle into question+description chunks, ready for embedding.
Fase 2, level 2 (chunking) -- one of 4 chunking Lambdas fanned out in
parallel by poly-rag-embed-orchestrator.

WHY THIS EXISTS, AND WHY IT DIFFERS FROM THE OTHER 3 CHUNKING LAMBDAS
-----------------------------------------------------------------------
Registry has no per-cycle S3 payload like News/Comments/Digest do -- it's a
live DynamoDB scan of current market metadata, decided 2026-08-21 (see
tech_debt.md / session_ledger.md, "Opcion C") after two alternatives (A:
treat first_seen as if it were a real per-cycle version; B: same but with
honest as-of-now metadata) were both rejected as reinventing a versioning
concept the registry doesn't have (no DynamoDB time-travel, unlike the
Delta Lake exploration in Databricks).

The actual mechanism, settled with the user 2026-08-22 (corrected once in the
same session -- see scan_new_registry_items docstring below for the earlier,
wrong version that computed a 12h lookback window): registry already carries
first_seen, written once when ingest_polymarket first adds the item, and
every cycle already carries cycle_started_at, captured once at the START of
the whole Fase 1 chain and threaded through every stage unchanged. Since
cycle_started_at is fixed BEFORE this cycle's new markets get written, any
item with first_seen > cycle_started_at is, by construction, new to this
exact cycle -- no lookback math, no assumption about cron cadence, no
dependency on knowing when the prior cycle ran. The question was never "what
happened since the last cycle" (which needs to know when that was), it was
always "what is new as of right now" (which first_seen already answers on
its own).

This means: NO new DynamoDB table, NO new field written to the registry, NO
re-reading of the growing history of chunks/registry/*.json files, and NO
lookback-window math. Just a scan filtered by first_seen > cycle_started_at.

WHAT GETS CHUNKED (unchanged from the one-off design, see architecture_canon.md
"Capa semantica de Polymarket"): question + description combined into one
text, 1 vector per market_id, no split within a market. This is ONLY the
semantic half of the Polymarket source -- odds/volume/liquidity are never
embedded, they live in the structured F1-F5 metadata filter (market_id +
date range), never semantic search. See tech_debt.md, "Vector Store Choice"
correction, for why this distinction matters for retrieval design.

TRIGGER
-------
Invoked by poly-rag-embed-orchestrator, carrying cycle_started_at.

OUTPUT
------
Writes s3://<bucket>/chunks/registry/<cycle date/hour>.json -- ONLY the
markets new to this cycle, unlike scripts/bootstrap_chunk_corpus.py's
--source registry (which always chunks the full current registry, by design,
since it's the one-off bootstrap path).
"""

import json
import os
from datetime import datetime, timezone

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "poly-rag-369970405415")
REGISTRY_TABLE = os.environ.get("REGISTRY_TABLE", "poly-rag-market-registry")

dynamodb = boto3.resource("dynamodb")
s3 = boto3.client("s3")


def scan_new_registry_items(cycle_started_at):
    """Full registry scan, filtered client-side by first_seen >= cycle_started_at.

    Corrected 2026-08-22, same session: the first version computed a fixed
    12h lookback window (cycle_started_at minus CYCLE_INTERVAL_HOURS) to
    approximate "the previous cycle's start", assuming the EventBridge cron
    cadence. The user caught the actual question being asked backwards --
    this Lambda doesn't need to know anything about the PREVIOUS cycle at
    all. It only needs "which market_ids are new AS OF right now", which
    first_seen already answers directly: cycle_started_at is captured once
    at the START of the whole Fase 1 chain (in ingest_polymarket), BEFORE any
    market from this cycle gets written -- so any item with
    first_seen >= cycle_started_at is, by construction, one that entered
    during this exact cycle, with no assumption about cadence, no lookback
    math, and no dependency on when the prior cycle happened to run.

    Second correction, also 2026-08-22 (found verifying cycle 14's real
    output was 0 despite 25 markets actually entering): ingest_polymarket
    captures a single now_iso and reuses it for BOTH first_seen (of every
    market upserted that cycle) AND cycle_started_at threaded through the
    whole chain -- so first_seen is never strictly greater than
    cycle_started_at, it is exactly equal. The filter must be >=, not >."""
    table = dynamodb.Table(REGISTRY_TABLE)
    new_items = []
    response = table.scan()
    for item in response["Items"]:
        first_seen = item.get("first_seen")
        if first_seen and first_seen >= cycle_started_at:
            new_items.append(item)
    while "LastEvaluatedKey" in response:
        response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        for item in response["Items"]:
            first_seen = item.get("first_seen")
            if first_seen and first_seen >= cycle_started_at:
                new_items.append(item)
    return new_items


def chunk_registry_item(item):
    question = (item.get("question") or "").strip()
    description = (item.get("description") or "").strip()
    if not question:
        return None
    text = f"{question}\n\n{description}" if description else question
    return {
        "chunk_id": item["market_id"],
        "market_id": item["market_id"],
        "status": item.get("status"),
        "end_date": item.get("end_date"),
        "resolution_date": item.get("resolution_date"),
        "text": text,
    }


def registry_output_key(cycle_started_at):
    dt = datetime.fromisoformat(cycle_started_at)
    return f"chunks/registry/{dt.strftime('%Y-%m-%d')}/{dt.strftime('%H')}.json"


def lambda_handler(event, context):
    cycle_started_at = event.get("cycle_started_at") or datetime.now(timezone.utc).isoformat()
    dry_run = event.get("dry_run", False)

    new_items = scan_new_registry_items(cycle_started_at)
    chunks = []
    skipped_no_question = 0
    for item in new_items:
        chunk = chunk_registry_item(item)
        if chunk is None:
            skipped_no_question += 1
            continue
        chunk["_lineage"] = {"written_by": "poly-rag-chunk-registry", "run_type": "automated_cycle"}
        chunks.append(chunk)

    output_key = registry_output_key(cycle_started_at)

    if dry_run:
        return {
            "source": "registry",
            "cycle_started_at": cycle_started_at,
            "new_markets_found": len(new_items),
            "chunks_would_write": len(chunks),
            "skipped_no_question": skipped_no_question,
            "output_key_would_be": output_key,
            "dry_run": True,
        }

    s3.put_object(
        Bucket=S3_BUCKET,
        Key=output_key,
        Body=json.dumps(chunks),
        ContentType="application/json",
    )

    return {
        "source": "registry",
        "cycle_started_at": cycle_started_at,
        "new_markets_found": len(new_items),
        "chunks_written": len(chunks),
        "skipped_no_question": skipped_no_question,
        "output_key": output_key,
    }
