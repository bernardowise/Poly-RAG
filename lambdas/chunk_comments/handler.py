"""
Poly-RAG chunk_comments Lambda: chunks the current cycle's comments into
entity-grouped chunks, ready for embedding. Fase 2, level 2 (chunking) -- one
of 4 chunking Lambdas fanned out in parallel by poly-rag-embed-orchestrator.

WHY THIS EXISTS
---------------
Ports comment_group_key/get_comment_entity_map/chunk_comments from
scripts/bootstrap_chunk_corpus.py (the one-off that chunked the full
historical corpus, 2026-08-20/21) to the per-cycle path. Same grouping logic,
same overflow rule -- the one-off covers the historical backfill, this Lambda
covers every cycle going forward.

Comments already has a natural per-cycle scope (comments/YYYY-MM-DD/HH.json is
already "only this cycle's new comments", per poly-rag-processed-comments
dedup in ingest_comments) -- no tracking table needed here, unlike registry.

CHUNKING RULE (unchanged from the one-off, see architecture_canon.md
"Chunking de Comments"): chunk unit depends on link_type -- `direct` groups by
market_id (1:1), `shared_event`/`shared_series` group by comment_entity_id (one
stream shared by every market under that entity, never re-embedded per
market). comment_entity_id does not live on the comment payload -- it's looked
up from the registry per market_id (see comment_group_key below, and the real
bug this fixed in the one-off: 67% of comments were misgrouped before this
lookup existed).

Chunk ids are entity-scoped ({entity_key}#{part}), which collide across
cycles for a persistent entity (the same market keeps getting new comments
cycle after cycle) -- unlike the bootstrap script's cycle-qualified ids
(@c<N>), the per-cycle Lambda path relies on Fase 3's write layer to append
new comment text to the entity's ongoing text rather than treating each
cycle's comments as a wholly separate chunk. This is a real open design
question, not resolved here -- see chunk_id note in lambda_handler below.

TRIGGER
-------
Invoked by poly-rag-embed-orchestrator, carrying cycle_started_at.

OUTPUT
------
Writes s3://<bucket>/chunks/comments/<cycle date/hour>.json.
"""

import json
import os
from collections import defaultdict
from datetime import datetime, timezone

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "poly-rag-369970405415")
REGISTRY_TABLE = os.environ.get("REGISTRY_TABLE", "poly-rag-market-registry")
MAX_COMMENT_CHUNK_CHARS = 6000  # ~1500 tokens, overflow spills to a new chunk

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")


def comments_cycle_prefix(cycle_started_at):
    dt = datetime.fromisoformat(cycle_started_at)
    return f"comments/{dt.strftime('%Y-%m-%d')}/{dt.strftime('%H')}.json"


def scan_registry_entity_map():
    """market_id -> comment_entity_id, one full registry scan reused for the
    whole cycle's comments (same pattern as the one-off script)."""
    table = dynamodb.Table(REGISTRY_TABLE)
    entity_map = {}
    response = table.scan(ProjectionExpression="market_id, comment_entity_id")
    for item in response["Items"]:
        if item.get("comment_entity_id"):
            entity_map[item["market_id"]] = item["comment_entity_id"]
    while "LastEvaluatedKey" in response:
        response = table.scan(
            ProjectionExpression="market_id, comment_entity_id",
            ExclusiveStartKey=response["LastEvaluatedKey"],
        )
        for item in response["Items"]:
            if item.get("comment_entity_id"):
                entity_map[item["market_id"]] = item["comment_entity_id"]
    return entity_map


def comment_group_key(comment, entity_map):
    link_type = comment.get("link_type", "unknown")
    market_ids = comment.get("market_ids") or []
    if link_type == "direct":
        entity_key = market_ids[0] if market_ids else "unknown_market"
    else:
        entity_key = next(
            (entity_map[mid] for mid in market_ids if mid in entity_map),
            "unknown_entity",
        )
    return link_type, entity_key


def chunk_comments(comments, entity_map, cycle_started_at):
    """market_ids_mentioned (added 2026-08-30): the union of market_ids of
    every comment folded into a group, carried onto every chunk written for
    that group -- makes comments_cohere filterable by market_id via
    list_contains(), same mechanism digest_cohere's market_ids_mentioned
    already uses (see retrieval/query.py, LIST_MARKET_IDS_SOURCES). This is
    metadata riding alongside the chunk, NOT the embedded text -- the vector
    computed by embed_comments is unaffected, only what's queryable
    afterward changes. See tech_debt.md, "Comments Not in Retrieval Cascade"
    for the full design discussion and the schema-evolution risk this
    required checking before deploying (LanceDB errors on merge_insert when
    a new row has a field the existing table schema doesn't -- see the
    Fase 3 _lineage/cycle_key precedent in session_ledger.md, 2026-08-22)."""
    groups = defaultdict(list)
    group_market_ids = defaultdict(set)
    for comment in comments:
        key = comment_group_key(comment, entity_map)
        text = (comment.get("text") or "").strip()
        if text:
            groups[key].append(text)
            group_market_ids[key].update(comment.get("market_ids") or [])

    chunks = []
    for (link_type, entity_key), texts in groups.items():
        market_ids_mentioned = sorted(group_market_ids[(link_type, entity_key)])
        buffer, buffer_len, part = [], 0, 0
        for text in texts:
            if buffer and buffer_len + len(text) > MAX_COMMENT_CHUNK_CHARS:
                chunks.append({
                    "chunk_id": f"{entity_key}#{part}@{cycle_started_at}",
                    "link_type": link_type,
                    "comment_entity_id": entity_key,
                    "cycle_started_at": cycle_started_at,
                    "market_ids_mentioned": market_ids_mentioned,
                    "text": "\n\n".join(buffer),
                })
                buffer, buffer_len = [], 0
                part += 1
            buffer.append(text)
            buffer_len += len(text)
        if buffer:
            chunks.append({
                "chunk_id": f"{entity_key}#{part}@{cycle_started_at}",
                "link_type": link_type,
                "comment_entity_id": entity_key,
                "cycle_started_at": cycle_started_at,
                "market_ids_mentioned": market_ids_mentioned,
                "text": "\n\n".join(buffer),
            })
    return chunks


def lambda_handler(event, context):
    cycle_started_at = event.get("cycle_started_at") or datetime.now(timezone.utc).isoformat()
    cycle_key = comments_cycle_prefix(cycle_started_at)
    dry_run = event.get("dry_run", False)

    try:
        payload = json.loads(
            s3.get_object(Bucket=S3_BUCKET, Key=cycle_key)["Body"].read()
        )
    except s3.exceptions.NoSuchKey:
        raise RuntimeError(f"comments payload not found at s3://{S3_BUCKET}/{cycle_key}")

    comments = payload.get("comments", [])
    entity_map = scan_registry_entity_map()
    chunks = chunk_comments(comments, entity_map, cycle_started_at)
    for c in chunks:
        c["_lineage"] = {"written_by": "poly-rag-chunk-comments", "run_type": "automated_cycle"}

    output_key = cycle_key.replace("comments/", "chunks/comments/")

    if dry_run:
        return {
            "source": "comments",
            "cycle_started_at": cycle_started_at,
            "comments_read": len(comments),
            "chunks_would_write": len(chunks),
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
        "source": "comments",
        "cycle_started_at": cycle_started_at,
        "comments_read": len(comments),
        "chunks_written": len(chunks),
        "output_key": output_key,
    }
