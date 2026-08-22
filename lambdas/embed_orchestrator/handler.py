"""
Poly-RAG embed_orchestrator Lambda: entry point for Fase 2 (embedding),
invoked by send_digest at the end of every Fase 1 ingestion cycle. Fase 2,
level 1.

WHY THIS EXISTS
---------------
architecture_canon.md's original Fase 2 design (2026-08-20) called for this
Lambda to fan OUT to the 4 chunking Lambdas in parallel. Revised 2026-08-22,
same session the chunking/embedding Lambdas were actually built: chunking IS
still fanned out in parallel here (registry/comments/digest/news_article
don't share any limited resource, so there's no coordination risk), but
embedding is NOT -- the 4 embed Lambdas run SEQUENTIALLY, chained to each
other directly (embed_digest -> embed_comments -> embed_registry ->
embed_news_article), because they all draw against the same Cohere Embed v4
TPM ceiling and running them in parallel would recreate the exact
uncoordinated-competing-processes problem that already caused the real News
double-invocation incident (see tech_debt.md, "Strict Ingestion Chaining").

This Lambda's only job: fan out the 4 chunking Lambdas, then invoke ONLY the
first embed Lambda (embed_digest) -- which invokes the rest of the embed
chain itself once it finishes. It does NOT wait for chunking to complete
before invoking embed_digest, because embed_digest's job is to read its OWN
chunk file (digest, the fastest of the 4 to chunk) and it will simply find
nothing new if chunk_digest hasn't finished yet -- but see the note below on
why this is safe in practice, and the real risk it accepts.

REAL OPEN RISK, accepted deliberately for now (not solved here): embed_digest
could run before chunk_digest's S3 write completes, or embed_news_article
(the last in the embed chain) could run before chunk_news_article finishes
its own S3 write, since chunking and embedding are two SEPARATE fan-outs with
no synchronization between them. In practice this is unlikely to bite at
current cycle volumes (each chunking Lambda finishes in seconds to low tens
of seconds; the embed chain takes longer per stage due to Bedrock latency and
pacing), but it is a real race, not a proven-safe design. Fase 1's
merge_batch_payloads pattern (detect "all expected files exist" before
advancing) is the correct fix if this ever causes a real miss -- deliberately
NOT implemented preemptively here, consistent with this project's "measure,
don't guess" discipline once real evidence of a miss exists.

TRIGGER
-------
Invoked by send_digest at the end of lambda_handler, carrying
cycle_started_at (same threading pattern as every Fase 1 stage).

OUTPUT
------
No S3 write of its own -- pure fan-out/dispatch Lambda, same shape as
ingest_news's batch dispatcher.
"""

import json
import os
from datetime import datetime, timezone

import boto3

CHUNK_LAMBDA_NAMES = [
    os.environ.get("CHUNK_REGISTRY_NAME", "poly-rag-chunk-registry"),
    os.environ.get("CHUNK_COMMENTS_NAME", "poly-rag-chunk-comments"),
    os.environ.get("CHUNK_DIGEST_NAME", "poly-rag-chunk-digest"),
    os.environ.get("CHUNK_NEWS_ARTICLE_NAME", "poly-rag-chunk-news-article"),
]
FIRST_EMBED_LAMBDA_NAME = os.environ.get("FIRST_EMBED_LAMBDA_NAME", "poly-rag-embed-digest")

lambda_client = boto3.client("lambda")


def invoke_async(function_name, payload):
    lambda_client.invoke(
        FunctionName=function_name,
        InvocationType="Event",
        Payload=json.dumps(payload),
    )


def lambda_handler(event, context):
    cycle_started_at = event.get("cycle_started_at") or datetime.now(timezone.utc).isoformat()
    payload = {"cycle_started_at": cycle_started_at}

    for name in CHUNK_LAMBDA_NAMES:
        invoke_async(name, payload)

    invoke_async(FIRST_EMBED_LAMBDA_NAME, payload)

    return {
        "cycle_started_at": cycle_started_at,
        "chunk_lambdas_dispatched": CHUNK_LAMBDA_NAMES,
        "embed_chain_started": FIRST_EMBED_LAMBDA_NAME,
    }
