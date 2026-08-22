"""
Poly-RAG embed_comments Lambda: embeds the current cycle's comment chunks
into vectors via Cohere Embed v4. Fase 2, level 3 (embedding), second in the
sequential chain (see embed_digest handler.py docstring for the full
sequencing rationale -- 4 embed Lambdas run in SEQUENCE, not parallel, to
avoid multiple processes competing for the same Cohere TPM ceiling).

Same mechanism as embed_digest -- see that handler's docstring for the full
design rationale (token-aware batching, sliding-window rate governor,
checkpointing, MODEL_ID choice). Only VARIANT and NEXT_LAMBDA_NAME differ.

TRIGGER: invoked by embed_digest, carries cycle_started_at.
OUTPUT: writes vectors/_checkpoints/comments/cohere/part_NNNNN.json, then
invokes embed_registry.
"""

import json
import os
import time
import uuid
from collections import deque
from datetime import datetime, timezone

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

S3_BUCKET = os.environ.get("S3_BUCKET", "poly-rag-369970405415")
NEXT_LAMBDA_NAME = os.environ.get("NEXT_LAMBDA_NAME", "poly-rag-embed-registry")
EMBEDDING_METRICS_TABLE = os.environ.get("EMBEDDING_METRICS_TABLE", "poly-rag-embedding-metrics")

MODEL_ID = "global.cohere.embed-v4:0"
EMBED_DIM = 1536
COST_PER_MILLION_INPUT_TOKENS = 0.12

TPM_LIMIT = 300_000
RPM_LIMIT = 200
TPM_TARGET = int(TPM_LIMIT * 0.50)
MAX_TOKENS_PER_REQUEST = 40_000
MAX_TEXTS_PER_REQUEST = 96
CHECKPOINT_SIZE = 500
MAX_RETRIES = 6
CHARS_PER_TOKEN = 4

VARIANT = "comments"

s3 = boto3.client("s3")
lambda_client = boto3.client("lambda")
dynamodb = boto3.resource("dynamodb")
bedrock = boto3.client(
    "bedrock-runtime",
    region_name="us-east-1",
    config=Config(retries={"max_attempts": 0}, max_pool_connections=10),
)


def write_embedding_metric(cycle_started_at, tokens_in, latency_ms, chunk_count):
    table = dynamodb.Table(EMBEDDING_METRICS_TABLE)
    now = datetime.now(timezone.utc).isoformat()
    cost = (tokens_in / 1_000_000) * COST_PER_MILLION_INPUT_TOKENS
    table.put_item(Item={
        "pk": f"{cycle_started_at}#{VARIANT}#{now}#{uuid.uuid4().hex[:8]}",
        "cycle_started_at": cycle_started_at,
        "source": VARIANT,
        "embedding_model": MODEL_ID,
        "tokens_in": tokens_in,
        "latency_ms": latency_ms,
        "chunk_count": chunk_count,
        "estimated_cost_usd": str(round(cost, 6)),
        "timestamp": now,
    })


def estimate_tokens(text):
    return max(1, len(text) // CHARS_PER_TOKEN)


def build_batches(chunks):
    batches, current, current_tokens = [], [], 0
    for chunk in chunks:
        tokens = estimate_tokens(chunk["text"])
        would_exceed_tokens = current and current_tokens + tokens > MAX_TOKENS_PER_REQUEST
        would_exceed_count = len(current) >= MAX_TEXTS_PER_REQUEST
        if would_exceed_tokens or would_exceed_count:
            batches.append(current)
            current, current_tokens = [], 0
        current.append(chunk)
        current_tokens += tokens
    if current:
        batches.append(current)
    return batches


class RateGovernor:
    def __init__(self, tpm_target=TPM_TARGET, rpm_limit=RPM_LIMIT):
        self.tpm_target = tpm_target
        self.rpm_limit = rpm_limit
        self.events = deque()

    def _prune(self, now):
        while self.events and now - self.events[0][0] >= 60.0:
            self.events.popleft()

    def _window(self):
        return sum(t for _, t in self.events), len(self.events)

    def wait_for(self, tokens):
        while True:
            now = time.monotonic()
            self._prune(now)
            win_tokens, win_requests = self._window()
            if (win_tokens + tokens <= self.tpm_target
                    and win_requests + 1 <= self.rpm_limit):
                return
            if not self.events:
                return
            need_tokens = (win_tokens + tokens) - self.tpm_target
            need_requests = (win_requests + 1) - self.rpm_limit
            freed_tokens = 0
            wait_until = None
            for i, (ts, tok) in enumerate(self.events, start=1):
                freed_tokens += tok
                wait_until = ts
                if freed_tokens >= need_tokens and i >= need_requests:
                    break
            sleep_for = max(0.05, 60.0 - (now - wait_until) + 0.05)
            time.sleep(sleep_for)

    def record(self, tokens):
        self.events.append((time.monotonic(), tokens))


THROTTLE_CODES = {"ThrottlingException", "TooManyRequestsException",
                  "ServiceQuotaExceededException"}
RETRYABLE_CODES = THROTTLE_CODES | {
    "500", "ModelErrorException", "ModelNotReadyException",
    "ServiceUnavailableException", "InternalServerException",
    "ModelTimeoutException",
}


def is_retryable(exc):
    error = exc.response.get("Error", {})
    code = str(error.get("Code", ""))
    if code in RETRYABLE_CODES:
        return True, code
    status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
    if status >= 500 or status == 429:
        return True, f"HTTP {status}"
    return False, code


def embed_texts(texts):
    body = json.dumps({
        "texts": texts,
        "input_type": "search_document",
        "embedding_types": ["float"],
    })
    delay = 2.0
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = bedrock.invoke_model(modelId=MODEL_ID, body=body)
            payload = json.loads(response["body"].read())
            vectors = payload["embeddings"]["float"]
            if len(vectors) != len(texts):
                raise RuntimeError(f"asked for {len(texts)} embeddings, got {len(vectors)}")
            return vectors
        except ClientError as exc:
            retryable, code = is_retryable(exc)
            if not retryable or attempt == MAX_RETRIES:
                raise
            time.sleep(delay)
            delay = min(delay * 2, 60.0)
        except Exception:
            if attempt == MAX_RETRIES:
                raise
            time.sleep(delay)
            delay = min(delay * 2, 60.0)
    raise RuntimeError("unreachable")


def checkpoint_key(part_index):
    return f"vectors/_checkpoints/{VARIANT}/cohere/part_{part_index:05d}.json"


def existing_checkpoints():
    prefix = f"vectors/_checkpoints/{VARIANT}/cohere/"
    found = set()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            name = obj["Key"].rsplit("/", 1)[-1]
            if name.startswith("part_") and name.endswith(".json"):
                found.add(int(name[5:-5]))
    return found


def already_embedded_ids():
    prefix = f"vectors/_checkpoints/{VARIANT}/cohere/"
    done = set()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if not obj["Key"].endswith(".json"):
                continue
            records = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=obj["Key"])["Body"].read())
            done.update(r["chunk_id"] for r in records)
    return done


def write_checkpoint(part_index, records):
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=checkpoint_key(part_index),
        Body=json.dumps(records),
        ContentType="application/json",
    )


def chunk_input_key(cycle_started_at):
    dt = datetime.fromisoformat(cycle_started_at)
    return f"chunks/{VARIANT}/{dt.strftime('%Y-%m-%d')}/{dt.strftime('%H')}.json"


def invoke_next_stage(cycle_started_at):
    lambda_client.invoke(
        FunctionName=NEXT_LAMBDA_NAME,
        InvocationType="Event",
        Payload=json.dumps({"cycle_started_at": cycle_started_at}),
    )


def lambda_handler(event, context):
    cycle_started_at = event.get("cycle_started_at") or datetime.now(timezone.utc).isoformat()
    skip_chain = event.get("skip_chain", False)

    input_key = chunk_input_key(cycle_started_at)
    try:
        chunks = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=input_key)["Body"].read())
    except s3.exceptions.NoSuchKey:
        raise RuntimeError(f"chunk file not found at s3://{S3_BUCKET}/{input_key}")

    done_ids = already_embedded_ids()
    if done_ids:
        chunks = [c for c in chunks if c["chunk_id"] not in done_ids]

    result = {
        "source": VARIANT,
        "cycle_started_at": cycle_started_at,
        "chunks_embedded": 0,
    }

    if chunks:
        batches = build_batches(chunks)
        governor = RateGovernor()
        existing_parts = existing_checkpoints()
        part_index = (max(existing_parts) + 1) if existing_parts else 0
        pending = []

        for batch in batches:
            tokens = sum(estimate_tokens(c["text"]) for c in batch)
            governor.wait_for(tokens)
            started = time.monotonic()
            vectors = embed_texts([c["text"] for c in batch])
            latency_ms = int((time.monotonic() - started) * 1000)
            governor.record(tokens)
            write_embedding_metric(cycle_started_at, tokens, latency_ms, len(batch))

            for chunk, vector in zip(batch, vectors):
                record = {k: v for k, v in chunk.items() if k != "text"}
                record["embedding"] = vector
                record["embedding_model"] = MODEL_ID
                record["embedding_dim"] = len(vector)
                pending.append(record)

            while len(pending) >= CHECKPOINT_SIZE:
                write_checkpoint(part_index, pending[:CHECKPOINT_SIZE])
                pending = pending[CHECKPOINT_SIZE:]
                part_index += 1

        if pending:
            write_checkpoint(part_index, pending)

        result["chunks_embedded"] = len(chunks)

    if not skip_chain:
        invoke_next_stage(cycle_started_at)
        result["next_stage_invoked"] = NEXT_LAMBDA_NAME

    return result
