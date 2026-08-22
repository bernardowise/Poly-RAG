"""
Poly-RAG embed_news_article Lambda: embeds this cycle's news article chunks
into vectors via Cohere Embed v4. Fase 2, level 3 (embedding), fourth and
LAST in the sequential chain (see embed_digest handler.py docstring for the
full sequencing rationale -- 4 embed Lambdas run in SEQUENCE, not parallel,
to avoid multiple processes competing for the same Cohere TPM ceiling).

Same mechanism as embed_digest -- see that handler's docstring for the full
design rationale (token-aware batching, sliding-window rate governor,
checkpointing, MODEL_ID choice, and why MODEL_ID is specifically
global.cohere.embed-v4:0 after two real daily-quota outages on the other two
routing paths, see tech_debt.md).

Placed last deliberately, same order already measured running these 4
sources by hand on 2026-08-21/22: news_article dominates token volume (~96%
of the 4-source total in the 13-cycle bootstrap), so it runs after the 3
cheap sources have already succeeded and written their checkpoints -- a
failure here costs the least amount of already-completed upstream work to
retry.

TERMINAL STAGE: this is the end of Fase 2. Fase 3 (write to vector store) is
explicitly out of scope for now (2026-08-22 user decision) -- this Lambda
does NOT invoke anything further downstream. NEXT_LAMBDA_NAME is
intentionally unset; when Fase 3 is built, wiring it in is a one-line
addition here, not a redesign.

METRICS REPORT EMAIL (added 2026-08-22, same session as the metrics table
itself, per explicit user request -- "una cerecita de pastel"): being the
last stage of BOTH Fase 2 chunking/embedding AND therefore the true end of
the whole cycle (Fase 1 -> Fase 2), this Lambda queries poly-rag-embedding-
metrics (Fase 2, written by all 4 embed Lambdas this cycle) and scans
poly-rag-architecture-metrics filtered to this cycle (Fase 1, already
written by the 4 ingestion Lambdas) and sends ONE summary email covering the
full cycle's cost/latency/tokens -- separate from send_digest's own email
(the market-content digest), which already goes out at the END of Fase 1,
before Fase 2 even starts. Two emails per cycle, by design: one about the
DATA (send_digest), one about the PIPELINE that produced it (this one).

TRIGGER: invoked by embed_registry, carries cycle_started_at.
OUTPUT: writes vectors/_checkpoints/news_article/cohere/part_NNNNN.json,
writes its own row(s) to poly-rag-embedding-metrics, sends the cycle metrics
report email via SES. Chain ends here.
"""

import json
import os
import time
import uuid
from collections import deque
from datetime import datetime, timedelta, timezone

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

S3_BUCKET = os.environ.get("S3_BUCKET", "poly-rag-369970405415")
EMBEDDING_METRICS_TABLE = os.environ.get("EMBEDDING_METRICS_TABLE", "poly-rag-embedding-metrics")
ARCHITECTURE_METRICS_TABLE = os.environ.get("ARCHITECTURE_METRICS_TABLE", "poly-rag-architecture-metrics")
SES_SENDER = os.environ.get("SES_SENDER", "bernardolw@gmail.com")
SES_RECIPIENT = os.environ.get("SES_RECIPIENT", "bernardolw@gmail.com")

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

VARIANT = "news_article"

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
ses = boto3.client("ses")
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


def fetch_embedding_metrics(cycle_started_at):
    """All Fase 2 rows for this cycle, across all 4 sources -- pk is prefixed
    with cycle_started_at#source#..., so a begins_with Query on pk (partition
    key equality doesn't apply here since pk isn't just cycle_started_at, so
    this uses Scan with a filter, same reasoning as fetch_architecture_metrics
    below) returns every row this cycle wrote, regardless of which of the 4
    embed Lambdas wrote it."""
    table = dynamodb.Table(EMBEDDING_METRICS_TABLE)
    items = []
    response = table.scan(
        FilterExpression="cycle_started_at = :c",
        ExpressionAttributeValues={":c": cycle_started_at},
    )
    items.extend(response["Items"])
    while "LastEvaluatedKey" in response:
        response = table.scan(
            FilterExpression="cycle_started_at = :c",
            ExpressionAttributeValues={":c": cycle_started_at},
            ExclusiveStartKey=response["LastEvaluatedKey"],
        )
        items.extend(response["Items"])
    return items


def fetch_architecture_metrics(cycle_started_at):
    """Fase 1 rows for this cycle. architecture_metrics has no cycle_started_at
    field (Fase 1 predates Fase 2 and was never designed to be queried by
    cycle, see ingest_lambda_role's PutItem-only grant) -- rows are matched by
    their own `timestamp` falling within [cycle_started_at, cycle_started_at +
    12h), same window used for the Fase 1 stages of THIS cycle specifically.
    A Scan, not a Query -- this table's hash key is a per-invocation pk
    string, not something a range query can use for a time filter."""
    cycle_dt = datetime.fromisoformat(cycle_started_at)
    window_end_iso = (cycle_dt + timedelta(hours=12)).isoformat()

    table = dynamodb.Table(ARCHITECTURE_METRICS_TABLE)
    items = []
    response = table.scan()
    for item in response["Items"]:
        ts = item.get("timestamp", "")
        if cycle_started_at <= ts < window_end_iso:
            items.append(item)
    while "LastEvaluatedKey" in response:
        response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        for item in response["Items"]:
            ts = item.get("timestamp", "")
            if cycle_started_at <= ts < window_end_iso:
                items.append(item)
    return items


def build_metrics_report_html(cycle_started_at, fase1_rows, fase2_rows):
    def fmt_usd(x):
        return f"${x:.4f}"

    fase1_by_source = {}
    for r in fase1_rows:
        src = r.get("source", "unknown")
        agg = fase1_by_source.setdefault(src, {"tokens_in": 0, "tokens_out": 0, "cost": 0.0, "latency_ms": 0, "count": 0})
        agg["tokens_in"] += int(r.get("tokens_in", 0))
        agg["tokens_out"] += int(r.get("tokens_out", 0))
        agg["cost"] += float(r.get("estimated_cost_usd", 0))
        agg["latency_ms"] += int(r.get("latency_ms", 0))
        agg["count"] += 1

    fase2_by_source = {}
    for r in fase2_rows:
        src = r.get("source", "unknown")
        agg = fase2_by_source.setdefault(src, {"tokens_in": 0, "cost": 0.0, "latency_ms": 0, "count": 0, "chunks": 0})
        agg["tokens_in"] += int(r.get("tokens_in", 0))
        agg["cost"] += float(r.get("estimated_cost_usd", 0))
        agg["latency_ms"] += int(r.get("latency_ms", 0))
        agg["count"] += 1
        agg["chunks"] += int(r.get("chunk_count", 0))

    total_cost = sum(a["cost"] for a in fase1_by_source.values()) + sum(a["cost"] for a in fase2_by_source.values())

    fase1_rows_html = "".join(
        f"<tr><td>{src}</td><td>{a['count']}</td><td>{a['tokens_in']:,}</td>"
        f"<td>{a['tokens_out']:,}</td><td>{a['latency_ms']:,} ms</td><td>{fmt_usd(a['cost'])}</td></tr>"
        for src, a in sorted(fase1_by_source.items())
    )
    fase2_rows_html = "".join(
        f"<tr><td>{src}</td><td>{a['chunks']:,}</td><td>{a['count']}</td>"
        f"<td>{a['tokens_in']:,}</td><td>{a['latency_ms']:,} ms</td><td>{fmt_usd(a['cost'])}</td></tr>"
        for src, a in sorted(fase2_by_source.items())
    )

    return f"""
    <html><body style="font-family: monospace; font-size: 13px;">
    <h2>Poly-RAG Cycle Metrics -- {cycle_started_at}</h2>

    <h3>Fase 1 (ingestion)</h3>
    <table border="1" cellpadding="4" cellspacing="0">
      <tr><th>source</th><th>invocations</th><th>tokens_in</th><th>tokens_out</th><th>latency</th><th>cost</th></tr>
      {fase1_rows_html or '<tr><td colspan="6">no rows found for this cycle</td></tr>'}
    </table>

    <h3>Fase 2 (chunking + embedding)</h3>
    <table border="1" cellpadding="4" cellspacing="0">
      <tr><th>source</th><th>chunks</th><th>requests</th><th>tokens_in</th><th>latency</th><th>cost</th></tr>
      {fase2_rows_html or '<tr><td colspan="6">no rows found for this cycle</td></tr>'}
    </table>

    <h3>Total estimated cost this cycle: {fmt_usd(total_cost)}</h3>
    </body></html>
    """


def send_metrics_report(cycle_started_at, fase1_rows, fase2_rows):
    html_body = build_metrics_report_html(cycle_started_at, fase1_rows, fase2_rows)
    ses.send_email(
        Source=SES_SENDER,
        Destination={"ToAddresses": [SES_RECIPIENT]},
        Message={
            "Subject": {"Data": f"Poly-RAG Cycle Metrics -- {cycle_started_at}"},
            "Body": {"Html": {"Data": html_body}},
        },
    )


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


def lambda_handler(event, context):
    cycle_started_at = event.get("cycle_started_at") or datetime.now(timezone.utc).isoformat()

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

    # Terminal stage of the whole cycle (Fase 1 + Fase 2) -- send the
    # cost/latency/tokens report email. Runs regardless of whether THIS
    # Lambda had new chunks to embed (chunks may be empty if news_article's
    # chunk file was already fully covered), since the report covers the
    # whole cycle's Fase 1+2 activity, not just this Lambda's own work.
    fase2_rows = fetch_embedding_metrics(cycle_started_at)
    fase1_rows = fetch_architecture_metrics(cycle_started_at)
    send_metrics_report(cycle_started_at, fase1_rows, fase2_rows)
    result["metrics_report_sent"] = True

    return result
