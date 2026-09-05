"""
Live-session logging for the Poly-RAG Gradio evaluation UI.

This UI is an internal instrument for a technical team evaluating retrieval +
synthesis quality, NOT a consumer chatbot (see gradio_app/app.py's header).
When "Log this session" is on (ON by default -- a viewer who does not want to
be recorded turns it off in Model parameters), every turn is persisted to S3
as a standalone JSON object so the interaction can be re-evaluated later
(RAGAS batch, the longitudinal drift judge, or a human review) without
depending on the state of the LanceDB index at that future moment -- hence the
FULL retrieved context is stored, not just ids.

S3 is the source of truth, same as every other corpus artifact in this
project (odds/, news/, chunks/, vectors/). Nothing is written to the repo or
to git -- git tracks this file, not its output.

Layout:  s3://<bucket>/evals/live_sessions/YYYY-MM-DD/<session_id>.json
One object per session, holding a growing list of per-turn records. Each turn
appends and re-PUTs the whole object (sessions are short -- a handful of turns
-- so a read-modify-write per turn is cheap and keeps one file per session
rather than one per turn).

The Space's IAM user (poly-rag-hf-spaces-readonly) is granted s3:PutObject and
s3:GetObject ONLY on evals/live_sessions/* for this -- see the inline policy
poly-rag-readonly-retrieval. A logging failure must never break the chat: all
writes are best-effort and swallow exceptions (returning False), the caller
just surfaces the state in the debug panel.
"""

import json
import os
import uuid
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

S3_BUCKET = os.environ.get("S3_BUCKET", "poly-rag-369970405415")
PREFIX = "evals/live_sessions"

_s3 = boto3.client("s3", region_name="us-east-1")


def new_session_id():
    """UTC-timestamp prefix keeps files sortable in an `aws s3 ls`; the short
    uuid tail disambiguates two sessions started in the same second."""
    return f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"


def _key(session_id):
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return f"{PREFIX}/{day}/{session_id}.json"


def _load(session_id):
    """Existing session object, or a fresh skeleton. Best-effort -- any read
    failure (missing key on the first turn included) starts a new skeleton."""
    try:
        obj = _s3.get_object(Bucket=S3_BUCKET, Key=_key(session_id))
        return json.loads(obj["Body"].read())
    except (ClientError, json.JSONDecodeError, KeyError):
        return {
            "session_id": session_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "turns": [],
        }


def log_turn(session_id, record):
    """Append one per-turn record and re-PUT the whole session object.

    record is built by the caller (app.py's chat()) and is expected to carry,
    at minimum: turn_index, timestamp, question, rewritten_query,
    resolved_market_ids, retrieved_context (FULL -- the merged/deduped results
    dict actually sent to synthesis), answer, latency_ms (a dict), tokens (a
    dict), estimated_cost_usd, flags (a dict of the UI toggles for this turn),
    ragas_scores (or null), window_state (live/evicted turns).

    Returns True on a successful write, False on any failure -- the caller
    shows this in the debug panel and never raises into the chat flow.
    """
    try:
        doc = _load(session_id)
        record = {**record, "logged_at": datetime.now(timezone.utc).isoformat()}
        doc["turns"].append(record)
        doc["updated_at"] = record["logged_at"]
        _s3.put_object(
            Bucket=S3_BUCKET,
            Key=_key(session_id),
            Body=json.dumps(doc, default=str).encode("utf-8"),
            ContentType="application/json",
        )
        return True
    except Exception as exc:  # noqa: BLE001 -- logging must never break chat
        print(f"[live_logging] write failed for session {session_id}: {exc!r}")
        return False
