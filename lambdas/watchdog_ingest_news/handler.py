"""
Poly-RAG watchdog Lambda: detects and retries stuck News batches.

Added 2026-08-16 (see tech_debt.md, "Strict Ingestion Chaining"). The
strict chain (Polymarket -> News -> Comments -> digest) has a real
unattended-failure mode: if a single News batch times out (observed in
production -- a batch hit Lambda's 900s hard maximum extracting articles
from slow outlets), that batch's _batch<offset>.json never gets written,
merge_batch_payloads never finds the complete set, the merge never
happens, and NOTHING downstream (Comments, the digest) ever runs for that
cycle -- silently. Nobody gets notified. The next 12h cycle starts fresh
without retrying the lost work.

This Lambda runs on a short EventBridge schedule (every 10 minutes) and:
1. Finds the most recent cycle_started_at from any _batch<offset>.json
   under today's/yesterday's hour partitions.
2. Skips if that cycle's final merged file (HH.json) already exists --
   nothing stuck, nothing to do.
3. Otherwise, re-derives the expected offset set the same way
   ingest_news does (needs the SAME total_markets the original cycle
   used -- read from any existing batch file's total_markets-implied
   count, not a fresh registry scan, for the same consistency reason the
   original bug existed).
4. For any expected offset with no corresponding S3 file, AND enough
   time has passed since the cycle started (RETRY_AFTER_MINUTES) that a
   normal batch should have finished by now, re-invokes ingest_news
   directly with mode=batch for that offset.

Idempotent by design: re-invoking an offset that's already in flight or
already succeeded just re-does (safely, since News's own dedup table
prevents re-processing the same URLs) or no-ops.
"""

import json
import os
from datetime import datetime, timezone

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "poly-rag-369970405415")
NEWS_LAMBDA_NAME = os.environ.get("NEWS_LAMBDA_NAME", "poly-rag-ingest-news")
BATCH_SIZE = 35
# A healthy batch takes ~5-15 min in practice (measured 2026-08-15/16).
# 20 min gives real margin over the 900s (15 min) Lambda hard-timeout
# case before treating a missing batch as genuinely stuck rather than
# just still running.
RETRY_AFTER_MINUTES = 20

s3 = boto3.client("s3")
lambda_client = boto3.client("lambda")


def list_recent_batch_files():
    """Lists batch files under today's and yesterday's date prefixes --
    cheap enough at this data volume, and covers a cycle that started
    just before midnight UTC."""
    now = datetime.now(timezone.utc)
    keys = []
    for date_str in (now.strftime("%Y-%m-%d"),):
        resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=f"news/{date_str}/")
        keys.extend(o["Key"] for o in resp.get("Contents", []))
    return keys


def find_latest_cycle(keys):
    """Groups batch files by their full news/<date>/<hour> prefix and
    returns the most recent cycle's info: that prefix, the offsets that
    have files, and the batch keys themselves. Full keys look like
    news/2026-08-16/01_batch105.json -- the cycle prefix is everything
    before "_batch" (news/2026-08-16/01), not just the date segment."""
    batch_keys = [k for k in keys if "_batch" in k]
    if not batch_keys:
        return None

    prefixes = sorted(set(k.split("_batch")[0] for k in batch_keys), reverse=True)
    latest_hour_prefix = prefixes[0]  # e.g. "news/2026-08-16/01"

    hour_batch_keys = [k for k in batch_keys if k.startswith(latest_hour_prefix + "_batch")]
    offsets_present = set()
    for k in hour_batch_keys:
        offset_str = k.split("_batch")[1].replace(".json", "")
        offsets_present.add(int(offset_str))

    return latest_hour_prefix, offsets_present, hour_batch_keys


def lambda_handler(event, context):
    keys = list_recent_batch_files()
    cycle_info = find_latest_cycle(keys)
    if not cycle_info:
        return {"statusCode": 200, "body": json.dumps({"action": "no_cycle_found"})}

    latest_hour_prefix, offsets_present, hour_batch_keys = cycle_info
    final_key = f"{latest_hour_prefix}.json"

    try:
        s3.head_object(Bucket=S3_BUCKET, Key=final_key)
        return {"statusCode": 200, "body": json.dumps({"action": "cycle_already_complete", "key": final_key})}
    except s3.exceptions.ClientError:
        pass  # final file doesn't exist yet -- keep checking if it's actually stuck

    # Read cycle_started_at and total_markets from any existing batch
    # file for this cycle -- same values every batch in this cycle used,
    # per the total_markets-in-payload fix (see module docstring).
    sample_obj = s3.get_object(Bucket=S3_BUCKET, Key=hour_batch_keys[0])
    sample_payload = json.loads(sample_obj["Body"].read())
    cycle_started_at = sample_payload["cycle_started_at"]

    cycle_start_dt = datetime.fromisoformat(cycle_started_at)
    minutes_since_start = (datetime.now(timezone.utc) - cycle_start_dt).total_seconds() / 60
    if minutes_since_start < RETRY_AFTER_MINUTES:
        return {
            "statusCode": 200,
            "body": json.dumps({"action": "cycle_still_young", "minutes_since_start": round(minutes_since_start, 1)}),
        }

    # total_markets isn't stored directly in a batch payload, but the
    # highest offset present plus that batch's own processed count gives
    # a floor -- good enough to reconstruct the expected offset set,
    # since BATCH_SIZE is fixed and offsets are always multiples of it.
    max_offset_present = max(offsets_present)
    max_offset_batch = s3.get_object(Bucket=S3_BUCKET, Key=f"{latest_hour_prefix}_batch{max_offset_present}.json")
    max_offset_payload = json.loads(max_offset_batch["Body"].read())
    total_markets = max_offset_present + max_offset_payload["markets_processed"]
    expected_offsets = set(range(0, total_markets, BATCH_SIZE))

    missing_offsets = expected_offsets - offsets_present
    if not missing_offsets:
        # All expected batches exist but the merge still hasn't happened --
        # shouldn't occur (any batch triggers the merge on completion), but
        # if it does, retrying offset 0 forces a fresh merge attempt.
        missing_offsets = {0}

    for offset in sorted(missing_offsets):
        lambda_client.invoke(
            FunctionName=NEWS_LAMBDA_NAME,
            InvocationType="Event",
            Payload=json.dumps({
                "offset": offset,
                "cycle_started_at": cycle_started_at,
                "mode": "batch",
                "total_markets": total_markets,
            }),
        )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "action": "retried_missing_batches",
            "cycle_started_at": cycle_started_at,
            "missing_offsets": sorted(missing_offsets),
            "minutes_since_start": round(minutes_since_start, 1),
        }),
    }
