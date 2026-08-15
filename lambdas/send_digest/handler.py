"""
Poly-RAG digest Lambda: consolidates the 3 ingestion Lambdas' LLM summaries
(Polymarket, News, Bluesky) from the most recent S3 objects and emails them
as a single digest via Amazon SES. Triggered by EventBridge right after each
12h ingestion cycle (00:00 and 12:00 UTC).

Permanent piece of infrastructure -- independent of whether the LLM-in-ingestion
trial (see CLAUDE.md Development Conventions) is ultimately kept or reverted.
If the trial is reverted, llm_summary will be null and this digest degrades
gracefully (reports "no summary available" per source) rather than failing.
"""

import json
import os
from datetime import datetime, timezone

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "poly-rag-369970405415")
SES_SENDER = os.environ.get("SES_SENDER", "bernardolw@gmail.com")
SES_RECIPIENT = os.environ.get("SES_RECIPIENT", "bernardolw@gmail.com")

SOURCES = ["polymarket", "news", "bluesky"]

s3 = boto3.client("s3")
ses = boto3.client("ses")


def get_latest_object_key(source):
    """Find the most recent object under s3://bucket/<source>/ by listing
    the current hour's UTC prefix, falling back to the prior hour if the
    ingestion Lambda for this cycle hasn't run/finished yet."""
    now = datetime.now(timezone.utc)
    for hours_back in (0, 1):
        ts = now.timestamp() - hours_back * 3600
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        prefix = f"{source}/{dt.strftime('%Y-%m-%d')}/"
        resp = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
        contents = resp.get("Contents", [])
        if contents:
            latest = max(contents, key=lambda o: o["LastModified"])
            return latest["Key"]
    return None


def fetch_summary(source):
    key = get_latest_object_key(source)
    if not key:
        return {"source": source, "summary": None, "item_count": 0, "error": "No recent S3 object found"}

    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    data = json.loads(obj["Body"].read())

    count_field = {
        "polymarket": "market_count",
        "news": "article_count",
        "bluesky": "post_count",
    }[source]

    return {
        "source": source,
        "summary": data.get("llm_summary"),
        "item_count": data.get(count_field, 0),
        "s3_key": key,
    }


def build_email_body(digests):
    now = datetime.now(timezone.utc)
    lines = [f"Poly-RAG Digest -- {now.strftime('%Y-%m-%d %H:%M UTC')}", ""]

    for d in digests:
        lines.append(f"=== {d['source'].upper()} ({d.get('item_count', 0)} items) ===")
        if d.get("summary"):
            lines.append(d["summary"])
        elif d.get("error"):
            lines.append(f"[No data: {d['error']}]")
        else:
            lines.append("[No LLM summary available for this cycle -- enrichment may be disabled]")
        lines.append("")

    return "\n".join(lines)


def lambda_handler(event, context):
    digests = [fetch_summary(source) for source in SOURCES]
    body = build_email_body(digests)

    now = datetime.now(timezone.utc)
    subject = f"Poly-RAG Digest -- {now.strftime('%Y-%m-%d %H:%M UTC')}"

    ses.send_email(
        Source=SES_SENDER,
        Destination={"ToAddresses": [SES_RECIPIENT]},
        Message={
            "Subject": {"Data": subject},
            "Body": {"Text": {"Data": body}},
        },
    )

    return {
        "statusCode": 200,
        "body": json.dumps({"sent": True, "sources_included": [d["source"] for d in digests]}),
    }
