"""
Poly-RAG chunk_digest Lambda: chunks the current cycle's digest into a single
text chunk, ready for embedding. Fase 2, level 2 (chunking) -- one of 4
chunking Lambdas fanned out in parallel by poly-rag-embed-orchestrator.

WHY THIS EXISTS
---------------
Ports the chunk_digest/digest_to_text logic from scripts/bootstrap_chunk_corpus.py
(the one-off that chunked the full historical corpus, see tech_debt.md/
architecture_canon.md, 2026-08-21) to the per-cycle path -- same template, same
deterministic text construction, no new Bedrock call. The one-off covers the
historical backfill; this Lambda covers every cycle going forward.

Digest is the simplest of the 4 sources: exactly 1 chunk per cycle (a digest is
already short, a single narrative summary of the cycle), no fan-out, no
tracking table needed -- unlike registry (needs a dedup table, see
chunk_registry) or News/Comments (already naturally scoped to one cycle's
payload by construction).

TRIGGER
-------
Invoked by poly-rag-embed-orchestrator, in parallel with chunk_registry/
chunk_comments/chunk_news_article, carrying cycle_started_at from the chain
(same threading pattern as every Fase 1 Lambda).

OUTPUT
------
Writes s3://<bucket>/chunks/digest/<cycle_started_at date/hour>.json -- a
single-element list, same list-of-chunk-dicts shape the one-off script writes,
so embed_digest (level 3) reads chunk files identically regardless of which
path produced them.
"""

import json
import os
from datetime import datetime, timezone

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "poly-rag-369970405415")

s3 = boto3.client("s3")


def digest_cycle_prefix(cycle_started_at):
    dt = datetime.fromisoformat(cycle_started_at)
    return f"digest/{dt.strftime('%Y-%m-%d')}/{dt.strftime('%H')}.json"


def digest_to_text(digest_data, cycle_label):
    """Verbatim port of scripts/bootstrap_chunk_corpus.py:digest_to_text --
    deterministic template, no LLM call. Kept identical so a chunk produced by
    this Lambda is indistinguishable from one produced by the one-off
    bootstrap for a historical cycle."""
    lines = [f"Poly-RAG Digest -- {cycle_label}"]

    newly_tracked = digest_data.get("newly_tracked_markets") or []
    if newly_tracked:
        lines.append(f"\nNew Markets ({len(newly_tracked)}):")
        for m in newly_tracked[:10]:
            lines.append(f"- {m.get('question', '')}")

    resolved = digest_data.get("resolved_markets") or []
    if resolved:
        lines.append(f"\nResolved ({len(resolved)}):")
        for m in resolved[:10]:
            lines.append(f"- {m.get('question', '')} -> {m.get('outcome_prices')}")

    top_volatility = digest_data.get("top_volatility") or []
    if top_volatility:
        lines.append("\nBiggest Moves:")
        for m in top_volatility:
            lines.append(
                f"- {m.get('question', '')}: {m.get('prev_price')} -> {m.get('curr_price')}"
            )

    snapshot = digest_data.get("world_snapshot") or {}
    if snapshot.get("top_conviction"):
        lines.append("\nHighest-Conviction Bets:")
        for m in snapshot["top_conviction"]:
            lines.append(f"- {m.get('question', '')}: {m.get('price')}")
    if snapshot.get("most_disputed"):
        lines.append("\nMost Disputed Bets:")
        for m in snapshot["most_disputed"]:
            lines.append(f"- {m.get('question', '')}: {m.get('price')}")

    quotes = digest_data.get("quotes") or {}
    for source, label in (("news", "News Highlights"), ("comments", "Trader Comments")):
        source_quotes = quotes.get(source) or []
        if source_quotes:
            lines.append(f"\n{label}:")
            for q in source_quotes:
                lines.append(f'- "{q.get("text", "")}" -- {q.get("source", "")}')

    summary = digest_data.get("executive_summary")
    if summary:
        lines.append(f"\nSynthesized narrative:\n{summary}")

    return "\n".join(lines)


def chunk_digest(digest_data, cycle_key, cycle_started_at):
    cycle_label = cycle_key.replace("digest/", "").replace(".json", "").replace("/", " ")
    text = digest_to_text(digest_data, cycle_label)
    market_ids_mentioned = sorted({
        m["market_id"]
        for section in (
            digest_data.get("newly_tracked_markets") or [],
            digest_data.get("resolved_markets") or [],
            digest_data.get("top_volatility") or [],
        )
        for m in section
        if m.get("market_id")
    } | {
        m["market_id"]
        for group in ("top_conviction", "most_disputed")
        for m in (digest_data.get("world_snapshot") or {}).get(group, [])
        if m.get("market_id")
    })
    return {
        "chunk_id": cycle_key,
        "cycle_started_at": cycle_started_at,
        "digest_s3_key": cycle_key,
        "market_ids_mentioned": market_ids_mentioned,
        "text": text,
    }


def lambda_handler(event, context):
    cycle_started_at = event.get("cycle_started_at") or datetime.now(timezone.utc).isoformat()
    cycle_key = digest_cycle_prefix(cycle_started_at)

    try:
        digest_data = json.loads(
            s3.get_object(Bucket=S3_BUCKET, Key=cycle_key)["Body"].read()
        )
    except s3.exceptions.NoSuchKey:
        raise RuntimeError(f"digest payload not found at s3://{S3_BUCKET}/{cycle_key}")

    chunk = chunk_digest(digest_data, cycle_key, cycle_started_at)

    output_key = cycle_key.replace("digest/", "chunks/digest/")
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=output_key,
        Body=json.dumps([chunk]),
        ContentType="application/json",
    )

    return {
        "source": "digest",
        "cycle_started_at": cycle_started_at,
        "chunks_written": 1,
        "output_key": output_key,
    }
