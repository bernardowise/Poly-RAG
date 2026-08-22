"""
Poly-RAG chunk_news_article Lambda: chunks the current cycle's news articles
into whole-article chunks (with overflow splitting for oversized articles),
ready for embedding. Fase 2, level 2 (chunking) -- one of 4 chunking Lambdas
fanned out in parallel by poly-rag-embed-orchestrator.

WHY THIS EXISTS
---------------
Ports force_split/chunk_news_article from scripts/bootstrap_chunk_corpus.py
(the one-off that chunked the full historical corpus, 2026-08-21) to the
per-cycle path. Same overflow rule, same cap -- the one-off covers the
historical backfill, this Lambda covers every cycle going forward.

The news_paragraph variant is deliberately NOT built here (see tech_debt.md,
"Embedding Model Choice" update 2026-08-21: Voyage out, project is
Cohere-only for now, paragraph variant paused until the article+LanceDB path
is proven end-to-end first, per explicit user decision).

News already has a natural per-cycle scope (news/YYYY-MM-DD/HH.json is
already "only this cycle's new articles", per poly-rag-processed-urls dedup
in ingest_news) -- no tracking table needed here, unlike registry.

OVERFLOW SPLIT (unchanged from the one-off, see architecture_canon.md and
tech_debt.md "Duplicate Article URLs Within a Single Cycle" for related
findings): articles over MAX_ARTICLE_CHARS split into linked parts
(article_id#part_index) rather than being truncated -- exists for PACING
against Bedrock's real per-minute token limit, not storage. 97.76% of
articles stay a single unsplit chunk (median 3,648 chars in the measured
12-cycle corpus).

KNOWN OPEN ISSUE, not fixed here: duplicate article URLs within a single
cycle (two News fan-out batches can fetch the same URL under two different
market_ids before either marks it processed) collide on chunk_id (=url) --
see tech_debt.md for the real cases found and why the fix belongs upstream in
ingest_news, not here.

TRIGGER
-------
Invoked by poly-rag-embed-orchestrator, carrying cycle_started_at.

OUTPUT
------
Writes s3://<bucket>/chunks/news_article/<cycle date/hour>.json.
"""

import json
import os
from datetime import datetime, timezone

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "poly-rag-369970405415")
MAX_ARTICLE_CHARS = 32000  # ~8K tokens, see architecture_canon.md for the real measurement behind this cap

s3 = boto3.client("s3")


def news_cycle_prefix(cycle_started_at):
    dt = datetime.fromisoformat(cycle_started_at)
    return f"news/{dt.strftime('%Y-%m-%d')}/{dt.strftime('%H')}.json"


def force_split(text, max_chars):
    """Hard cap on chunk size. Splits on word boundaries so it never cuts
    mid-word. Verbatim port from the one-off script."""
    if len(text) <= max_chars:
        return [text]
    words = text.split(" ")
    parts, buffer = [], ""
    for word in words:
        candidate = f"{buffer} {word}".strip() if buffer else word
        if len(candidate) > max_chars and buffer:
            parts.append(buffer)
            buffer = word
        else:
            buffer = candidate
    if buffer:
        parts.append(buffer)
    return parts


def chunk_news_article(article, article_id):
    body_text = (article.get("body_text") or "").strip()
    if not body_text:
        return []
    market_id = (article.get("market_ids") or [None])[0]
    base_meta = {
        "article_id": article_id,
        "market_id": market_id,
        "temporal_tier": article.get("temporal_tier"),
        "market_status_at_publish": article.get("market_status_at_publish"),
        "pubDate": article.get("pubDate"),
        "source": article.get("source"),
    }

    parts = force_split(body_text, MAX_ARTICLE_CHARS)
    if len(parts) == 1:
        return [{
            **base_meta,
            "chunk_id": article_id,
            "part_index": 0,
            "part_count": 1,
            "text": parts[0],
        }]
    return [{
        **base_meta,
        "chunk_id": f"{article_id}#{idx}",
        "part_index": idx,
        "part_count": len(parts),
        "text": part,
    } for idx, part in enumerate(parts)]


def lambda_handler(event, context):
    cycle_started_at = event.get("cycle_started_at") or datetime.now(timezone.utc).isoformat()
    cycle_key = news_cycle_prefix(cycle_started_at)
    dry_run = event.get("dry_run", False)

    try:
        payload = json.loads(
            s3.get_object(Bucket=S3_BUCKET, Key=cycle_key)["Body"].read()
        )
    except s3.exceptions.NoSuchKey:
        raise RuntimeError(f"news payload not found at s3://{S3_BUCKET}/{cycle_key}")

    articles = payload.get("articles", [])
    chunks = []
    split_count = 0
    for i, article in enumerate(articles):
        article_id = article.get("url") or f"{cycle_key}#{i}"
        article_chunks = chunk_news_article(article, article_id)
        if len(article_chunks) > 1:
            split_count += 1
        chunks.extend(article_chunks)

    for c in chunks:
        c["_lineage"] = {"written_by": "poly-rag-chunk-news-article", "run_type": "automated_cycle"}

    output_key = cycle_key.replace("news/", "chunks/news_article/")

    if dry_run:
        return {
            "source": "news_article",
            "cycle_started_at": cycle_started_at,
            "articles_read": len(articles),
            "chunks_would_write": len(chunks),
            "articles_split": split_count,
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
        "source": "news_article",
        "cycle_started_at": cycle_started_at,
        "articles_read": len(articles),
        "chunks_written": len(chunks),
        "articles_split": split_count,
        "output_key": output_key,
    }
