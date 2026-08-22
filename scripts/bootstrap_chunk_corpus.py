"""
One-off: chunk the full existing corpus (registry, News x2 variants, Comments,
Digest) into text-only chunks, ready for embedding -- but does NOT call any
embedding model. This is step 1 of the Phase 2 (embedding) bootstrap, kept
deliberately separate from step 2 (embedding) so chunk quality can be verified
against real data before spending on Titan/Cohere/Voyage calls.

WHY THIS EXISTS
---------------
Day 4 block F (see architecture_canon.md, "Chunking de News"/"Chunking de
Comments"/"Chunking/embedding de Digest"/"Capa semantica de Polymarket") closed
the chunking design for all 4 sources, but nothing has been executed against
the real corpus yet. Same bootstrap-vs-incremental split already used by the
registry/odds backfills: this script covers the ONE-TIME pass over everything
that already exists; the per-cycle incremental path lives in the Phase 2
Lambdas (`chunk_registry`, `chunk_news_paragraph`, `chunk_news_article`,
`chunk_comments`, `chunk_digest`), not here.

WHAT EACH SOURCE PRODUCES (see architecture_canon.md for the full design)
---------------------------------------------------------------------------
- registry: 1 chunk per market_id, text = question + "\n\n" + description
  (combined, not separate -- decided 2026-08-20), no chunking within a market.
- news_paragraph: N chunks per article, one per paragraph of body_text, plus
  paragraph_index/paragraph_count metadata for neighbor lookup at query time.
- news_article: 1 chunk per article, text = full body_text, no split.
- comments: chunk unit depends on link_type -- `direct` chunks by market_id
  (1:1), `shared_event`/`shared_series` chunk by comment_entity_id (one stream
  shared by every market under that entity, never re-embedded per market).
- digest: 1 chunk per cycle, text = a deterministic narrative built from the
  JSON fields (no new LLM call -- see digest_to_text below), covering
  newly_tracked_markets/resolved_markets/top_volatility/world_snapshot/quotes/
  executive_summary.

CYCLE SLICING (added 2026-08-21)
--------------------------------
`--cycles A-B` restricts chunking to an inclusive, 1-indexed range of cycles,
where cycle 1 is the earliest complete cycle in the bucket. This exists for the
4-day sliced embedding bootstrap: the full corpus does fit inside one day's
Bedrock token quota, but the project deliberately drains it in daily slices so
that each day is independently verifiable and a failure costs one slice, not
the whole run. Each day re-chunks its own slice immediately before embedding it
(the corpus keeps growing at 2 cycles/day underneath, so a chunk file written
yesterday is already stale), and writes to its own labelled file rather than
overwriting a shared one.

The registry source has no cycle axis -- it is a live DynamoDB scan of current
market metadata, not a per-cycle S3 payload -- so `--cycles` does not apply to
it and it always chunks the full current registry.

OUTPUT
------
Writes one JSON file per source to s3://<bucket>/chunks/<source>/<label>.json
(only with --apply), where <label> defaults to `bootstrap` for a full run or
`cycles_<first>-<last>` for a sliced one. Each file is a flat list of chunk
dicts, each carrying at minimum `chunk_id` and `text`, plus the metadata
documented per-source above, plus `cycle_number`/`cycle_key` for the three
cycle-based sources.

SAFETY
------
Read-only against News/Comments/Digest cycle files and the registry (no writes
to any existing prefix). Only ever writes to the NEW `chunks/` prefix, and only
under --apply. Dry run is the default, same as every other one-off script in
this directory.

USAGE
-----
    python scripts/bootstrap_chunk_corpus.py                        # dry run, all sources, all cycles
    python scripts/bootstrap_chunk_corpus.py --source news           # dry run, one source
    python scripts/bootstrap_chunk_corpus.py --cycles 1-5            # dry run, cycles 1-5 only
    python scripts/bootstrap_chunk_corpus.py --cycles 1-5 --apply    # write cycles 1-5 to S3
"""

import argparse
import json
import time
from collections import Counter, defaultdict

import boto3

S3_BUCKET = "poly-rag-369970405415"
REGISTRY_TABLE = "poly-rag-market-registry"

CHUNKS_PREFIX = "chunks/"
MAX_COMMENT_CHUNK_CHARS = 6000  # ~1500 tokens, overflow spills to a new chunk

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")


# ---------------------------------------------------------------------------
# S3 / DynamoDB read helpers
# ---------------------------------------------------------------------------

def list_cycle_keys(prefix, cycle_range=None):
    """All final cycle payload keys under a prefix, excluding fan-out batch
    intermediates (same filter as tag_news_temporal_tier.py).

    `cycle_range` is an inclusive (first, last) pair of 1-indexed cycle
    NUMBERS, not dates -- cycle 1 is the earliest cycle in the bucket, in
    chronological order. Added 2026-08-21 for the 4-day sliced bootstrap: each
    day re-chunks and embeds only its own slice of cycles, so a day's chunk
    file is a self-contained artifact of exactly what that day embedded.

    Chronological ordering is safe from a plain `sorted()` here because the key
    layout is zero-padded and fixed-width (`<source>/YYYY-MM-DD/HH.json`), so
    lexicographic order IS chronological order. Cycle numbers are assigned per
    prefix, but every prefix has the same 12 cycles by construction (a cycle is
    only complete when all 4 stages wrote their file -- see
    runbook_verify_phase1_health.md), so cycle N means the same instant in
    `news/` as it does in `digest/`.
    """
    keys = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json") and "_batch" not in obj["Key"]:
                keys.append(obj["Key"])
    keys = sorted(keys)

    if cycle_range is None:
        return keys

    first, last = cycle_range
    if first < 1:
        raise ValueError(f"cycle numbers are 1-indexed, got first={first}")
    selected = keys[first - 1:last]
    if not selected:
        raise ValueError(
            f"{prefix}: no cycles in range {first}-{last} "
            f"(only {len(keys)} cycles exist)"
        )
    if last > len(keys):
        print(f"  NOTE {prefix}: requested cycles {first}-{last} but only "
              f"{len(keys)} exist -- processing {first}-{len(keys)}")
    return selected


def cycle_number_map(prefix):
    """S3 key -> 1-indexed cycle number, for labelling chunks with the cycle
    they came from. Same ordering guarantee as list_cycle_keys above."""
    return {key: i for i, key in enumerate(list_cycle_keys(prefix), start=1)}


def cycle_started_at_from_key(key):
    """S3 key (e.g. 'comments/2026-08-16/01.json') -> the canon
    cycle_started_at ISO string ('2026-08-16T01:00:00+00:00').

    Added 2026-08-22 to unify chunk_id qualifiers between this one-off script
    and the per-cycle chunk_comments Lambda, which was found using a
    DIFFERENT qualifier (the real cycle_started_at threaded through Fase 1)
    while this script used a locally-numbered @c<N> -- same intent (keep
    entity-scoped comment chunk_ids globally unique across cycles), two
    incompatible formats, real tech debt caught before it accumulated. The
    payload's own ingested_at field was considered and rejected: it's the
    Lambda's completion timestamp (with variable run duration and
    microseconds baked in), not the canon cycle identity -- the S3 key itself
    (date/hour) IS the canon, same value chunk_registry/chunk_comments derive
    cycle_started_at from in the other direction."""
    parts = key.split("/")  # ['comments', 'YYYY-MM-DD', 'HH.json']
    date_part, hour_part = parts[1], parts[2].replace(".json", "")
    return f"{date_part}T{hour_part}:00:00+00:00"


def read_json(key):
    return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())


def scan_registry():
    table = dynamodb.Table(REGISTRY_TABLE)
    items = []
    response = table.scan()
    items.extend(response["Items"])
    while "LastEvaluatedKey" in response:
        response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        items.extend(response["Items"])
    return items


# ---------------------------------------------------------------------------
# Registry: question + description combined, 1 chunk per market_id
# ---------------------------------------------------------------------------

def chunk_registry(registry_items):
    chunks = []
    skipped_no_question = 0
    for item in registry_items:
        question = (item.get("question") or "").strip()
        description = (item.get("description") or "").strip()
        if not question:
            skipped_no_question += 1
            continue
        text = f"{question}\n\n{description}" if description else question
        chunks.append({
            "chunk_id": item["market_id"],
            "market_id": item["market_id"],
            "status": item.get("status"),
            "end_date": item.get("end_date"),
            "resolution_date": item.get("resolution_date"),
            "text": text,
        })
    return chunks, skipped_no_question


# ---------------------------------------------------------------------------
# News: paragraph+neighbors variant, and whole-article variant
# ---------------------------------------------------------------------------

MIN_PARAGRAPH_CHARS = 200   # short lines get merged into the next one instead of
                             # becoming their own low-signal chunk (titles, dates,
                             # byline fragments -- confirmed 50% of raw lines are
                             # under 60 chars in a real sample, 2026-08-20)
MAX_PARAGRAPH_CHARS = 2000  # ~500 tokens -- hard cap, force-split anything still
                             # over this after merging short lines. Real case that
                             # motivated this, 2026-08-20: one article's body_text
                             # had zero real newlines, so split_paragraphs' no-
                             # newline fallback returned the ENTIRE 36,971-char
                             # article as a single "paragraph" -- exactly the
                             # single-line-article scenario the user flagged as a
                             # real future risk, not hypothetical.


def split_paragraphs(body_text):
    """Paragraph split on single-newline boundaries, merging short lines
    forward into their nearest downstream neighbor.

    Real body_text from trafilatura extraction uses single \\n between
    paragraphs, not \\n\\n (confirmed against real News data 2026-08-20 --
    splitting on \\n\\n silently collapsed every article to 1 paragraph,
    making news_paragraph identical to news_article; caught by comparing
    chunk counts between the two variants in a dry run before writing
    anything). Splitting on plain \\n alone then produced a different real
    problem: 50% of resulting lines are under 60 chars (titles, standalone
    dates, subheadings extracted as their own line) -- too short to carry
    real semantic content as an independent chunk. Per the user's explicit
    call, 2026-08-20: merge a short line into its next neighbor (forward, not
    backward) until the combined text reaches MIN_PARAGRAPH_CHARS, rather than
    discarding it -- keeps every word instead of dropping context.
    A trailing short paragraph with no next neighbor is merged backward into
    the last real paragraph instead of being dropped, so nothing at the end
    of an article is silently lost.
    """
    raw_paragraphs = [p.strip() for p in body_text.split("\n")]
    lines = [p for p in raw_paragraphs if p]
    if not lines:
        return [body_text.strip()] if body_text.strip() else []

    merged = []
    buffer = ""
    for line in lines:
        buffer = f"{buffer} {line}".strip() if buffer else line
        if len(buffer) >= MIN_PARAGRAPH_CHARS:
            merged.append(buffer)
            buffer = ""
    if buffer:
        if merged:
            merged[-1] = f"{merged[-1]} {buffer}".strip()
        else:
            merged.append(buffer)

    final = []
    for paragraph in merged:
        final.extend(force_split(paragraph, MAX_PARAGRAPH_CHARS))
    return final


def force_split(text, max_chars):
    """Hard cap on chunk size, applied AFTER the min-length merge above --
    catches paragraphs (or a whole single-line article via the no-newline
    fallback) that stayed oversized despite normal splitting, e.g. a
    malformed extraction with no real line breaks at all. Splits on word
    boundaries so it never cuts mid-word."""
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


def chunk_news_paragraph(article, article_id):
    body_text = article.get("body_text", "")
    paragraphs = split_paragraphs(body_text)
    market_id = (article.get("market_ids") or [None])[0]
    base_meta = {
        "article_id": article_id,
        "market_id": market_id,
        "temporal_tier": article.get("temporal_tier"),
        "market_status_at_publish": article.get("market_status_at_publish"),
        "pubDate": article.get("pubDate"),
        "source": article.get("source"),
    }
    chunks = []
    for idx, paragraph in enumerate(paragraphs):
        chunks.append({
            **base_meta,
            "chunk_id": f"{article_id}#{idx}",
            "paragraph_index": idx,
            "paragraph_count": len(paragraphs),
            "text": paragraph,
        })
    return chunks


MAX_ARTICLE_CHARS = 32000   # ~8K tokens. Overflow cap for the whole-article
                             # variant, added 2026-08-21. NOT truncation -- an
                             # article over this splits into N parts that all
                             # stay in the corpus and reassemble via article_id
                             # (same pattern chunk_comments already uses for
                             # oversized comment groups). Measured against the
                             # real 12-cycle corpus before choosing the cap:
                             # median article 3,648 chars, p95 16,284, and only
                             # 184/8,201 articles (2.24%) exceed 32K -- so the
                             # "whole article, no split" property of this
                             # variant still holds for 97.76% of it. The cap
                             # exists for PACING, not storage: Cohere's real
                             # binding limit is 300K tokens/min, and a single
                             # 240,387-char article (~60K tokens, real case in
                             # cycle 6) inside one batched request blows a
                             # fifth of a minute's entire budget in one call --
                             # the actual cause of the sustained throttling
                             # that blocked the previous bootstrap attempts,
                             # which no amount of backoff could fix.


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
        # Unsplit articles keep a bare article_id as chunk_id -- no "#0" suffix,
        # so the overwhelming majority of this variant is addressed exactly as
        # it was before overflow splitting existed.
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


# ---------------------------------------------------------------------------
# Comments: unit depends on link_type -- direct=market_id,
# shared_event/shared_series=comment_entity_id (one stream, not re-embedded
# per market)
# ---------------------------------------------------------------------------

def comment_group_key(comment, entity_map):
    """(link_type, entity_key) -- entity_key is the shared comment_entity_id
    for shared_event/shared_series, or the single market_id for direct (1:1,
    no entity concept needed beyond the market itself).

    Fixed 2026-08-20: comment_entity_id does NOT live on the comment payload
    itself (confirmed against real comments/*.json -- fields are comment_id/
    author/created_at/reaction_count/market_ids/link_type only). It lives on
    the REGISTRY, one per market_id (see architecture_canon.md, "Comments"),
    so shared groups need a real lookup: market_id -> comment_entity_id via
    `entity_map`, built once from a registry scan. The original version
    assumed comment.get("comment_entity_id") would just be there and silently
    misgrouped 9,685/14,404 comments (67%) into a single "unknown_entity"
    bucket -- caught by inspecting a real chunk_comments sample before
    running --apply, not by the code raising an error."""
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


def get_comment_entity_map(registry_items):
    """market_id -> comment_entity_id, from a registry scan. Same pattern as
    get_registry_dates() in tag_news_temporal_tier.py -- one full scan,
    reused across all comment cycle files rather than a lookup per comment."""
    return {
        item["market_id"]: item["comment_entity_id"]
        for item in registry_items
        if item.get("comment_entity_id")
    }


def chunk_comments(comments, entity_map):
    """Groups comments by (link_type, entity_key), concatenates text within
    each group, and splits into multiple chunks only if the group exceeds
    MAX_COMMENT_CHUNK_CHARS -- same overflow pattern as a long News article,
    not a redesign."""
    groups = defaultdict(list)
    for comment in comments:
        key = comment_group_key(comment, entity_map)
        text = (comment.get("text") or "").strip()
        if text:
            groups[key].append(text)

    chunks = []
    for (link_type, entity_key), texts in groups.items():
        buffer, buffer_len, part = [], 0, 0
        for text in texts:
            if buffer and buffer_len + len(text) > MAX_COMMENT_CHUNK_CHARS:
                chunks.append({
                    "chunk_id": f"{entity_key}#{part}",
                    "link_type": link_type,
                    "comment_entity_id": entity_key,
                    "text": "\n\n".join(buffer),
                })
                buffer, buffer_len = [], 0
                part += 1
            buffer.append(text)
            buffer_len += len(text)
        if buffer:
            chunks.append({
                "chunk_id": f"{entity_key}#{part}",
                "link_type": link_type,
                "comment_entity_id": entity_key,
                "text": "\n\n".join(buffer),
            })
    return chunks


# ---------------------------------------------------------------------------
# Digest: deterministic template, no LLM call, 1 chunk per cycle
# ---------------------------------------------------------------------------

def digest_to_text(digest_data, cycle_label):
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


def chunk_digest(digest_data, key):
    cycle_label = key.replace("digest/", "").replace(".json", "").replace("/", " ")
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
    return [{
        "chunk_id": key,
        "cycle_started_at": digest_data.get("ingested_at"),
        "digest_s3_key": key,
        "market_ids_mentioned": market_ids_mentioned,
        "text": text,
    }]


# ---------------------------------------------------------------------------
# Per-source drivers
# ---------------------------------------------------------------------------

def run_registry():
    items = scan_registry()
    chunks, skipped = chunk_registry(items)
    print(f"registry: {len(items)} markets scanned, {len(chunks)} chunks, "
          f"{skipped} skipped (no question)")
    return chunks


def run_news(cycle_range=None):
    cycle_of = cycle_number_map("news/")
    keys = list_cycle_keys("news/", cycle_range)
    paragraph_chunks, article_chunks = [], []
    errors = 0
    split_articles = 0
    for key in keys:
        try:
            payload = read_json(key)
        except Exception as exc:
            print(f"  {key}: READ ERROR {type(exc).__name__}")
            errors += 1
            continue
        cycle_meta = {"cycle_number": cycle_of[key], "cycle_key": key}
        for i, article in enumerate(payload.get("articles", [])):
            article_id = article.get("url") or f"{key}#{i}"
            paragraph_chunks.extend(
                {**c, **cycle_meta} for c in chunk_news_paragraph(article, article_id)
            )
            parts = chunk_news_article(article, article_id)
            if len(parts) > 1:
                split_articles += 1
            article_chunks.extend({**c, **cycle_meta} for c in parts)
    cycles_seen = sorted({cycle_of[k] for k in keys})
    print(f"news: {len(keys)} cycle files (cycles {cycles_seen[0]}-{cycles_seen[-1]}), "
          f"{errors} read errors, {len(paragraph_chunks)} paragraph chunks, "
          f"{len(article_chunks)} article chunks "
          f"({split_articles} articles over {MAX_ARTICLE_CHARS:,} chars split into parts)")
    return paragraph_chunks, article_chunks


def run_comments(cycle_range=None):
    entity_map = get_comment_entity_map(scan_registry())
    cycle_of = cycle_number_map("comments/")
    keys = list_cycle_keys("comments/", cycle_range)
    all_chunks = []
    link_type_counts = Counter()
    unknown_entity_comments = 0
    errors = 0
    for key in keys:
        try:
            payload = read_json(key)
        except Exception as exc:
            print(f"  {key}: READ ERROR {type(exc).__name__}")
            errors += 1
            continue
        comments = payload.get("comments", [])
        for c in comments:
            _, entity_key = comment_group_key(c, entity_map)
            if entity_key == "unknown_entity":
                unknown_entity_comments += 1
        cycle_number = cycle_of[key]
        cycle_started_at = cycle_started_at_from_key(key)
        cycle_chunks = chunk_comments(comments, entity_map)
        for c in cycle_chunks:
            link_type_counts[c["link_type"]] += 1
            # chunk_id is entity-scoped ({entity}#{part}), so the SAME entity in
            # two different cycles would collide on id. Qualify with
            # cycle_started_at (not a locally-numbered @c<N> -- see
            # cycle_started_at_from_key docstring, 2026-08-22, for why that was
            # corrected: it must match the qualifier chunk_comments Lambda uses,
            # so ids stay comparable and globally unique across BOTH the one-off
            # bootstrap and the per-cycle Lambda path) so ids stay globally
            # unique -- this matters now that cycles are embedded in separate
            # slices on separate days and later merged into one vector store,
            # where a duplicate id would silently overwrite an earlier cycle's
            # chunk instead of coexisting with it.
            c["chunk_id"] = f"{c['chunk_id']}@{cycle_started_at}"
            c["cycle_number"] = cycle_number
            c["cycle_key"] = key
        all_chunks.extend(cycle_chunks)
    cycles_seen = sorted({cycle_of[k] for k in keys})
    print(f"comments: {len(keys)} cycle files (cycles {cycles_seen[0]}-{cycles_seen[-1]}), "
          f"{errors} read errors, {len(all_chunks)} chunks -- {dict(link_type_counts)}"
          f"{f', {unknown_entity_comments} comments with no resolvable entity' if unknown_entity_comments else ''}")
    return all_chunks


def run_digest(cycle_range=None):
    cycle_of = cycle_number_map("digest/")
    keys = list_cycle_keys("digest/", cycle_range)
    chunks = []
    errors = 0
    for key in keys:
        try:
            payload = read_json(key)
        except Exception as exc:
            print(f"  {key}: READ ERROR {type(exc).__name__}")
            errors += 1
            continue
        chunks.extend(
            {**c, "cycle_number": cycle_of[key], "cycle_key": key}
            for c in chunk_digest(payload, key)
        )
    cycles_seen = sorted({cycle_of[k] for k in keys})
    print(f"digest: {len(keys)} cycle files (cycles {cycles_seen[0]}-{cycles_seen[-1]}), "
          f"{errors} read errors, {len(chunks)} chunks")
    return chunks


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

SOURCE_RUNNERS = {
    # registry takes no cycle_range on purpose -- it is a live DynamoDB scan of
    # current market metadata, not a per-cycle S3 payload, so it has no cycle
    # axis to slice on. A registry chunk is always "as of now"; see the note
    # printed in main() when --cycles is used with it.
    "registry": lambda cr: {"registry": run_registry()},
    "news": lambda cr: dict(zip(("news_paragraph", "news_article"), run_news(cr))),
    "comments": lambda cr: {"comments": run_comments(cr)},
    "digest": lambda cr: {"digest": run_digest(cr)},
}


def write_chunks(source_name, chunks, label):
    for c in chunks:
        c["_lineage"] = {
            "written_by": "scripts/bootstrap_chunk_corpus.py",
            "run_type": "manual_backfill",
        }
    key = f"{CHUNKS_PREFIX}{source_name}/{label}.json"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(chunks),
        ContentType="application/json",
    )
    return key


def parse_cycles(raw):
    """'1-5' -> (1, 5); '7' -> (7, 7). 1-indexed and inclusive on both ends."""
    if raw is None:
        return None
    text = raw.strip()
    if "-" in text:
        first_s, last_s = text.split("-", 1)
        first, last = int(first_s), int(last_s)
    else:
        first = last = int(text)
    if first > last:
        raise ValueError(f"--cycles {raw}: first cycle is after last")
    return first, last


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="actually write chunk files to S3 (default is a dry run that writes nothing)",
    )
    parser.add_argument(
        "--source",
        choices=sorted(SOURCE_RUNNERS.keys()),
        help="only process one source (default: all 4)",
    )
    parser.add_argument(
        "--cycles",
        help="inclusive 1-indexed cycle range to chunk, e.g. '1-5' (default: all "
             "cycles). Cycle 1 is the earliest complete cycle in the bucket. Does "
             "not apply to the registry source, which is always a live scan.",
    )
    parser.add_argument(
        "--label",
        help="output filename stem, written to chunks/<source>/<label>.json "
             "(default: 'bootstrap', or 'cycles_<first>-<last>' when --cycles is given)",
    )
    args = parser.parse_args()

    cycle_range = parse_cycles(args.cycles)
    if args.label:
        label = args.label
    elif cycle_range:
        label = f"cycles_{cycle_range[0]:02d}-{cycle_range[1]:02d}"
    else:
        label = "bootstrap"

    mode = "APPLY (writing to S3)" if args.apply else "DRY RUN (writing nothing)"
    sources = [args.source] if args.source else list(SOURCE_RUNNERS.keys())
    print(f"=== Bootstrap chunk corpus -- {mode} ===")
    print(f"Sources: {', '.join(sources)}")
    print(f"Cycles:  {args.cycles if cycle_range else 'ALL'}")
    print(f"Output:  chunks/<source>/{label}.json")
    if cycle_range and "registry" in sources:
        print("NOTE: --cycles does not apply to registry (live DynamoDB scan, "
              "no cycle axis) -- it will chunk the full current registry.")
    print()

    started = time.time()
    all_results = {}
    for source in sources:
        all_results.update(SOURCE_RUNNERS[source](cycle_range))

    print()
    print("=== Sample chunks (first of each) ===")
    for name, chunks in all_results.items():
        if chunks:
            sample = chunks[0]
            preview = sample["text"][:200].replace("\n", " ")
            print(f"  {name} [{sample['chunk_id']}]: {preview}...")

    if args.apply:
        print()
        print("=== Writing to S3 ===")
        for name, chunks in all_results.items():
            key = write_chunks(name, chunks, label)
            print(f"  {name}: {len(chunks)} chunks -> s3://{S3_BUCKET}/{key}")

    elapsed = time.time() - started
    print()
    print("=== Summary ===")
    for name, chunks in all_results.items():
        total_chars = sum(len(c.get("text", "")) for c in chunks)
        print(f"  {name}: {len(chunks)} chunks, {total_chars:,} chars "
              f"(~{total_chars // 4:,} tokens)")
    print(f"  elapsed: {elapsed:.1f}s")
    if not args.apply:
        print()
        print("DRY RUN -- nothing was written. Re-run with --apply to write.")


if __name__ == "__main__":
    main()
