"""
One-off: embed the full chunked corpus (written by bootstrap_chunk_corpus.py)
against 2 embedding models (Cohere v4, Voyage voyage-finance-2), across the 2
News-chunking corpus variants. Step 2 of the Phase 2 (embedding) bootstrap --
step 1 (chunking, text-only, no model calls) is bootstrap_chunk_corpus.py,
kept deliberately separate so chunk quality could be verified before spending
on real embedding calls.

WHY THIS EXISTS
---------------
Day 4 block F/Day 6 (see architecture_canon.md, "Modelo de embeddings" and
tech_debt.md, "Embedding Model Choice") decided all comparison models run
against the real corpus from day one. This script produces the 4 corpus
variants (2 chunking x 2 embedding) documented in architecture_canon.md as
`poly-rag-embed-orchestrator`'s eventual per-cycle job, run once here as a
one-off over the existing bootstrap chunks.

TITAN V2 DROPPED, 2026-08-21 (see tech_debt.md, "Embedding Model Choice")
--------------------------------------------------------------------------
Amazon Titan Embeddings V2 was the original default production model, but was
dropped from the project entirely after a real bootstrap run hit AWS's hard
account quota for it: "On-demand model inference requests per minute for
Amazon Titan Text Embeddings V2" = 600 (confirmed via
`aws service-quotas list-service-quotas --service-code bedrock`), and Titan's
Bedrock API has no batch field at all (`{"inputText": "<one string>"}`, one
request per text, no way around it -- confirmed via
`aws bedrock get-foundation-model`, inferenceTypesSupported=[ON_DEMAND], no
BATCH, so Bedrock's async Batch Inference service doesn't apply either). At
~120K chunks per corpus variant that quota projects to ~3.5 hours just for
Titan on news_paragraph_variant alone -- and since this same 600/min ceiling
is permanent (an AWS account quota, not a one-off bootstrap fluke), it would
also be the bottleneck of EVERY future incremental Phase 2 cycle in
production, not just this bootstrap. Cohere v4 (96 texts/call) and Voyage
(128 texts/call) both have real multi-text batch APIs and finish in minutes,
not hours, for the same corpus size -- confirmed by real partial progress
before Titan was dropped (Voyage: 38/62 checkpoints, ~76,000 vectors, in under
20 minutes). Cost was never the issue (Titan is the cheapest of the three,
see tech_debt.md pricing table) -- the issue is that "free in dollars" turned
out to cost hours of wall-clock time every cycle, which is the resource this
project actually budgets against day to day.

INPUT
-----
Reads the 5 files bootstrap_chunk_corpus.py already wrote:
  chunks/registry/bootstrap.json
  chunks/news_paragraph/bootstrap.json
  chunks/news_article/bootstrap.json
  chunks/comments/bootstrap.json
  chunks/digest/bootstrap.json

Combined into 2 corpus variants (registry/comments/digest are shared, only the
News chunking axis differs):
  news_paragraph_variant = registry + news_paragraph + comments + digest
  news_article_variant   = registry + news_article   + comments + digest

OUTPUT
------
One file per (variant, model) combination, 4 total, only under --apply:
  vectors/news_paragraph_variant/cohere.json
  vectors/news_paragraph_variant/voyage.json
  vectors/news_article_variant/cohere.json
  vectors/news_article_variant/voyage.json

Each file is a flat list of {chunk_id, source, embedding, model_id} dicts --
`source` (registry/news_paragraph/news_article/comments/digest) is preserved
per-vector so cost/latency/token tracking can be sliced by source x model, per
the requirement already logged in session_ledger.md 2026-08-20.

CHECKPOINTING (added 2026-08-20 after two real --apply runs died with ZERO
vectors written)
------------------------------------------------------------------------------
Each (variant, model) combination embeds in CHECKPOINT_SIZE=2000-chunk groups,
writing each group to `vectors/_checkpoints/<variant>/<model>/part_NNNNN.json`
in S3 as soon as it's ready -- not accumulating the whole combination in
memory before writing anything, which is exactly what turned two real
interruptions (one silent stdout-buffering death, one external SIGTERM at
~10-13 min, both 2026-08-20) into a total loss of the run's real Bedrock/
Voyage spend with nothing to show for it. A re-run of the same --apply command
SKIPS any checkpoint part already in S3 rather than re-embedding it (same
_batch<offset>.json + merge pattern ingest_news already uses for its own
fan-out). Once all parts for a combination exist, they're read back,
concatenated in order, written as the final `vectors/<variant>/<model>.json`,
and the checkpoint parts are deleted.

MODELS
------
- Cohere v4 (cohere.embed-v4:0) via Bedrock InvokeModel, IAM auth, confirmed
  AUTHORIZED with agreementAvailability NOT_AVAILABLE (= no agreement needed;
  verified read-only via `aws bedrock get-foundation-model-availability`,
  2026-08-20).
- Voyage (voyage-finance-2) via Voyage's own REST API (no boto3 -- this is not
  a Bedrock model), key read from VOYAGE_API_KEY in the environment (source
  .secrets before running, same pattern as every other script needing a
  non-AWS credential).

BATCHING
--------
Cohere (96 texts/call) and Voyage (128 texts/call, capped by that model's
per-request token limit rather than count) both accept real multi-text batches
per HTTP call -- ~1,257 and ~942 total calls respectively for the full corpus
(2026-08-21 counts), minutes not hours.

SAFETY
------
Dry run is the default and calls NO model and spends nothing -- it only loads
chunk counts and prints what WOULD be embedded (source breakdown, estimated
call count). Real model calls (real Bedrock/Voyage cost) only happen with
--apply. Never touches the chunks/ prefix -- read-only there. Only ever writes
to the NEW vectors/ prefix.

USAGE
-----
    python scripts/bootstrap_embed_corpus.py                       # dry run, all
    python scripts/bootstrap_embed_corpus.py --model cohere        # dry run, one model
    python scripts/bootstrap_embed_corpus.py --variant news_paragraph_variant --apply
"""

import argparse
import json
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
import botocore.exceptions
import urllib.request
import urllib.error

S3_BUCKET = "poly-rag-369970405415"
CHUNKS_PREFIX = "chunks/"
VECTORS_PREFIX = "vectors/"

COHERE_MODEL_ID = "cohere.embed-v4:0"
VOYAGE_MODEL_ID = "voyage-finance-2"
VOYAGE_API_URL = "https://api.voyageai.com/v1/embeddings"

BATCH_SIZES = {
    # Cohere Embed v4 on Bedrock accepts up to 96 texts per invoke_model call
    # ("texts": [...]).
    "cohere": 96,
    # Voyage accepts up to 1000 texts per request, but also caps total batch
    # tokens (~320K) -- our paragraph chunks run up to ~500 tokens each
    # (MAX_PARAGRAPH_CHARS=2000 in bootstrap_chunk_corpus.py), so 1000 texts
    # could exceed the token cap before hitting the count cap. 128 keeps
    # worst-case batch tokens (~500*128=64K) safely under the limit while
    # still being a real batch, not a token-by-token call.
    "voyage": 128,
}

s3 = boto3.client("s3")
bedrock = boto3.client("bedrock-runtime")


# ---------------------------------------------------------------------------
# Load chunks, build the 2 corpus variants
# ---------------------------------------------------------------------------

def read_chunks(source_name):
    key = f"{CHUNKS_PREFIX}{source_name}/bootstrap.json"
    return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())


def load_corpus_variants():
    """Returns {variant_name: [(source_name, chunk), ...]} -- source_name is
    kept per-chunk so per-source cost/latency can be sliced after embedding."""
    registry = read_chunks("registry")
    comments = read_chunks("comments")
    digest = read_chunks("digest")
    news_paragraph = read_chunks("news_paragraph")
    news_article = read_chunks("news_article")

    def tag(source_name, chunks):
        return [(source_name, c) for c in chunks]

    shared = tag("registry", registry) + tag("comments", comments) + tag("digest", digest)
    return {
        "news_paragraph_variant": shared + tag("news_paragraph", news_paragraph),
        "news_article_variant": shared + tag("news_article", news_article),
    }


# ---------------------------------------------------------------------------
# Per-model embedding calls -- each takes a list of raw texts, returns a list
# of vectors in the same order. No model call happens unless invoked from
# --apply code paths.
# ---------------------------------------------------------------------------

# A real --apply run hit a hard ThrottlingException ("Too many tokens, please
# wait before trying again") after ~30s, even with the real 96-text batch API
# -- confirms Cohere v4's account token/min quota is real (150,000 tokens/min,
# confirmed via `aws service-quotas`), not just theoretical. Reactive backoff
# (retry-after-error, exponential+jitter) was tried first and measured live:
# ~20 throttles/min sustained meant the backoff kept escalating into long
# waits (16s, 32s, 64s+), landing at ~1.5 successful req/min in practice --
# 4.5x worse than the ~6.8 req/min the token quota theoretically allows.
# Switched to a FIXED PACING model instead: sleep a calculated interval
# BEFORE each request so the account's token budget is never approached in
# the first place, rather than reacting to throttles after they happen.
# COHERE_PACE_SECONDS = 96 texts * ~231 tokens/chunk (real measured average,
# 2026-08-20 Voyage run) / (150,000 tokens/min / 60s), with a safety margin
# since token-per-chunk varies by source (registry/comments are much shorter
# than news_article).
COHERE_PACE_SECONDS = 10.0
COHERE_MAX_RETRIES = 8


def embed_batch_cohere(texts):
    time.sleep(COHERE_PACE_SECONDS)
    body = json.dumps({
        "texts": texts,
        "input_type": "search_document",
        "embedding_types": ["float"],
    })
    for attempt in range(COHERE_MAX_RETRIES):
        try:
            response = bedrock.invoke_model(modelId=COHERE_MODEL_ID, body=body)
            payload = json.loads(response["body"].read())
            return payload["embeddings"]["float"]
        except botocore.exceptions.ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code", "")
            if error_code not in ("ThrottlingException", "TooManyRequestsException"):
                raise
            if attempt == COHERE_MAX_RETRIES - 1:
                raise
            # still throttled despite pacing -- fall back to backoff, but
            # this should be rare, not the normal path
            sleep_s = (2 ** attempt) + random.uniform(0, 1)
            time.sleep(sleep_s)
    raise RuntimeError("unreachable")


def embed_batch_voyage(texts):
    api_key = os.environ.get("VOYAGE_API_KEY")
    if not api_key:
        raise RuntimeError("VOYAGE_API_KEY not set -- source .secrets before running")
    body = json.dumps({"input": texts, "model": VOYAGE_MODEL_ID}).encode("utf-8")
    request = urllib.request.Request(
        VOYAGE_API_URL,
        data=body,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request) as response:
            payload = json.loads(response.read())
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"Voyage API error {exc.code}: {exc.read().decode()}") from exc
    return [item["embedding"] for item in payload["data"]]


EMBED_FUNCS = {
    "cohere": embed_batch_cohere,
    "voyage": embed_batch_voyage,
}

MODEL_IDS = {
    "cohere": COHERE_MODEL_ID,
    "voyage": VOYAGE_MODEL_ID,
}


def batched(items, size):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def embed_variant(variant_name, model_name, tagged_chunks, dry_run):
    """Embeds tagged_chunks in CHECKPOINT_SIZE-sized groups, writing each
    group to S3 as a checkpoint part as soon as it's ready -- NOT accumulating
    the whole combination in memory before writing. Resumable: any part
    already present in S3 (from a previous run that died mid-way) is skipped
    rather than re-embedded, so an interrupted run only re-pays for the chunks
    it hadn't gotten to yet.

    Returns (final_key_or_None, vector_count, elapsed_seconds). In dry-run
    mode nothing is written or resumed -- it just reports counts."""
    embed_func = EMBED_FUNCS[model_name]
    model_id = MODEL_IDS[model_name]
    started = time.time()

    checkpoint_groups = list(batched(tagged_chunks, CHECKPOINT_SIZE))
    total_parts = len(checkpoint_groups)

    if dry_run:
        return None, len(tagged_chunks), time.time() - started

    already_done = list_checkpoint_parts(variant_name, model_name)
    if len(already_done) == total_parts:
        print(f"    {variant_name} x {model_name}: all {total_parts} checkpoints already exist, skipping re-embed")
    else:
        for part, group in enumerate(checkpoint_groups):
            if part in already_done:
                continue
            vectors = []
            for sub_batch in batched(group, BATCH_SIZES[model_name]):
                texts = [chunk["text"] for _, chunk in sub_batch]
                sub_vectors = embed_func(texts)
                if len(sub_vectors) != len(sub_batch):
                    raise RuntimeError(
                        f"{model_name}: batch returned {len(sub_vectors)} vectors for {len(sub_batch)} texts"
                    )
                vectors.extend(sub_vectors)
            records = [
                {"chunk_id": chunk["chunk_id"], "source": source, "model_id": model_id, "embedding": vector}
                for (source, chunk), vector in zip(group, vectors)
            ]
            write_checkpoint(variant_name, model_name, part, records)
            print(f"    {variant_name} x {model_name}: checkpoint {part + 1}/{total_parts} written ({len(records)} vectors)")

    final_key, count = merge_checkpoints_and_finalize(variant_name, model_name, total_parts)
    return final_key, count, time.time() - started


def write_vectors(variant_name, model_name, records):
    key = f"{VECTORS_PREFIX}{variant_name}/{model_name}.json"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(records),
        ContentType="application/json",
    )
    return key


# ---------------------------------------------------------------------------
# Incremental checkpointing -- two real background --apply runs died before
# finishing (one silently from stdout buffering without -u, one killed by an
# external SIGTERM at ~10-13 min, neither explained by this script's own
# code) with ZERO vectors written to S3 either time, because write_vectors()
# above only ran after an ENTIRE variant x model combination finished and sat
# fully in memory. A checkpoint every CHECKPOINT_SIZE vectors means a future
# interruption loses at most one checkpoint's worth of work, not the whole
# run -- same _batch<offset>.json pattern ingest_news already uses for its
# own fan-out, applied here to a single long-running process instead of
# parallel Lambda invocations.
# ---------------------------------------------------------------------------

CHECKPOINT_SIZE = 2000
CHECKPOINT_PREFIX = "vectors/_checkpoints/"


def write_checkpoint(variant_name, model_name, part, records):
    key = f"{CHECKPOINT_PREFIX}{variant_name}/{model_name}/part_{part:05d}.json"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=json.dumps(records),
        ContentType="application/json",
    )
    return key


def list_checkpoint_parts(variant_name, model_name):
    """Existing checkpoint part numbers for a combination, so a re-run can
    resume instead of re-embedding chunks already paid for."""
    prefix = f"{CHECKPOINT_PREFIX}{variant_name}/{model_name}/"
    parts = set()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            name = obj["Key"].rsplit("/", 1)[-1]
            if name.startswith("part_") and name.endswith(".json"):
                parts.add(int(name[len("part_"):-len(".json")]))
    return parts


def merge_checkpoints_and_finalize(variant_name, model_name, total_parts):
    """Reads all part_NNNNN.json checkpoints back, concatenates in order,
    writes the final vectors/<variant>/<model>.json, then deletes the
    checkpoint parts -- same read-merge-delete shape as News's
    merge_batch_payloads."""
    records = []
    for part in range(total_parts):
        key = f"{CHECKPOINT_PREFIX}{variant_name}/{model_name}/part_{part:05d}.json"
        records.extend(json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()))
    final_key = write_vectors(variant_name, model_name, records)
    for part in range(total_parts):
        key = f"{CHECKPOINT_PREFIX}{variant_name}/{model_name}/part_{part:05d}.json"
        s3.delete_object(Bucket=S3_BUCKET, Key=key)
    return final_key, len(records)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="actually call the embedding models and write vectors to S3 (default is a dry run, no cost)",
    )
    parser.add_argument(
        "--model",
        choices=sorted(EMBED_FUNCS.keys()),
        help="only run one model (default: both)",
    )
    parser.add_argument(
        "--variant",
        choices=["news_paragraph_variant", "news_article_variant"],
        help="only run one corpus variant (default: both)",
    )
    args = parser.parse_args()

    mode = "APPLY (calling models, real cost)" if args.apply else "DRY RUN (no model calls, no cost)"
    models = [args.model] if args.model else list(EMBED_FUNCS.keys())
    print(f"=== Bootstrap embed corpus -- {mode} ===")
    print(f"Models: {', '.join(models)}")
    print()

    variants = load_corpus_variants()
    if args.variant:
        variants = {args.variant: variants[args.variant]}

    for variant_name, tagged_chunks in variants.items():
        source_counts = {}
        for source, _ in tagged_chunks:
            source_counts[source] = source_counts.get(source, 0) + 1
        print(f"{variant_name}: {len(tagged_chunks)} chunks total -- {source_counts}")

    print()
    print("=== Embedding ===")
    # Cohere/Voyage are independent APIs with no data dependency between them
    # (unlike Phase 1's ingest_polymarket -> news -> comments -> digest chain,
    # which has a real cycle_started_at ordering requirement) -- so every
    # (variant, model) combination runs concurrently here, not sequentially.
    jobs = [
        (variant_name, model_name, tagged_chunks)
        for variant_name, tagged_chunks in variants.items()
        for model_name in models
    ]
    started = time.time()
    summary = []
    with ThreadPoolExecutor(max_workers=len(jobs)) as pool:
        future_to_job = {
            pool.submit(embed_variant, variant_name, model_name, tagged_chunks, not args.apply): (variant_name, model_name)
            for variant_name, model_name, tagged_chunks in jobs
        }
        for future in as_completed(future_to_job):
            variant_name, model_name = future_to_job[future]
            final_key, count, elapsed = future.result()
            print(f"  {variant_name} x {model_name}: {count} vectors, {elapsed:.1f}s")
            if final_key:
                print(f"    -> s3://{S3_BUCKET}/{final_key}")
            summary.append((variant_name, model_name, count, elapsed))

    elapsed_total = time.time() - started
    print()
    print("=== Summary ===")
    for variant_name, model_name, count, elapsed in summary:
        print(f"  {variant_name} x {model_name}: {count} vectors ({elapsed:.1f}s)")
    print(f"  elapsed total: {elapsed_total:.1f}s")
    if not args.apply:
        print()
        print("DRY RUN -- no model was called, nothing was written. Re-run with --apply to spend for real.")


if __name__ == "__main__":
    main()
