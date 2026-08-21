"""
One-off: embed one SLICE of the chunked corpus (a range of cycles, one source,
one model) into vectors, and persist them to S3. Step 2 of the Phase 2
bootstrap. Writes no vector store -- that is Phase 3, deliberately decoupled.

WHY THIS EXISTS, AND WHY IT REPLACES bootstrap_embed_corpus.py
--------------------------------------------------------------
The previous embedding script never completed a corpus. Its failure was
diagnosed at the time as a DAILY token quota problem; that diagnosis was wrong
and is corrected here from real measurements (2026-08-21):

  - The daily quota blamed in tech_debt.md ("Model invocation max tokens per day
    for Cohere Embed V4 = 8,100,000") DOES NOT EXIST in this account. The real
    daily quota is `Global cross-region model inference tokens per day for
    Cohere Embed V4` = 16,200,000 -- double the figure that was cited. Real
    consumption on the day of the failures was 6,032,944 tokens, i.e. 37% of the
    ceiling, not the 65% that was believed. The account was never near its daily
    limit when it was being throttled.
  - The real binding limit is `On-demand model inference tokens per minute for
    Cohere Embed English` = 300,000 TPM (verified, NOT adjustable), alongside
    200 RPM (adjustable).
  - The actual cause of the throttling was UNBOUNDED REQUEST SIZE. The old
    script batched by COUNT (96 texts/request, Cohere's documented max). But
    this corpus contains articles up to 240,387 chars (~60K tokens) -- a single
    request of 96 such chunks would carry millions of tokens and blow a whole
    minute's budget in one call. Measured evidence: 648 throttles against 141
    successful invocations, a 4.6:1 failure ratio, which no amount of backoff
    could fix because backoff addresses REQUEST RATE, and the constraint being
    violated was TOKENS PER MINUTE.

Hence this script's two structural differences, which are the entire redesign:

  1. TOKEN-AWARE BATCHING -- a request is filled until it reaches a token
     budget, never until it reaches a count. Count is only a secondary cap
     (Cohere's own 96-text API limit).
  2. A TOKEN-RATE GOVERNOR -- a sliding 60s window tracks tokens actually sent
     and sleeps before a request that would exceed the TPM ceiling. Pacing is
     computed from real token accounting, not a fixed sleep guessed in advance
     (the old script's `COHERE_PACE_SECONDS=10` was a guess, and at ~96K tokens
     per full batch it was still roughly 2x over the real ceiling).

Backoff still exists, but demoted to what it should always have been: a safety
net for transient errors, not the pacing strategy.

The chunking side complements this: articles over MAX_ARTICLE_CHARS are now
SPLIT (not truncated) by bootstrap_chunk_corpus.py, so no single chunk can
carry a disproportionate share of a minute's token budget.

WHAT IS KEPT FROM THE OLD SCRIPT
--------------------------------
Checkpointing, deliberately (tech_debt.md: "Mitigation already built (keep
it)"). Every CHECKPOINT_SIZE chunks are written to their own S3 part file, and a
resume skips parts already present. This is the only reason any progress
survived the previous attempts, and the 4-day sliced plan depends on it.

SAFETY
------
Read-only against `chunks/`. Writes only under `vectors/`, only with --apply.
Dry run is the default and makes ZERO Bedrock calls -- it reports the batching
plan, token totals, and quota projections so the cost and duration of a run are
known before any money is spent.

USAGE
-----
    python scripts/embed_corpus_slice.py --chunks-key chunks/news_article/cycles_01-05.json
    python scripts/embed_corpus_slice.py --chunks-key ... --apply
    python scripts/embed_corpus_slice.py --chunks-key ... --apply --limit 50   # small live test
"""

import argparse
import json
import time
from collections import deque

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

S3_BUCKET = "poly-rag-369970405415"
REGION = "us-east-1"

MODEL_ID = "cohere.embed-v4:0"
MODEL_LABEL = "cohere"
EMBED_DIM = 1536

# --- Real, verified account quotas (aws service-quotas, 2026-08-21) ----------
# "On-demand model inference tokens per minute for Cohere Embed English"
TPM_LIMIT = 300_000
# "Global cross-region model inference requests per minute for Cohere Embed V4"
RPM_LIMIT = 200
# "Global cross-region model inference tokens per day for Cohere Embed V4"
TPD_LIMIT = 16_200_000

# Use a fraction of the ceiling rather than riding it exactly -- token counts
# here are an estimate (chars/4), and Cohere's real tokenizer will disagree
# somewhat in both directions. Leaving headroom means an underestimate does not
# silently become a throttle.
TPM_TARGET = int(TPM_LIMIT * 0.50)      # 150,000 tokens/min
# TUNING NOTE FOR FUTURE SLICES (measured live, 2026-08-21, cycles 1-5 run):
# 0.70 is very close to Bedrock's real enforcement point and produces steady --
# though recoverable -- throttling. Measured during the run: the chars/4 token
# estimate is ACCURATE (CloudWatch counted 663,197 real tokens against ~700,000
# estimated, ratio ~0.95, i.e. the estimate slightly OVER-counts, so it is not
# the source of the throttles). Real consumption sat at 135K-215K tokens/min --
# never near the published 300K ceiling -- yet ThrottlingException still fired
# in bursts. Conclusion: Bedrock enforces over a window SHORTER than 60s, so a
# governor that is correct on a 60-second average can still burst past the real
# limiter. The run survives this (backoff absorbs it: 8 throttles, all
# recovered, vs the previous bootstrap's 648 throttles against 141 successes),
# but the retries cost wall-clock time and the effective rate decayed over the
# run (214K -> 187K -> 154K -> 135K tokens/min).
#
# For the remaining slices (Saturday cycles 6-10, Sunday 11-15, Monday 16-18):
# drop TPM_TARGET to ~0.50 (150,000 tokens/min). Expected to finish in similar
# wall-clock time with far fewer retries, since time currently lost to backoff
# is roughly what the lower target gives up in nominal rate. Do NOT raise it
# above 0.70 -- that is already past the practical enforcement point.
MAX_TOKENS_PER_REQUEST = 40_000         # a single request never exceeds ~19% of
                                        # the per-minute budget. Lowered from
                                        # 90_000 on 2026-08-21 after the first
                                        # real run: request size determines how
                                        # finely the rate governor can pack a
                                        # 60s window, and at 90K only 2 requests
                                        # fit in a 210K target, so the window
                                        # emptied and refilled in coarse steps.
                                        # At 40K, ~5 fit per window and pacing
                                        # is smooth. Smaller also bounds the
                                        # blast radius of a failed request.
MAX_TEXTS_PER_REQUEST = 96              # Cohere's documented per-call max

CHECKPOINT_SIZE = 500                   # chunks per part file
MAX_RETRIES = 6                         # transient-error safety net only

CHARS_PER_TOKEN = 4                     # same proxy used by the chunking script

s3 = boto3.client("s3")
bedrock = boto3.client(
    "bedrock-runtime",
    region_name=REGION,
    # One client, used sequentially. The old script shared a module-level client
    # across ~44 threads and saturated urllib3's default ~10-connection pool
    # (tech_debt.md, "Real lateral finding") -- this run is deliberately
    # single-threaded, so the pool only needs a few connections, and retries are
    # handled explicitly below rather than by botocore's opaque default.
    config=Config(retries={"max_attempts": 0}, max_pool_connections=10),
)


def estimate_tokens(text):
    return max(1, len(text) // CHARS_PER_TOKEN)


# ---------------------------------------------------------------------------
# Token-aware batching -- fill by TOKENS, not by count
# ---------------------------------------------------------------------------

def build_batches(chunks):
    """Group chunks into requests bounded by BOTH a token budget and Cohere's
    text-count limit, whichever binds first.

    This is the core fix. Batching purely by count (the old script's approach)
    makes request size proportional to the average chunk size in that window,
    which for this corpus ranges from a 200-char comment to a 32,000-char
    article -- a 160x spread. Bounding by tokens makes every request cost
    roughly the same share of the per-minute budget regardless of what happens
    to be in it.
    """
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


# ---------------------------------------------------------------------------
# Rate governor -- sliding 60s window over tokens AND requests actually sent
# ---------------------------------------------------------------------------

class RateGovernor:
    """Sleeps before a request that would breach the per-minute token or
    request ceiling, based on what was actually sent in the trailing 60s.

    Reactive backoff cannot solve a TPM breach: by the time the throttle is
    returned, the tokens have already been counted against the window, so
    retrying just re-spends budget that is not there. The only thing that works
    is not sending too much in the first place -- which requires tracking the
    window locally, since Bedrock's embed response carries no billed-token
    field (verified 2026-08-21: the response has no `meta`, so there is no
    server-side count to read back).
    """

    def __init__(self, tpm_target=TPM_TARGET, rpm_limit=RPM_LIMIT):
        self.tpm_target = tpm_target
        self.rpm_limit = rpm_limit
        self.events = deque()  # (timestamp, tokens)
        self.total_slept = 0.0

    def _prune(self, now):
        while self.events and now - self.events[0][0] >= 60.0:
            self.events.popleft()

    def _window(self):
        return sum(t for _, t in self.events), len(self.events)

    def wait_for(self, tokens):
        """Sleep only until ENOUGH budget has freed up for this request -- not
        until the window is empty.

        Fixed 2026-08-21 after watching the first real run: the original version
        slept until the OLDEST event left the 60s window, which is the moment
        the FULL budget frees up, not the moment THIS request fits. With ~88K
        tokens per request against a 210K target, two requests fill the window,
        and the third then waited out almost a full 60s -- collapsing the
        effective rate to roughly one request per minute (~88K tokens/min
        measured, against a 210K target, i.e. 60% of the available rate left
        unused). Correct behaviour: expire events one at a time, and stop as
        soon as the freed tokens make room for this request.
        """
        while True:
            now = time.monotonic()
            self._prune(now)
            win_tokens, win_requests = self._window()
            if (win_tokens + tokens <= self.tpm_target
                    and win_requests + 1 <= self.rpm_limit):
                return
            if not self.events:
                # Nothing to wait for: a single request larger than the whole
                # target would loop forever. build_batches caps request size and
                # main() refuses to start if any batch exceeds the target, so
                # this is a guard against a future regression, not a live case.
                return

            # Walk the window oldest-first, accumulating what each expiry frees,
            # and wait only for as many as this request actually needs.
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
            self.total_slept += sleep_for
            time.sleep(sleep_for)

    def record(self, tokens):
        self.events.append((time.monotonic(), tokens))

    def current_tpm(self):
        self._prune(time.monotonic())
        return self._window()[0]


# ---------------------------------------------------------------------------
# Bedrock call, with retry as a safety net (NOT as the pacing strategy)
# ---------------------------------------------------------------------------

THROTTLE_CODES = {"ThrottlingException", "TooManyRequestsException",
                  "ServiceQuotaExceededException"}

# Transient server-side failures that are NOT throttling but are equally
# retryable. Added 2026-08-21 after a real crash: the cycles 1-5 run died at
# 52.5% (batch 60/109) on `An error occurred (500) ... Internal Server Error`,
# because the original retry logic only caught THROTTLE_CODES and let
# everything else propagate. A 500 from Bedrock is a transient server fault --
# the correct response is to retry it exactly like a throttle, not to abandon a
# half-finished run. Checkpointing meant the crash cost only the un-checkpointed
# tail (~2.27M tokens survived in 3 parts), which is precisely why that
# mechanism was kept, but the run should not have died at all.
RETRYABLE_CODES = THROTTLE_CODES | {
    "500", "ModelErrorException", "ModelNotReadyException",
    "ServiceUnavailableException", "InternalServerException",
    "ModelTimeoutException",
}


def is_retryable(exc):
    """A ClientError is retryable if its error code says so, or if the HTTP
    status is 5xx (server-side) or 429 (rate limited). Checking the status code
    directly matters because Bedrock returned a bare "500" as the error code in
    the real crash -- there was no symbolic name to match on."""
    error = exc.response.get("Error", {})
    code = str(error.get("Code", ""))
    if code in RETRYABLE_CODES:
        return True, code
    status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0)
    if status >= 500 or status == 429:
        return True, f"HTTP {status}"
    return False, code


def embed_texts(texts, attempt_log):
    body = json.dumps({
        "texts": texts,
        "input_type": "search_document",   # corpus side; queries must later use
                                            # "search_query" -- asymmetric by design
        "embedding_types": ["float"],
    })
    delay = 2.0
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = bedrock.invoke_model(modelId=MODEL_ID, body=body)
            payload = json.loads(response["body"].read())
            vectors = payload["embeddings"]["float"]
            if len(vectors) != len(texts):
                raise RuntimeError(
                    f"asked for {len(texts)} embeddings, got {len(vectors)}"
                )
            return vectors
        except ClientError as exc:
            retryable, code = is_retryable(exc)
            if not retryable or attempt == MAX_RETRIES:
                raise
            attempt_log.append(code)
            print(f"      {code} (attempt {attempt}/{MAX_RETRIES}) "
                  f"-- backing off {delay:.0f}s", flush=True)
            time.sleep(delay)
            delay = min(delay * 2, 60.0)
        except Exception as exc:
            # Network-level faults (connection reset, read timeout) are not
            # ClientErrors but are just as transient over a run this long.
            if attempt == MAX_RETRIES:
                raise
            attempt_log.append(type(exc).__name__)
            print(f"      {type(exc).__name__} (attempt {attempt}/{MAX_RETRIES}) "
                  f"-- backing off {delay:.0f}s", flush=True)
            time.sleep(delay)
            delay = min(delay * 2, 60.0)
    raise RuntimeError("unreachable")


# ---------------------------------------------------------------------------
# Checkpointing -- kept from the old script by explicit decision
# ---------------------------------------------------------------------------

def checkpoint_key(variant, part_index):
    return f"vectors/_checkpoints/{variant}/{MODEL_LABEL}/part_{part_index:05d}.json"


def existing_checkpoints(variant):
    """Part NUMBERS already in S3 -- used only to pick the next part index so a
    resume never overwrites an existing part file."""
    prefix = f"vectors/_checkpoints/{variant}/{MODEL_LABEL}/"
    found = set()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            name = obj["Key"].rsplit("/", 1)[-1]
            if name.startswith("part_") and name.endswith(".json"):
                found.add(int(name[5:-5]))
    return found


def already_embedded_ids(variant):
    """The set of chunk_ids already embedded, read from the checkpoint files
    themselves.

    Rewritten 2026-08-21 after the cycles 1-5 crash. The first version resumed
    by POSITION -- it counted how many chunks had gone by and skipped batches
    whose index fell inside an existing part. That is only correct if batch
    boundaries are byte-identical between runs, which they are NOT: batching is
    token-driven, so changing MAX_TOKENS_PER_REQUEST (which happened between
    the two runs, 90K -> 40K) re-cuts every boundary and silently shifts which
    chunks land in which part. Resuming by identity instead makes the resume
    independent of batch size, chunk order, and how many parts exist -- a chunk
    is skipped if and only if its own id was actually embedded and persisted.
    """
    prefix = f"vectors/_checkpoints/{variant}/{MODEL_LABEL}/"
    done = set()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if not obj["Key"].endswith(".json"):
                continue
            records = json.loads(
                s3.get_object(Bucket=S3_BUCKET, Key=obj["Key"])["Body"].read()
            )
            done.update(r["chunk_id"] for r in records)
    return done


def write_checkpoint(variant, part_index, records):
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=checkpoint_key(variant, part_index),
        Body=json.dumps(records),
        ContentType="application/json",
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--chunks-key", required=True,
                        help="S3 key of the chunk file, e.g. chunks/news_article/cycles_01-05.json")
    parser.add_argument("--variant",
                        help="output namespace under vectors/_checkpoints/ "
                             "(default: derived from the chunks key)")
    parser.add_argument("--apply", action="store_true",
                        help="actually call Bedrock and write vectors (default: dry run, zero calls)")
    parser.add_argument("--limit", type=int,
                        help="only process the first N chunks (for a small live test)")
    parser.add_argument("--restart", action="store_true",
                        help="ignore existing checkpoints instead of resuming")
    args = parser.parse_args()

    variant = args.variant or args.chunks_key.split("/")[1]

    print(f"=== Embed corpus slice -- {'APPLY' if args.apply else 'DRY RUN (no Bedrock calls)'} ===")
    print(f"chunks : s3://{S3_BUCKET}/{args.chunks_key}")
    print(f"model  : {MODEL_ID}  (dim {EMBED_DIM}, input_type=search_document)")
    print(f"variant: {variant}")
    print()

    chunks = json.loads(
        s3.get_object(Bucket=S3_BUCKET, Key=args.chunks_key)["Body"].read()
    )
    if args.limit:
        chunks = chunks[:args.limit]
        print(f"LIMIT: only the first {args.limit} chunks\n")

    # Resume BEFORE batching, so the plan printed below describes the work that
    # will actually be done, not the work the full slice would have needed.
    skipped = 0
    if not args.restart:
        done_ids = already_embedded_ids(variant)
        if done_ids:
            before = len(chunks)
            chunks = [c for c in chunks if c["chunk_id"] not in done_ids]
            skipped = before - len(chunks)
            print(f"RESUME: {len(done_ids):,} chunk_ids already embedded in S3 "
                  f"-- skipping {skipped:,}, {len(chunks):,} remaining\n")
    if not chunks:
        print("Nothing left to embed -- every chunk in this slice is already "
              "checkpointed.")
        return

    total_tokens = sum(estimate_tokens(c["text"]) for c in chunks)
    batches = build_batches(chunks)
    batch_tokens = [sum(estimate_tokens(c["text"]) for c in b) for b in batches]

    print("=== Plan ===")
    print(f"  chunks           : {len(chunks):,}")
    print(f"  est. tokens      : {total_tokens:,}  (chars/{CHARS_PER_TOKEN})")
    print(f"  requests         : {len(batches):,}")
    print(f"  texts/request    : min {min(len(b) for b in batches)}, "
          f"max {max(len(b) for b in batches)}, "
          f"avg {sum(len(b) for b in batches)/len(batches):.1f}")
    print(f"  tokens/request   : min {min(batch_tokens):,}, max {max(batch_tokens):,}, "
          f"avg {sum(batch_tokens)/len(batch_tokens):,.0f}")
    print(f"  floor at {TPM_TARGET:,} TPM : {total_tokens/TPM_TARGET:.1f} min")
    print(f"  daily quota use  : {total_tokens/TPD_LIMIT*100:.1f}% of {TPD_LIMIT:,}")

    # Fail loudly rather than discovering mid-run that a single request cannot fit.
    oversized = [i for i, t in enumerate(batch_tokens) if t > TPM_TARGET]
    if oversized:
        print(f"\n  ERROR: {len(oversized)} request(s) exceed the per-minute target "
              f"on their own -- they can never be sent. Re-chunk with a smaller cap.")
        return

    # Start numbering new parts after the highest existing one, so a resume
    # appends rather than overwriting a part written by an earlier run.
    existing_parts = set() if args.restart else existing_checkpoints(variant)
    next_part = (max(existing_parts) + 1) if existing_parts else 0
    if existing_parts:
        print(f"\n  {len(existing_parts)} existing checkpoint part(s); "
              f"new parts start at part_{next_part:05d}")

    if not args.apply:
        print("\nDRY RUN -- no Bedrock calls made, nothing written.")
        print("Re-run with --apply to execute.")
        return

    print("\n=== Running ===")
    governor = RateGovernor()
    throttles = []
    pending, done_chunks, part_index = [], 0, next_part
    started = time.time()
    embedded_tokens = 0

    # No in-loop skipping: `chunks` was already filtered by chunk_id above, so
    # every batch here is genuinely un-embedded work.
    for batch_no, batch in enumerate(batches, start=1):
        tokens = batch_tokens[batch_no - 1]

        governor.wait_for(tokens)
        vectors = embed_texts([c["text"] for c in batch], throttles)
        governor.record(tokens)
        embedded_tokens += tokens

        for chunk, vector in zip(batch, vectors):
            record = {k: v for k, v in chunk.items() if k != "text"}
            record["embedding"] = vector
            record["embedding_model"] = MODEL_ID
            record["embedding_dim"] = len(vector)
            pending.append(record)

        done_chunks += len(batch)

        while len(pending) >= CHECKPOINT_SIZE:
            write_checkpoint(variant, part_index, pending[:CHECKPOINT_SIZE])
            pending = pending[CHECKPOINT_SIZE:]
            part_index += 1

        if batch_no % 5 == 0 or batch_no == len(batches):
            elapsed = time.time() - started
            pct = done_chunks / len(chunks) * 100
            rate = embedded_tokens / elapsed * 60 if elapsed else 0
            eta = (total_tokens - embedded_tokens) / rate if rate else 0
            print(f"  [{batch_no}/{len(batches)}] {done_chunks:,}/{len(chunks):,} chunks "
                  f"({pct:.1f}%) | {embedded_tokens:,} tok | "
                  f"{rate:,.0f} tok/min | window {governor.current_tpm():,} | "
                  f"ETA {eta:.1f} min", flush=True)

    if pending:
        write_checkpoint(variant, part_index, pending)
        part_index += 1

    elapsed = time.time() - started
    print("\n=== Done ===")
    print(f"  chunks embedded  : {done_chunks:,}")
    print(f"  tokens sent      : {embedded_tokens:,}")
    print(f"  checkpoint parts : {part_index}")
    print(f"  elapsed          : {elapsed/60:.1f} min")
    print(f"  throttles        : {len(throttles)}"
          f"{' -- ' + str(dict((c, throttles.count(c)) for c in set(throttles))) if throttles else ''}")
    print(f"  time spent paced : {governor.total_slept/60:.1f} min")
    print(f"  effective rate   : {embedded_tokens/elapsed*60:,.0f} tokens/min")


if __name__ == "__main__":
    main()
