"""
Poly-RAG ingestion Lambda: News, via targeted Google News RSS search.

Redesigned 2026-08-15 (see tech_debt.md, "News Source Redesign" entry).
Replaces the 10 fixed curated RSS feeds (BBC/CBC/NYT/CNN/France24) and
keyword-based linking entirely. For each open market in the registry, its
`question` text is used AS-IS (not reformulated by an LLM, not reduced to
keywords) as a Google News RSS search query -- Google's own relevance
engine handles synonym/paraphrase matching, which a hand-built AND-keyword
list could not (see the redesign entry for the empirical evidence: markets
about "Federal Reserve" and "President of Russia" -- Polymarket's formal
contract wording -- matched zero real articles, which actually say "Fed"
and "Putin").

CONSCIOUS ToS EXCEPTION: Google News RSS's own response copyright
restricts use to "personal, non-commercial use... within a personal feed
reader" -- an automated Lambda pipeline does not fit that description.
This was a deliberate, informed choice (see tech_debt.md for the full
reasoning and the ToS-clean alternatives considered and set aside). Revisit
if Google enforces the restriction in practice (this endpoint is
undocumented, no API contract, can change or block without notice).

Pipeline per market: search -> decode each result's obfuscated Google
redirect URL to the real article URL (googlenewsdecoder, no headless
browser needed) -> skip if already processed (dedup by exact URL in
poly-rag-processed-urls, permanent, no TTL -- see tech_debt.md for why) ->
extract article body text (trafilatura). If extraction fails (paywall/
anti-scraping block, confirmed happens even with major outlets like
Reuters returning 401), the result is DISCARDED, not kept as a title-only
fallback -- the top 5 per market must be full-coverage articles, not a mix
of extracted and title-only. Stops once 5 results have SUCCESSFULLY
extracted per market, trying more RSS results if earlier ones fail
(network error, decode failure, dedup, or extraction failure) -- "top 5
that actually scrape and aren't duplicates," not blindly the first 5
positions.

BATCHING (added 2026-08-16, see tech_debt.md "News Source Redesign" update):
measured ~21s/market end-to-end (search+decode+extract), so all ~230 open
markets (~80 minutes) cannot fit in one invocation under Lambda's 900s hard
maximum. The Lambda processes markets[offset:offset+BATCH_SIZE] per
invocation.

PARALLEL FAN-OUT (revised 2026-08-16, same day as the sequential-chain
first deploy): the first production run showed each batch taking ~13min,
so a full 7-batch sequential chain would take ~1.5h. Switched from
self-chaining (one invoke() per batch, next one only starts after the
previous returns) to fan-out (all remaining batches invoked at once from
a single dispatch point). Each batch writes to ITS OWN S3 key
(news/.../HH_batch<offset>.json), not a shared cycle payload -- concurrent
batches writing read-modify-write to the SAME key would race (last
writer wins, earlier batches' articles silently lost). A merge step
(merge_batch_payloads) combines the per-batch files into the final
cycle payload once all batches are done; dedup safety still comes from
poly-rag-processed-urls (per-URL atomic put, safe under concurrency
regardless of batching strategy).
"""

import json
import os
import time
import urllib.parse
import urllib.request
import uuid
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

import boto3
import trafilatura
from googlenewsdecoder import gnewsdecoder

S3_BUCKET = os.environ.get("S3_BUCKET", "poly-rag-369970405415")
METRICS_TABLE = os.environ.get("METRICS_TABLE", "poly-rag-architecture-metrics")
REGISTRY_TABLE = os.environ.get("REGISTRY_TABLE", "poly-rag-market-registry")
PROCESSED_URLS_TABLE = os.environ.get("PROCESSED_URLS_TABLE", "poly-rag-processed-urls")
BEDROCK_MODEL_ID = os.environ.get("BEDROCK_MODEL_ID", "us.anthropic.claude-sonnet-4-5-20250929-v1:0")
USE_LLM_ENRICHMENT = os.environ.get("USE_LLM_ENRICHMENT", "true").lower() == "true"

GOOGLE_NEWS_RSS_URL = "https://news.google.com/rss/search"
RESULTS_TARGET_PER_MARKET = 5
RSS_FETCH_COUNT = 15  # over-fetch since some results will fail decode/extract/dedup

# ~21s/market measured (search+decode+extract) -- 35 markets is ~735s, safely
# under Lambda's 900s hard maximum with margin for Bedrock enrichment + S3 I/O.
BATCH_SIZE = 35

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
bedrock = boto3.client("bedrock-runtime")
lambda_client = boto3.client("lambda")

# trafilatura's default DOWNLOAD_TIMEOUT is 30s -- observed in production
# (2026-08-15) causing 3 consecutive Lambda timeouts: a single slow/blocked
# outlet (e.g. washingtonpost.com) can eat 30s before failing, and across
# ~230 markets x up to 5 candidates each, that adds up far faster than the
# Lambda's own timeout budget. 8s is enough for a real page to respond; if
# an outlet hasn't responded by then, it's overwhelmingly likely to be a
# block/paywall anyway, not worth waiting out.
_TRAFILATURA_CONFIG = trafilatura.settings.use_config()
_TRAFILATURA_CONFIG.set("DEFAULT", "DOWNLOAD_TIMEOUT", "8")


def get_open_markets(table):
    """Scans the registry for open markets and their question text. Same
    small-table scan approach used elsewhere in the pipeline -- revisit if
    the registry grows large enough for this to matter."""
    markets = []
    resp = table.scan(
        FilterExpression="#s = :open",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={":open": "open"},
        ProjectionExpression="market_id, question",
    )
    markets.extend(resp.get("Items", []))
    while "LastEvaluatedKey" in resp:
        resp = table.scan(
            FilterExpression="#s = :open",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":open": "open"},
            ProjectionExpression="market_id, question",
            ExclusiveStartKey=resp["LastEvaluatedKey"],
        )
        markets.extend(resp.get("Items", []))
    return [(m["market_id"], m["question"]) for m in markets if m.get("question")]


def search_google_news(question):
    params = urllib.parse.urlencode({"q": question, "hl": "en-US", "gl": "US", "ceid": "US:en"})
    url = f"{GOOGLE_NEWS_RSS_URL}?{params}"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=10) as response:
        xml_bytes = response.read()

    results = []
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return results  # malformed response this cycle -- skip, don't crash

    for item in root.findall(".//item")[:RSS_FETCH_COUNT]:
        title = item.findtext("title", default="").strip()
        link = item.findtext("link", default="").strip()
        source = item.findtext("source", default="").strip()
        pub_date = item.findtext("pubDate", default="").strip()
        if not title or not link:
            continue
        results.append({"title": title, "google_link": link, "source": source, "pubDate": pub_date})
    return results


def is_url_processed(table, url):
    resp = table.get_item(Key={"url": url})
    return "Item" in resp


def mark_url_processed(table, url):
    table.put_item(Item={"url": url, "processed_at": datetime.now(timezone.utc).isoformat()})


def extract_article(real_url):
    """Returns article body text, or None if extraction fails (paywall,
    anti-scraping block, network error, or timeout) -- callers discard the
    result entirely on None, they do not fall back to title-only (see
    process_market_news)."""
    try:
        downloaded = trafilatura.fetch_url(real_url, config=_TRAFILATURA_CONFIG)
        if not downloaded:
            return None
        return trafilatura.extract(downloaded)
    except Exception:
        return None


def process_market_news(market_id, question, processed_urls_table):
    """Searches Google News for this market's question verbatim, decodes
    and dedups results, and extracts full article text -- returns up to
    RESULTS_TARGET_PER_MARKET items. A result only counts toward the target
    if trafilatura successfully extracts its body text; results that fail
    extraction (paywall, anti-scraping block) are discarded, not kept as
    title-only -- the top 5 must be full-coverage articles, not a mix."""
    try:
        search_results = search_google_news(question)
    except Exception as e:
        return [], {"market_id": market_id, "error": f"search failed: {e}"}

    articles = []
    for result in search_results:
        if len(articles) >= RESULTS_TARGET_PER_MARKET:
            break
        try:
            decoded = gnewsdecoder(result["google_link"], interval=1)
        except Exception:
            continue  # decode failed -- skip this result, try the next
        if not decoded.get("status"):
            continue
        real_url = decoded["decoded_url"]

        if is_url_processed(processed_urls_table, real_url):
            continue  # already ingested in a prior cycle -- skip, don't duplicate

        body_text = extract_article(real_url)
        if not body_text:
            continue  # extraction failed (paywall/block) -- discard, don't count as coverage

        articles.append({
            "market_ids": [market_id],
            "title": result["title"],
            "source": result["source"],
            "url": real_url,
            "pubDate": result["pubDate"],
            "body_text": body_text,
        })
        mark_url_processed(processed_urls_table, real_url)

    return articles, None


def enrich_with_bedrock(articles_batch):
    """
    Single Bedrock call summarizing up to 20 articles -- keeps token cost
    bounded regardless of total article count fetched.
    """
    lines = []
    for a in articles_batch[:20]:
        snippet = a["body_text"][:200]
        lines.append(f"- {a['title']} ({a['source']}) {snippet}")
    listing = "\n".join(lines)
    prompt = (
        "Below are news articles matched to active prediction markets. In "
        "3-5 bullet points, summarize the dominant themes relevant to "
        "prediction market research.\n\n" + listing
    )

    start = time.time()
    response = bedrock.invoke_model(
        modelId=BEDROCK_MODEL_ID,
        body=json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 400,
            "messages": [{"role": "user", "content": prompt}],
        }),
    )
    latency_ms = int((time.time() - start) * 1000)

    body = json.loads(response["body"].read())
    summary_text = body["content"][0]["text"]
    usage = body.get("usage", {})

    return {
        "summary": summary_text,
        "tokens_in": usage.get("input_tokens", 0),
        "tokens_out": usage.get("output_tokens", 0),
        "latency_ms": latency_ms,
    }


def estimate_cost_usd(tokens_in, tokens_out):
    return round((tokens_in / 1000) * 0.003 + (tokens_out / 1000) * 0.015, 6)


def write_metrics(source, llm_used, tokens_in, tokens_out, latency_ms, items_processed):
    table = dynamodb.Table(METRICS_TABLE)
    now = datetime.now(timezone.utc).isoformat()
    table.put_item(Item={
        "pk": f"{source}#{now}#{uuid.uuid4().hex[:8]}",
        "source": source,
        "llm_used": llm_used,
        "tokens_in": tokens_in,
        "tokens_out": tokens_out,
        "latency_ms": latency_ms,
        "estimated_cost_usd": str(estimate_cost_usd(tokens_in, tokens_out)),
        "items_processed": items_processed,
        "timestamp": now,
    })


def get_cycle_prefix(cycle_started_at):
    dt = datetime.fromisoformat(cycle_started_at)
    return f"news/{dt.strftime('%Y-%m-%d')}/{dt.strftime('%H')}"


def get_batch_s3_key(cycle_started_at, offset):
    """Each batch owns its own S3 object -- required for safe parallel fan-out
    (see module docstring). Merged into the final cycle key by merge_batch_payloads."""
    return f"{get_cycle_prefix(cycle_started_at)}_batch{offset}.json"


def get_final_cycle_s3_key(cycle_started_at):
    return f"{get_cycle_prefix(cycle_started_at)}.json"


DISPATCH_STAGGER_SECONDS = 3  # small gap between fanned-out invokes -- avoids
# firing all remaining batches at the exact same instant against Google
# News / outlet servers, without adding proxy/VPN infrastructure (considered
# and rejected: no cheap way to rotate Lambda's egress IP per batch --
# NAT Gateway is a fixed IP and breaks the budget on its own; third-party
# rotating proxies add cost and a new external dependency for what's really
# a request-concurrency concern, not an IP-identity one).


def dispatch_remaining_batches(context, offsets, cycle_started_at):
    """Fires every remaining batch as an independent async invocation
    (fan-out), instead of chaining one invoke per batch. Each batch writes
    to its own S3 key, so there's no shared-state race between them.
    Staggered by DISPATCH_STAGGER_SECONDS to avoid a simultaneous burst
    against the same outlets/Google -- this call itself only costs a few
    seconds total (invoke() with InvocationType=Event returns immediately,
    it doesn't wait for the batch to run), it doesn't block the dispatcher's
    own batch-0 work from starting late."""
    for i, offset in enumerate(offsets):
        if i > 0:
            time.sleep(DISPATCH_STAGGER_SECONDS)
        lambda_client.invoke(
            FunctionName=context.function_name,
            InvocationType="Event",
            Payload=json.dumps({
                "offset": offset,
                "cycle_started_at": cycle_started_at,
                "mode": "batch",
            }),
        )


def merge_batch_payloads(cycle_started_at, total_markets, expected_offsets):
    """Reads every batch's own S3 object and combines them into the final
    cycle payload. Called by whichever batch invocation happens to be last
    to finish (detected via S3 listing, not a fixed order -- batches run
    in parallel so completion order isn't deterministic)."""
    prefix = get_cycle_prefix(cycle_started_at)
    articles = []
    markets_failed = []
    markets_processed = 0
    llm_summaries = []

    for offset in expected_offsets:
        key = f"{prefix}_batch{offset}.json"
        try:
            obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        except s3.exceptions.ClientError:
            return None  # not all batches have written yet
        batch_payload = json.loads(obj["Body"].read())
        articles.extend(batch_payload["articles"])
        markets_failed.extend(batch_payload["markets_failed"])
        markets_processed += batch_payload["markets_processed"]
        if batch_payload.get("llm_summary"):
            llm_summaries.append(batch_payload["llm_summary"])

    return {
        "source": "news",
        "cycle_started_at": cycle_started_at,
        "markets_queried": total_markets,
        "markets_processed": markets_processed,
        "markets_failed": markets_failed,
        "articles": articles,
        "llm_summary": " | ".join(llm_summaries) if llm_summaries else None,
    }


def lambda_handler(event, context):
    registry_table = dynamodb.Table(REGISTRY_TABLE)
    processed_urls_table = dynamodb.Table(PROCESSED_URLS_TABLE)
    open_markets = get_open_markets(registry_table)
    total_markets = len(open_markets)
    all_offsets = list(range(0, total_markets, BATCH_SIZE))

    offset = event.get("offset", 0)
    cycle_started_at = event.get("cycle_started_at") or datetime.now(timezone.utc).isoformat()
    mode = event.get("mode")  # None on the dispatch/first invocation, "batch" on fanned-out workers
    batch = open_markets[offset:offset + BATCH_SIZE]

    # Dispatch step: only the very first invocation of a cycle (offset=0,
    # no mode set) fans out the REMAINING batches before doing its own work.
    # This runs once per cycle regardless of how many batches exist.
    if mode is None and offset == 0:
        remaining_offsets = [o for o in all_offsets if o != 0]
        if remaining_offsets:
            dispatch_remaining_batches(context, remaining_offsets, cycle_started_at)

    fetch_start = time.time()
    batch_articles = []
    batch_markets_failed = []

    for market_id, question in batch:
        articles, error = process_market_news(market_id, question, processed_urls_table)
        if error:
            batch_markets_failed.append(error)
            continue
        batch_articles.extend(articles)

    fetch_latency_ms = int((time.time() - fetch_start) * 1000)

    llm_enrichment = None
    tokens_in, tokens_out, llm_latency_ms = 0, 0, 0
    if USE_LLM_ENRICHMENT and batch_articles:
        llm_enrichment = enrich_with_bedrock(batch_articles)
        tokens_in = llm_enrichment["tokens_in"]
        tokens_out = llm_enrichment["tokens_out"]
        llm_latency_ms = llm_enrichment["latency_ms"]

    total_latency_ms = fetch_latency_ms + llm_latency_ms
    batch_payload = {
        "source": "news",
        "cycle_started_at": cycle_started_at,
        "markets_processed": len(batch),
        "markets_failed": batch_markets_failed,
        "articles": batch_articles,
        "llm_summary": llm_enrichment["summary"] if llm_enrichment else None,
    }

    batch_key = get_batch_s3_key(cycle_started_at, offset)
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=batch_key,
        Body=json.dumps(batch_payload),
        ContentType="application/json",
    )

    write_metrics(
        source="ingest_news",
        llm_used=USE_LLM_ENRICHMENT,
        tokens_in=tokens_in,
        tokens_out=tokens_out,
        latency_ms=total_latency_ms,
        items_processed=len(batch_articles),
    )

    # Whichever batch happens to finish last attempts the merge -- safe to
    # attempt from multiple batches if they finish close together, since
    # merge_batch_payloads just overwrites the same final key with the same
    # result once every batch key exists (idempotent, not a race).
    merged = merge_batch_payloads(cycle_started_at, total_markets, all_offsets)
    cycle_complete = merged is not None
    if merged is not None:
        merged["ingested_at"] = datetime.now(timezone.utc).isoformat()
        merged["metadata"] = {
            "schema_version": "v3",
            "lambda_name": context.function_name,
            "lambda_request_id": context.aws_request_id,
            "llm_used": USE_LLM_ENRICHMENT,
            "llm_model_id": BEDROCK_MODEL_ID if USE_LLM_ENRICHMENT else None,
            "cycle_complete": True,
        }
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=get_final_cycle_s3_key(cycle_started_at),
            Body=json.dumps(merged),
            ContentType="application/json",
        )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "batch_key": batch_key,
            "batch_offset": offset,
            "batch_size": len(batch),
            "batch_article_count": len(batch_articles),
            "markets_queried": total_markets,
            "cycle_complete": cycle_complete,
            "llm_used": USE_LLM_ENRICHMENT,
        }),
    }
