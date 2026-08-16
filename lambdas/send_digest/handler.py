"""
Poly-RAG digest Lambda: consolidates each 12h ingestion cycle into a single
structured digest and emails it via Amazon SES. Triggered by EventBridge
right after each cycle (00:00 and 12:00 UTC).

Redesigned 2026-08-16 (see tech_debt.md, "Bespoke Digest Redesign") from a
flat text email that just concatenated each source's individual llm_summary.
Two things changed:

1. The digest is now a real data artifact, not just an email. A structured
   JSON is written to s3://bucket/digest/YYYY-MM-DD/HH.json FIRST -- that's
   the source of truth, since this digest is meant to be ingested into the
   RAG corpus later (Day 4/5), and a rendered HTML email is a poor format to
   re-parse for that. The email body is generated FROM that JSON, not the
   other way around.

2. Content is synthesized across sources, not just concatenated per-source
   summaries. Specifically: which markets newly entered the registry this
   cycle, which resolved (with real outcome), which open markets moved the
   most since the prior odds snapshot (volatility, not just current price),
   a "world snapshot" of current market belief -- highest-conviction bets
   (top volume24hr) and most-disputed bets (price near 50/50), independent
   of what moved this cycle (added 2026-08-16, see tech_debt.md) -- and a
   handful of real verbatim quotes (comments/articles) rather than only
   LLM-paraphrased prose. A single new Bedrock call synthesizes all of this
   into one executive-summary paragraph, seeing all sources together -- the
   per-source llm_summary fields from each ingestion Lambda were each
   written blind to the other two sources.

Permanent piece of infrastructure -- independent of whether the
LLM-in-ingestion trial (see CLAUDE.md Development Conventions) is ultimately
kept or reverted. Degrades gracefully per-section if a source's S3 object is
missing or a field isn't present, rather than failing the whole digest.
"""

import json
import os
import time
import uuid
from datetime import datetime, timezone

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "poly-rag-369970405415")
SES_SENDER = os.environ.get("SES_SENDER", "bernardolw@gmail.com")
SES_RECIPIENT = os.environ.get("SES_RECIPIENT", "bernardolw@gmail.com")
REGISTRY_TABLE = os.environ.get("REGISTRY_TABLE", "poly-rag-market-registry")
METRICS_TABLE = os.environ.get("METRICS_TABLE", "poly-rag-architecture-metrics")
BEDROCK_MODEL_ID = os.environ.get("BEDROCK_MODEL_ID", "us.anthropic.claude-sonnet-4-5-20250929-v1:0")

# Sources with a per-cycle S3 payload (source -> list field whose length is
# shown as "item count" for that source's cycle). Polymarket's own payload
# doesn't have a single "count" field the same way News/Comments do
# (candidate_count is the whole candidate pool size, not what changed this
# cycle) -- newly-tracked-market count is a more meaningful number for it.
SOURCE_LIST_FIELDS = {
    "polymarket": "newly_tracked_markets",
    "news": "articles",
    "comments": "comments",
}

TOP_VOLATILITY_COUNT = 5
QUOTE_COUNT_PER_SOURCE = 3
SNAPSHOT_VOLUME_COUNT = 5
SNAPSHOT_UNCERTAIN_COUNT = 5
SNAPSHOT_UNCERTAIN_LOW = 0.40
SNAPSHOT_UNCERTAIN_HIGH = 0.60

s3 = boto3.client("s3")
ses = boto3.client("ses")
dynamodb = boto3.resource("dynamodb")
bedrock = boto3.client("bedrock-runtime")


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


def fetch_source_payload(source):
    key = get_latest_object_key(source)
    if not key:
        return None
    obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
    return json.loads(obj["Body"].read())


def get_question(registry_table, market_id):
    resp = registry_table.get_item(Key={"market_id": market_id}, ProjectionExpression="question")
    return resp.get("Item", {}).get("question", "")


def estimate_cost_usd(tokens_in, tokens_out):
    return round((tokens_in / 1000) * 0.003 + (tokens_out / 1000) * 0.015, 6)


def write_metrics(source, llm_used, tokens_in, tokens_out, latency_ms, items_processed):
    """Same shape/pricing as the 3 ingestion Lambdas' write_metrics (see
    ingest_comments/handler.py) -- send_digest's own Bedrock call
    (synthesize_executive_summary) previously only landed in the S3 digest
    JSON, invisible to the architecture-metrics cost table (see
    tech_debt.md, added 2026-08-16 to close that gap before the next
    12:00 UTC cycle)."""
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


def _load_open_market_odds(registry_table):
    """Scans open markets and reads each one's odds snapshots from S3 once.
    Shared by compute_top_volatility (movement) and compute_world_snapshot
    (current belief) so a cycle costs one registry scan + one S3 GET per
    open market, not two full passes over the same ~300+ markets."""
    resp = registry_table.scan(
        FilterExpression="#s = :open",
        ExpressionAttributeNames={"#s": "status"},
        ExpressionAttributeValues={":open": "open"},
        ProjectionExpression="market_id, question",
    )
    open_markets = resp.get("Items", [])

    loaded = []
    for m in open_markets:
        market_id = m["market_id"]
        try:
            obj = s3.get_object(Bucket=S3_BUCKET, Key=f"odds/{market_id}.json")
            odds_data = json.loads(obj["Body"].read())
        except s3.exceptions.ClientError:
            continue
        snapshots = odds_data.get("snapshots", [])
        if not snapshots:
            continue
        loaded.append((m, snapshots))
    return loaded


def compute_top_volatility(loaded_markets, limit=TOP_VOLATILITY_COUNT):
    """Ranks open markets by absolute price movement between their last two
    odds snapshots. Movement, not current price -- the digest's job is to
    say what CHANGED this cycle, which a bare price snapshot can't show on
    its own."""
    movements = []
    for m, snapshots in loaded_markets:
        if len(snapshots) < 2:
            continue
        try:
            prev_prices = json.loads(snapshots[-2]["outcomePrices"])
            curr_prices = json.loads(snapshots[-1]["outcomePrices"])
            delta = abs(float(curr_prices[0]) - float(prev_prices[0]))
        except (ValueError, IndexError, KeyError):
            continue

        movements.append({
            "market_id": m["market_id"],
            "question": m.get("question", ""),
            "prev_price": prev_prices[0],
            "curr_price": curr_prices[0],
            "delta": round(delta, 4),
        })

    movements.sort(key=lambda x: x["delta"], reverse=True)
    return movements[:limit]


def compute_world_snapshot(loaded_markets):
    """A belief snapshot, not a movement snapshot -- what does the market
    currently think will happen, independent of what changed this cycle.
    Two groups over the same open-market pool: highest-conviction bets (top
    volume24hr, where the most real money is placed) and most-disputed bets
    (current price within SNAPSHOT_UNCERTAIN_LOW/HIGH of 50/50 -- the market
    is genuinely split)."""
    current_state = []
    for m, snapshots in loaded_markets:
        latest = snapshots[-1]
        try:
            price = float(json.loads(latest["outcomePrices"])[0])
        except (ValueError, IndexError, KeyError, TypeError):
            continue

        current_state.append({
            "market_id": m["market_id"],
            "question": m.get("question", ""),
            "price": round(price, 4),
            "volume24hr": float(latest.get("volume24hr", 0) or 0),
        })

    by_volume = sorted(current_state, key=lambda x: x["volume24hr"], reverse=True)[:SNAPSHOT_VOLUME_COUNT]

    uncertain_pool = [
        x for x in current_state
        if SNAPSHOT_UNCERTAIN_LOW <= x["price"] <= SNAPSHOT_UNCERTAIN_HIGH
    ]
    by_uncertainty = sorted(uncertain_pool, key=lambda x: abs(x["price"] - 0.5))[:SNAPSHOT_UNCERTAIN_COUNT]

    return {
        "top_conviction": by_volume,
        "most_disputed": by_uncertainty,
    }


def extract_quotes(source, payload):
    """Pulls a handful of real, verbatim text snippets per source -- not
    LLM-paraphrased -- so the digest keeps some human voice, not just
    generated prose."""
    if not payload:
        return []
    if source == "news":
        articles = payload.get("articles", [])[:QUOTE_COUNT_PER_SOURCE]
        return [
            {"text": a.get("title", ""), "source": a.get("source", ""), "url": a.get("url", "")}
            for a in articles
        ]
    if source == "comments":
        comments = payload.get("comments", [])[:QUOTE_COUNT_PER_SOURCE]
        return [
            {"text": c.get("text", ""), "source": c.get("author", ""), "url": None}
            for c in comments
        ]
    return []


def synthesize_executive_summary(digest_data):
    """Single Bedrock call that sees ALL sources together -- unlike each
    ingestion Lambda's own llm_summary, which is written blind to the other
    two sources. Produces one short paragraph tying odds movement, news,
    and sentiment into one narrative instead of three disconnected blocks."""
    lines = []
    if digest_data["newly_tracked_markets"]:
        lines.append("New markets this cycle: " + "; ".join(
            m["question"] for m in digest_data["newly_tracked_markets"][:10]
        ))
    if digest_data["resolved_markets"]:
        lines.append("Resolved this cycle: " + "; ".join(
            f"{m['question']} -> {m['outcome_prices']}" for m in digest_data["resolved_markets"][:10]
        ))
    if digest_data["top_volatility"]:
        lines.append("Biggest odds moves: " + "; ".join(
            f"{m['question']} ({m['prev_price']} -> {m['curr_price']})"
            for m in digest_data["top_volatility"]
        ))
    snapshot = digest_data.get("world_snapshot") or {}
    if snapshot.get("top_conviction"):
        lines.append("Highest-conviction bets (top volume, current price): " + "; ".join(
            f"{m['question']} @ {m['price']}" for m in snapshot["top_conviction"]
        ))
    if snapshot.get("most_disputed"):
        lines.append("Most disputed bets (price near 50/50): " + "; ".join(
            f"{m['question']} @ {m['price']}" for m in snapshot["most_disputed"]
        ))
    for source in ("news", "comments"):
        quotes = digest_data["quotes"].get(source, [])
        if quotes:
            lines.append(f"{source} samples: " + " | ".join(q["text"][:150] for q in quotes))

    if not lines:
        return None

    prompt = (
        "You are writing the opening paragraph of a prediction-market research digest. "
        "Below is structured data from one 12h ingestion cycle across three sources: "
        "Polymarket odds, news coverage, and trader comments. Write 2-3 sentences tying "
        "them into one narrative -- what moved, why (if news/comments suggest a reason), "
        "and what's notable. Also weave in what the market currently BELIEVES, not just "
        "what changed -- the highest-conviction and most-disputed bets below are a "
        "snapshot of current sentiment, independent of this cycle's movement. Be concrete, "
        "cite specific markets by name, no generic filler.\n\n"
        + "\n".join(lines)
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
    usage = body.get("usage", {})

    return {
        "text": body["content"][0]["text"],
        "tokens_in": usage.get("input_tokens", 0),
        "tokens_out": usage.get("output_tokens", 0),
        "latency_ms": latency_ms,
    }


def build_digest_data(registry_table, now_iso):
    """Assembles the structured JSON that's the source of truth for this
    digest -- written to S3 before anything else, independent of whether
    the email send succeeds."""
    source_payloads = {source: fetch_source_payload(source) for source in SOURCE_LIST_FIELDS}
    polymarket_payload = source_payloads.get("polymarket") or {}

    newly_tracked = polymarket_payload.get("newly_tracked_markets", [])
    resolved = polymarket_payload.get("resolved_markets", [])
    loaded_markets = _load_open_market_odds(registry_table)
    top_volatility = compute_top_volatility(loaded_markets)
    world_snapshot = compute_world_snapshot(loaded_markets)

    quotes = {
        source: extract_quotes(source, payload)
        for source, payload in source_payloads.items()
        if source in ("news", "comments")
    }

    source_stats = {}
    for source, list_field in SOURCE_LIST_FIELDS.items():
        payload = source_payloads.get(source)
        item_count = len((payload or {}).get(list_field, []))
        source_stats[source] = {
            "available": payload is not None,
            "item_count": item_count,
        }

    return {
        "ingested_at": now_iso,
        "newly_tracked_markets": newly_tracked,
        "resolved_markets": resolved,
        "top_volatility": top_volatility,
        "world_snapshot": world_snapshot,
        "quotes": quotes,
        "source_stats": source_stats,
    }


def build_email_html(digest_data, executive_summary):
    def escape(text):
        return (text or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

    sections = []

    if executive_summary:
        sections.append(f"""
        <div style="background:#f5f5f7;border-radius:8px;padding:16px 20px;margin-bottom:24px;">
          <p style="margin:0;font-size:15px;line-height:1.5;color:#1a1a1a;">{escape(executive_summary)}</p>
        </div>""")

    if digest_data["newly_tracked_markets"]:
        rows = "".join(
            f'<li style="margin-bottom:4px;">{escape(m["question"])}</li>'
            for m in digest_data["newly_tracked_markets"]
        )
        sections.append(f"""
        <h3 style="color:#2563eb;font-size:14px;margin:20px 0 8px;">New Markets ({len(digest_data["newly_tracked_markets"])})</h3>
        <ul style="margin:0;padding-left:20px;font-size:13px;color:#333;">{rows}</ul>""")

    if digest_data["resolved_markets"]:
        rows = "".join(
            f'<li style="margin-bottom:4px;">{escape(m["question"])} &rarr; <code>{escape(str(m["outcome_prices"]))}</code></li>'
            for m in digest_data["resolved_markets"]
        )
        sections.append(f"""
        <h3 style="color:#16a34a;font-size:14px;margin:20px 0 8px;">Resolved ({len(digest_data["resolved_markets"])})</h3>
        <ul style="margin:0;padding-left:20px;font-size:13px;color:#333;">{rows}</ul>""")

    if digest_data["top_volatility"]:
        rows = "".join(
            f'<li style="margin-bottom:4px;">{escape(m["question"])}: {m["prev_price"]} &rarr; {m["curr_price"]} '
            f'<span style="color:#dc2626;">(&Delta;{m["delta"]})</span></li>'
            for m in digest_data["top_volatility"]
        )
        sections.append(f"""
        <h3 style="color:#dc2626;font-size:14px;margin:20px 0 8px;">Biggest Moves</h3>
        <ul style="margin:0;padding-left:20px;font-size:13px;color:#333;">{rows}</ul>""")

    snapshot = digest_data.get("world_snapshot") or {}
    if snapshot.get("top_conviction"):
        rows = "".join(
            f'<li style="margin-bottom:4px;">{escape(m["question"])}: '
            f'<strong>{m["price"]:.0%}</strong></li>'
            for m in snapshot["top_conviction"]
        )
        sections.append(f"""
        <h3 style="color:#0891b2;font-size:14px;margin:20px 0 8px;">Highest-Conviction Bets</h3>
        <ul style="margin:0;padding-left:20px;font-size:13px;color:#333;">{rows}</ul>""")

    if snapshot.get("most_disputed"):
        rows = "".join(
            f'<li style="margin-bottom:4px;">{escape(m["question"])}: '
            f'<strong>{m["price"]:.0%}</strong></li>'
            for m in snapshot["most_disputed"]
        )
        sections.append(f"""
        <h3 style="color:#0891b2;font-size:14px;margin:20px 0 8px;">Most Disputed Bets</h3>
        <ul style="margin:0;padding-left:20px;font-size:13px;color:#333;">{rows}</ul>""")

    for source, label in (("news", "News Highlights"), ("comments", "Trader Comments")):
        source_quotes = digest_data["quotes"].get(source, [])
        if not source_quotes:
            continue
        rows = "".join(
            f'<li style="margin-bottom:8px;font-style:italic;">"{escape(q["text"])}" '
            f'<span style="color:#666;font-style:normal;">-- {escape(q["source"])}</span></li>'
            for q in source_quotes
        )
        sections.append(f"""
        <h3 style="color:#7c3aed;font-size:14px;margin:20px 0 8px;">{label}</h3>
        <ul style="margin:0;padding-left:20px;font-size:13px;color:#333;">{rows}</ul>""")

    stats_row = " &nbsp;|&nbsp; ".join(
        f'{source}: {stats["item_count"]}' if stats["available"] else f'{source}: no data'
        for source, stats in digest_data["source_stats"].items()
    )
    sections.append(f"""
    <p style="margin-top:24px;padding-top:12px;border-top:1px solid #e5e5e5;font-size:11px;color:#999;">{stats_row}</p>""")

    now = datetime.now(timezone.utc)
    return f"""
    <div style="font-family:-apple-system,Segoe UI,Roboto,sans-serif;max-width:600px;margin:0 auto;padding:20px;">
      <h2 style="font-size:18px;margin:0 0 16px;color:#1a1a1a;">Poly-RAG Digest &mdash; {now.strftime('%Y-%m-%d %H:%M UTC')}</h2>
      {''.join(sections)}
    </div>"""


def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    now_iso = now.isoformat()
    registry_table = dynamodb.Table(REGISTRY_TABLE)

    # cycle_started_at (fixed 2026-08-16, see tech_debt.md "Strict
    # Ingestion Chaining"): the last stage of the chain -- must use the
    # cycle's own start time for its own S3 key, not now(), for the same
    # reason ingest_comments needed the same fix (this Lambda can run in
    # a later UTC hour than the cycle logically started in). Falls back
    # to now() only for a standalone/manual invocation with no upstream
    # cycle context.
    cycle_started_at = event.get("cycle_started_at") or now_iso

    digest_data = build_digest_data(registry_table, now_iso)
    exec_summary_result = synthesize_executive_summary(digest_data)
    executive_summary = exec_summary_result["text"] if exec_summary_result else None
    digest_data["executive_summary"] = executive_summary

    digest_data["metadata"] = {
        "schema_version": "v1",
        "lambda_name": context.function_name,
        "lambda_request_id": context.aws_request_id,
        "llm_used": exec_summary_result is not None,
        "llm_model_id": BEDROCK_MODEL_ID if exec_summary_result else None,
        "tokens_in": exec_summary_result["tokens_in"] if exec_summary_result else 0,
        "tokens_out": exec_summary_result["tokens_out"] if exec_summary_result else 0,
    }

    write_metrics(
        source="send_digest",
        llm_used=exec_summary_result is not None,
        tokens_in=exec_summary_result["tokens_in"] if exec_summary_result else 0,
        tokens_out=exec_summary_result["tokens_out"] if exec_summary_result else 0,
        latency_ms=exec_summary_result["latency_ms"] if exec_summary_result else 0,
        items_processed=1,
    )

    # Structured JSON is the source of truth, written before the email --
    # this is what gets ingested into the RAG corpus later, independent of
    # whether the email send below succeeds.
    cycle_dt = datetime.fromisoformat(cycle_started_at)
    digest_s3_key = f"digest/{cycle_dt.strftime('%Y-%m-%d')}/{cycle_dt.strftime('%H')}.json"
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=digest_s3_key,
        Body=json.dumps(digest_data),
        ContentType="application/json",
    )

    html_body = build_email_html(digest_data, executive_summary)
    subject = f"Poly-RAG Digest -- {now.strftime('%Y-%m-%d %H:%M UTC')}"

    ses.send_email(
        Source=SES_SENDER,
        Destination={"ToAddresses": [SES_RECIPIENT]},
        Message={
            "Subject": {"Data": subject},
            "Body": {"Html": {"Data": html_body}},
        },
    )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "sent": True,
            "digest_s3_key": digest_s3_key,
            "newly_tracked_count": len(digest_data["newly_tracked_markets"]),
            "resolved_count": len(digest_data["resolved_markets"]),
        }),
    }
