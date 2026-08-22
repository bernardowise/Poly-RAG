"""
Poly-RAG digest_metrics Lambda: sends the full-cycle cost/latency/tokens
report email, covering both Fase 1 (ingestion) and Fase 2 (chunking +
embedding). Terminal stage of the whole cycle -- invokes nothing further.

WHY THIS EXISTS AS ITS OWN LAMBDA (2026-08-22, split out of embed_news_article
the same day)
--------------------------------------------------------------------------
Originally this logic lived inline at the end of embed_news_article/handler.py
(built earlier the same day). The naming was misleading: "the cycle metrics
report" and "embedding news_article" are two unrelated responsibilities that
happened to share one Lambda only because embed_news_article was the last
stage of the embedding chain. Splitting them out means Fase 3 (write to
LanceDB, see write_to_lancedb.py) can chain cleanly after "the cycle is truly
done, including its report" instead of piggybacking on a Lambda named for a
different job. embed_news_article still embeds news_article chunks and writes
its own Fase 2 metrics rows -- it now invokes digest_metrics at the end
instead of sending the report itself.

WHAT THIS DOES
--------------
1. Reads every poly-rag-embedding-metrics row for this cycle_started_at
   (Fase 2, all 4 sources).
2. Reads every poly-rag-architecture-metrics row whose timestamp falls in
   [cycle_started_at, cycle_started_at + 12h) (Fase 1, this cycle's window --
   that table predates Fase 2 and has no cycle_started_at field of its own).
3. Builds one HTML report covering both, sends it via SES.

TRIGGER
-------
Invoked by embed_news_article at the very end of the embedding chain, with
cycle_started_at. Terminal -- sends no further invocation.
"""

import os
from datetime import datetime, timedelta, timezone

import boto3

EMBEDDING_METRICS_TABLE = os.environ.get("EMBEDDING_METRICS_TABLE", "poly-rag-embedding-metrics")
ARCHITECTURE_METRICS_TABLE = os.environ.get("ARCHITECTURE_METRICS_TABLE", "poly-rag-architecture-metrics")
SES_SENDER = os.environ.get("SES_SENDER", "bernardolw@gmail.com")
SES_RECIPIENT = os.environ.get("SES_RECIPIENT", "bernardolw@gmail.com")

dynamodb = boto3.resource("dynamodb")
ses = boto3.client("ses")


def fetch_embedding_metrics(cycle_started_at):
    """All Fase 2 rows for this cycle, across all 4 sources -- pk is prefixed
    with cycle_started_at#source#..., so a begins_with Query on pk (partition
    key equality doesn't apply here since pk isn't just cycle_started_at, so
    this uses Scan with a filter, same reasoning as fetch_architecture_metrics
    below) returns every row this cycle wrote, regardless of which of the 4
    embed Lambdas wrote it."""
    table = dynamodb.Table(EMBEDDING_METRICS_TABLE)
    items = []
    response = table.scan(
        FilterExpression="cycle_started_at = :c",
        ExpressionAttributeValues={":c": cycle_started_at},
    )
    items.extend(response["Items"])
    while "LastEvaluatedKey" in response:
        response = table.scan(
            FilterExpression="cycle_started_at = :c",
            ExpressionAttributeValues={":c": cycle_started_at},
            ExclusiveStartKey=response["LastEvaluatedKey"],
        )
        items.extend(response["Items"])
    return items


def fetch_architecture_metrics(cycle_started_at):
    """Fase 1 rows for this cycle. architecture_metrics has no cycle_started_at
    field (Fase 1 predates Fase 2 and was never designed to be queried by
    cycle, see ingest_lambda_role's PutItem-only grant) -- rows are matched by
    their own `timestamp` falling within [cycle_started_at, cycle_started_at +
    12h), same window used for the Fase 1 stages of THIS cycle specifically.
    A Scan, not a Query -- this table's hash key is a per-invocation pk
    string, not something a range query can use for a time filter."""
    cycle_dt = datetime.fromisoformat(cycle_started_at)
    window_end_iso = (cycle_dt + timedelta(hours=12)).isoformat()

    table = dynamodb.Table(ARCHITECTURE_METRICS_TABLE)
    items = []
    response = table.scan()
    for item in response["Items"]:
        ts = item.get("timestamp", "")
        if cycle_started_at <= ts < window_end_iso:
            items.append(item)
    while "LastEvaluatedKey" in response:
        response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        for item in response["Items"]:
            ts = item.get("timestamp", "")
            if cycle_started_at <= ts < window_end_iso:
                items.append(item)
    return items


def build_metrics_report_html(cycle_started_at, fase1_rows, fase2_rows):
    def fmt_usd(x):
        return f"${x:.4f}"

    fase1_by_source = {}
    for r in fase1_rows:
        src = r.get("source", "unknown")
        agg = fase1_by_source.setdefault(src, {"tokens_in": 0, "tokens_out": 0, "cost": 0.0, "latency_ms": 0, "count": 0})
        agg["tokens_in"] += int(r.get("tokens_in", 0))
        agg["tokens_out"] += int(r.get("tokens_out", 0))
        agg["cost"] += float(r.get("estimated_cost_usd", 0))
        agg["latency_ms"] += int(r.get("latency_ms", 0))
        agg["count"] += 1

    fase2_by_source = {}
    for r in fase2_rows:
        src = r.get("source", "unknown")
        agg = fase2_by_source.setdefault(src, {"tokens_in": 0, "cost": 0.0, "latency_ms": 0, "count": 0, "chunks": 0})
        agg["tokens_in"] += int(r.get("tokens_in", 0))
        agg["cost"] += float(r.get("estimated_cost_usd", 0))
        agg["latency_ms"] += int(r.get("latency_ms", 0))
        agg["count"] += 1
        agg["chunks"] += int(r.get("chunk_count", 0))

    total_cost = sum(a["cost"] for a in fase1_by_source.values()) + sum(a["cost"] for a in fase2_by_source.values())

    fase1_rows_html = "".join(
        f"<tr><td>{src}</td><td>{a['count']}</td><td>{a['tokens_in']:,}</td>"
        f"<td>{a['tokens_out']:,}</td><td>{a['latency_ms']:,} ms</td><td>{fmt_usd(a['cost'])}</td></tr>"
        for src, a in sorted(fase1_by_source.items())
    )
    fase2_rows_html = "".join(
        f"<tr><td>{src}</td><td>{a['chunks']:,}</td><td>{a['count']}</td>"
        f"<td>{a['tokens_in']:,}</td><td>{a['latency_ms']:,} ms</td><td>{fmt_usd(a['cost'])}</td></tr>"
        for src, a in sorted(fase2_by_source.items())
    )

    return f"""
    <html><body style="font-family: monospace; font-size: 13px;">
    <h2>Poly-RAG Cycle Metrics -- {cycle_started_at}</h2>

    <h3>Fase 1 (ingestion)</h3>
    <table border="1" cellpadding="4" cellspacing="0">
      <tr><th>source</th><th>invocations</th><th>tokens_in</th><th>tokens_out</th><th>latency</th><th>cost</th></tr>
      {fase1_rows_html or '<tr><td colspan="6">no rows found for this cycle</td></tr>'}
    </table>

    <h3>Fase 2 (chunking + embedding)</h3>
    <table border="1" cellpadding="4" cellspacing="0">
      <tr><th>source</th><th>chunks</th><th>requests</th><th>tokens_in</th><th>latency</th><th>cost</th></tr>
      {fase2_rows_html or '<tr><td colspan="6">no rows found for this cycle</td></tr>'}
    </table>

    <h3>Total estimated cost this cycle: {fmt_usd(total_cost)}</h3>
    </body></html>
    """


def send_metrics_report(cycle_started_at, fase1_rows, fase2_rows):
    html_body = build_metrics_report_html(cycle_started_at, fase1_rows, fase2_rows)
    ses.send_email(
        Source=SES_SENDER,
        Destination={"ToAddresses": [SES_RECIPIENT]},
        Message={
            "Subject": {"Data": f"Poly-RAG Cycle Metrics -- {cycle_started_at}"},
            "Body": {"Html": {"Data": html_body}},
        },
    )


def lambda_handler(event, context):
    cycle_started_at = event.get("cycle_started_at") or datetime.now(timezone.utc).isoformat()

    fase2_rows = fetch_embedding_metrics(cycle_started_at)
    fase1_rows = fetch_architecture_metrics(cycle_started_at)
    send_metrics_report(cycle_started_at, fase1_rows, fase2_rows)

    return {
        "cycle_started_at": cycle_started_at,
        "fase1_rows": len(fase1_rows),
        "fase2_rows": len(fase2_rows),
        "metrics_report_sent": True,
    }
