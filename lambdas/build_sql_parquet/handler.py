"""
Poly-RAG build_sql_parquet Lambda: Phase 4 (SQL layer), per-cycle refresh of
the SQL-queryable Parquet on S3 that retrieval/query.py's text-to-SQL route
reads. Chained after write_lancedb (Phase 3); invokes rag_eval (Phase 5)
when that Lambda exists -- for now it is the terminal stage and sends the
cycle's FOURTH checkpoint email.

WHAT THIS DOES, PER CYCLE (deliberately unlike scripts/build_sql_parquet.py)
---------------------------------------------------------------------------
The one-off script (scripts/build_sql_parquet.py) rebuilds ALL 15 monthly
partitions of sql/odds_snapshots/ plus sql/markets.parquet. This Lambda only
rewrites:
  - sql/markets.parquet            -- always (whole registry, ~2,600 rows,
                                      trivial: one DynamoDB scan)
  - sql/odds_snapshots/YYYY-MM.parquet  -- ONLY the partition for the month
                                          of cycle_started_at
Past months' partitions are immutable (their snapshots never change once
written) and are left untouched. To rebuild the current month's partition
completely it still reads every odds/<market_id>.json (a new snapshot for
this cycle may land in any market's file) and filters to the month in
memory -- ~2,600 S3 GETs in a 32-worker thread pool, ~15s.

WHY A CONTAINER IMAGE, NOT A ZIP
-------------------------------
pandas + pyarrow together are near Lambda's 250MB zip/Layer limit. Rather
than fight it, this uses the Image package type -- the same pattern as
write_lancedb, the project's other container Lambda (its own ECR repo,
5-image lifecycle policy, pushed manually via docker/aws ecr, not by
terraform).

Schema notes (identical to the one-off script -- kept in sync deliberately):
  - registry resolution_date/final_outcome are the string "None" when empty
    -> normalized to real NULL.
  - outcomePrices is a JSON string ('["0.795", "0.205"]') -> split into
    yes_price/no_price doubles + outcome_prices_raw.
  - clob_backfill snapshots have no volume/volume24hr/liquidity -> NULL.
  - numeric columns pinned to float64 so DuckDB's multi-file read does not
    choke on early all-clob_backfill months.
"""

import io
import json
import os
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import boto3
import pandas as pd

S3_BUCKET = os.environ.get("S3_BUCKET", "poly-rag-369970405415")
REGISTRY_TABLE = os.environ.get("REGISTRY_TABLE", "poly-rag-market-registry")
SES_SENDER = os.environ.get("SES_SENDER", "bernardolw@gmail.com")
SES_RECIPIENT = os.environ.get("SES_RECIPIENT", "bernardolw@gmail.com")
# Set once rag_eval (Phase 5) exists; empty means this is still the terminal stage.
RAG_EVAL_LAMBDA_NAME = os.environ.get("RAG_EVAL_LAMBDA_NAME", "")

ODDS_PREFIX = "odds/"
OUT_MARKETS = "sql/markets.parquet"
OUT_ODDS_PREFIX = "sql/odds_snapshots/"
ODDS_READ_WORKERS = 32

NUMERIC = ["yes_price", "no_price", "volume", "volume24hr", "liquidity"]
STRING = ["market_id", "timestamp", "source", "outcome_prices_raw", "backfilled_at"]
COL_ORDER = STRING[:3] + NUMERIC[:2] + ["outcome_prices_raw"] + NUMERIC[2:] + ["backfilled_at"]

s3 = boto3.client("s3")
ddb = boto3.resource("dynamodb")
ses = boto3.client("ses")
lam = boto3.client("lambda")


# --------------------------------------------------------------------------
# helpers (kept byte-for-byte in step with scripts/build_sql_parquet.py)
# --------------------------------------------------------------------------
def _none_str_to_null(v):
    if v is None or v == "None" or v == "":
        return None
    return v


def _parse_outcome_prices(raw):
    if not raw:
        return None, None, raw
    try:
        arr = json.loads(raw)
        if isinstance(arr, list) and len(arr) == 2:
            return float(arr[0]), float(arr[1]), raw
    except (json.JSONDecodeError, ValueError, TypeError):
        pass
    return None, None, raw


def _month_of(ts):
    """cycle_started_at / a snapshot timestamp -> 'YYYY-MM'."""
    return (ts or "")[:7]


# --------------------------------------------------------------------------
# registry -> markets.parquet
# --------------------------------------------------------------------------
def build_markets_df():
    tbl = ddb.Table(REGISTRY_TABLE)
    items = []
    resp = tbl.scan()
    items.extend(resp["Items"])
    while "LastEvaluatedKey" in resp:
        resp = tbl.scan(ExclusiveStartKey=resp["LastEvaluatedKey"])
        items.extend(resp["Items"])

    rows = []
    for it in items:
        rows.append({
            "market_id": str(it.get("market_id")),
            "question": it.get("question"),
            "description": it.get("description"),
            "status": it.get("status"),
            "resolution_source": _none_str_to_null(it.get("resolution_source")),
            "created_at": _none_str_to_null(it.get("created_at")),
            "first_seen": _none_str_to_null(it.get("first_seen")),
            "last_updated": _none_str_to_null(it.get("last_updated")),
            "end_date": _none_str_to_null(it.get("end_date")),
            "resolution_date": _none_str_to_null(it.get("resolution_date")),
            "final_outcome": _none_str_to_null(it.get("final_outcome")),
            "comment_entity_type": _none_str_to_null(it.get("comment_entity_type")),
            "comment_entity_id": _none_str_to_null(it.get("comment_entity_id")),
            "comment_link_type": _none_str_to_null(it.get("comment_link_type")),
            "post_resolution_cycles_remaining": int(it.get("post_resolution_cycles_remaining", 0) or 0),
        })
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------
# odds/*.json -> current month's odds_snapshots partition
# --------------------------------------------------------------------------
def _iter_odds_keys():
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=ODDS_PREFIX):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json"):
                yield obj["Key"]


def _read_one_odds(key):
    data = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    mid = str(data.get("market_id") or key.split("/")[-1].removesuffix(".json"))
    rows = []
    for snap in data.get("snapshots", []):
        yes_p, no_p, raw = _parse_outcome_prices(snap.get("outcomePrices"))
        rows.append({
            "market_id": mid,
            "timestamp": snap.get("timestamp", ""),
            "source": snap.get("source"),
            "yes_price": yes_p,
            "no_price": no_p,
            "outcome_prices_raw": raw,
            "volume": snap.get("volume"),
            "volume24hr": snap.get("volume24hr"),
            "liquidity": snap.get("liquidity"),
            "backfilled_at": snap.get("backfilled_at"),
        })
    return rows


def build_current_month_odds_df(month):
    """Every odds file, filtered to `month`, as one DataFrame with pinned
    dtypes. A market with no snapshot in `month` contributes nothing."""
    keys = list(_iter_odds_keys())
    rows = []
    with ThreadPoolExecutor(max_workers=ODDS_READ_WORKERS) as ex:
        for file_rows in ex.map(_read_one_odds, keys):
            rows.extend(r for r in file_rows if _month_of(r["timestamp"]) == month)
    df = pd.DataFrame(rows)
    if df.empty:
        # keep the column shape stable even on an (unexpected) empty month
        df = pd.DataFrame(columns=COL_ORDER)
    for c in NUMERIC:
        df[c] = pd.to_numeric(df.get(c), errors="coerce").astype("float64")
    for c in STRING:
        df[c] = df.get(c).astype("object")
    return df[COL_ORDER], len(keys)


# --------------------------------------------------------------------------
# write + report
# --------------------------------------------------------------------------
def _put_parquet(df, key):
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", index=False, compression="snappy")
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=buf.getvalue(),
                  ContentType="application/octet-stream")
    return buf.tell()


# --------------------------------------------------------------------------
# smoke-test queries -- run against the Parquet this Lambda just wrote, via
# DuckDB reading straight from S3. The report email carries their results so
# a human can tell from the inbox that the SQL layer is actually queryable
# end to end, not just that a file got written. Any query that errors or
# returns an unexpectedly empty result is rendered in red.
# --------------------------------------------------------------------------
MARKETS_URI = f"read_parquet('s3://{S3_BUCKET}/{OUT_MARKETS}')"
ODDS_URI = f"read_parquet('s3://{S3_BUCKET}/{OUT_ODDS_PREFIX}*.parquet')"


def _duckdb_conn():
    import duckdb
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("SET s3_region='us-east-1';")
    cr = boto3.Session().get_credentials().get_frozen_credentials()
    con.execute(f"SET s3_access_key_id='{cr.access_key}';")
    con.execute(f"SET s3_secret_access_key='{cr.secret_key}';")
    if cr.token:
        con.execute(f"SET s3_session_token='{cr.token}';")
    return con


def _top10_by_volume(where_clause, label):
    return (label, f"""
        SELECT o.market_id, m.question, MAX(o.volume) AS max_volume
        FROM {ODDS_URI} o
        LEFT JOIN {MARKETS_URI} m USING (market_id)
        WHERE o.source = 'cycle'{where_clause}
        GROUP BY o.market_id, m.question
        ORDER BY max_volume DESC
        LIMIT 10
    """)


def run_smoke_queries(cycle_started_at, month):
    """Returns a list of (label, columns, rows, error) tuples."""
    cycle_day = cycle_started_at[:10]
    queries = [
        _top10_by_volume(f" AND o.timestamp LIKE '{cycle_started_at[:13]}%'", "Top 10 by volume -- this cycle"),
        _top10_by_volume(f" AND o.timestamp >= (DATE '{cycle_day}' - INTERVAL 7 DAY)::VARCHAR", "Top 10 by volume -- last 7 days"),
        _top10_by_volume(f" AND o.timestamp LIKE '{month}%'", "Top 10 by volume -- month to date"),
        _top10_by_volume("", "Top 10 by volume -- all time"),
        ("Market count by status", f"SELECT status, COUNT(*) AS n FROM {MARKETS_URI} GROUP BY status ORDER BY n DESC"),
        ("Latest snapshot per source", f"SELECT source, MAX(timestamp) AS latest_ts, COUNT(*) AS n FROM {ODDS_URI} GROUP BY source"),
        ("Total odds_snapshots rows (all partitions)", f"SELECT COUNT(*) AS total_rows FROM {ODDS_URI}"),
        ("Biggest yes_price swing -- month to date", f"""
            WITH c AS (
              SELECT market_id, timestamp, yes_price,
                ROW_NUMBER() OVER (PARTITION BY market_id ORDER BY timestamp)      AS rn_f,
                ROW_NUMBER() OVER (PARTITION BY market_id ORDER BY timestamp DESC) AS rn_l
              FROM {ODDS_URI} WHERE source='cycle' AND timestamp LIKE '{month}%'
            )
            SELECT f.market_id, m.question,
                   ROUND(f.yes_price, 3) AS first_yes,
                   ROUND(l.yes_price, 3) AS last_yes,
                   ROUND(l.yes_price - f.yes_price, 3) AS swing
            FROM c f JOIN c l USING (market_id)
            LEFT JOIN {MARKETS_URI} m USING (market_id)
            WHERE f.rn_f = 1 AND l.rn_l = 1
            ORDER BY ABS(l.yes_price - f.yes_price) DESC
            LIMIT 10
        """),
    ]
    out = []
    con = None
    try:
        con = _duckdb_conn()
        for label, sql in queries:
            try:
                cur = con.execute(sql)
                cols = [d[0] for d in cur.description]
                rows = cur.fetchall()
                out.append((label, cols, rows, None))
            except Exception as exc:  # noqa: BLE001 -- one bad query must not kill the report
                out.append((label, [], [], f"{type(exc).__name__}: {exc}"))
    except Exception as exc:  # noqa: BLE001 -- DuckDB/httpfs setup failed entirely
        out.append(("DuckDB connection", [], [], f"{type(exc).__name__}: {exc}"))
    finally:
        if con is not None:
            con.close()
    return out


# "this cycle" can legitimately be empty (Lambda ran before this cycle's
# snapshots landed, or a cycle with no volume rows) -- an empty result there
# is informational, not a failure. Every other query returning 0 rows IS a
# real problem.
EMPTY_OK = {"Top 10 by volume -- this cycle"}


def _render_query_block(label, cols, rows, error):
    if error:
        return (f"<h3 style='color:#c00'>{label}</h3>"
                f"<pre style='color:#c00'>{error}</pre>")
    if not rows:
        color = "#888" if label in EMPTY_OK else "#c00"
        note = "0 rows (ok -- cycle snapshots may not be in yet)" if label in EMPTY_OK else "0 rows returned (unexpected)"
        return (f"<h3 style='color:{color}'>{label}</h3>"
                f"<p style='color:{color}'>{note}</p>")
    head = "".join(f"<th>{c}</th>" for c in cols)
    body = ""
    for r in rows:
        body += "<tr>" + "".join(f"<td>{'' if v is None else v}</td>" for v in r) + "</tr>"
    return (f"<h3>{label}</h3>"
            f"<table border='1' cellpadding='4' cellspacing='0'>"
            f"<tr>{head}</tr>{body}</table>")


def _send_report(cycle_started_at, month, n_markets, n_odds_rows, n_files,
                 size_markets, size_odds, elapsed_s, smoke_results):
    smoke_html = "".join(_render_query_block(*t) for t in smoke_results)
    n_failed = sum(1 for label, _, rows, err in smoke_results
                   if err or (not rows and label not in EMPTY_OK))
    status_line = (f"<p style='color:#c00'><b>{n_failed} smoke query(ies) failed or empty</b></p>"
                   if n_failed else "<p style='color:#080'><b>all smoke queries OK</b></p>")
    html = f"""
    <html><body style="font-family: monospace; font-size: 13px;">
    <h2>Poly-RAG Phase 4 (build_sql_parquet) -- {cycle_started_at}</h2>
    <table border="1" cellpadding="4" cellspacing="0">
      <tr><th>output</th><th>rows</th><th>size</th></tr>
      <tr><td>sql/markets.parquet</td><td>{n_markets:,}</td><td>{size_markets/1024:,.0f} KB</td></tr>
      <tr><td>sql/odds_snapshots/{month}.parquet</td><td>{n_odds_rows:,}</td><td>{size_odds/1024:,.0f} KB</td></tr>
    </table>
    <p>{n_files:,} odds/*.json read (filtered to month {month}). Write time: {elapsed_s:.1f}s</p>
    <hr>
    <h2>SQL smoke test (DuckDB over s3://.../sql/*.parquet)</h2>
    {status_line}
    {smoke_html}
    </body></html>
    """
    ses.send_email(
        Source=SES_SENDER,
        Destination={"ToAddresses": [SES_RECIPIENT]},
        Message={
            "Subject": {"Data": f"Poly-RAG Phase 4 (SQL layer) -- {cycle_started_at}"},
            "Body": {"Html": {"Data": html}},
        },
    )


def lambda_handler(event, context):
    cycle_started_at = event["cycle_started_at"]  # no fallback -- Phase 4 only
    # makes sense chained from Phase 3, never invoked standalone.
    month = _month_of(cycle_started_at)

    started = time.time()

    markets = build_markets_df()
    size_markets = _put_parquet(markets, OUT_MARKETS)

    odds_df, n_files = build_current_month_odds_df(month)
    size_odds = _put_parquet(odds_df, f"{OUT_ODDS_PREFIX}{month}.parquet")

    elapsed_s = time.time() - started

    # Query the Parquet we just wrote -- proves the SQL layer is queryable
    # end to end, not just that a file landed. Best-effort: a smoke-test
    # failure is reported in the email, never raised.
    smoke_results = run_smoke_queries(cycle_started_at, month)

    _send_report(cycle_started_at, month, len(markets), len(odds_df), n_files,
                 size_markets, size_odds, elapsed_s, smoke_results)

    if RAG_EVAL_LAMBDA_NAME:
        lam.invoke(
            FunctionName=RAG_EVAL_LAMBDA_NAME,
            InvocationType="Event",
            Payload=json.dumps({"cycle_started_at": cycle_started_at}).encode("utf-8"),
        )

    return {
        "cycle_started_at": cycle_started_at,
        "month_partition": month,
        "markets_rows": len(markets),
        "odds_rows": len(odds_df),
        "odds_files_read": n_files,
        "elapsed_s": round(elapsed_s, 1),
        "smoke_failed": sum(1 for label, _, rows, err in smoke_results
                            if err or (not rows and label not in EMPTY_OK)),
        "rag_eval_invoked": bool(RAG_EVAL_LAMBDA_NAME),
    }
