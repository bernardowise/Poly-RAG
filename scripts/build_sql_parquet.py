#!/usr/bin/env python3
"""
build_sql_parquet.py -- Phase 4 (SQL layer), one-off retroactive run.

Flattens the two STRUCTURED halves of the corpus -- the market registry
(DynamoDB) and the odds time-series (S3, one JSON per market) -- into
SQL-queryable Parquet on S3, so retrieval/query.py can answer aggregate /
ranking / point questions ("top 10 markets by volume last week", "how many
markets resolved YES in September", "biggest price swing between two cycles")
that semantic search structurally cannot.

This is the manual bootstrap. A Lambda `build_sql_parquet` chained after
write_lancedb (Phase 4, before Phase 5 rag_eval) will do the same per cycle
later -- NOT built here, and per CLAUDE.md that Lambda change waits for a
real cycle to verify, never a manual invoke.

Outputs (S3, bucket poly-rag-369970405415):
  sql/markets.parquet                     -- one row per registry market
  sql/odds_snapshots/YYYY-MM.parquet      -- one row per (market_id, snapshot),
                                             partitioned by the snapshot month;
                                             DuckDB reads sql/odds_snapshots/*.parquet
                                             as one table

Schema notes from real data (inspected 2026-09-05):
  - registry `resolution_date`/`final_outcome` are the string "None" (not null)
    when empty; normalized to real NULL here.
  - `outcomePrices` in odds snapshots is a JSON STRING ('["0.795", "0.205"]'),
    parsed here into yes_price/no_price doubles plus outcome_prices_raw (the
    original string, kept for any multi-outcome market).
  - `post_resolution_cycles_remaining` is a string ("0"); cast to int.
  - clob_backfill snapshots have no volume/volume24hr/liquidity -> NULL.
  - There are ~2,600 odds/*.json files but only ~740 registry markets (older
    markets were purged from the registry in Aug cleanups). odds_snapshots
    therefore can carry market_ids with no row in markets -- that's expected,
    queries should LEFT JOIN.

Conventions (shared with the rest of scripts/): dry-run by default, needs
--apply to write. Additive -- writes new Parquet, never deletes source data.
Idempotent -- rewrites the Parquet in place, safe to run repeatedly.
"""

import argparse
import io
import json
import sys
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

import boto3
import pandas as pd

ODDS_READ_WORKERS = 32  # parallel S3 GETs -- ~2,600 files, sequential is ~4 min, this is ~15s

BUCKET = "poly-rag-369970405415"
REGISTRY_TABLE = "poly-rag-market-registry"
ODDS_PREFIX = "odds/"
OUT_MARKETS = "sql/markets.parquet"
OUT_ODDS_PREFIX = "sql/odds_snapshots/"

s3 = boto3.client("s3", region_name="us-east-1")
ddb = boto3.resource("dynamodb", region_name="us-east-1")


# --------------------------------------------------------------------------
# helpers
# --------------------------------------------------------------------------
def _none_str_to_null(v):
    """Registry stores empty optional fields as the literal string 'None'."""
    if v is None or v == "None" or v == "":
        return None
    return v


def _parse_outcome_prices(raw):
    """`outcomePrices` is a JSON string like '["0.795", "0.205"]'. Return
    (yes_price, no_price, raw) -- yes/no as floats for binary markets, None
    if it isn't a clean 2-element numeric array. raw is always kept."""
    if not raw:
        return None, None, raw
    try:
        arr = json.loads(raw)
        if isinstance(arr, list) and len(arr) == 2:
            return float(arr[0]), float(arr[1]), raw
    except (json.JSONDecodeError, ValueError, TypeError):
        pass
    return None, None, raw


def _iso_month(ts):
    """'2026-09-05T12:00:08...' -> '2026-09'. Falls back to 'unknown'."""
    if not ts or len(ts) < 7:
        return "unknown"
    return ts[:7]


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
    df = pd.DataFrame(rows)
    return df


# --------------------------------------------------------------------------
# odds/*.json -> odds_snapshots/YYYY-MM.parquet
# --------------------------------------------------------------------------
def iter_odds_keys():
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=BUCKET, Prefix=ODDS_PREFIX):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".json"):
                yield obj["Key"]


def _read_one_odds(key):
    """Fetch + parse one odds/*.json into a list of snapshot row dicts."""
    data = json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    mid = str(data.get("market_id") or key.split("/")[-1].removesuffix(".json"))
    rows = []
    for snap in data.get("snapshots", []):
        ts = snap.get("timestamp", "")
        yes_p, no_p, raw = _parse_outcome_prices(snap.get("outcomePrices"))
        rows.append({
            "market_id": mid,
            "timestamp": ts,
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


def build_odds_partitions():
    """Returns {month: DataFrame}. One row per (market_id, snapshot). The
    ~2,600 S3 GETs run in a thread pool -- sequential was ~4 min, this is
    ~15s (see CLAUDE.md, optimize a long process by its real cost)."""
    keys = list(iter_odds_keys())
    print(f"  {len(keys)} odds files to read ({ODDS_READ_WORKERS} workers)...", flush=True)
    by_month = defaultdict(list)
    done = 0
    with ThreadPoolExecutor(max_workers=ODDS_READ_WORKERS) as ex:
        for rows in ex.map(_read_one_odds, keys):
            for row in rows:
                by_month[_iso_month(row["timestamp"])].append(row)
            done += 1
            if done % 500 == 0:
                print(f"  ... {done}/{len(keys)} read", flush=True)
    print(f"  {len(keys)} odds files read total", flush=True)
    # Force a stable column dtype across all monthly partitions. Early
    # months are 100% clob_backfill (no volume/volume24hr/liquidity), so
    # pandas would infer those columns as all-NaN object/NULL and DuckDB's
    # multi-file read (schema from the first file) then fails to cast the
    # later DOUBLE columns. Pin the numeric cols to float64 and the string
    # cols to object everywhere.
    NUMERIC = ["yes_price", "no_price", "volume", "volume24hr", "liquidity"]
    STRING = ["market_id", "timestamp", "source", "outcome_prices_raw", "backfilled_at"]
    out = {}
    for m, rows in by_month.items():
        df = pd.DataFrame(rows)
        for c in NUMERIC:
            df[c] = pd.to_numeric(df.get(c), errors="coerce").astype("float64")
        for c in STRING:
            df[c] = df.get(c).astype("object")
        out[m] = df[STRING[:3] + NUMERIC[:2] + ["outcome_prices_raw"] + NUMERIC[2:] + ["backfilled_at"]]
    return out


# --------------------------------------------------------------------------
# write
# --------------------------------------------------------------------------
def write_parquet(df, key, apply, local_dir=None):
    buf = io.BytesIO()
    df.to_parquet(buf, engine="pyarrow", index=False, compression="snappy")
    size_kb = buf.tell() / 1024
    if local_dir:
        import os
        path = os.path.join(local_dir, key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            f.write(buf.getvalue())
        print(f"  WROTE {path}  ({len(df):,} rows, {size_kb:,.0f} KB)")
    elif apply:
        buf.seek(0)
        s3.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue(),
                      ContentType="application/octet-stream")
        print(f"  WROTE s3://{BUCKET}/{key}  ({len(df):,} rows, {size_kb:,.0f} KB)")
    else:
        print(f"  [dry-run] would write s3://{BUCKET}/{key}  ({len(df):,} rows, {size_kb:,.0f} KB)")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true", help="actually write to S3 (default: dry-run)")
    ap.add_argument("--local-dir", default=None,
                    help="write the parquet under this local dir instead of S3 (for testing)")
    args = ap.parse_args()

    mode = "LOCAL" if args.local_dir else ("APPLY" if args.apply else "DRY-RUN")
    print(f"=== build_sql_parquet ({mode}) -- {datetime.now(timezone.utc).isoformat()} ===\n")

    print("[1/2] registry -> markets")
    markets = build_markets_df()
    print(f"  {len(markets):,} markets")
    print(f"  status: {markets['status'].value_counts().to_dict()}")
    write_parquet(markets, OUT_MARKETS, args.apply, args.local_dir)

    print("\n[2/2] odds/*.json -> odds_snapshots (partitioned by month)")
    parts = build_odds_partitions()
    total = sum(len(df) for df in parts.values())
    print(f"  {total:,} snapshot rows across {len(parts)} month partitions")
    for month in sorted(parts):
        df = parts[month]
        src = df["source"].value_counts().to_dict()
        print(f"    {month}: {len(df):,} rows  {src}")
        write_parquet(df, f"{OUT_ODDS_PREFIX}{month}.parquet", args.apply, args.local_dir)

    print(f"\n=== done ({mode}) ===")
    if not args.apply and not args.local_dir:
        print("re-run with --apply to write.")


if __name__ == "__main__":
    sys.exit(main())
