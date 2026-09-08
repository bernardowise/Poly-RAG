"""Deterministic ground truth for the Phase 5 RAG-eval question set.

Every function here is PURE with respect to its inputs: given a
`cycle_started_at` (ISO 8601 string, call it T) it returns the correct
answer for that cycle, computed straight from S3 / DynamoDB / the Phase 4
Parquet -- never from the RAG's own output. The eval runner scores the
RAG's answer against what these return.

Each returns a dict with a common shape:
    {
      "market_ids": [str, ...],      # the markets this answer is about --
                                     #   NOT scored (RAGAS scores prose, not
                                     #   id lists), kept only for the drift
                                     #   judge's cross-cycle bookkeeping
      "text": str,                   # rendered reference answer (RAGAS
                                     #   answer_correctness / factual_correctness
                                     #   / context_recall are scored against this)
      "detail": {...},               # structured values, for the results JSON
    }

q4 is the exception -- it is a faithfulness check against real comments,
there is no single "correct answer", so `text` is the concatenated comment
evidence.

There is NO topic/vertical filter here -- the project does not have one.
The corpus is whatever the ingestion verifiability filter let in (sports
included). Questions that would otherwise have hundreds of correct answers
(q2, q6) are framed as "count + top-5 by volume" so the reference stays
bounded.

Read paths only. No writes, no Lambda invokes. Safe to run against
production data any time.
"""

import json
import os
from datetime import datetime, timedelta, timezone

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "poly-rag-369970405415")
REGISTRY_TABLE = os.environ.get("REGISTRY_TABLE", "poly-rag-market-registry")

_s3 = boto3.client("s3", region_name="us-east-1")
_ddb = boto3.resource("dynamodb", region_name="us-east-1")

# DuckDB connection over the Phase 4 Parquet -- lazily built, mirrors
# retrieval/query.py's _get_sql_con so the eval reads the exact same tables
# the text-to-SQL route does.
_con = None
SQL_MARKETS = f"read_parquet('s3://{S3_BUCKET}/sql/markets.parquet')"
SQL_ODDS = f"read_parquet('s3://{S3_BUCKET}/sql/odds_snapshots/*.parquet')"


def _sql():
    global _con
    if _con is None:
        import duckdb
        c = duckdb.connect()
        c.execute("INSTALL httpfs; LOAD httpfs;")
        c.execute("SET s3_region='us-east-1';")
        cr = boto3.Session().get_credentials().get_frozen_credentials()
        c.execute(f"SET s3_access_key_id='{cr.access_key}';")
        c.execute(f"SET s3_secret_access_key='{cr.secret_key}';")
        if cr.token:
            c.execute(f"SET s3_session_token='{cr.token}';")
        _con = c
    return _con


def _q(sql, params=None):
    cur = _sql().execute(sql, params or [])
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def _parse_T(cycle_started_at):
    t = cycle_started_at.replace("Z", "+00:00")
    dt = datetime.fromisoformat(t)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _iso(dt):
    return dt.astimezone(timezone.utc).isoformat()


def _yes_price(outcome_prices_raw):
    """odds_snapshots stores yes_price/no_price as columns already, but the
    digest and odds/*.json carry the raw JSON string -- helper for those."""
    try:
        return float(json.loads(outcome_prices_raw)[0])
    except (ValueError, TypeError, IndexError, json.JSONDecodeError):
        return None


# --------------------------------------------------------------------------
# q1 -- Bitcoin markets, odds move over the last 24h
# --------------------------------------------------------------------------
def gt_q1_bitcoin_24h(cycle_started_at):
    T = _parse_T(cycle_started_at)
    lo = _iso(T - timedelta(hours=24))
    hi = _iso(T)
    rows = _q(f"""
        WITH bt AS (
            SELECT market_id, question
            FROM {SQL_MARKETS}
            WHERE lower(question) LIKE '%bitcoin%'
        ),
        win AS (
            SELECT o.market_id, o.timestamp, o.yes_price
            FROM {SQL_ODDS} o
            JOIN bt USING (market_id)
            WHERE o.timestamp >= ? AND o.timestamp <= ?
              AND o.yes_price IS NOT NULL
        ),
        ordered AS (
            SELECT market_id, yes_price,
                   ROW_NUMBER() OVER (PARTITION BY market_id ORDER BY timestamp ASC)  AS rn_first,
                   ROW_NUMBER() OVER (PARTITION BY market_id ORDER BY timestamp DESC) AS rn_last,
                   COUNT(*)    OVER (PARTITION BY market_id)                          AS n
            FROM win
        )
        SELECT o.market_id, bt.question,
               MAX(CASE WHEN rn_first = 1 THEN yes_price END) AS first_yes,
               MAX(CASE WHEN rn_last  = 1 THEN yes_price END) AS last_yes,
               MAX(n) AS n_snapshots
        FROM ordered o JOIN bt USING (market_id)
        GROUP BY o.market_id, bt.question
        HAVING MAX(n) >= 2
        ORDER BY abs(
            MAX(CASE WHEN rn_last = 1 THEN yes_price END)
          - MAX(CASE WHEN rn_first = 1 THEN yes_price END)
        ) DESC
    """, [lo, hi])
    for r in rows:
        r["delta"] = round((r["last_yes"] or 0) - (r["first_yes"] or 0), 4)
    lines = [f"Bitcoin markets with odds movement in the 24h to {hi}:"]
    for r in rows:
        d = r["delta"]
        arrow = "up" if d > 0 else "down" if d < 0 else "flat"
        lines.append(
            f"- [{r['market_id']}] {r['question']}: {r['first_yes']:.3f} -> "
            f"{r['last_yes']:.3f} ({arrow} {abs(d):.3f})"
        )
    if not rows:
        lines.append("- (no Bitcoin market had 2+ snapshots in this window)")
    return {
        "market_ids": [str(r["market_id"]) for r in rows],
        "text": "\n".join(lines),
        "detail": {"window": [lo, hi], "markets": rows},
    }


# --------------------------------------------------------------------------
# q2 -- COUNT of markets resolved in the past 7 days, plus detail on the 5
# highest-volume ones. The full window is hundreds of markets (mostly
# sports -- legitimately, there is no vertical filter); "how many + top 5"
# keeps the reference bounded and scoreable. The RAG is judged on getting
# the count roughly right and describing those 5 correctly.
# --------------------------------------------------------------------------
Q2_TOP_N = 5


def gt_q2_resolved_7d(cycle_started_at):
    T = _parse_T(cycle_started_at)
    lo = _iso(T - timedelta(days=7))
    hi = _iso(T)
    total = _q(f"""
        SELECT COUNT(*) AS n
        FROM {SQL_MARKETS}
        WHERE status = 'resolved'
          AND resolution_date >= ? AND resolution_date <= ?
    """, [lo, hi])[0]["n"]
    top = _q(f"""
        WITH resolved AS (
            SELECT market_id, question, resolution_date, final_outcome
            FROM {SQL_MARKETS}
            WHERE status = 'resolved'
              AND resolution_date >= ? AND resolution_date <= ?
        ),
        vol AS (
            SELECT market_id, MAX(volume) AS v
            FROM {SQL_ODDS}
            WHERE source = 'cycle' AND volume IS NOT NULL
            GROUP BY market_id
        )
        SELECT r.market_id, r.question, r.resolution_date, r.final_outcome,
               COALESCE(v.v, 0) AS volume
        FROM resolved r
        LEFT JOIN vol v USING (market_id)
        ORDER BY volume DESC
        LIMIT {Q2_TOP_N}
    """, [lo, hi])
    out, lines = [], [
        f"{total} markets resolved between {lo} and {hi}.",
        f"The {len(top)} highest-volume among them:",
    ]
    for r in top:
        raw = r.get("final_outcome")
        try:
            oc = json.loads(raw) if raw else None
            winner = "YES" if oc and str(oc[0]) in ("1", "1.0") else "NO" if oc else "?"
        except (ValueError, TypeError, json.JSONDecodeError):
            winner = "?"
        out.append({"market_id": str(r["market_id"]), "question": r["question"],
                    "resolution_date": r["resolution_date"], "final_outcome": raw,
                    "winner": winner, "volume": r["volume"]})
        lines.append(f"- [{r['market_id']}] {r['question']} -> {winner} "
                     f"(volume {r['volume']:,.0f})")
    if not top:
        lines.append("- (no markets resolved in this window)")
    return {
        "market_ids": [o["market_id"] for o in out],
        "text": "\n".join(lines),
        "detail": {"window": [lo, hi], "total_resolved": total,
                   "top_n": Q2_TOP_N, "top": out},
    }


# --------------------------------------------------------------------------
# q3 -- the 2028 Democratic presidential nomination FIELD over the last 15
# days. HARDCODED to the ~51 "Will <candidate> win the 2028 Democratic
# presidential nomination?" markets (all one Polymarket Event). GT is the
# leaderboard at T plus each candidate's 15-day delta -- a stable market
# set every cycle, only the prices move. Tests retrieval over a large
# family of related long-horizon markets, which no other question covers.
# --------------------------------------------------------------------------
Q3_LIKE = "%2028 democratic presidential nomination%"
Q3_WINDOW_DAYS = 15
Q3_LEADERBOARD_N = 8   # how many of the top candidates the reference names


def _q3_candidate(name_row):
    # "Will Alexandria Ocasio-Cortez win the 2028 Democratic presidential
    #  nomination?" -> "Alexandria Ocasio-Cortez"
    q = name_row["question"]
    m = q.replace("Will ", "").split(" win the 2028")[0].strip()
    return m or q


def gt_q3_dem2028_15d(cycle_started_at):
    T = _parse_T(cycle_started_at)
    lo = _iso(T - timedelta(days=Q3_WINDOW_DAYS))
    hi = _iso(T)
    rows = _q(f"""
        WITH field AS (
            SELECT market_id, question FROM {SQL_MARKETS}
            WHERE lower(question) LIKE '{Q3_LIKE}'
        ),
        latest AS (
            SELECT o.market_id, o.yes_price, o.timestamp,
                   ROW_NUMBER() OVER (PARTITION BY o.market_id ORDER BY o.timestamp DESC) rn
            FROM {SQL_ODDS} o JOIN field USING (market_id)
            WHERE o.timestamp <= ? AND o.yes_price IS NOT NULL
        ),
        past AS (
            SELECT o.market_id, o.yes_price, o.timestamp,
                   ROW_NUMBER() OVER (PARTITION BY o.market_id ORDER BY o.timestamp DESC) rn
            FROM {SQL_ODDS} o JOIN field USING (market_id)
            WHERE o.timestamp <= ? AND o.yes_price IS NOT NULL
        )
        SELECT f.market_id, f.question,
               l.yes_price AS now_yes, l.timestamp AS now_ts,
               p.yes_price AS past_yes, p.timestamp AS past_ts
        FROM field f
        LEFT JOIN latest l ON f.market_id = l.market_id AND l.rn = 1
        LEFT JOIN past   p ON f.market_id = p.market_id AND p.rn = 1
    """, [hi, lo])
    field = []
    for r in rows:
        now_y = r["now_yes"]
        past_y = r["past_yes"]
        delta = round((now_y - past_y), 4) if (now_y is not None and past_y is not None) else None
        field.append({
            "market_id": str(r["market_id"]),
            "candidate": _q3_candidate(r),
            "now_yes": now_y, "past_yes": past_y, "delta_15d": delta,
        })
    ranked = sorted((f for f in field if f["now_yes"] is not None),
                    key=lambda f: f["now_yes"], reverse=True)
    movers = sorted((f for f in field if f["delta_15d"] is not None),
                    key=lambda f: f["delta_15d"])
    gainers = list(reversed(movers[-3:]))
    losers = movers[:3]

    lines = [f"2028 Democratic presidential nomination field, {len(field)} candidate "
             f"markets, {lo[:10]} to {hi[:10]}."]
    lines.append(f"Leading as of {hi[:10]}:")
    for f in ranked[:Q3_LEADERBOARD_N]:
        lines.append(f"- {f['candidate']}: {f['now_yes']:.1%}")
    lines.append("Biggest 15-day gainers:")
    for f in gainers:
        lines.append(f"- {f['candidate']}: {f['delta_15d']:+.3f} "
                     f"(now {f['now_yes']:.1%})")
    lines.append("Biggest 15-day losers:")
    for f in losers:
        lines.append(f"- {f['candidate']}: {f['delta_15d']:+.3f} "
                     f"(now {f['now_yes']:.1%})")
    return {
        "market_ids": [f["market_id"] for f in ranked],
        "text": "\n".join(lines),
        "detail": {"window": [lo, hi], "n_candidates": len(field),
                   "leaderboard": ranked[:Q3_LEADERBOARD_N],
                   "gainers": gainers, "losers": losers, "field": field},
    }


# --------------------------------------------------------------------------
# q4 -- Lake America comments around 2026-08-27 (HARDCODED fixed point)
# --------------------------------------------------------------------------
Q4_WINDOW = ("2026-08-25", "2026-08-29")   # inclusive date prefixes
Q4_QUESTION_LIKE = "%lake %"               # matched against markets.question, then filtered to ontario/america


def _q4_market_ids():
    rows = _q(f"""
        SELECT market_id, question FROM {SQL_MARKETS}
        WHERE lower(question) LIKE '%lake %'
          AND (lower(question) LIKE '%ontario%' OR lower(question) LIKE '%america%')
    """)
    return [(str(r["market_id"]), r["question"]) for r in rows]


def gt_q4_lake_america_comments(cycle_started_at):
    mkts = _q4_market_ids()
    ids = {m for m, _ in mkts}
    lo, hi = Q4_WINDOW
    # comments/YYYY-MM-DD/HH.json for every day in the window
    day = datetime.fromisoformat(lo)
    end = datetime.fromisoformat(hi)
    evidence = []
    while day <= end:
        d = day.strftime("%Y-%m-%d")
        for hh in ("00", "12"):
            key = f"comments/{d}/{hh}.json"
            try:
                obj = _s3.get_object(Bucket=S3_BUCKET, Key=key)
            except _s3.exceptions.NoSuchKey:
                continue
            except Exception:
                continue
            payload = json.loads(obj["Body"].read())
            for c in payload.get("comments", []):
                if ids.intersection(str(x) for x in c.get("market_ids", [])):
                    evidence.append({
                        "comment_id": c.get("comment_id"), "author": c.get("author"),
                        "text": c.get("text", ""), "created_at": c.get("created_at"),
                        "market_ids": c.get("market_ids"),
                    })
        day += timedelta(days=1)
    lines = [f"Trader comments on the Lake America / Lake Ontario markets "
             f"({', '.join(sorted(ids))}) between {lo} and {hi}:"]
    for e in evidence[:60]:
        lines.append(f"- ({e['author']}, {e['created_at']}) {e['text'][:280]}")
    if not evidence:
        lines.append("- (no comments found on these markets in the window)")
    return {
        "market_ids": sorted(ids),
        "text": "\n".join(lines),
        "detail": {"window": [lo, hi], "markets": mkts, "n_comments": len(evidence),
                   "comments": evidence},
    }


# --------------------------------------------------------------------------
# q5 -- what moved the most this cycle (digest top_volatility)
# --------------------------------------------------------------------------
def gt_q5_moved_most_this_cycle(cycle_started_at):
    T = _parse_T(cycle_started_at)
    key = f"digest/{T.strftime('%Y-%m-%d')}/{T.strftime('%H')}.json"
    try:
        obj = _s3.get_object(Bucket=S3_BUCKET, Key=key)
    except Exception as exc:
        return {"market_ids": [], "text": f"(digest {key} not readable: {exc})",
                "detail": {"digest_key": key, "error": str(exc)}}
    d = json.loads(obj["Body"].read())
    tv = d.get("top_volatility", []) or []
    lines = [f"Biggest movers this cycle ({key}):"]
    for it in tv:
        lines.append(
            f"- [{it['market_id']}] {it.get('question')}: "
            f"{it.get('prev_price')} -> {it.get('curr_price')} (delta {it.get('delta')})"
        )
    if not tv:
        lines.append("- (top_volatility was empty this cycle)")
    return {
        "market_ids": [str(it["market_id"]) for it in tv],
        "text": "\n".join(lines),
        "detail": {"digest_key": key, "top_volatility": tv},
    }


# --------------------------------------------------------------------------
# q6 -- COUNT of currently-open markets within 5 points of 50/50, plus
# detail on the 5 highest-volume among them. Same "count + top 5" framing
# as q2: the raw set is large and mostly sports (no vertical filter), so
# the reference is bounded to the count and the 5 that carry the most
# volume. "Within 5 points" = abs(yes_price - 0.5) <= 0.05.
# --------------------------------------------------------------------------
Q6_BAND = 0.05
Q6_TOP_N = 5


def gt_q6_closest_to_5050(cycle_started_at):
    T = _parse_T(cycle_started_at)
    hi = _iso(T)
    rows = _q(f"""
        WITH last_snap AS (
            SELECT market_id, yes_price,
                   ROW_NUMBER() OVER (PARTITION BY market_id ORDER BY timestamp DESC) AS rn
            FROM {SQL_ODDS}
            WHERE timestamp <= ? AND yes_price IS NOT NULL
        ),
        vol AS (
            SELECT market_id, MAX(volume) AS v
            FROM {SQL_ODDS}
            WHERE source = 'cycle' AND volume IS NOT NULL
            GROUP BY market_id
        ),
        band AS (
            SELECT s.market_id, m.question, s.yes_price,
                   abs(s.yes_price - 0.5) AS dist,
                   COALESCE(v.v, 0) AS volume
            FROM last_snap s
            JOIN {SQL_MARKETS} m USING (market_id)
            LEFT JOIN vol v USING (market_id)
            WHERE s.rn = 1 AND m.status = 'open'
              AND abs(s.yes_price - 0.5) <= {Q6_BAND}
        )
        SELECT * FROM band ORDER BY volume DESC
    """, [hi])
    total = len(rows)
    top = rows[:Q6_TOP_N]
    lines = [
        f"{total} currently-open markets are within {Q6_BAND:.0%} points of 50/50 "
        f"as of {hi[:10]}.",
        f"The {len(top)} highest-volume among them:",
    ]
    for r in top:
        lines.append(f"- [{r['market_id']}] {r['question']}: yes {r['yes_price']:.3f} "
                     f"(volume {r['volume']:,.0f})")
    if not top:
        lines.append("- (no open markets in the band with a recent snapshot)")
    return {
        "market_ids": [str(r["market_id"]) for r in top],
        "text": "\n".join(lines),
        "detail": {"as_of": hi, "band": Q6_BAND, "total_in_band": total,
                   "top_n": Q6_TOP_N, "top": top},
    }


REGISTRY = {
    "gt_q1_bitcoin_24h": gt_q1_bitcoin_24h,
    "gt_q2_resolved_7d": gt_q2_resolved_7d,
    "gt_q3_dem2028_15d": gt_q3_dem2028_15d,
    "gt_q4_lake_america_comments": gt_q4_lake_america_comments,
    "gt_q5_moved_most_this_cycle": gt_q5_moved_most_this_cycle,
    "gt_q6_closest_to_5050": gt_q6_closest_to_5050,
}
