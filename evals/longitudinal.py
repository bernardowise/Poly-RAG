"""Longitudinal / drift judge for the Phase 5 RAG-eval.

The per-cycle RAGAS scores answer "did the pipeline respond well THIS
cycle". This judge answers a different question that no single cycle can
see: "is the pipeline's behaviour drifting over TIME".

It runs every cycle from cycle 1 (chained after the per-cycle eval), even
though with one data point it can only record a baseline. As results/
accumulate it starts comparing.

What it checks, per question id, across the accumulated results:

  1. ROLLING-WINDOW CONSISTENCY (rolling: true questions -- q1, q2, q3)
     The GT window slides with T. Between two consecutive cycles the
     answer's market set SHOULD change only by (what left the window) and
     (what entered it). This judge recomputes the expected window delta
     from the stored GT market_ids and flags:
       - stale: a market the answer still talks about that has dropped out
         of the current window   -> "last week" stopped anchoring to its week
       - dropped: a market still inside the window that the answer stopped
         mentioning               -> recall lost in the stable part of the window

  2. SCORE DRIFT (all questions)
     A sustained downward trend in any RAGAS metric for a question, or a
     sudden step change, over the last N cycles.

  3. STRUCTURE DEGRADATION (all questions, LLM-judged)
     The answer for the same question going from specific (named markets,
     numeric deltas) to vague ("some markets moved"). One Bedrock call per
     question comparing the earliest vs latest stored answer.

  4. LATENCY DRIFT (all questions)
     Sustained upward trend in retrieval + synthesis latency.

Output: one drift report per run, written to
  s3://<bucket>/evals/drift_reports/YYYY-MM-DD/HH.json
Never raises -- a check that can't run records a note and is skipped.
"""

import json
import os
import statistics
from datetime import datetime, timezone

import boto3

S3_BUCKET = os.environ.get("S3_BUCKET", "poly-rag-369970405415")
RESULTS_PREFIX = "evals/results"
DRIFT_PREFIX = "evals/drift_reports"

# how many recent cycles a trend check looks back over
TREND_WINDOW = 6
# a metric drop of this much between the window's mean and its last value
# is flagged as a step change
STEP_DROP = 0.15
# a linear-fit slope past this (per cycle) over TREND_WINDOW is a trend
TREND_SLOPE = 0.03

_s3 = boto3.client("s3", region_name="us-east-1")
_bedrock = boto3.client("bedrock-runtime", region_name="us-east-1")
JUDGE_MODEL_ID = "us.anthropic.claude-sonnet-4-5-20250929-v1:0"


# --------------------------------------------------------------------------
# loading the accumulated per-cycle results
# --------------------------------------------------------------------------
def load_results(limit_cycles=None):
    """Every evals/results/*.json object, oldest first. Each is one cycle's
    scored eval (the same shape scripts/rag_eval_oneoff.py + ragas_score.py
    produce, plus a cycle_started_at)."""
    keys = []
    paginator = _s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=RESULTS_PREFIX + "/"):
        for o in page.get("Contents", []):
            if o["Key"].endswith(".json"):
                keys.append((o["LastModified"], o["Key"]))
    keys.sort()
    if limit_cycles:
        keys = keys[-limit_cycles:]
    out = []
    for _, key in keys:
        try:
            doc = json.loads(_s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
            out.append(doc)
        except Exception as exc:  # noqa: BLE001
            out.append({"_load_error": f"{key}: {exc}"})
    return out


def _by_question(cycles):
    """Reshape [cycle -> records] into {question_id -> [per-cycle record]}
    ordered oldest to newest."""
    series = {}
    for c in cycles:
        if "_load_error" in c:
            continue
        cyc = c.get("cycle_started_at")
        for rec in c.get("records", []):
            qid = rec["question_id"]
            series.setdefault(qid, []).append({
                "cycle_started_at": cyc,
                "answer": rec.get("answer", ""),
                "gt_market_ids": set(str(x) for x in
                                     rec.get("ground_truth", {}).get("market_ids", [])),
                "answer_market_ids": _ids_in_answer(rec.get("answer", ""),
                                                    rec.get("ground_truth", {})),
                "ragas_scores": rec.get("ragas_scores", {}) or {},
                "latency_s": rec.get("latency_s", {}) or {},
                "rolling": rec.get("rolling", False),
            })
    return series


def _ids_in_answer(answer, ground_truth):
    """Which of the GT's (and this-cycle-resolved) market_ids the answer
    mentions -- substring match. Used only for the rolling-window delta
    check, not scored."""
    universe = set(str(x) for x in ground_truth.get("market_ids", []))
    # also consider ids named in the GT detail so a market that left the
    # window is still recognised if the answer clings to it
    detail = ground_truth.get("detail", {})
    for v in detail.values():
        if isinstance(v, list):
            for item in v:
                if isinstance(item, dict) and "market_id" in item:
                    universe.add(str(item["market_id"]))
    return {mid for mid in universe if mid and mid in answer}


# --------------------------------------------------------------------------
# 1. rolling-window consistency
# --------------------------------------------------------------------------
def check_rolling_window(qid, records):
    """For a rolling question, between each consecutive pair of cycles:
      expected_in  = gt_ids[t]   - gt_ids[t-1]   (entered the window)
      expected_out = gt_ids[t-1] - gt_ids[t]     (left the window)
    Then:
      stale   = ids the answer at t still mentions that are in expected_out
      dropped = ids in (gt_ids[t] & gt_ids[t-1]) -- stable in the window --
                that answer[t-1] mentioned but answer[t] does not
    """
    if len(records) < 2:
        return {"status": "baseline", "note": "need >=2 cycles for a window delta"}
    findings = []
    for prev, cur in zip(records, records[1:]):
        gt_prev, gt_cur = prev["gt_market_ids"], cur["gt_market_ids"]
        left = gt_prev - gt_cur
        stable = gt_prev & gt_cur
        ans_prev, ans_cur = prev["answer_market_ids"], cur["answer_market_ids"]

        stale = sorted(ans_cur & left)
        dropped = sorted((ans_prev & stable) - ans_cur)
        if stale or dropped:
            findings.append({
                "from_cycle": prev["cycle_started_at"],
                "to_cycle": cur["cycle_started_at"],
                "stale_ids": stale,       # answer clings to markets outside its window
                "dropped_ids": dropped,   # answer forgot markets still in-window
            })
    return {"status": "ok" if not findings else "drift", "findings": findings}


# --------------------------------------------------------------------------
# 2. score drift
# --------------------------------------------------------------------------
def _slope(ys):
    n = len(ys)
    if n < 2:
        return 0.0
    xs = list(range(n))
    mx, my = sum(xs) / n, sum(ys) / n
    denom = sum((x - mx) ** 2 for x in xs) or 1e-9
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / denom


def check_score_drift(qid, records):
    recent = records[-TREND_WINDOW:]
    if len(recent) < 3:
        return {"status": "baseline", "note": f"need >=3 cycles, have {len(recent)}"}
    out = {}
    metric_names = set()
    for r in recent:
        metric_names.update(r["ragas_scores"].keys())
    for m in sorted(metric_names):
        ys = [r["ragas_scores"].get(m) for r in recent]
        ys = [y for y in ys if isinstance(y, (int, float))]
        if len(ys) < 3:
            continue
        s = _slope(ys)
        mean = statistics.mean(ys[:-1])
        step = mean - ys[-1]
        flag = None
        if s <= -TREND_SLOPE:
            flag = f"downward trend (slope {s:+.3f}/cycle over {len(ys)} cycles)"
        elif step >= STEP_DROP:
            flag = f"step drop ({mean:.2f} avg -> {ys[-1]:.2f} last)"
        out[m] = {"values": ys, "slope": round(s, 4), "flag": flag}
    drifting = [m for m, v in out.items() if v["flag"]]
    return {"status": "drift" if drifting else "ok", "metrics": out,
            "drifting": drifting}


# --------------------------------------------------------------------------
# 3. structure degradation (LLM-judged)
# --------------------------------------------------------------------------
def check_structure(qid, records):
    if len(records) < 2:
        return {"status": "baseline", "note": "need >=2 answers to compare"}
    first, last = records[0], records[-1]
    prompt = (
        "Two answers to the SAME recurring question, from an automated RAG "
        "system, months apart. Judge ONLY whether the LATER answer is LESS "
        "specific / more vague than the EARLIER one -- fewer named markets, "
        "fewer concrete numbers, hedged where the earlier was precise. Do "
        "NOT judge factual correctness or which is 'better' overall.\n\n"
        f"QUESTION: {qid}\n\n"
        f"EARLIER ANSWER (cycle {first['cycle_started_at']}):\n{first['answer'][:2500]}\n\n"
        f"LATER ANSWER (cycle {last['cycle_started_at']}):\n{last['answer'][:2500]}\n\n"
        'Respond ONLY JSON: {"later_is_vaguer": true/false, '
        '"earlier_specificity": 0-1, "later_specificity": 0-1, "note": "..."}'
    )
    try:
        body = json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 400, "temperature": 0,
            "messages": [{"role": "user", "content": prompt}],
        })
        resp = _bedrock.invoke_model(modelId=JUDGE_MODEL_ID, body=body)
        text = json.loads(resp["body"].read())["content"][0]["text"]
        start, end = text.find("{"), text.rfind("}")
        r = json.loads(text[start:end + 1])
        return {"status": "drift" if r.get("later_is_vaguer") else "ok", **r}
    except Exception as exc:  # noqa: BLE001
        return {"status": "skipped", "error": f"{type(exc).__name__}: {exc}"}


# --------------------------------------------------------------------------
# 4. latency drift
# --------------------------------------------------------------------------
def check_latency(qid, records):
    recent = records[-TREND_WINDOW:]
    if len(recent) < 3:
        return {"status": "baseline"}
    totals = []
    for r in recent:
        l = r["latency_s"]
        t = (l.get("retrieval") or 0) + (l.get("synthesis") or 0)
        totals.append(t)
    s = _slope(totals)
    # flag if latency is climbing by >2s/cycle sustained
    flag = f"latency climbing {s:+.1f}s/cycle" if s > 2.0 else None
    return {"status": "drift" if flag else "ok",
            "totals_s": [round(t, 1) for t in totals], "slope": round(s, 2),
            "flag": flag}


# --------------------------------------------------------------------------
# entry point
# --------------------------------------------------------------------------
def run_drift_report(cycle_started_at, persist=True):
    cycles = load_results()
    series = _by_question(cycles)

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "cycle_started_at": cycle_started_at,
        "n_cycles_seen": len([c for c in cycles if "_load_error" not in c]),
        "questions": {},
    }
    for qid, records in series.items():
        rolling = records[-1]["rolling"] if records else False
        report["questions"][qid] = {
            "n_points": len(records),
            "rolling_window": check_rolling_window(qid, records) if rolling
                              else {"status": "n/a", "note": "not a rolling question"},
            "score_drift": check_score_drift(qid, records),
            "structure": check_structure(qid, records),
            "latency": check_latency(qid, records),
        }

    # a top-line: any question with a 'drift' status anywhere
    drifting = [
        qid for qid, q in report["questions"].items()
        if any(isinstance(v, dict) and v.get("status") == "drift" for v in q.values())
    ]
    report["drifting_questions"] = drifting

    if persist:
        T = datetime.fromisoformat(cycle_started_at.replace("Z", "+00:00"))
        key = f"{DRIFT_PREFIX}/{T.strftime('%Y-%m-%d')}/{T.strftime('%H')}.json"
        _s3.put_object(Bucket=S3_BUCKET, Key=key,
                       Body=json.dumps(report, indent=2, default=str).encode("utf-8"),
                       ContentType="application/json")
        report["_written_to"] = f"s3://{S3_BUCKET}/{key}"
    return report


if __name__ == "__main__":
    import sys
    cyc = sys.argv[1] if len(sys.argv) > 1 else datetime.now(timezone.utc).isoformat()
    persist = "--persist" in sys.argv
    r = run_drift_report(cyc, persist=persist)
    print(json.dumps(r, indent=2, default=str))
