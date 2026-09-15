"""Phase 5 RAG-eval -- the scorer.

Home-grown LLM judges, same pattern as gradio_app/app.py's in-line judges
(one Bedrock invoke_model call per metric, structured JSON out). NOT ragas.

Why not ragas: it works with Bedrock (verified), but Bedrock has no batch
API, so every ragas metric -- which internally makes several sequential
LLM calls, and context_precision one PER context chunk -- costs ~70s per
(question x metric). For a 6-question set on a 12h cadence that projects to
15-90 min of Lambda per cycle. The project is IAM-only (no separate
Anthropic/OpenAI key), so a batched provider isn't an option either. These
judges give the same drift signal in ~3-4 calls per question (~30-60s for
the whole set), which is what Phase 5 actually needs: internal
self-consistency over time, not matching a public benchmark.

Metrics (0..1, higher better):
  faithfulness        claims in the answer supported by the retrieved context
  answer_correctness  F1 of the answer's claims vs the ground-truth text's
                      claims (precision = answer claims that are in the GT,
                      recall = GT claims the answer covers)
  context_recall      GT claims that the retrieved context supports -- did
                      retrieval bring what's needed, independent of what the
                      answer did with it

q4 has no canonical answer, so it gets only faithfulness (+ context_recall
against its comment-evidence text). Which metrics apply is read from
questions.yaml's `score` list per question.

Every metric is best-effort: a parse/call failure records null + an error
string, never aborts the run.
"""

import json
import os

import boto3

JUDGE_MODEL_ID = os.environ.get(
    "JUDGE_MODEL_ID", "us.anthropic.claude-sonnet-4-5-20250929-v1:0")
_bedrock = boto3.client("bedrock-runtime", region_name="us-east-1")

# our metric name -> the judge that computes it
SUPPORTED = {"faithfulness", "answer_correctness", "context_recall"}


def _extract_json(text):
    """First {...} span, tolerating code fences and prose. Raises ValueError
    with the raw text so a caller's error is actionable."""
    t = text.strip()
    if t.startswith("```"):
        inner = t[3:]
        if inner.lstrip().lower().startswith("json"):
            inner = inner.lstrip()[4:]
        t = inner.split("```", 1)[0].strip() if "```" in inner else inner.strip()
    try:
        return json.loads(t)
    except json.JSONDecodeError:
        a, b = t.find("{"), t.rfind("}")
        if 0 <= a < b:
            try:
                return json.loads(t[a:b + 1])
            except json.JSONDecodeError:
                pass
    raise ValueError(f"judge did not return JSON; raw: {text[:200]!r}")


def _call(prompt, max_tokens=1200):
    body = json.dumps({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": max_tokens,
        "temperature": 0,
        "messages": [{"role": "user", "content": prompt}],
    })
    resp = _bedrock.invoke_model(modelId=JUDGE_MODEL_ID, body=body)
    return _extract_json(json.loads(resp["body"].read())["content"][0]["text"])


# --------------------------------------------------------------------------
def judge_faithfulness(question, context_text, answer):
    prompt = (
        "Grade whether an answer is faithful to its retrieved context.\n\n"
        f"CONTEXT:\n{context_text[:16000]}\n\n"
        f"QUESTION: {question}\n\nANSWER:\n{answer[:6000]}\n\n"
        "Break the ANSWER into atomic factual claims. For each, decide if it "
        "is directly supported by the CONTEXT (not outside knowledge). "
        'Respond ONLY JSON: {"claims": [{"claim": "...", "supported": true/false}], '
        '"n_claims": int, "n_supported": int}. If the answer makes no '
        'checkable factual claims: {"claims": [], "n_claims": 0, "n_supported": 0}.'
    )
    r = _call(prompt)
    n = int(r.get("n_claims", 0))
    score = 1.0 if n == 0 else round(int(r.get("n_supported", 0)) / n, 4)
    return score, r


def judge_answer_correctness(question, ground_truth_text, answer):
    prompt = (
        "Compare an ANSWER against the REFERENCE (the known-correct answer) "
        "for the same question.\n\n"
        f"QUESTION: {question}\n\n"
        f"REFERENCE:\n{ground_truth_text[:8000]}\n\n"
        f"ANSWER:\n{answer[:8000]}\n\n"
        "Decompose both into atomic factual claims. Then classify:\n"
        "- TP: an ANSWER claim that agrees with the REFERENCE\n"
        "- FP: an ANSWER claim that contradicts or is absent from the REFERENCE\n"
        "- FN: a REFERENCE claim the ANSWER fails to state\n"
        'Respond ONLY JSON: {"tp": int, "fp": int, "fn": int, '
        '"note": "one sentence"}.'
    )
    r = _call(prompt)
    tp, fp, fn = int(r.get("tp", 0)), int(r.get("fp", 0)), int(r.get("fn", 0))
    prec = tp / (tp + fp) if (tp + fp) else 0.0
    rec = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = 2 * prec * rec / (prec + rec) if (prec + rec) else 0.0
    return round(f1, 4), {**r, "precision": round(prec, 4), "recall": round(rec, 4)}


def judge_context_recall(question, ground_truth_text, context_text):
    prompt = (
        "Grade whether retrieved context contains what is needed to produce "
        "the reference answer.\n\n"
        f"QUESTION: {question}\n\n"
        f"REFERENCE ANSWER:\n{ground_truth_text[:8000]}\n\n"
        f"RETRIEVED CONTEXT:\n{context_text[:16000]}\n\n"
        "Decompose the REFERENCE ANSWER into atomic claims. For each, decide "
        "if the RETRIEVED CONTEXT supports it (the info is present, even if "
        "phrased differently). Respond ONLY JSON: "
        '{"claims": [{"claim": "...", "supported": true/false}], '
        '"n_claims": int, "n_supported": int}.'
    )
    r = _call(prompt)
    n = int(r.get("n_claims", 0))
    score = None if n == 0 else round(int(r.get("n_supported", 0)) / n, 4)
    return score, r


# --------------------------------------------------------------------------
def score_record(rec):
    """rec is one record from scripts/rag_eval_oneoff.py's intermediate
    JSON. Returns (scores dict, details dict, errors dict)."""
    q = rec["question"]
    ans = rec["answer"]
    ctx = rec["retrieved_context"]
    gt = rec["ground_truth"]["text"]
    wanted = [m for m in rec.get("metrics_requested", []) if m in SUPPORTED]

    jobs = {
        "faithfulness": lambda: judge_faithfulness(q, ctx, ans),
        "answer_correctness": lambda: judge_answer_correctness(q, gt, ans),
        "context_recall": lambda: judge_context_recall(q, gt, ctx),
    }
    scores, details, errors = {}, {}, {}
    for name in wanted:
        try:
            s, d = jobs[name]()
            scores[name], details[name] = s, d
        except Exception as exc:  # noqa: BLE001
            scores[name] = None
            errors[name] = f"{type(exc).__name__}: {exc}"
    return scores, details, errors


def score_file(in_path, out_path):
    with open(in_path) as f:
        doc = json.load(f)
    for rec in doc["records"]:
        scores, details, errors = score_record(rec)
        rec["judge_scores"] = scores
        rec["judge_details"] = details
        if errors:
            rec["judge_errors"] = errors
        print(f"  {rec['question_id']}: {scores}"
              + (f"  ERR {list(errors)}" if errors else ""), flush=True)
        with open(out_path, "w") as f:
            json.dump(doc, f, indent=2, default=str)
    from datetime import datetime, timezone
    doc["judged_at"] = datetime.now(timezone.utc).isoformat()
    with open(out_path, "w") as f:
        json.dump(doc, f, indent=2, default=str)
    print(f"\nWrote {out_path}", flush=True)


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_path", required=True)
    ap.add_argument("--out", dest="out_path", required=True)
    a = ap.parse_args()
    score_file(a.in_path, a.out_path)
