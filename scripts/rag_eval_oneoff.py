#!/usr/bin/env python3
"""Phase 5 RAG-eval -- ONE-OFF validation runner (process 1 of 2).

Runs the fixed question set (evals/questions.yaml) against a chosen cycle,
COLD (mode A1: no conversation history), producing for each question:
  - the RAG's answer (real cascade + real Bedrock synthesis)
  - the retrieved context actually sent to synthesis
  - the deterministic ground truth for that cycle (evals/ground_truth.py)
  - our own id_recall (market_ids cited vs the GT set)
  - latency

It writes an INTERMEDIATE json. Scoring is RAGAS only, run as a SEPARATE
process (evals/ragas_score.py, in the isolated ragas venv) because ragas
cannot share an environment with retrieval/query.py's langchain-aws pin.
Then --merge folds the two together.

No home-grown market_id-recall metric: a real user asks for prose about
markets, not a list of ids, and scoring for id recall would optimise the
wrong behaviour. "Did it get the right markets" lives inside RAGAS's
answer_correctness / factual_correctness against the GT text.

This is the validation harness for the question set + GT + scoring BEFORE
the logic is copied into the Phase 5 container Lambda. It does NOT write to
the production evals/results/ prefix -- output goes to a local file (or an
explicit --out).

Usage:
  # 1. generate answers + GT for the last completed cycle
  python3 scripts/rag_eval_oneoff.py --cycle 2026-09-07T12:00:00+00:00 \
      --out /tmp/.../rageval_intermediate.json

  # 2. score with ragas (separate venv)
  <ragas_venv>/bin/python evals/ragas_score.py \
      --in /tmp/.../rageval_intermediate.json \
      --out /tmp/.../rageval_scored.json

  # 3. merge + print report
  python3 scripts/rag_eval_oneoff.py --merge /tmp/.../rageval_scored.json
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import boto3
import yaml

from retrieval.query import search_cascade
from evals.ground_truth import REGISTRY as GT_REGISTRY
from evals.render import (
    truncate_results, context_to_text, SYSTEM_PROMPT, SYNTHESIS_MODEL_ID,
)

QUESTIONS_YAML = os.path.join(os.path.dirname(__file__), "..", "evals", "questions.yaml")
_bedrock = boto3.client("bedrock-runtime", region_name="us-east-1")


def load_questions():
    with open(QUESTIONS_YAML) as f:
        return yaml.safe_load(f)["questions"]


def synthesize(question, context_text):
    """Same shape as gradio_app/app.py synthesize_answer, but a direct
    boto3 Bedrock call (no langchain) -- COLD, no history, no reasoning."""
    body = json.dumps({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 1500,
        "system": SYSTEM_PROMPT,
        "messages": [{
            "role": "user",
            "content": f"Question: {question}\n\nRetrieved context:\n{context_text}",
        }],
    })
    resp = _bedrock.invoke_model(modelId=SYNTHESIS_MODEL_ID, body=body)
    return json.loads(resp["body"].read())["content"][0]["text"].strip()


def run(cycle_started_at, out_path):
    questions = load_questions()
    records = []
    for q in questions:
        qid = q["id"]
        print(f"\n=== {qid} ===", flush=True)
        print(f"  Q: {q['question']}", flush=True)

        t0 = time.perf_counter()
        results, rewritten, market_ids = search_cascade(q["question"], history_text=None)
        capped = truncate_results(results)
        context_text = context_to_text(capped)
        retrieval_s = time.perf_counter() - t0

        t1 = time.perf_counter()
        answer = synthesize(q["question"], context_text)
        synth_s = time.perf_counter() - t1

        gt = GT_REGISTRY[q["gt"]](cycle_started_at)

        print(f"  resolved market_ids: {len(market_ids)} | "
              f"retrieval {retrieval_s:.1f}s | synth {synth_s:.1f}s", flush=True)
        print(f"  answer[:200]: {answer[:200]}", flush=True)

        records.append({
            "question_id": qid,
            "question": q["question"],
            "horizon": q["horizon"],
            "rolling": q["rolling"],
            "metrics_requested": q["score"],
            "cycle_started_at": cycle_started_at,
            "rewritten_query": rewritten,
            "resolved_market_ids": market_ids,
            "retrieved_context": context_text,
            "answer": answer,
            "ground_truth": {
                # market_ids kept for the drift judge's bookkeeping only --
                # NOT a scored metric (see module docstring).
                "market_ids": gt["market_ids"],
                "text": gt["text"],
                "detail": gt["detail"],
            },
            "latency_s": {"retrieval": round(retrieval_s, 2),
                          "synthesis": round(synth_s, 2)},
        })

    doc = {
        "phase5_oneoff": True,
        "cycle_started_at": cycle_started_at,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "records": records,
    }
    with open(out_path, "w") as f:
        json.dump(doc, f, indent=2, default=str)
    print(f"\nWrote {out_path} ({len(records)} questions). "
          f"Next: score with evals/ragas_score.py in the ragas venv.", flush=True)


def merge(scored_path):
    """Read the ragas-scored file and print the combined report."""
    with open(scored_path) as f:
        doc = json.load(f)
    print(f"\n{'='*78}\nPhase 5 one-off -- cycle {doc['cycle_started_at']}\n{'='*78}")
    for r in doc["records"]:
        print(f"\n{r['question_id']}  ({r['horizon']})")
        for m, v in (r.get("ragas_scores") or {}).items():
            print(f"  {m:<20}: {v}")
        if r.get("ragas_errors"):
            print(f"  ragas_errors        : {r['ragas_errors']}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cycle", help="cycle_started_at ISO 8601, e.g. 2026-09-07T12:00:00+00:00")
    ap.add_argument("--out", help="intermediate json path")
    ap.add_argument("--merge", help="path to the ragas-scored json; print the merged report and exit")
    args = ap.parse_args()

    if args.merge:
        merge(args.merge)
        return
    if not args.cycle or not args.out:
        ap.error("--cycle and --out are required unless --merge is given")
    run(args.cycle, args.out)


if __name__ == "__main__":
    main()
