#!/usr/bin/env python3
"""Phase 5 RAG-eval -- RAGAS scoring (process 2 of 2).

Run this with the ISOLATED ragas venv, NOT the repo's normal python:
    <ragas_venv>/bin/python evals/ragas_score.py --in <intermediate> --out <scored>

Why a separate process/venv: ragas 0.4.3 imports a dead langchain path
(`langchain_community.chat_models.vertexai.ChatVertexAI`) at module load,
and its dependency tree conflicts with retrieval/query.py's
langchain-aws==1.7.4 pin. Here we:
  1. stub the dead vertexai module before importing ragas
  2. use the LEGACY ragas.metrics API (the new ragas.metrics.collections
     rejects LangchainLLMWrapper -- it only takes InstructorLLM / OpenAI)
  3. drive it with ChatBedrockConverse + BedrockEmbeddings

Metrics, per the `metrics_requested` list on each record:
  faithfulness         -> ragas Faithfulness           (reference-free)
  answer_correctness   -> ragas AnswerCorrectness      (needs GT text)
  factual_correctness  -> ragas FactualCorrectness     (needs GT text)
  context_precision    -> ragas LLMContextPrecisionWithReference (needs GT)
  context_recall       -> ragas LLMContextRecall       (needs GT text)
  id_recall            -> already computed by process 1, left untouched

Each metric is best-effort per record: a failure records null + the error
string, never aborts the run.
"""

import argparse
import json
import sys
import types

# --- 1. stub the dead import path ragas.llms.base does at module load ---
_stub = types.ModuleType("langchain_community.chat_models.vertexai")
class ChatVertexAI:  # noqa: E701  -- never instantiated, evaluator is Bedrock
    pass
_stub.ChatVertexAI = ChatVertexAI
sys.modules["langchain_community.chat_models.vertexai"] = _stub

from langchain_aws import ChatBedrockConverse, BedrockEmbeddings  # noqa: E402
from ragas.llms import LangchainLLMWrapper                        # noqa: E402
from ragas.embeddings import LangchainEmbeddingsWrapper           # noqa: E402
from ragas import evaluate                                        # noqa: E402
from ragas.run_config import RunConfig                            # noqa: E402
from ragas.dataset_schema import EvaluationDataset, SingleTurnSample  # noqa: E402
from ragas.metrics import (                                       # noqa: E402
    Faithfulness, AnswerCorrectness, FactualCorrectness,
    LLMContextPrecisionWithReference, LLMContextRecall,
)

# ChatBedrockConverse without batch is slow; ragas' default per-call
# timeout (180s) fires on the heavier metrics (context_precision reasons
# over every context line). Give it real headroom and few retries -- a
# metric that still times out is recorded as null, not fatal.
RUN_CONFIG = RunConfig(timeout=600, max_retries=2, max_wait=90, max_workers=4)

JUDGE_MODEL_ID = "us.anthropic.claude-sonnet-4-5-20250929-v1:0"
JUDGE_EMBED_MODEL_ID = "amazon.titan-embed-text-v2:0"

_METRIC_BUILDERS = {
    "faithfulness": Faithfulness,
    "answer_correctness": AnswerCorrectness,
    "factual_correctness": FactualCorrectness,
    "context_precision": LLMContextPrecisionWithReference,
    "context_recall": LLMContextRecall,
}


def _split_context(context_text):
    """RAGAS wants a list of context chunks. The eval renders one text
    block; split it into non-empty lines -- coarse but consistent, and the
    context_* metrics operate line-wise anyway."""
    return [ln for ln in context_text.split("\n") if ln.strip()]


def _write(doc, out_path):
    with open(out_path, "w") as f:
        json.dump(doc, f, indent=2, default=str)


def _score_one(name, ds, llm, emb):
    """Run a single ragas metric, return (value_or_None, error_or_None).
    Anything -- TimeoutError, asyncio errors, ragas internals -- is caught;
    a metric that fails is null, never fatal."""
    try:
        metric = _METRIC_BUILDERS[name]()
        res = evaluate(dataset=ds, metrics=[metric], llm=llm, embeddings=emb,
                       run_config=RUN_CONFIG, show_progress=False)
        val = res.to_pandas().to_dict(orient="records")[0]
        key = next((k for k in val
                    if k not in ("user_input", "response",
                                 "retrieved_contexts", "reference")), name)
        v = val[key]
        return (round(float(v), 4) if v == v else None), None  # v == v is False for NaN
    except BaseException as exc:  # noqa: BLE001 -- includes TimeoutError / asyncio
        return None, f"{type(exc).__name__}: {exc}"


def score(in_path, out_path):
    with open(in_path) as f:
        doc = json.load(f)

    llm = LangchainLLMWrapper(ChatBedrockConverse(
        model=JUDGE_MODEL_ID, region_name="us-east-1", temperature=0))
    emb = LangchainEmbeddingsWrapper(BedrockEmbeddings(
        model_id=JUDGE_EMBED_MODEL_ID, region_name="us-east-1"))

    for rec in doc["records"]:
        requested = [m for m in rec["metrics_requested"] if m in _METRIC_BUILDERS]
        sample = SingleTurnSample(
            user_input=rec["question"],
            response=rec["answer"],
            retrieved_contexts=_split_context(rec["retrieved_context"]),
            reference=rec["ground_truth"]["text"],
        )
        ds = EvaluationDataset(samples=[sample])
        scores, errors = {}, {}
        for name in requested:
            v, err = _score_one(name, ds, llm, emb)
            scores[name] = v
            if err:
                errors[name] = err
            print(f"  {rec['question_id']} {name}: {v}"
                  + (f"  [{err}]" if err else ""), flush=True)
            # incremental write after every metric -- a crash keeps prior work
            rec["ragas_scores"] = scores
            if errors:
                rec["ragas_errors"] = errors
            _write(doc, out_path)

    doc["ragas_scored_at"] = __import__("datetime").datetime.now(
        __import__("datetime").timezone.utc).isoformat()
    _write(doc, out_path)
    print(f"\nWrote {out_path}", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="in_path", required=True)
    ap.add_argument("--out", dest="out_path", required=True)
    args = ap.parse_args()
    score(args.in_path, args.out_path)


if __name__ == "__main__":
    main()
