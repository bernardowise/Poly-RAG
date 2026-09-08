# Phase 5 -- RAG evaluation

Runs a fixed question set through the live retrieval + synthesis path every
cycle, scores each answer with RAGAS against a deterministic ground truth,
and (separately) watches for drift over time. Chained after Phase 4
(`build_sql_parquet`), so it never runs standalone in production.

Nothing here writes to git except the code + config in this directory. The
per-cycle results and drift reports live on S3:

    s3://poly-rag-369970405415/evals/results/YYYY-MM-DD/HH.json
    s3://poly-rag-369970405415/evals/drift_reports/YYYY-MM-DD/HH.json

(the manual Space sessions logged by the Gradio app live alongside, under
`evals/live_sessions/` -- a different, human-driven dataset, not this.)

## Files

| file | what it is |
|---|---|
| `questions.yaml` | the fixed set: 6 questions, the GT function each maps to, and which RAGAS metrics apply. Run COLD (mode A1 -- no conversation history). |
| `ground_truth.py` | 6 pure `gt_qN(cycle_started_at)` functions. Deterministic, computed straight from S3 / DynamoDB / the Phase 4 Parquet -- never from the RAG's output. Read-only, safe against production any time. |
| `render.py` | a byte-for-byte copy of `gradio_app/app.py`'s `_truncate_results` + `_context_to_text` + `SYSTEM_PROMPT`, so the eval scores the SAME prompt the Space sends. **If those change in app.py, change them here too.** |
| `ragas_score.py` | RAGAS scoring. Run with the ISOLATED ragas venv, not the repo python (see below). |
| `longitudinal.py` | the drift judge -- rolling-window consistency, score drift, structure degradation, latency drift. Runs from cycle 1; with one data point it only records a baseline. |
| `../scripts/rag_eval_oneoff.py` | the one-off validation runner (process 1): runs the set against a chosen past cycle, writes an intermediate JSON for `ragas_score.py` to score. Does NOT write to the production `results/` prefix. |

## The questions

| id | horizon | what it stresses | rolling |
|---|---|---|---|
| q1_bitcoin_24h | last 24h | fresh retrieval, short window | yes |
| q2_resolved_7d | past 7d | count + top-5-by-volume of resolutions | yes |
| q3_dem2028_15d | last 15d | a large field of related long-horizon markets (~51 "2028 Democratic nomination" markets, one Event) | yes |
| q4_lake_america_comments | fixed point (2026-08-27) | deep comment retrieval, not recent | no |
| q5_moved_most_this_cycle | this cycle | digest / top_volatility | no |
| q6_closest_to_5050 | current state | count + top-5-by-volume in the 50/50 band | no |

No topic/vertical filter anywhere -- the project doesn't have one, so the
GT reflects the corpus as is (sports included). Questions that would
otherwise have hundreds of correct answers are framed as "how many + tell
me about the top N by volume".

## Metrics

RAGAS only (`faithfulness`, `answer_correctness`, `factual_correctness`,
`context_precision`, `context_recall`). No home-grown market_id-recall check:
a real user asks for prose about markets, not an id list -- optimising for
id recall optimises the wrong behaviour. "Did it get the right markets" is
already inside `answer_correctness` / `factual_correctness` vs the GT text.

q4 keeps only `faithfulness` + the two `context_*` metrics (no single
canonical answer to score correctness against -- its GT text is the real
comment evidence).

## Why RAGAS needs an isolated venv

`ragas` (all versions) imports a dead path at module load
(`langchain_community.chat_models.vertexai.ChatVertexAI`, removed from
modern langchain-community) AND its dependency tree conflicts with
`retrieval/query.py`'s `langchain-aws==1.7.4` pin. So:

1. build a venv with just `ragas` + `langchain-aws` (its own free version)
2. `ragas_score.py` stubs the dead `vertexai` module before importing ragas
3. it uses the LEGACY `ragas.metrics` API -- the new `ragas.metrics.collections`
   only accepts `InstructorLLM` (OpenAI/Anthropic direct), not a
   `LangchainLLMWrapper` over Bedrock
4. evaluator = `ChatBedrockConverse` (Claude Sonnet 4.5) + `BedrockEmbeddings`
   (Titan v2)

This is slow -- `ChatBedrockConverse` has no batch, and the context_*
metrics reason line-by-line. Budget ~2-4 min per metric per question. The
Phase 5 container Lambda carries this cost; it runs off the user path, but
it is a real Lambda-minutes + token cost to watch (see the Latency
Management entry in tech_debt.md).

## Running the one-off (validation)

```bash
# 1. generate answers + GT for a completed cycle
python3 scripts/rag_eval_oneoff.py \
    --cycle 2026-09-07T12:00:00+00:00 \
    --out /tmp/rageval_intermediate.json

# 2. score with ragas, isolated venv
<ragas_venv>/bin/python evals/ragas_score.py \
    --in /tmp/rageval_intermediate.json \
    --out /tmp/rageval_scored.json

# 3. merged report
python3 scripts/rag_eval_oneoff.py --merge /tmp/rageval_scored.json

# drift report over whatever is in evals/results/ (needs S3 write for --persist)
python3 evals/longitudinal.py 2026-09-07T12:00:00+00:00
```

## Production wiring (not built yet)

A container-image Lambda `rag_eval`, chained after `build_sql_parquet`
(the hooks are already in place: `RAG_EVAL_LAMBDA_NAME` env var +
`lambda:InvokeFunction` note in the Phase 4 handler / terraform). Its own
`requirements-ragas.txt` WITHOUT `langchain-aws==1.7.4`. It writes
`evals/results/` and then runs `longitudinal.py` to write
`evals/drift_reports/`, and sends the cycle's 5th checkpoint email.

The one-off here is the validation harness for the question set + GT +
scoring BEFORE that logic is copied into the Lambda.
