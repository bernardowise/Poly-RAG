"""
Poly-RAG Gradio MVP -- retrieval cascade (retrieval/query.py) + conversational
synthesis over the retrieved chunks.

Synthesis follows the same pattern already used by send_digest's
executive_summary (lambdas/send_digest/handler.py): one Bedrock Claude Sonnet
call that sees the user's question plus the retrieved context and writes a
natural-language answer. This is genuinely Day 5 scope pulled forward at the
user's explicit request (2026-08-29) -- the retrieval side (Bloque G) is not
"finished" in the sense of every tuning lever being resolved (see
tech_debt.md, "Registry semantic branch cutoff" and the top-k-when-filtered
open question), but the user wants an end-to-end conversational MVP now
rather than after every lever is calibrated.

Context sent to the LLM is capped per source (see MAX_PER_SOURCE below) --
the cascade can resolve 100+ market_ids for a broad query like "Bitcoin"
(see tech_debt.md), and passing all of that raw text to the synthesis call
would blow past a reasonable context/cost budget for a single answer.
"""

import json
import os
import sys

import boto3
from botocore.config import Config

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import gradio as gr

from retrieval.query import search_cascade

SYNTHESIS_MODEL_ID = "us.anthropic.claude-sonnet-4-5-20250929-v1:0"

# How many items per source get sent to the synthesis LLM -- a cap on
# context/cost, not a retrieval-quality decision (retrieval itself still
# returns the full filtered set, this only trims what reaches the prompt).
MAX_PER_SOURCE = {
    "registry": 15,
    "news_article": 15,
    "digest": 3,
    "odds": 10,  # markets, not snapshots -- all snapshots for each kept market are sent
}

bedrock = boto3.client(
    "bedrock-runtime",
    region_name="us-east-1",
    config=Config(retries={"max_attempts": 3}, max_pool_connections=10),
)


def _truncate_results(results):
    """Caps each source's result list/dict to MAX_PER_SOURCE before it goes
    into the synthesis prompt. odds is a dict (market_id -> snapshots), the
    other sources are lists -- handled separately."""
    truncated = {}
    for source, rows in results.items():
        cap = MAX_PER_SOURCE.get(source)
        if source == "odds":
            items = list(rows.items())[:cap] if cap else list(rows.items())
            truncated[source] = dict(items)
        else:
            truncated[source] = rows[:cap] if cap else rows
    return truncated


def _context_to_text(results):
    """Deterministic, template-based conversion of cascade results into a
    text block for the synthesis prompt -- same discipline already used by
    digest_to_text (see architecture_canon.md, Digest chunking section): no
    LLM call to build the context, only to write the final answer."""
    parts = []

    registry_rows = results.get("registry", [])
    if registry_rows:
        parts.append("MARKETS FOUND:")
        for r in registry_rows:
            question = (r.get("text") or "").split("\n")[0]
            parts.append(f"- [{r.get('market_id')}] {question} (status: {r.get('status')})")

    news_rows = results.get("news_article", [])
    if news_rows:
        parts.append("\nNEWS ARTICLES:")
        for r in news_rows:
            preview = (r.get("text") or "")[:400].replace("\n", " ")
            parts.append(f"- ({r.get('market_id')}, {r.get('pubDate', '?')}) {preview}")

    odds_rows = results.get("odds", {})
    if odds_rows:
        parts.append("\nODDS MOVEMENT:")
        for mid, snaps in odds_rows.items():
            question = next(
                (r.get("text", "").split("\n")[0] for r in registry_rows if r.get("market_id") == mid),
                mid,
            )
            snap_strs = [f"{s.get('timestamp', '?')[:16]}={s.get('outcomePrices', '?')}" for s in snaps]
            parts.append(f"- {question}: {' -> '.join(snap_strs)}")

    digest_rows = results.get("digest", [])
    if digest_rows:
        parts.append("\nCYCLE DIGESTS:")
        for r in digest_rows:
            preview = (r.get("text") or "")[:500].replace("\n", " ")
            parts.append(f"- {preview}")

    return "\n".join(parts) if parts else "No relevant data found in the corpus."


def synthesize_answer(question, results):
    context_text = _context_to_text(_truncate_results(results))

    system_prompt = """You are Poly-RAG's assistant, answering questions about
Polymarket prediction markets using retrieved context (market data, news
coverage, odds movement, cycle digests). Answer conversationally and
directly, grounded ONLY in the provided context -- do not invent facts not
present in it. If the context doesn't contain enough information to answer,
say so plainly instead of guessing. Cite specific markets by their question
text when relevant, not just their id."""

    body = json.dumps({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 1000,
        "system": system_prompt,
        "messages": [{
            "role": "user",
            "content": f"Question: {question}\n\nRetrieved context:\n{context_text}",
        }],
    })
    response = bedrock.invoke_model(modelId=SYNTHESIS_MODEL_ID, body=body)
    payload = json.loads(response["body"].read())
    return payload["content"][0]["text"]


def ask(question):
    if not question or not question.strip():
        return "Ask a question about Polymarket markets, news, or odds movement.", "{}"
    results, rewritten, market_ids = search_cascade(question)
    answer = synthesize_answer(question, results)
    debug_info = json.dumps({
        "query_rewriting": rewritten,
        "resolved_market_ids": market_ids,
        "result_counts": {
            source: len(rows) for source, rows in results.items()
        },
    }, indent=2)
    return answer, debug_info


with gr.Blocks(title="Poly-RAG") as demo:
    gr.Markdown("# Poly-RAG\nAsk about Polymarket prediction markets, news coverage, or odds movement.")
    question_input = gr.Textbox(label="Question", placeholder="e.g. how did Bitcoin markets move this week?")
    ask_button = gr.Button("Ask")
    answer_output = gr.Markdown(label="Answer")
    with gr.Accordion("Debug: retrieval details", open=False):
        debug_output = gr.Code(label="Query rewriting + resolved market_ids", language="json")

    ask_button.click(fn=ask, inputs=question_input, outputs=[answer_output, debug_output])
    question_input.submit(fn=ask, inputs=question_input, outputs=[answer_output, debug_output])


if __name__ == "__main__":
    demo.launch(server_name="0.0.0.0", server_port=7860)
