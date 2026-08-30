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

MULTI-TURN (added 2026-08-30, LangChain on top of the existing boto3
cascade): retrieval/query.py stays pure boto3 -- LangChain (ChatBedrock)
is scoped to the synthesis call and conversation memory ONLY, here in this
file, per the design discussed with the user. gr.ChatInterface manages
chat history natively (list of {"role","content"} dicts); that history is
used two ways per turn: (1) as plain text passed into
search_cascade(history_text=...), so a follow-up like "what about last
month" can resolve against the prior turn's topic in retrieval's existing
rewrite_query() call (no extra Bedrock call needed for that -- see its
docstring), and (2) as LangChain messages prepended to the synthesis call,
so the model actually remembers what it already said. Verified: without
history "what about last month" resolves with no topic; with history it
correctly picks up the prior turn's subject (see tech_debt.md for the
concrete before/after)."""

import json
import os
import sys

import gradio as gr
from langchain_aws import ChatBedrock
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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
    "comments": 15,
}

llm = ChatBedrock(
    model_id=SYNTHESIS_MODEL_ID,
    region_name="us-east-1",
    max_tokens=1000,
)

# Editable from the UI (see the "System prompt" accordion below) -- this is
# just the starting default, not the only prompt ever used. Lets the user
# try different phrasings live without redeploying.
DEFAULT_SYSTEM_PROMPT = """You are Poly-RAG's assistant, answering questions about
Polymarket prediction markets using retrieved context (market data, news
coverage, odds movement, cycle digests, trader comments). Answer
conversationally and directly, grounded ONLY in the provided context -- do
not invent facts not present in it. If the context doesn't contain enough
information to answer, say so plainly instead of guessing. Cite specific
markets by their question text when relevant, not just their id."""


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

    comment_rows = results.get("comments", [])
    if comment_rows:
        parts.append("\nTRADER COMMENTS:")
        for r in comment_rows:
            preview = (r.get("text") or "")[:400].replace("\n", " ")
            parts.append(f"- (entity {r.get('comment_entity_id')}, {r.get('link_type')}) {preview}")

    return "\n".join(parts) if parts else "No relevant data found in the corpus."


def synthesize_answer(question, results, system_prompt=DEFAULT_SYSTEM_PROMPT, lc_history=None):
    """lc_history: list of LangChain HumanMessage/AIMessage from prior turns
    (see _gradio_history_to_langchain below) -- prepended between the
    system prompt and this turn's question+context, so the model actually
    remembers what it already said, not just what was retrieved this turn."""
    context_text = _context_to_text(_truncate_results(results))
    messages = [SystemMessage(content=system_prompt)]
    if lc_history:
        messages.extend(lc_history)
    messages.append(HumanMessage(content=f"Question: {question}\n\nRetrieved context:\n{context_text}"))
    response = llm.invoke(messages)
    return response.content


def _gradio_history_to_text(history):
    """gr.ChatInterface's history is a list of {"role","content"} dicts --
    flattened to plain text for search_cascade's history_text (used only to
    resolve context-dependent follow-ups in rewrite_query, see its
    docstring in retrieval/query.py)."""
    if not history:
        return None
    lines = [f"{'User' if h['role'] == 'user' else 'Assistant'}: {h['content']}" for h in history]
    return "\n".join(lines)


def _gradio_history_to_langchain(history):
    """Same history, converted to LangChain message objects for the
    synthesis call's actual conversational memory."""
    if not history:
        return None
    return [
        HumanMessage(content=h["content"]) if h["role"] == "user" else AIMessage(content=h["content"])
        for h in history
    ]


def chat(message, history, system_prompt):
    """gr.ChatInterface's fn -- message: this turn's text, history: prior
    turns (list of {"role","content"} dicts, managed by Gradio itself, not
    us). Returns (answer, debug_info) since debug_output is wired as an
    additional_output below."""
    if not message or not message.strip():
        return "Ask a question about Polymarket markets, news, or odds movement.", "{}"

    history_text = _gradio_history_to_text(history)
    lc_history = _gradio_history_to_langchain(history)

    results, rewritten, market_ids = search_cascade(message, history_text=history_text)
    answer = synthesize_answer(message, results, system_prompt or DEFAULT_SYSTEM_PROMPT, lc_history=lc_history)
    debug_info = json.dumps({
        "query_rewriting": rewritten,
        "resolved_market_ids": market_ids,
        "result_counts": {
            source: len(rows) for source, rows in results.items()
        },
    }, indent=2)
    return answer, debug_info


with gr.Blocks(title="Poly-RAG") as demo:
    gr.Markdown("# Poly-RAG\nAsk about Polymarket prediction markets, news coverage, or odds movement. "
                "Conversational -- follow-up questions like \"what about last month?\" resolve against prior turns.")
    with gr.Accordion("System prompt (editable -- try different phrasings without redeploying)", open=False):
        system_prompt_input = gr.Textbox(
            value=DEFAULT_SYSTEM_PROMPT,
            label="System prompt",
            lines=8,
        )
        reset_prompt_button = gr.Button("Reset to default")
        reset_prompt_button.click(fn=lambda: DEFAULT_SYSTEM_PROMPT, outputs=system_prompt_input)
    with gr.Accordion("Debug: retrieval details (last turn)", open=False):
        debug_output = gr.Code(label="Query rewriting + resolved market_ids", language="json")

    gr.ChatInterface(
        fn=chat,
        additional_inputs=[system_prompt_input],
        additional_outputs=[debug_output],
    )


if __name__ == "__main__":
    demo.launch(server_name="0.0.0.0", server_port=7860)
