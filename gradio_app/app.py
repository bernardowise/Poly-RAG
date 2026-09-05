"""
Poly-RAG Gradio evaluation UI -- retrieval cascade (retrieval/query.py) +
conversational synthesis over the retrieved chunks, instrumented for a
technical team evaluating RAG quality (token usage, latency, cost per turn,
and per-session logging to S3). This is NOT a consumer chatbot -- the model
parameters, the sliding-context controls, the metrics panel and the
session-logging toggle are all exposed on purpose so an evaluator can A/B
configuration mid-conversation without restarting.

Synthesis follows the same pattern already used by send_digest's
executive_summary (lambdas/send_digest/handler.py): one Bedrock Claude Sonnet
call that sees the user's question plus the retrieved context and writes a
natural-language answer.

MULTI-TURN + SLIDING CONTEXT WINDOW (2026-09-05, replaces the earlier
history-only memory): retrieval/query.py stays pure boto3 -- LangChain
(ChatBedrock) is scoped to the synthesis call ONLY, here in this file.
gr.ChatInterface manages the chat transcript natively; on top of that this
file keeps a per-session buffer of the FULL retrieved context from prior
turns (gr.State), merged and de-duplicated by id against the current turn's
retrieval, so a follow-up question still "sees" the news/markets/comments an
earlier turn pulled in. The buffer is a sliding window bounded by a token
budget that counts BOTH the chat transcript and the retained retrieved
context together -- when the total exceeds the budget, whole oldest items are
dropped (sliding, no backfill). Budget and whether retention is on at all are
UI controls, defaulting to 10k tokens / on.

RAGAS live evaluation is Deploy 2 -- the checkbox is present and wired, but
the scorer is a stub until `ragas` is added to requirements.txt with its
dependency tree resolved against the pinned langchain/gradio versions.
"""

import json
import os
import sys
import time
from datetime import datetime, timezone

import gradio as gr
from langchain_aws import ChatBedrock
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from retrieval.query import search_cascade
from gradio_app.live_logging import log_turn, new_session_id

SYNTHESIS_MODEL_ID = "us.anthropic.claude-sonnet-4-5-20250929-v1:0"

# Rough USD per 1k tokens for Claude Sonnet 4.5 on Bedrock (us-east-1,
# on-demand) -- same figures the project's own cost metrics use for
# estimates. Not billed values, an estimate for the live panel.
COST_PER_1K_INPUT = 0.003
COST_PER_1K_OUTPUT = 0.015

# How many items per source get sent to the synthesis LLM PER TURN -- a cap
# on how big any single turn's retrieved block can be before it enters the
# sliding buffer. The buffer's total token budget (a UI control) is the
# second, global bound.
MAX_PER_SOURCE = {
    "registry": 15,
    "news_article": 15,
    "digest": 3,
    "odds": 10,  # markets, not snapshots -- all snapshots for each kept market are sent
    "comments": 15,
}

DEFAULT_CONTEXT_BUDGET = 10_000  # tokens: chat transcript + retained retrieved context, combined
THINKING_BUDGET_TOKENS = 1024  # Bedrock's minimum for Claude's extended thinking

# Fixed system prompt -- grounding stays ON by design: the whole point of
# this being RAG (not a general chatbot) is answers coming from the corpus,
# not the model's training data. The UI toggles change HOW the model reasons
# over that context, not WHETHER it's grounded.
SYSTEM_PROMPT = """You are Poly-RAG's assistant, answering questions about
Polymarket prediction markets using retrieved context (market data, news
coverage, odds movement, cycle digests, trader comments). Answer
conversationally and directly, grounded ONLY in the provided context -- do
not invent facts not present in it. If the context doesn't contain enough
information to answer, say so plainly instead of guessing. Cite specific
markets by their question text when relevant, not just their id."""


# --------------------------------------------------------------------------
# token counting -- rough len//4 approximation, consistent with the estimates
# used elsewhere in the project (no tokenizer dependency).
# --------------------------------------------------------------------------
def _approx_tokens(text):
    return len(text) // 4 if text else 0


# --------------------------------------------------------------------------
# retrieved-context handling: truncation, id extraction for dedup, and the
# deterministic text rendering that goes into the synthesis prompt.
# --------------------------------------------------------------------------
def _truncate_results(results):
    """Caps each source's result list/dict to MAX_PER_SOURCE. odds is a dict
    (market_id -> snapshots), the other sources are lists."""
    truncated = {}
    for source, rows in results.items():
        cap = MAX_PER_SOURCE.get(source)
        if source == "odds":
            items = list(rows.items())[:cap] if cap else list(rows.items())
            truncated[source] = dict(items)
        else:
            truncated[source] = rows[:cap] if cap else rows
    return truncated


def _row_id(source, row):
    """Stable de-dup key per source. news_article: the url (also its
    article_id, see retrieval/query.py). registry/odds: market_id.
    comments: comment_entity_id (a thread shared by many markets).
    digest: cycle_started_at if present, else the text hash."""
    if source == "news_article":
        return row.get("url") or row.get("article_id") or row.get("chunk_id")
    if source == "registry":
        return row.get("market_id")
    if source == "comments":
        return row.get("comment_entity_id") or row.get("chunk_id")
    if source == "digest":
        return row.get("cycle_started_at") or row.get("chunk_id") or hash((row.get("text") or "")[:200])
    return row.get("chunk_id") or hash(json.dumps(row, sort_keys=True, default=str)[:200])


def _merge_results(retained, current):
    """Merge the current turn's retrieved results into the retained buffer,
    de-duplicating by _row_id. retained/current are both dicts of the same
    shape search_cascade returns (lists per source, plus odds as a
    market_id -> snapshots dict). Current-turn rows win on a key clash (they
    reflect the latest state of the corpus)."""
    merged = {}
    sources = set(retained) | set(current)
    for source in sources:
        if source == "odds":
            combined = dict(retained.get("odds", {}))
            combined.update(current.get("odds", {}))
            merged["odds"] = combined
            continue
        seen = {}
        for row in retained.get(source, []):
            seen[_row_id(source, row)] = row
        for row in current.get(source, []):
            seen[_row_id(source, row)] = row
        merged[source] = list(seen.values())
    return merged


def _context_to_text(results):
    """Deterministic, template-based conversion of cascade results into a
    text block for the synthesis prompt -- same discipline used by
    digest_to_text: no LLM call to build the context, only to write the
    final answer."""
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


def _count_results(results):
    """Per-source item counts for the debug/log payload. odds counts markets."""
    return {s: (len(rows) if not isinstance(rows, dict) else len(rows)) for s, rows in results.items()}


# --------------------------------------------------------------------------
# sliding window: the retained buffer is a list of per-turn entries, oldest
# first. Each entry is {"turn": int, "kind": "context"|"exchange", "text":
# str, "tokens": int, "results": dict|None}. "exchange" entries hold the
# rendered "User: .. / Assistant: .." transcript text for that turn;
# "context" entries hold that turn's retrieved-context text + its raw
# results dict (for the next turn's merge). Both kinds count against the
# same budget; when the total exceeds it, whole oldest entries are dropped.
# --------------------------------------------------------------------------
def _apply_window(buffer, budget):
    """Drop whole oldest entries until the total token count fits `budget`.
    Never drops the last two entries (this turn's exchange + context) even
    if a single turn exceeds the budget -- MAX_PER_SOURCE already bounds a
    single turn's context, and dropping the current turn would defeat the
    point. Returns (kept_buffer, evicted_turn_numbers)."""
    total = sum(e["tokens"] for e in buffer)
    evicted = []
    i = 0
    while total > budget and i < len(buffer) - 2:
        evicted.append(buffer[i]["turn"])
        total -= buffer[i]["tokens"]
        i += 1
    return buffer[i:], evicted


def _retained_results(buffer):
    """Rebuild the merged retrieved-context dict from the retained 'context'
    entries in the buffer (oldest -> newest), so the current turn's merge
    starts from everything still in the window."""
    retained = {}
    for entry in buffer:
        if entry["kind"] == "context" and entry["results"]:
            retained = _merge_results(retained, entry["results"])
    return retained


# --------------------------------------------------------------------------
# synthesis
# --------------------------------------------------------------------------
def build_llm(reasoning, temperature):
    """New ChatBedrock instance per call, not .bind() -- Bedrock's Claude
    requires `thinking` inside model_kwargs at CONSTRUCTION time, and
    temperature can only be 1 when thinking is enabled (the UI disables the
    temperature slider while reasoning is on; this is defense in depth)."""
    kwargs = {
        "model_id": SYNTHESIS_MODEL_ID,
        "region_name": "us-east-1",
        "max_tokens": 2000 if reasoning else 1000,
        "temperature": 1 if reasoning else temperature,
    }
    if reasoning:
        kwargs["model_kwargs"] = {"thinking": {"type": "enabled", "budget_tokens": THINKING_BUDGET_TOKENS}}
    return ChatBedrock(**kwargs)


def _extract_text(content):
    """response.content is a plain string normally, but a LIST of typed
    blocks when reasoning is enabled -- only the "text" block is the answer."""
    if isinstance(content, str):
        return content
    text_blocks = [b.get("text", "") for b in content if isinstance(b, dict) and b.get("type") == "text"]
    return "\n".join(text_blocks) if text_blocks else str(content)


def synthesize_answer(question, context_text, lc_history, reasoning, temperature):
    """lc_history: list of LangChain HumanMessage/AIMessage from prior turns,
    prepended between the system prompt and this turn's question+context."""
    messages = [SystemMessage(content=SYSTEM_PROMPT)]
    if lc_history:
        messages.extend(lc_history)
    messages.append(HumanMessage(content=f"Question: {question}\n\nRetrieved context:\n{context_text}"))
    llm = build_llm(reasoning, temperature)
    response = llm.invoke(messages)
    return _extract_text(response.content)


# --------------------------------------------------------------------------
# LLM-judge live evaluation (Deploy 2). Reference-free, in-line, no new
# dependencies -- three artisanal judges built the same way rewrite_query
# and send_digest's executive_summary are: a Bedrock Claude call with a
# structured prompt and JSON output, plus Cohere embeddings (already on the
# Space) for the relevancy cosine step. This is deliberately NOT the ragas
# library -- ragas cannot coexist with langchain-aws==1.7.4 in one env (its
# ChatVertexAI import breaks against modern langchain-community). The
# canonical ragas run lives in the isolated Phase 4 (evals/), reading these
# same turns back from s3://.../evals/live_sessions/. These scores are for
# tracking drift within this system over time, where self-consistency
# matters more than matching public ragas benchmarks.
#
# Metrics (all 0..1, higher is better):
#   faithfulness       -- claims in the answer supported by the context
#   answer_relevancy   -- how directly the answer addresses the question
#   context_relevance  -- fraction of retrieved context relevant to the question
# --------------------------------------------------------------------------
JUDGE_MODEL_ID = "us.anthropic.claude-sonnet-4-5-20250929-v1:0"
# Reuse the exact embed model + call shape retrieval/query.py already uses --
# it is the only Cohere model the Space's IAM user is authorized for
# (global.cohere.embed-v4:0, foundation-model + inference-profile). Using
# cohere.embed-english-v3 here got AccessDeniedException in production.
JUDGE_EMBED_MODEL_ID = "global.cohere.embed-v4:0"

_judge_bedrock = None
_judge_embed_bedrock = None


def _get_judge_clients():
    """Lazy boto3 clients for the judge -- separate from retrieval/query.py's
    so a judge failure never touches the retrieval path."""
    global _judge_bedrock, _judge_embed_bedrock
    if _judge_bedrock is None:
        import boto3
        _judge_bedrock = boto3.client("bedrock-runtime", region_name="us-east-1")
        _judge_embed_bedrock = _judge_bedrock
    return _judge_bedrock, _judge_embed_bedrock


def _extract_judge_json(text):
    """Pull a JSON object out of a judge response. Handles code fences and
    leading/trailing prose -- falls back to the first {...} span. Raises
    ValueError (not a bare JSONDecodeError) with the raw text attached so a
    caller's error message is actionable."""
    t = text.strip()
    if t.startswith("```"):
        inner = t[3:]
        if inner.lstrip().startswith("json"):
            inner = inner.lstrip()[4:]
        t = inner.split("```", 1)[0].strip() if "```" in inner else inner.strip()
    try:
        return json.loads(t)
    except json.JSONDecodeError:
        start, end = t.find("{"), t.rfind("}")
        if 0 <= start < end:
            try:
                return json.loads(t[start:end + 1])
            except json.JSONDecodeError:
                pass
    raise ValueError(f"judge did not return JSON; raw: {text[:200]!r}")


def _judge_call(prompt, max_tokens=700):
    """One Bedrock Claude call, returns parsed JSON. Raises on unparseable
    output -- the caller catches and records the metric as null with the
    raw text in the error."""
    client, _ = _get_judge_clients()
    body = json.dumps({
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": max_tokens,
        "temperature": 0,
        "messages": [{"role": "user", "content": prompt}],
    })
    resp = client.invoke_model(modelId=JUDGE_MODEL_ID, body=body)
    text = json.loads(resp["body"].read())["content"][0]["text"]
    return _extract_judge_json(text)


def _embed_one(text):
    """Same call shape as retrieval/query.py's embed_query -- Cohere Embed v4
    via the cross-region inference profile, input_type=search_query, response
    at embeddings.float[0]."""
    _, client = _get_judge_clients()
    body = json.dumps({
        "texts": [text[:2000]],
        "input_type": "search_query",
        "embedding_types": ["float"],
    })
    resp = client.invoke_model(modelId=JUDGE_EMBED_MODEL_ID, body=body)
    payload = json.loads(resp["body"].read())
    emb = payload["embeddings"]
    return emb["float"][0] if isinstance(emb, dict) else emb[0]


def _cosine(a, b):
    dot = sum(x * y for x, y in zip(a, b))
    na = sum(x * x for x in a) ** 0.5
    nb = sum(y * y for y in b) ** 0.5
    return dot / (na * nb) if na and nb else 0.0


def _judge_faithfulness(question, context_text, answer):
    """Decompose the answer into atomic claims, mark each supported/not by
    the context. Score = supported / total (1.0 if the answer makes no
    checkable factual claims)."""
    prompt = (
        "You are grading whether an answer is faithful to its retrieved context.\n\n"
        f"CONTEXT:\n{context_text[:6000]}\n\n"
        f"QUESTION: {question}\n\nANSWER:\n{answer}\n\n"
        "Break the ANSWER into atomic factual claims. For each, decide if it is "
        "directly supported by the CONTEXT (not by outside knowledge). Respond with "
        'ONLY JSON: {"claims": [{"claim": "...", "supported": true/false}], '
        '"n_claims": int, "n_supported": int}. If the answer makes no checkable '
        'factual claims, return {"claims": [], "n_claims": 0, "n_supported": 0}.'
    )
    r = _judge_call(prompt)
    n = int(r.get("n_claims", 0))
    if n == 0:
        return 1.0, r
    return round(int(r.get("n_supported", 0)) / n, 3), r


def _judge_answer_relevancy(question, answer):
    """Generate questions the answer would be a good answer to, embed each,
    cosine against the real question, average. Low when the answer is
    evasive or off-topic."""
    prompt = (
        "Given this ANSWER, write 3 distinct questions that this answer would "
        "directly and completely address. Respond with ONLY JSON: "
        '{"questions": ["...", "...", "..."]}.\n\n'
        f"ANSWER:\n{answer}"
    )
    r = _judge_call(prompt, max_tokens=400)
    gen_qs = r.get("questions", [])[:3]
    if not gen_qs:
        return None, r
    q_vec = _embed_one(question)
    sims = [_cosine(q_vec, _embed_one(gq)) for gq in gen_qs]
    return round(sum(sims) / len(sims), 3), {"generated_questions": gen_qs, "cosines": [round(s, 3) for s in sims]}


def _judge_context_relevance(question, context_text):
    """Fraction of the retrieved context (by line/item) that is relevant to
    answering the question. Low = retrieval pulled in noise."""
    prompt = (
        "You are grading whether retrieved context is relevant to a question.\n\n"
        f"QUESTION: {question}\n\nRETRIEVED CONTEXT (line-numbered):\n"
        + "\n".join(f"{i}: {ln}" for i, ln in enumerate(context_text.split("\n")[:120]) if ln.strip())
        + "\n\nCount how many non-empty lines are relevant to answering the question "
        '(directly or as useful supporting detail). Respond with ONLY JSON: '
        '{"n_relevant": int, "n_total": int}.'
    )
    r = _judge_call(prompt)
    total = int(r.get("n_total", 0))
    if total == 0:
        return None, r
    return round(int(r.get("n_relevant", 0)) / total, 3), r


def llm_judge_evaluate(question, context_text, answer):
    """Run the three judges. Each metric is independently best-effort -- a
    failure records null for that metric and an 'errors' list, never raises
    into the chat flow. Returns a dict:
      {faithfulness, answer_relevancy, context_relevance, details, errors}
    """
    out = {"faithfulness": None, "answer_relevancy": None, "context_relevance": None,
           "details": {}, "errors": []}
    for name, fn in [
        ("faithfulness", lambda: _judge_faithfulness(question, context_text, answer)),
        ("answer_relevancy", lambda: _judge_answer_relevancy(question, answer)),
        ("context_relevance", lambda: _judge_context_relevance(question, context_text)),
    ]:
        try:
            score, detail = fn()
            out[name] = score
            out["details"][name] = detail
        except Exception as exc:  # noqa: BLE001 -- judge must never break chat
            out["errors"].append(f"{name}: {type(exc).__name__}: {exc}")
    return out


# --------------------------------------------------------------------------
# history helpers
# --------------------------------------------------------------------------
def _gradio_history_to_text(history):
    if not history:
        return None
    lines = [f"{'User' if h['role'] == 'user' else 'Assistant'}: {h['content']}" for h in history]
    return "\n".join(lines)


def _gradio_history_to_langchain(history):
    if not history:
        return None
    return [
        HumanMessage(content=h["content"]) if h["role"] == "user" else AIMessage(content=h["content"])
        for h in history
    ]


# --------------------------------------------------------------------------
# main turn handler
# --------------------------------------------------------------------------
def _fmt_score(v):
    return f"{v:.2f}" if isinstance(v, (int, float)) else "n/a"


def _panel_markdown(turn_idx, latency_ms, tokens, cost_usd, window_state, judge_scores):
    """Formatted one-glance panel (b) for the current turn."""
    lat = latency_ms
    tok = tokens
    over = " OVER" if tok["window_over_budget"] else ""
    lines = [
        f"### Turn {turn_idx}",
        f"**Latency** retrieval {lat['retrieval']} ms | synthesis {lat['synthesis']} ms | "
        f"judge {lat['judge']} ms | **total {lat['total']} ms**",
        f"**Tokens** retrieved {tok['retrieved_context']} | history {tok['chat_history']} | "
        f"answer {tok['answer']} | window {tok['window_total']}/{tok['window_budget']}{over}",
        f"**Cost (est)** ${cost_usd:.5f}",
        f"**Window** live turns {window_state['live_turns']} | evicted this turn {window_state['evicted_this_turn']}",
    ]
    if judge_scores:
        lines.append(
            f"**LLM judge** faithfulness {_fmt_score(judge_scores.get('faithfulness'))} | "
            f"answer relevancy {_fmt_score(judge_scores.get('answer_relevancy'))} | "
            f"context relevance {_fmt_score(judge_scores.get('context_relevance'))}"
        )
        if judge_scores.get("errors"):
            lines.append(f"_judge errors: {'; '.join(judge_scores['errors'])}_")
    return "\n\n".join(lines)


def chat(message, history, reasoning, temperature, retain_context, context_budget, run_judge, log_session, state):
    """gr.ChatInterface fn. `state` is our gr.State dict:
    {"session_id": str|None, "buffer": list, "turn": int, "rows": list}.
    Returns (answer, panel_md, table_rows, debug_json, state) -- the last
    four are additional outputs wired below."""
    state = state or {"session_id": None, "buffer": [], "turn": 0, "rows": []}
    state.setdefault("rows", [])
    if not message or not message.strip():
        return ("Ask a question about Polymarket markets, news, or odds movement.",
                "", state["rows"], "{}", state)

    state["turn"] += 1
    turn_idx = state["turn"]
    budget = int(context_budget)

    history_text = _gradio_history_to_text(history)
    lc_history = _gradio_history_to_langchain(history)

    # --- retrieval (always fresh) ---
    t0 = time.perf_counter()
    results, rewritten, market_ids = search_cascade(message, history_text=history_text)
    retrieval_ms = int((time.perf_counter() - t0) * 1000)

    current_capped = _truncate_results(results)

    # --- merge with retained context (if retention is on) ---
    if retain_context:
        retained = _retained_results(state["buffer"])
        merged = _merge_results(retained, current_capped)
    else:
        state["buffer"] = []
        merged = current_capped

    context_text = _context_to_text(merged)
    context_tokens = _approx_tokens(context_text)

    # --- synthesis ---
    t1 = time.perf_counter()
    answer = synthesize_answer(message, context_text, lc_history, reasoning, temperature)
    synthesis_ms = int((time.perf_counter() - t1) * 1000)

    # --- LLM judge (opt-in) ---
    judge_ms = 0
    judge_scores = None
    if run_judge:
        t2 = time.perf_counter()
        judge_scores = llm_judge_evaluate(message, context_text, answer)
        judge_ms = int((time.perf_counter() - t2) * 1000)

    # --- update the sliding buffer ---
    exchange_text = f"User: {message}\nAssistant: {answer}"
    history_tokens = _approx_tokens(_gradio_history_to_text(history) or "") + _approx_tokens(exchange_text)
    if retain_context:
        state["buffer"].append({
            "turn": turn_idx, "kind": "exchange",
            "text": exchange_text, "tokens": _approx_tokens(exchange_text), "results": None,
        })
        this_ctx_text = _context_to_text(current_capped)
        state["buffer"].append({
            "turn": turn_idx, "kind": "context",
            "text": this_ctx_text, "tokens": _approx_tokens(this_ctx_text), "results": current_capped,
        })
        state["buffer"], evicted = _apply_window(state["buffer"], budget)
    else:
        evicted = []

    live_turns = sorted({e["turn"] for e in state["buffer"]}) if retain_context else [turn_idx]
    window_total_tokens = sum(e["tokens"] for e in state["buffer"]) if retain_context else context_tokens + history_tokens

    # --- token / cost accounting ---
    thinking_tokens = THINKING_BUDGET_TOKENS if reasoning else 0
    answer_tokens = _approx_tokens(answer)
    input_tokens_est = context_tokens + history_tokens + _approx_tokens(SYSTEM_PROMPT)
    output_tokens_est = answer_tokens + thinking_tokens
    cost_usd = round(
        input_tokens_est / 1000 * COST_PER_1K_INPUT + output_tokens_est / 1000 * COST_PER_1K_OUTPUT, 6
    )

    tokens = {
        "retrieved_context": context_tokens,
        "chat_history": history_tokens,
        "system_prompt": _approx_tokens(SYSTEM_PROMPT),
        "answer": answer_tokens,
        "thinking_budget": thinking_tokens,
        "input_total_est": input_tokens_est,
        "output_total_est": output_tokens_est,
        "window_total": window_total_tokens,
        "window_budget": budget,
        "window_over_budget": window_total_tokens > budget,
    }
    latency_ms = {
        "retrieval": retrieval_ms,
        "synthesis": synthesis_ms,
        "judge": judge_ms,
        "total": retrieval_ms + synthesis_ms + judge_ms,
    }
    flags = {
        "reasoning": reasoning,
        "temperature": 1 if reasoning else temperature,
        "retain_context": retain_context,
        "context_budget": budget,
        "run_judge": run_judge,
        "log_session": log_session,
    }
    window_state = {
        "live_turns": live_turns,
        "evicted_this_turn": evicted,
        "buffer_entries": len(state["buffer"]) if retain_context else 0,
    }

    # --- session logging (opt-out) ---
    log_status = "off"
    if log_session:
        if not state["session_id"]:
            state["session_id"] = new_session_id()
        record = {
            "turn_index": turn_idx,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "question": message,
            "rewritten_query": rewritten,
            "resolved_market_ids": market_ids,
            "retrieved_context": merged,          # FULL merged/deduped context sent to synthesis
            "retrieved_counts": _count_results(merged),
            "answer": answer,
            "latency_ms": latency_ms,
            "tokens": tokens,
            "estimated_cost_usd": cost_usd,
            "flags": flags,
            "llm_judge_scores": judge_scores,
            "window_state": window_state,
        }
        ok = log_turn(state["session_id"], record)
        log_status = f"written ({state['session_id']})" if ok else "WRITE FAILED"

    # --- (b) formatted panel + (c) accumulating per-session table ---
    panel_md = _panel_markdown(turn_idx, latency_ms, tokens, cost_usd, window_state, judge_scores)
    js = judge_scores or {}
    state["rows"].append([
        turn_idx,
        (message[:60] + "...") if len(message) > 60 else message,
        latency_ms["total"],
        tokens["window_total"],
        round(cost_usd, 5),
        _fmt_score(js.get("faithfulness")),
        _fmt_score(js.get("answer_relevancy")),
        _fmt_score(js.get("context_relevance")),
    ])

    debug_info = json.dumps({
        "session_id": state["session_id"],
        "turn": turn_idx,
        "query_rewriting": rewritten,
        "resolved_market_ids": market_ids,
        "retrieved_counts": _count_results(merged),
        "latency_ms": latency_ms,
        "tokens": tokens,
        "estimated_cost_usd": cost_usd,
        "window_state": window_state,
        "llm_judge_scores": judge_scores,
        "log_session": log_status,
        "flags": flags,
    }, indent=2, default=str)
    return answer, panel_md, state["rows"], debug_info, state


with gr.Blocks(title="Poly-RAG eval") as demo:
    gr.Markdown(
        "# Poly-RAG -- retrieval evaluation UI\n"
        "Internal instrument for evaluating retrieval + synthesis quality (token usage, "
        "latency, cost per turn, session logging). Not a consumer chatbot -- every model "
        "and context parameter is exposed on purpose.\n\n"
        "**Sessions are logged to S3 for quality evaluation by default.** Turn off "
        "*Log this session* below if you don't want your turns recorded."
    )
    session_state = gr.State({"session_id": None, "buffer": [], "turn": 0, "rows": []})

    # Metrics components are CREATED here (so they can be wired as
    # additional_outputs of the ChatInterface below) but RENDERED later,
    # after the chat, via .render() inside the post-chat layout block.
    metrics_panel = gr.Markdown(value="_Metrics for the latest turn appear here._", render=False)
    metrics_table = gr.Dataframe(
        headers=["turn", "question", "latency ms", "window tok", "cost $",
                 "faithful", "ans relev", "ctx relev"],
        datatype=["number", "str", "number", "number", "number", "str", "str", "str"],
        row_count=(0, "dynamic"),
        column_count=(8, "fixed"),
        interactive=False,
        wrap=True,
        render=False,
    )
    debug_output = gr.Code(
        label="rewriting / market_ids / latency / tokens / cost / window / judge",
        language="json",
        render=False,
    )

    with gr.Accordion("Model parameters", open=True):
        reasoning_input = gr.Checkbox(
            value=False,
            label="Reasoning (extended thinking)",
            info="Step-by-step reasoning before answering. Forces temperature to 1 (Bedrock/Anthropic requirement) and adds latency.",
        )
        temperature_input = gr.Slider(
            minimum=0.0, maximum=1.0, value=0.7, step=0.1,
            label="Temperature",
            info="Disabled while Reasoning is on -- Bedrock requires temperature=1 when extended thinking is enabled.",
        )
        retain_context_input = gr.Checkbox(
            value=True,
            label="Retained context (sliding window)",
            info="Keep the full retrieved context from prior turns, merged and de-duplicated, so follow-ups still see earlier news/markets/comments. Off = every turn retrieves fresh with no carry-over.",
        )
        context_budget_input = gr.Slider(
            minimum=2000, maximum=20000, value=DEFAULT_CONTEXT_BUDGET, step=1000,
            label="Context window budget (tokens)",
            info="Sliding-window cap counting chat transcript + retained retrieved context together. Whole oldest turns are dropped when exceeded. Only applies when Retained context is on.",
        )
        run_judge_input = gr.Checkbox(
            value=False,
            label="Evaluate answer (LLM judge)",
            info="Reference-free in-line scoring per turn -- three artisanal Bedrock judges (faithfulness, answer relevancy, context relevance), ~5 model calls, adds latency. NOT the ragas library (ragas cannot coexist with langchain-aws); the canonical ragas run is the separate Phase 4 over the S3 session logs.",
        )
        log_session_input = gr.Checkbox(
            value=True,
            label="Log this session",
            info="Write every turn (question, full retrieved context, answer, metrics) to s3://.../evals/live_sessions/ for later evaluation. On by default.",
        )
        reasoning_input.change(
            fn=lambda r: gr.update(interactive=not r, value=1.0 if r else 0.7),
            inputs=reasoning_input,
            outputs=temperature_input,
        )

    gr.ChatInterface(
        fn=chat,
        additional_inputs=[
            reasoning_input, temperature_input, retain_context_input,
            context_budget_input, run_judge_input, log_session_input, session_state,
        ],
        additional_outputs=[metrics_panel, metrics_table, debug_output, session_state],
    )

    # --- metrics, rendered below the chat: what an evaluator reads after
    # each turn, kept out of the way of the conversation itself ---
    gr.Markdown("---")
    metrics_panel.render()
    with gr.Accordion("Session metrics (all turns)", open=True):
        metrics_table.render()
    with gr.Accordion("Debug: raw JSON (last turn)", open=True):
        debug_output.render()


if __name__ == "__main__":
    demo.launch(server_name="0.0.0.0", server_port=7860)
