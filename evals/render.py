"""Context rendering for the Phase 5 eval -- kept a byte-for-byte copy of
gradio_app/app.py's _truncate_results + _context_to_text so the eval scores
the SAME prompt the Space actually sends to synthesis.

If those functions change in app.py, change them here too. (A shared module
would avoid the copy but drags gradio into app.py's import chain, which the
eval must not pull in -- accepted small duplication for a one-off.)
"""

MAX_PER_SOURCE = {
    "registry": 15,
    "news_article": 15,
    "digest": 3,
    "odds": 10,
    "comments": 15,
    "sql": 50,
}


def truncate_results(results):
    truncated = {}
    for source, rows in results.items():
        cap = MAX_PER_SOURCE.get(source)
        if source == "odds":
            items = list(rows.items())[:cap] if cap else list(rows.items())
            truncated[source] = dict(items)
        elif source == "sql":
            r = dict(rows)
            if cap and isinstance(r.get("rows"), list):
                r["rows"] = r["rows"][:cap]
            truncated[source] = r
        else:
            truncated[source] = rows[:cap] if cap else rows
    return truncated


def context_to_text(results):
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

    sql_res = results.get("sql")
    if sql_res:
        parts.append("\nSQL QUERY RESULT (computed over the full markets / odds tables):")
        parts.append(f"query: {sql_res.get('sql', '').strip()}")
        if sql_res.get("error"):
            parts.append(f"ERROR: {sql_res['error']}")
        else:
            cols = sql_res.get("columns", [])
            rows = sql_res.get("rows", [])
            parts.append(" | ".join(cols))
            for r in rows[:50]:
                parts.append(" | ".join(
                    f"{r.get(c)}" if not isinstance(r.get(c), float) else f"{r.get(c):.4g}"
                    for c in cols
                ))
            if len(rows) > 50:
                parts.append(f"... ({len(rows)} rows total)")

    return "\n".join(parts) if parts else "No relevant data found in the corpus."


SYSTEM_PROMPT = """You are Poly-RAG's assistant, answering questions about
Polymarket prediction markets using retrieved context (market data, news
coverage, odds movement, cycle digests, trader comments). Answer
conversationally and directly, grounded ONLY in the provided context -- do
not invent facts not present in it. If the context doesn't contain enough
information to answer, say so plainly instead of guessing. Cite specific
markets by their question text when relevant, not just their id."""

SYNTHESIS_MODEL_ID = "us.anthropic.claude-sonnet-4-5-20250929-v1:0"
