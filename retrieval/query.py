"""
Poly-RAG retrieval, Bloque G / Dia 4, G1: retrieval puro.

Embeds a free-text question with the same model used for the corpus (Cohere
Embed v4, global.cohere.embed-v4:0 -- see lambdas/embed_registry/handler.py
for the reference implementation this mirrors) and searches each of the 4
LanceDB tables written by write_lancedb (registry_cohere, comments_cohere,
digest_cohere, news_article_cohere).

No synthesis here -- returns raw chunks + metadata. Sintesis LLM (segunda
pasada) is explicitly Dia 5 scope, not this file (see tech_debt.md, "Second
LLM Pass").

Cross-source fusion: top-k PER SOURCE, not one global ranked list -- cosine
scores aren't guaranteed comparable across tables with different embedding
distributions (see architecture_canon.md, Retrieval section). Optional
market_id filter is exact-match metadata filtering, not auto-detected from
the question text (that's a G1 decision explicitly left open -- v1 ships
semantic-only, filter is opt-in via the function argument).
"""

import json
import os

import boto3
import lancedb
from botocore.config import Config

S3_BUCKET = os.environ.get("S3_BUCKET", "poly-rag-369970405415")
LANCEDB_URI = f"s3://{S3_BUCKET}/lancedb/"
MODEL_LABEL = "cohere"
MODEL_ID = "global.cohere.embed-v4:0"

SOURCES = ["registry", "comments", "digest", "news_article"]

bedrock = boto3.client(
    "bedrock-runtime",
    region_name="us-east-1",
    config=Config(retries={"max_attempts": 3}, max_pool_connections=10),
)


def embed_query(text):
    """input_type=search_query, not search_document -- Cohere's asymmetric
    mode expects the query side tagged differently from the corpus side that
    was embedded by the embed_* Lambdas."""
    body = json.dumps({
        "texts": [text],
        "input_type": "search_query",
        "embedding_types": ["float"],
    })
    response = bedrock.invoke_model(modelId=MODEL_ID, body=body)
    payload = json.loads(response["body"].read())
    return payload["embeddings"]["float"][0]


# Schema is NOT uniform across the 4 tables -- market_id filtering has to
# know each source's real column, or it either errors (column doesn't
# exist) or silently returns 0 rows (comparing = against a list column).
# registry/news_article: scalar market_id, direct equality.
# digest: market_ids_mentioned is a LIST column -- needs list_contains, not =.
# comments: has NO market_id column at all (it links via comment_entity_id,
# a separate lookup through the registry -- see architecture_canon.md,
# "Comments" section). Filtering comments by market_id is out of scope for
# G1 -- skipped explicitly (not silently ignored) rather than faked.
SCALAR_MARKET_ID_SOURCES = {"registry", "news_article"}
LIST_MARKET_IDS_SOURCES = {"digest": "market_ids_mentioned"}
NO_MARKET_ID_FILTER_SOURCES = {"comments"}


def search_source(db, source, query_vector, k=5, market_id=None):
    table_name = f"{source}_{MODEL_LABEL}"
    if table_name not in db.list_tables().tables:
        return []
    tbl = db.open_table(table_name)
    search = tbl.search(query_vector).metric("cosine").limit(k)

    if market_id is not None:
        if source in SCALAR_MARKET_ID_SOURCES:
            search = search.where(f"market_id = '{market_id}'")
        elif source in LIST_MARKET_IDS_SOURCES:
            col = LIST_MARKET_IDS_SOURCES[source]
            search = search.where(f"list_contains({col}, '{market_id}')")
        elif source in NO_MARKET_ID_FILTER_SOURCES:
            return []  # explicit no-op, not a silent 0-row false negative

    rows = search.to_list()
    for r in rows:
        r["_source"] = source
        r.pop("embedding", None)  # not useful past this point, keeps output small
    return rows


def search(question, k=5, market_id=None, sources=None):
    """Returns {source: [chunk, ...]} -- one ranked list per source, not a
    single merged ranking (see module docstring)."""
    query_vector = embed_query(question)
    db = lancedb.connect(LANCEDB_URI)
    targets = sources or SOURCES
    return {
        source: search_source(db, source, query_vector, k=k, market_id=market_id)
        for source in targets
    }


if __name__ == "__main__":
    import sys
    question = sys.argv[1] if len(sys.argv) > 1 else "what happened with Bitcoin markets this week"
    results = search(question)
    for source, rows in results.items():
        print(f"\n=== {source} ({len(rows)} results) ===")
        for r in rows:
            preview = (r.get("text") or "")[:160].replace("\n", " ")
            print(f"  [{r.get('_distance', '?')}] {r.get('chunk_id', '?')}: {preview}")
