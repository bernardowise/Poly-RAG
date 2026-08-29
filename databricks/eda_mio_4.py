# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # eda_mio_4 -- Cobertura del corpus (2026-08-29)
# MAGIC
# MAGIC Exploracion interactiva sobre el corpus real, arrancando con la pregunta que
# MAGIC origino esta libreta: **cuantos markets del registry NO tienen ningun articulo
# MAGIC de News vinculado?** Sigue el mismo patron de setup que `eda_mio_3` (boto3 via
# MAGIC secrets scope `poly-rag`, pandas para normalizar antes de `spark.createDataFrame`
# MAGIC -- Serverless/Spark Connect no soporta `spark.read.json()` sobre datos bajados por
# MAGIC boto3 directamente).
# MAGIC
# MAGIC Pensada para crecer con lo que se te ocurra explorar en vivo, no solo la
# MAGIC pregunta inicial -- deja celdas de setup reusables (registry, news, chunks) y
# MAGIC un par de analisis de arranque.

# COMMAND ----------

import boto3
import json
import pandas as pd
from collections import Counter

aws_access_key_id = dbutils.secrets.get(scope="poly-rag", key="aws_access_key_id")
aws_secret_access_key = dbutils.secrets.get(scope="poly-rag", key="aws_secret_access_key")

boto_session = boto3.Session(
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name="us-east-1",
)
s3 = boto_session.client("s3")
dynamodb = boto_session.resource("dynamodb")

BUCKET = "poly-rag-369970405415"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cargar el registry completo (DynamoDB)

# COMMAND ----------

def scan_table(table_name, **kwargs):
    table = dynamodb.Table(table_name)
    items = []
    resp = table.scan(**kwargs)
    items.extend(resp.get("Items", []))
    while "LastEvaluatedKey" in resp:
        resp = table.scan(ExclusiveStartKey=resp["LastEvaluatedKey"], **kwargs)
        items.extend(resp.get("Items", []))
    return items

registry_items = scan_table("poly-rag-market-registry")
registry_df = pd.DataFrame(registry_items)
print(f"registry: {len(registry_df)} markets")
registry_df.head()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cargar TODOS los articulos de News (todos los ciclos en S3)
# MAGIC
# MAGIC Lee cada `news/YYYY-MM-DD/HH.json` (payload final de ciclo, no los
# MAGIC `_batch<offset>.json` intermedios) y junta los `market_ids` que aparecen en
# MAGIC cada articulo.

# COMMAND ----------

def list_news_cycle_keys():
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix="news/"):
        for obj in page.get("Contents", []):
            k = obj["Key"]
            if k.endswith(".json") and "_batch" not in k:
                keys.append(k)
    return sorted(keys)

news_keys = list_news_cycle_keys()
print(f"{len(news_keys)} ciclos de News en S3")

market_ids_with_news = set()
articles_per_market = Counter()
total_articles = 0

for key in news_keys:
    payload = json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    for article in payload.get("articles", []):
        total_articles += 1
        for mid in article.get("market_ids", []):
            market_ids_with_news.add(mid)
            articles_per_market[mid] += 1

print(f"total articulos (todos los ciclos): {total_articles}")
print(f"market_ids distintos con al menos 1 articulo: {len(market_ids_with_news)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## La pregunta: cuantos markets del registry NO tienen ningun articulo

# COMMAND ----------

all_market_ids = set(registry_df["market_id"])
sin_cobertura = all_market_ids - market_ids_with_news

print(f"markets en el registry: {len(all_market_ids)}")
print(f"markets CON al menos 1 articulo: {len(all_market_ids & market_ids_with_news)}")
print(f"markets SIN ningun articulo: {len(sin_cobertura)}")
print(f"% sin cobertura: {100 * len(sin_cobertura) / len(all_market_ids):.1f}%")

sin_cobertura_df = registry_df[registry_df["market_id"].isin(sin_cobertura)]
sin_cobertura_df[["market_id", "question", "status", "first_seen"]].head(20)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Distribucion de articulos por market (entre los que SI tienen cobertura)

# COMMAND ----------

dist_df = pd.DataFrame(
    [{"market_id": k, "article_count": v} for k, v in articles_per_market.items()]
)
print(dist_df["article_count"].describe())
dist_df.sort_values("article_count", ascending=False).head(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Espacio libre para exploracion ad-hoc
# MAGIC
# MAGIC registry_df, news_keys, market_ids_with_news, articles_per_market y
# MAGIC sin_cobertura_df ya estan en memoria -- seguir explorando desde aqui.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Markets huerfanos: tienen articulos de News pero YA NO estan en el registry
# MAGIC
# MAGIC No hay forma de recuperar el `question` real de Polymarket para estos IDs (esa
# MAGIC info solo vive en el registry, y ya no estan ahi) -- como proxy, se usa el
# MAGIC `title` de sus articulos de News (titulo de la NOTICIA, no la pregunta del
# MAGIC market) para poder identificarlos a simple vista.

# COMMAND ----------

huerfanos = market_ids_with_news - all_market_ids
print(f"markets huerfanos: {len(huerfanos)}")

titles_by_orphan = {}
for key in news_keys:
    payload = json.loads(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    for article in payload.get("articles", []):
        for mid in article.get("market_ids", []):
            if mid in huerfanos:
                titles_by_orphan.setdefault(mid, []).append(article.get("title"))

orphan_rows = [
    {
        "market_id": mid,
        "article_count": len(titles),
        "example_titles": " | ".join(titles[:3]),
    }
    for mid, titles in titles_by_orphan.items()
]
orphan_df = pd.DataFrame(orphan_rows).sort_values("article_count", ascending=False)
orphan_df