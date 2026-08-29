# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # healthcheck_ultimo_ciclo (2026-08-29)
# MAGIC
# MAGIC Version de un solo boton: detecta SOLA el ciclo mas reciente (el ultimo
# MAGIC `digest/YYYY-MM-DD/HH.json` en S3, sin configurar nada) y corre el mismo
# MAGIC nivel de chequeo que `runbook_verify_phase1_health.md` +
# MAGIC `runbook_verify_phase2_health.md`, pero solo sobre ESE ciclo.
# MAGIC
# MAGIC Para verificar un RANGO de varios ciclos pendientes (ej. despues de una
# MAGIC pausa de varios dias), usar `healthcheck_consolidado` en vez de esta.
# MAGIC
# MAGIC **Como usarla:** correr todas las celdas, sin tocar nada. Cada corrida
# MAGIC re-detecta el ciclo mas reciente en ese momento -- si el cron de las
# MAGIC 00:00/12:00 UTC ya disparo uno nuevo desde la ultima vez que corriste esto,
# MAGIC checa el nuevo automaticamente.

# COMMAND ----------

import boto3
import json
import pandas as pd
from datetime import datetime, timezone
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
logs = boto_session.client("logs")

BUCKET = "poly-rag-369970405415"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 0 -- detectar el ciclo mas reciente

# COMMAND ----------

paginator = s3.get_paginator("list_objects_v2")
cycles = []
for page in paginator.paginate(Bucket=BUCKET, Prefix="digest/"):
    for obj in page.get("Contents", []):
        k = obj["Key"]
        if k.endswith(".json"):
            parts = k.split("/")
            if len(parts) == 3:
                day, hh = parts[1], parts[2].replace(".json", "")
                cycles.append(f"{day}T{hh}")
cycles.sort()

CYCLE = cycles[-1]
DAY, HH = CYCLE.split("T")
print(f"ciclo mas reciente detectado: {CYCLE} UTC")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 -- cycle_complete y markets_queried vs markets_processed

# COMMAND ----------

n = json.loads(s3.get_object(Bucket=BUCKET, Key=f"news/{DAY}/{HH}.json")["Body"].read())
cc = n["metadata"].get("cycle_complete")
mq, mp = n.get("markets_queried"), n.get("markets_processed")

print(f"cycle_complete: {cc} -- {'OK' if cc else 'FALLA'}")
print(f"markets_queried={mq}  markets_processed={mp}", end="  ")
if mq == mp:
    print("-- OK")
else:
    print(f"-- MISMATCH (diff={mq - mp})")
    print("   ver tech_debt.md, 'ingest_news Batch Re-Scan Race Condition' (fix desplegado 2026-08-29)")
    print("   si este ciclo es POSTERIOR al fix y sigue apareciendo, es una regresion real")

cycle_started_at = n["cycle_started_at"]
print(f"\ncycle_started_at real: {cycle_started_at}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 -- registry: post_resolution_cycles_remaining y created_at
# MAGIC
# MAGIC Mismos dos chequeos que el runbook manual, acotados a lo que este ciclo tocaria:
# MAGIC markets que resolvieron ESTE ciclo (contador deberia ser {3,4}, ver
# MAGIC runbook_verify_phase1_health.md Paso 3) y markets nuevos de ESTE ciclo (deben
# MAGIC tener created_at).

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

resueltos_ciclo = [i for i in registry_items if (i.get("resolution_date") or "").startswith(f"{DAY}T{HH}")]
print(f"markets resueltos este ciclo: {len(resueltos_ciclo)}")
counts = Counter(int(i.get("post_resolution_cycles_remaining", 0)) for i in resueltos_ciclo)
print("distribucion post_resolution_cycles_remaining:", dict(counts))
print("  (se espera {3,4} uniforme -- ver runbook_verify_phase1_health.md Paso 3)")

nuevos_ciclo = [i for i in registry_items if (i.get("first_seen") or "").startswith(cycle_started_at[:16])]
print(f"\nmarkets nuevos este ciclo: {len(nuevos_ciclo)}")
sin_created_at = [i for i in nuevos_ciclo if "created_at" not in i]
print(f"  sin created_at: {len(sin_created_at)} -- {'OK' if not sin_created_at else 'FALLA'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3 -- CloudWatch: invocaciones + errores/timeouts de este ciclo
# MAGIC
# MAGIC Ventana: desde cycle_started_at hasta +2h (margen generoso para que la cadena
# MAGIC completa de Fase 1+2+3 termine). PAGINADO -- ver nota en healthcheck_consolidado
# MAGIC sobre por que esto importa.

# COMMAND ----------

PHASE1_LAMBDAS = [
    "poly-rag-ingest-polymarket", "poly-rag-ingest-news",
    "poly-rag-ingest-comments", "poly-rag-send-digest",
    "poly-rag-watchdog-ingest-news",
]
PHASE2_LAMBDAS = [
    "poly-rag-embed-orchestrator", "poly-rag-chunk-registry", "poly-rag-chunk-comments",
    "poly-rag-chunk-digest", "poly-rag-chunk-news-article", "poly-rag-embed-digest",
    "poly-rag-embed-comments", "poly-rag-embed-registry", "poly-rag-embed-news-article",
    "poly-rag-digest-metrics", "poly-rag-write-lancedb",
]

cycle_dt = datetime.fromisoformat(cycle_started_at)
start_ms = int(cycle_dt.timestamp() * 1000)
end_ms = int((cycle_dt.timestamp() + 2 * 3600) * 1000)
log_paginator = logs.get_paginator("filter_log_events")

print(f"{'lambda':35s} {'invocaciones':>13s} {'errores/timeouts':>18s}")
for fn in PHASE1_LAMBDAS + PHASE2_LAMBDAS:
    try:
        n_start = sum(
            len(page.get("events", []))
            for page in log_paginator.paginate(
                logGroupName=f"/aws/lambda/{fn}", startTime=start_ms, endTime=end_ms,
                filterPattern='"START RequestId"')
        )
        n_err = sum(
            len(page.get("events", []))
            for page in log_paginator.paginate(
                logGroupName=f"/aws/lambda/{fn}", startTime=start_ms, endTime=end_ms,
                filterPattern='?ERROR ?Exception ?Traceback ?"Status: timeout"')
        )
        flag = "  <-- revisar" if n_err > 0 else ""
        print(f"{fn:35s} {n_start:>13d} {n_err:>18d}{flag}")
    except Exception as e:
        print(f"{fn:35s} ERROR consultando -- {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen
# MAGIC
# MAGIC Ciclo sano: cycle_complete true, markets_queried == markets_processed,
# MAGIC post_resolution_cycles_remaining en {3,4} uniforme, created_at presente en
# MAGIC todo market nuevo, invocaciones = 1 por Lambda (salvo retries/timeouts
# MAGIC legitimos ya conocidos), 0 errores nuevos.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 4 -- markets nuevos de este ciclo: cuantos News y Comments trajeron
# MAGIC
# MAGIC Util para ver a simple vista si la busqueda de News/Comments funciono bien
# MAGIC para los markets recien agregados -- 0 en ambas columnas para MUCHOS markets
# MAGIC a la vez es la senal exacta de un ciclo roto (ver incidente 2026-08-29,
# MAGIC "ingest_news Batch Re-Scan Race Condition Fix -- Regression" en tech_debt.md).

# COMMAND ----------

news_articles = n.get("articles", [])
try:
    comments_payload = json.loads(s3.get_object(Bucket=BUCKET, Key=f"comments/{DAY}/{HH}.json")["Body"].read())
    comments = comments_payload.get("comments", [])
except s3.exceptions.NoSuchKey:
    comments = []

news_count_by_market = Counter()
for a in news_articles:
    for mid in a.get("market_ids", []):
        news_count_by_market[mid] += 1

comments_count_by_market = Counter()
for c in comments:
    for mid in c.get("market_ids", []):
        comments_count_by_market[mid] += 1

rows = []
for m in nuevos_ciclo:
    mid = m["market_id"]
    rows.append({
        "market_id": mid,
        "question": m.get("question"),
        "news_count": news_count_by_market.get(mid, 0),
        "comments_count": comments_count_by_market.get(mid, 0),
    })

nuevos_df = pd.DataFrame(rows).sort_values(["news_count", "comments_count"])
print(f"markets nuevos este ciclo: {len(nuevos_df)}")
print(f"  con 0 news Y 0 comments: {len(nuevos_df[(nuevos_df.news_count == 0) & (nuevos_df.comments_count == 0)])}")
nuevos_df