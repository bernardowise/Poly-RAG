# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # healthcheck_consolidado (2026-08-29)
# MAGIC
# MAGIC Espejo en Databricks del healthcheck consolidado que corrimos manualmente el
# MAGIC 2026-08-29 sobre 12 ciclos pendientes -- mismo rigor que
# MAGIC `runbook_verify_phase1_health.md` + `runbook_verify_phase2_health.md`, pero en
# MAGIC una sola pasada agregada sobre un rango, no ciclo por ciclo. Pensada para
# MAGIC correrse por cuenta propia, sin depender de pedirselo a Claude Code cada vez.
# MAGIC
# MAGIC **Como usarla:** ajustar `RANGE_START` en la siguiente celda (formato
# MAGIC `YYYY-MM-DDTHH`, ej. `2026-08-23T12`) a la fecha/hora UTC del primer ciclo que
# MAGIC quieres verificar, y correr todas las celdas. Si solo quieres el ultimo ciclo,
# MAGIC pon el `RANGE_START` de ese mismo ciclo -- la logica es identica para 1 ciclo
# MAGIC o para N, no hace falta una rama separada.
# MAGIC
# MAGIC **Que SI hace:** cycle_complete, markets_queried vs markets_processed
# MAGIC (deteccion de la condicion de carrera de ingest_news, ver tech_debt.md),
# MAGIC distribucion de post_resolution_cycles_remaining, created_at en markets nuevos,
# MAGIC y un sweep de CloudWatch (invocaciones + errores/timeouts, PAGINADO -- ver
# MAGIC tech_debt.md, el primer intento de esto en la sesion de origen uso
# MAGIC filter_log_events SIN paginar y dio resultados falsos) sobre las 5 Lambdas de
# MAGIC Fase 1 + 11 de Fase 2/3.
# MAGIC
# MAGIC **Que NO hace (a proposito):** no arregla nada, no invoca ningun Lambda de
# MAGIC produccion -- solo lee S3/DynamoDB/CloudWatch. Si encuentra algo raro, lo
# MAGIC imprime para que tu decidas que hacer, igual que el runbook manual.

# COMMAND ----------

import boto3
import json
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
# MAGIC ## Configuracion -- AJUSTAR ANTES DE CORRER

# COMMAND ----------

RANGE_START = "2026-08-23T12"  # <-- cambiar aqui, formato YYYY-MM-DDTHH, UTC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 0 -- ciclos en el rango

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
                ts = f"{day}T{hh}"
                if ts >= RANGE_START:
                    cycles.append(ts)
cycles.sort()
print(f"ciclos en el rango (>= {RANGE_START}): {len(cycles)}")
for c in cycles:
    print(" ", c)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1 -- cycle_complete y markets_queried vs markets_processed

# COMMAND ----------

bad_complete = []
mismatch_processed = []

for c in cycles:
    day, hh = c.split("T")
    try:
        n = json.loads(s3.get_object(Bucket=BUCKET, Key=f"news/{day}/{hh}.json")["Body"].read())
    except Exception as e:
        print(f"{c}: FALTA news/ -- {e}")
        continue
    cc = n["metadata"].get("cycle_complete")
    mq, mp = n.get("markets_queried"), n.get("markets_processed")
    if not cc:
        bad_complete.append(c)
    if mq != mp:
        mismatch_processed.append((c, mq, mp))

print("cycle_complete != true en:", bad_complete or "NINGUNO -- OK")
print("markets_queried != markets_processed en:")
if mismatch_processed:
    for c, mq, mp in mismatch_processed:
        print(f"  {c}: queried={mq} processed={mp} (diff={mq - mp})")
    print("  (ver tech_debt.md, 'ingest_news Batch Re-Scan Race Condition' -- fix desplegado 2026-08-29,")
    print("   si sigue apareciendo DESPUES de esa fecha es una regresion real)")
else:
    print("  NINGUNO -- OK")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2 -- registry: post_resolution_cycles_remaining y created_at

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

resueltos_rango = [i for i in registry_items if (i.get("resolution_date") or "") >= RANGE_START]
print(f"markets resueltos en el rango: {len(resueltos_rango)}")
counts = Counter(int(i.get("post_resolution_cycles_remaining", 0)) for i in resueltos_rango)
print("distribucion post_resolution_cycles_remaining:", dict(counts))
print("  (valores fuera de {0,1,2,3,4} son sospechosos -- ver runbook_verify_phase1_health.md Paso 3)")

nuevos_rango = [i for i in registry_items if (i.get("first_seen") or "") >= RANGE_START]
print(f"\nmarkets nuevos en el rango: {len(nuevos_rango)}")
sin_created_at = [i for i in nuevos_rango if "created_at" not in i]
print(f"  sin created_at: {len(sin_created_at)} -- {'OK' if not sin_created_at else 'FALLA'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3 -- sweep de CloudWatch (Fase 1 + Fase 2/3), PAGINADO
# MAGIC
# MAGIC Usa `get_paginator` para `filter_log_events` -- sin esto, el conteo de
# MAGIC invocaciones puede salir falso (visto en produccion el 2026-08-29: una query
# MAGIC sin paginar reporto 1 invocacion cuando en realidad hubo 12).

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

range_start_dt = datetime.strptime(RANGE_START, "%Y-%m-%dT%H").replace(tzinfo=timezone.utc)
start_ms = int(range_start_dt.timestamp() * 1000)
log_paginator = logs.get_paginator("filter_log_events")

print(f"{'lambda':35s} {'invocaciones':>13s} {'errores/timeouts':>18s}")
for fn in PHASE1_LAMBDAS + PHASE2_LAMBDAS:
    try:
        n_start = sum(
            len(page.get("events", []))
            for page in log_paginator.paginate(
                logGroupName=f"/aws/lambda/{fn}", startTime=start_ms,
                filterPattern='"START RequestId"')
        )
        n_err = sum(
            len(page.get("events", []))
            for page in log_paginator.paginate(
                logGroupName=f"/aws/lambda/{fn}", startTime=start_ms,
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
# MAGIC Un rango sano da: cycle_complete true en todos, markets_queried ==
# MAGIC markets_processed en todos (post-fix 2026-08-29), post_resolution_cycles_remaining
# MAGIC en {0,1,2,3,4} sin valores raros, created_at presente en todo market nuevo, y
# MAGIC conteos de invocacion de CloudWatch coherentes con el numero de ciclos del rango
# MAGIC (N lambdas x N ciclos, salvo timeouts/retries legitimos ya conocidos).
# MAGIC
# MAGIC Cualquier desviacion no es automaticamente un incidente -- puede ser un bug nuevo.
# MAGIC Documentar en tech_debt.md si aparece algo no visto antes.