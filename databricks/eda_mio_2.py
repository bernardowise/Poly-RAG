# Databricks notebook source
import boto3
from collections import Counter

aws_access_key_id = dbutils.secrets.get(scope="poly-rag", key="aws_access_key_id")
aws_secret_access_key = dbutils.secrets.get(scope="poly-rag", key="aws_secret_access_key")

dynamodb = boto3.resource(
    "dynamodb",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name="us-east-1",
)

REGISTRY_TABLE = "poly-rag-market-registry"

def scan_full_table(table_name):
    table = dynamodb.Table(table_name)
    items = []
    resp = table.scan()
    items.extend(resp.get("Items", []))
    while "LastEvaluatedKey" in resp:
        resp = table.scan(ExclusiveStartKey=resp["LastEvaluatedKey"])
        items.extend(resp.get("Items", []))
    return items

registry_items = scan_full_table(REGISTRY_TABLE)

status_counts = Counter(item.get("status") for item in registry_items)

print(f"Total markets en el registry: {len(registry_items)}")
for status, count in status_counts.items():
    print(f"  {status}: {count}")

# COMMAND ----------

import re
import json

s3 = boto3.client(
    "s3",
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    region_name="us-east-1",
)

BUCKET = "poly-rag-369970405415"

paginator = s3.get_paginator("list_objects_v2")
all_keys = []
for page in paginator.paginate(Bucket=BUCKET):
    for obj in page.get("Contents", []):
        all_keys.append(obj["Key"])

# Mismo criterio de "ciclo completo" que en eda_mio: las 4 etapas con su
# archivo FINAL bajo la misma fecha/hora, excluyendo _batchN.json de News.
PATTERN = re.compile(r"^(polymarket|news|comments|digest)/(\d{4}-\d{2}-\d{2})/(\d{2})\.json$")

by_source = {"polymarket": set(), "news": set(), "comments": set(), "digest": set()}
for key in all_keys:
    m = PATTERN.match(key)
    if m:
        source, date, hour = m.groups()
        by_source[source].add((date, hour))

complete_cycles = sorted(
    by_source["polymarket"] & by_source["news"] & by_source["comments"] & by_source["digest"]
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Correccion real (2026-08-17): salieron NO es resolved_this_cycle crudo
# MAGIC
# MAGIC Bug encontrado por el usuario: `resolved_this_cycle` del payload cuenta CUALQUIER
# MAGIC resolucion detectada ese ciclo, incluyendo markets que nunca entraron por la ventana
# MAGIC de estos 4 ciclos (venian del registry viejo pre-rediseno, ya borrado). Usar ese
# MAGIC numero tal cual le resta a `entraron` un cargo que no le pertenece -- de los 75
# MAGIC "salieron" en total, solo 48 eran markets que de verdad habian entrado por esta
# MAGIC ventana; los otros 27 eran resoluciones de markets ajenos.
# MAGIC
# MAGIC Fix: en vez de sumar `resolved_this_cycle` a ciegas, se filtra `resolved_markets`
# MAGIC (la lista real de market_id, no solo el conteo) contra el set ACUMULADO de ids que
# MAGIC ya entraron hasta ese punto -- un market solo puede "salir" si ya "entro" antes,
# MAGIC dentro de esta misma ventana.

# COMMAND ----------

print(f"Ciclos completos: {len(complete_cycles)}")
print()

saldo = 0
entered_so_far = set()

for i, (date, hour) in enumerate(complete_cycles, start=1):
    obj = s3.get_object(Bucket=BUCKET, Key=f"polymarket/{date}/{hour}.json")
    data = json.loads(obj["Body"].read())

    newly_tracked_ids = {m["market_id"] for m in data.get("newly_tracked_markets", [])}
    resolved_ids_raw = {m["market_id"] for m in data.get("resolved_markets", [])}

    entraron = len(newly_tracked_ids)
    entered_so_far |= newly_tracked_ids

    # Solo cuenta como "salida" si el market ya era parte de la ventana --
    # descarta resoluciones de markets ajenos (registry viejo, ya borrado).
    salieron_ids = resolved_ids_raw & entered_so_far
    salieron = len(salieron_ids)
    descartadas = len(resolved_ids_raw) - salieron

    entered_so_far -= salieron_ids  # ya no estan abiertos, salen del acumulado

    saldo_anterior = saldo
    saldo = saldo_anterior + entraron - salieron

    print(f"Ciclo {i} ({date} {hour}:00 UTC)")
    print(f"  saldo anterior: {saldo_anterior}")
    print(f"  + entraron:     {entraron}")
    print(f"  - salieron:     {salieron}  (descartadas por ser ajenas a la ventana: {descartadas})")
    print(f"  saldo final:    {saldo}")
    print()

# COMMAND ----------

registry_summary = sorted(
    [
        {
            "market_id": item.get("market_id"),
            "first_entry": item.get("first_seen"),
            "status": item.get("status"),
            "last_entry": item.get("last_updated"),
        }
        for item in registry_items
    ],
    key=lambda r: r["first_entry"] or "",
)

print(f"Total registry: {len(registry_summary)}")
registry_df = spark.createDataFrame(registry_summary)
display(registry_df)

# COMMAND ----------

# El registry no guarda volumen -- vive en el ultimo snapshot de cada
# odds/<market_id>.json. Se toma el volume24hr del snapshot MAS RECIENTE
# por market (no un promedio ni el historico completo). Solo markets
# open -- el volumen de uno resuelto ya no refleja actividad actual.
volume_rows = []
for item in registry_items:
    if item.get("status") != "open":
        continue
    mid = item.get("market_id")
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=f"odds/{mid}.json")
        data = json.loads(obj["Body"].read())
    except s3.exceptions.NoSuchKey:
        continue
    snaps = data.get("snapshots", [])
    if not snaps:
        continue
    latest = snaps[-1]
    volume_rows.append({
        "market_id": mid,
        "question": item.get("question", ""),
        "status": item.get("status"),
        "volume24hr": float(latest.get("volume24hr", 0) or 0),
    })

volume_sorted = sorted(volume_rows, key=lambda r: r["volume24hr"], reverse=True)

print(f"Markets con volumen disponible: {len(volume_sorted)}")
print()
print("--- TOP 10 por volumen ---")
for r in volume_sorted[:10]:
    print(f"  {r['volume24hr']:>15,.2f}  {r['status']:<9}  {r['question'][:60]}")

print()
print("--- BOTTOM 10 por volumen ---")
for r in volume_sorted[-10:]:
    print(f"  {r['volume24hr']:>15,.2f}  {r['status']:<9}  {r['question'][:60]}")

# COMMAND ----------

# "Pastel" total de los markets abiertos -- suma de volume24hr (actividad
# reciente, ventana movil de 24h) y volume (volumeNum, total acumulado desde
# que cada market abrio). Usa volume_rows ya calculado en la celda anterior
# (solo status=="open"), pero vuelve a leer volume/volumeNum del ultimo
# snapshot ya que la celda de arriba solo guardo volume24hr.
pastel_rows = []
for item in registry_items:
    if item.get("status") != "open":
        continue
    mid = item.get("market_id")
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=f"odds/{mid}.json")
        data = json.loads(obj["Body"].read())
    except s3.exceptions.NoSuchKey:
        continue
    snaps = data.get("snapshots", [])
    if not snaps:
        continue
    latest = snaps[-1]
    pastel_rows.append({
        "market_id": mid,
        "volume24hr": float(latest.get("volume24hr", 0) or 0),
        "volume_total": float(latest.get("volume", 0) or 0),
    })

total_volume24hr = sum(r["volume24hr"] for r in pastel_rows)
total_volume_total = sum(r["volume_total"] for r in pastel_rows)

print(f"Markets abiertos con volumen disponible: {len(pastel_rows)}")
print()
print(f"Pastel total por volume24hr (actividad ultimas 24h, suma de todos):    \${total_volume24hr:>18,.2f}")
print(f"Pastel total por volume total (acumulado desde que abrio cada market): \${total_volume_total:>18,.2f}")