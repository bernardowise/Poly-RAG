# Databricks notebook source
import boto3

aws_access_key_id = dbutils.secrets.get(scope="poly-rag", key="aws_access_key_id")
aws_secret_access_key = dbutils.secrets.get(scope="poly-rag", key="aws_secret_access_key")

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

print(f"Total archivos: {len(all_keys)}")
all_keys

# COMMAND ----------

import re

# Un ciclo completo = las 4 etapas (polymarket, news, comments, digest) tienen
# su archivo FINAL bajo la misma fecha/hora -- excluye explicitamente los
# _batchN.json intermedios de News (esos no cuentan como "la etapa termino",
# solo el merge final s.json lo hace). El regex exige exactamente HH.json, sin
# sufijo, por eso los batch files no matchean.
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

print(f"Ciclos completos (4/4 etapas presentes): {len(complete_cycles)}")
for date, hour in complete_cycles:
    print(f"  {date} {hour}:00 UTC")

# COMMAND ----------

first_date, first_hour = complete_cycles[0]
print(f"Primer ciclo completo: {first_date} {first_hour}:00 UTC")

# Filtra all_keys por fecha/hora en el path -- incluye tambien los _batchN.json
# de News si algun dia quedaran (hoy ya no existen para este ciclo, se
# fusionaron/purgaron), no solo los 4 archivos finales.
first_cycle_keys = [
    k for k in all_keys
    if f"/{first_date}/{first_hour}" in k
]

print(f"Archivos del primer ciclo: {len(first_cycle_keys)}")
first_cycle_keys

# COMMAND ----------

second_date, second_hour = complete_cycles[1]
print(f"Segundo ciclo completo: {second_date} {second_hour}:00 UTC")

second_cycle_keys = [
    k for k in all_keys
    if f"/{second_date}/{second_hour}" in k
]

print(f"Archivos del segundo ciclo: {len(second_cycle_keys)}")
second_cycle_keys