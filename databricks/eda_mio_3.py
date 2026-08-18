# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # eda_mio_3 -- Delta Lake + Unity Catalog (limpio, 2026-08-17)
# MAGIC
# MAGIC Reemplaza a `poly_rag_exploration` (borrado 2026-08-17) -- ese notebook escribia
# MAGIC sobre una MUESTRA de 25 markets tomada de un registry que en ese momento todavia
# MAGIC tenia residuo del pipeline pre-rediseno, generando tablas Delta contaminadas
# MAGIC (ver tech_debt.md, "Odds Snapshot Coverage" y las entradas de limpieza de
# MAGIC architecture_canon.md del 2026-08-17). Las tablas viejas (`market_registry`,
# MAGIC `odds_snapshots`) se borraron por completo antes de este notebook -- no hay
# MAGIC ninguna version anterior a la que hacer time travel, arranca en version 0 real.
# MAGIC
# MAGIC Este notebook usa el registry YA RECONCILIADO (`eda_mio`/`eda_mio_2`): 285
# MAGIC market_ids, exactamente los que entraron via los 4 ciclos completos verificados
# MAGIC (2026-08-16 01:00/12:00 UTC, 2026-08-17 00:00/12:00 UTC), sin ningun rastro de
# MAGIC invocaciones de debugging o del pipeline viejo.
# MAGIC
# MAGIC **Nota de compute:** Serverless (Spark Connect) no soporta `spark.conf.set` para
# MAGIC S3 ni `spark.read.json()` sobre datos traidos por boto3 -- boto3 baja los datos,
# MAGIC pandas normaliza tipos, `spark.createDataFrame(pandas_df)` construye el DataFrame.

# COMMAND ----------

import boto3
import json
import pandas as pd

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
REGISTRY_TABLE = "poly-rag-market-registry"

CATALOG = "workspace"
SCHEMA = "poly_rag"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 1: Registry completo -> Delta Table

# COMMAND ----------

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
print(f"Total registry items: {len(registry_items)}")

pdf_registry = pd.DataFrame(registry_items)
for col in pdf_registry.columns:
    pdf_registry[col] = pdf_registry[col].astype("string")

registry_df = spark.createDataFrame(pdf_registry)
registry_df.printSchema()
display(registry_df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## >>> AQUI ES DELTA LAKE (escritura #1) <<<
# MAGIC
# MAGIC `.format("delta")` es lo que le dice a Spark que no escriba Parquet plano, sino
# MAGIC Delta -- Parquet + un transaction log (`_delta_log/`) que registra cada operacion.
# MAGIC `.mode("overwrite")` reemplaza la tabla completa cada corrida (el registry es
# MAGIC metadata que cambia, no una serie de tiempo que deba acumularse).
# MAGIC
# MAGIC `DESCRIBE HISTORY` en la celda de abajo es un comando EXCLUSIVO de Delta -- no
# MAGIC existe para Parquet plano. Lee directo ese transaction log y te muestra cada
# MAGIC version de la tabla.

# COMMAND ----------

(
    registry_df.write
    .format("delta")  # <-- esto activa Delta Lake, no Parquet plano
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.market_registry")
)

# DESCRIBE HISTORY = leer el transaction log de Delta directamente.
# Cada fila de este resultado es una version de la tabla.
spark.sql(f"DESCRIBE HISTORY {CATALOG}.{SCHEMA}.market_registry").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 2: Odds Time-Series -- TODOS los markets abiertos, no una muestra
# MAGIC
# MAGIC A diferencia de `poly_rag_exploration` (SAMPLE_SIZE=25 por costo/tiempo, cuando
# MAGIC el dataset todavia estaba sucio y era mas grande), aqui el universo ya es
# MAGIC limpio y acotado (237 markets abiertos) -- un pull completo es barato y da la
# MAGIC serie de tiempo real completa, no una muestra.

# COMMAND ----------

def load_odds_snapshots(market_id):
    try:
        obj = s3.get_object(Bucket=BUCKET, Key=f"odds/{market_id}.json")
        data = json.loads(obj["Body"].read())
    except s3.exceptions.NoSuchKey:
        return []
    rows = []
    for snap in data.get("snapshots", []):
        try:
            prices = json.loads(snap["outcomePrices"])
        except (KeyError, json.JSONDecodeError, TypeError):
            continue
        rows.append({
            "market_id": market_id,
            "timestamp": snap.get("timestamp"),
            "price_yes": float(prices[0]) if prices else None,
            "volume": float(snap.get("volume", 0) or 0),
            "volume24hr": float(snap.get("volume24hr", 0) or 0),
            "liquidity": float(snap.get("liquidity", 0) or 0),
        })
    return rows

open_market_ids = [item["market_id"] for item in registry_items if item.get("status") == "open"]
print(f"Markets abiertos: {len(open_market_ids)}")

odds_rows = []
for mid in open_market_ids:
    odds_rows.extend(load_odds_snapshots(mid))

print(f"Odds snapshot rows cargados: {len(odds_rows)}")

# COMMAND ----------

pdf_odds = pd.DataFrame(odds_rows)
odds_df = spark.createDataFrame(pdf_odds)
odds_df.printSchema()
display(odds_df.orderBy("market_id", "timestamp").limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## >>> AQUI ES DELTA LAKE (escritura #2) <<<
# MAGIC
# MAGIC Misma idea que la escritura #1, pero con `.mode("append")` en vez de
# MAGIC `overwrite` -- esta tabla es la serie de tiempo real (odds), asi que cada
# MAGIC corrida AGREGA filas nuevas sin tocar las viejas. Delta soporta ambos modos
# MAGIC sobre el mismo transaction log; la diferencia es una decision de diseno, no
# MAGIC una limitacion tecnica.

# COMMAND ----------

# Chequeo de dedup ANTES de escribir (agregado 2026-08-18, bug real
# encontrado por el usuario: correr este notebook 2 veces duplico 371 filas
# a 742 -- append puro sin dedup vuelve a re-agregar TODO lo leido de S3 cada
# vez, aunque ya estuviera en la tabla). El objetivo es que NUNCA existan
# duplicados de (market_id, timestamp), no solo ignorarlos en silencio -- por
# eso esto imprime una alerta visible con el conteo exacto de lo que excluyo,
# en vez de filtrar callado como el dedup de comment_id en ingest_comments.
table_exists = spark.catalog.tableExists(f"{CATALOG}.{SCHEMA}.odds_snapshots")

if table_exists:
    existing_keys = spark.table(f"{CATALOG}.{SCHEMA}.odds_snapshots").select("market_id", "timestamp")
    odds_df_to_write = odds_df.join(existing_keys, on=["market_id", "timestamp"], how="left_anti")
    duplicate_count = odds_df.count() - odds_df_to_write.count()
    if duplicate_count > 0:
        print("=" * 70)
        print(f"ALERTA: {duplicate_count} filas ya existian en la tabla (mismo market_id + timestamp)")
        print("EXCLUIDAS del append -- la tabla nunca debe acumular duplicados.")
        print("=" * 70)
    else:
        print("Sin duplicados detectados -- todas las filas son genuinamente nuevas.")
else:
    odds_df_to_write = odds_df

(
    odds_df_to_write.write
    .format("delta")  # <-- Delta Lake otra vez, aqui en modo append (no overwrite)
    .mode("append")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.odds_snapshots")
)

spark.sql(f"""
    SELECT market_id, COUNT(*) as snapshot_count, MIN(timestamp) as first_seen, MAX(timestamp) as last_seen
    FROM {CATALOG}.{SCHEMA}.odds_snapshots
    GROUP BY market_id
    ORDER BY snapshot_count DESC
""").show(10, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Invariante de la tabla: solo markets que existen en el registry vivo ahora
# MAGIC
# MAGIC Se corre en TODA corrida futura -- compara contra `registry_items` completo (no
# MAGIC una muestra), asi que si algun market se borra del registry despues de esta
# MAGIC corrida, la proxima ejecucion de este notebook lo purga solo, sin intervencion
# MAGIC manual. Ver tech_debt.md / architecture_canon.md 2026-08-17 para el historial
# MAGIC completo de por que esto hizo falta (market 2252242, huerfano en la tabla vieja).

# COMMAND ----------

current_market_ids = {item["market_id"] for item in registry_items}
placeholders = ", ".join(f"'{mid}'" for mid in current_market_ids)

orphaned = spark.sql(f"""
    SELECT DISTINCT market_id FROM {CATALOG}.{SCHEMA}.odds_snapshots
    WHERE market_id NOT IN ({placeholders})
""")
orphaned_count = orphaned.count()
print(f"Markets huerfanos encontrados: {orphaned_count}")

if orphaned_count > 0:
    spark.sql(f"""
        DELETE FROM {CATALOG}.{SCHEMA}.odds_snapshots
        WHERE market_id NOT IN ({placeholders})
    """)
    print("Purgados.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 3: Unity Catalog -- governance basico

# COMMAND ----------

spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").show(truncate=False)

# COMMAND ----------

spark.sql(f"DESCRIBE TABLE EXTENDED {CATALOG}.{SCHEMA}.odds_snapshots").show(50, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parte 4: Time Travel real (2026-08-18)
# MAGIC
# MAGIC Ultimo pendiente del Dia 3. `market_registry` ya tiene 3 versiones reales
# MAGIC (0, 1, 2) porque este notebook se corrio varias veces -- cada corrida hizo un
# MAGIC `CREATE OR REPLACE TABLE AS SELECT` (overwrite) contra el registry en vivo de
# MAGIC ese momento. Time travel = consultar una version pasada con `VERSION AS OF`,
# MAGIC sin necesidad de haber guardado un backup manual de esa version -- Delta ya
# MAGIC la tiene completa en el transaction log.

# COMMAND ----------

history = spark.sql(f"DESCRIBE HISTORY {CATALOG}.{SCHEMA}.market_registry").collect()
versions = sorted(row["version"] for row in history)
oldest, newest = versions[0], versions[-1]
print(f"Versiones disponibles: {versions}")
print(f"Comparando version {oldest} (mas vieja) contra version {newest} (mas reciente)")

# COMMAND ----------

# La sintaxis real de time travel: VERSION AS OF <n> despues del nombre de tabla.
df_oldest = spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.market_registry VERSION AS OF {oldest}")
df_newest = spark.sql(f"SELECT * FROM {CATALOG}.{SCHEMA}.market_registry VERSION AS OF {newest}")

print(f"Version {oldest}: {df_oldest.count()} markets")
print(f"Version {newest}: {df_newest.count()} markets")

# COMMAND ----------

# Diff real entre versiones -- que market_ids aparecen en la version nueva
# que NO estaban en la vieja (y viceversa). Esto es posible SOLO porque Delta
# guardo la version vieja completa, no solo la mas reciente.
ids_oldest = set(row["market_id"] for row in df_oldest.select("market_id").collect())
ids_newest = set(row["market_id"] for row in df_newest.select("market_id").collect())

solo_en_nueva = ids_newest - ids_oldest
solo_en_vieja = ids_oldest - ids_newest

print(f"Market_ids solo en version {newest} (aparecieron despues): {len(solo_en_nueva)}")
print(f"Market_ids solo en version {oldest} (ya no estan en la nueva): {len(solo_en_vieja)}")