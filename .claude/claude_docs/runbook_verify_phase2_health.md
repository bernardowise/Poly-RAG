# Runbook: Verificar que Fase 2 (chunking + embedding) corrio sano

**Complementario a `runbook_verify_phase1_health.md`, no lo reemplaza.** Ese runbook
verifica la cadena de ingesta (4 Lambdas). Este verifica el eslabon que crece a partir
de `send_digest` -- 4 Lambdas de chunking en paralelo, luego 4 de embedding en
secuencia estricta (ver `.claude/claude_docs/architecture_canon.md`).

**Origen de este runbook (2026-08-22):** el primer ciclo real de Fase 2 en produccion
(ciclo 14) fallo de dos formas distintas y silenciosas, ninguna de las cuales habria
sido visible solo por "llego el correo o no":

1. `embed_digest` tronaba con `AccessDeniedException` en la primera llamada real a
   Bedrock (IAM insuficiente para el routing del profile `global.`) -- la cadena
   murio ahi, `embed_comments`/`embed_registry`/`embed_news_article` nunca se
   invocaron. El unico sintoma visible desde afuera era que el segundo correo (el de
   metricas) nunca llego -- pero eso tampoco distingue "la cadena murio" de "el
   correo se fue a spam" sin revisar CloudWatch.
2. `chunk_registry` reporto **0 markets nuevos** cuando en realidad habian entrado
   **25** ese mismo ciclo (confirmado cruzando contra `newly_tracked_markets` del
   digest de Fase 1) -- un bug real de comparacion (`first_seen > cycle_started_at`
   en vez de `>=`) que existia desde que se escribio `chunk_registry`, no algo nuevo
   de ese ciclo. Sin cruzar el conteo contra un dato ya confiable de Fase 1, "0" se
   ve identico a "no habia nada nuevo que hacer" -- exactamente el mismo tipo de
   falla silenciosa que motivo la correccion de Paso 5 en el runbook de Fase 1 (ver
   ahi: un chequeo de salud no debe tener clausula de "0 es aceptable por default").

**Regla que hereda de Fase 1 y aplica aqui sin excepcion:** cualquier chequeo de este
runbook es un `assert` real, nunca una nota pasiva ni una clausula de "esto se salta
mientras no este conectado."

---

## Paso 0 -- identificar el ciclo

```python
DAY = "2026-08-22"   # fecha UTC del ciclo
HH = "12"            # hora UTC del ciclo (00 o 12)
```

`cycle_started_at` real (no asumir `{DAY}T{HH}:00:00` nominal -- el ciclo arranca
segundos despues) se lee del propio digest de Fase 1:

```python
import boto3, json
s3 = boto3.client("s3"); B = "poly-rag-369970405415"
digest = json.loads(s3.get_object(Bucket=B, Key=f"digest/{DAY}/{HH}.json")["Body"].read())
cycle_started_at = digest["ingested_at"]  # aproximado -- ver nota abajo
```

**Nota:** el digest de Fase 1 no expone `cycle_started_at` explicito en su JSON
(solo `ingested_at`, que es cuando `send_digest` mismo corrio, no el inicio real de
la cadena). El valor exacto que Fase 2 recibio hay que leerlo de un chunk ya escrito
(cualquier chunk trae `cycle_started_at` en su propio contenido si el chunker lo
incluyo) o de un log de CloudWatch de `chunk_registry`/`embed_digest`. Si ninguno
esta disponible, usar el `first_seen` de cualquier market en
`newly_tracked_markets` del digest -- es el mismo valor exacto (ver Paso 1).

---

## Paso 1 -- las 4 fuentes de chunks existen y tienen conteo coherente contra Fase 1

```python
def load_chunks(source):
    key = f"chunks/{source}/{DAY}/{HH}.json"
    return json.loads(s3.get_object(Bucket=B, Key=key)["Body"].read())

chunks_digest = load_chunks("digest")
chunks_comments = load_chunks("comments")
chunks_registry = load_chunks("registry")
chunks_news = load_chunks("news_article")

assert len(chunks_digest) == 1, f"digest debe ser exactamente 1 chunk, salio {len(chunks_digest)}"

n_new_markets = len(digest["newly_tracked_markets"])
assert len(chunks_registry) == n_new_markets, (
    f"chunk_registry desfasado: {len(chunks_registry)} chunks vs "
    f"{n_new_markets} newly_tracked_markets ya confirmados por Fase 1 -- "
    f"esto es exactamente el bug del 2026-08-22 (first_seen > vs >= cycle_started_at)"
)
```

**Por que cruzar contra `newly_tracked_markets` y no solo contra "algo > 0":** Fase 1
ya escribio ese numero con su propia logica (independiente del filtro de
`chunk_registry`), asi que sirve como fuente de verdad externa. Un `chunk_registry`
roto que siempre devuelve 0 pasaria cualquier chequeo tipo "hubo actividad" con falso
verde -- necesita comparar contra un numero que se sepa correcto por otro camino.

Ningun chunk file deberia faltar. Si falta alguno, revisar CloudWatch Logs de esa
Lambda de chunking especifica antes de seguir (Paso 3 mas abajo).

---

## Paso 2 -- las 4 fuentes de vectores existen, sin gaps por identidad (no solo conteo)

```python
def checkpoint_ids(source, since_hour_utc):
    # since_hour_utc: filtra checkpoints por LastModified para no arrastrar
    # vectores de ciclos anteriores en el mismo prefijo
    resp = s3.list_objects_v2(Bucket=B, Prefix=f"vectors/_checkpoints/{source}/cohere/")
    ids = set()
    for o in resp.get("Contents", []):
        if o["LastModified"].strftime("%Y-%m-%d %H") >= since_hour_utc:
            recs = json.loads(s3.get_object(Bucket=B, Key=o["Key"])["Body"].read())
            ids.update(r["chunk_id"] for r in recs)
    return ids

for source, chunks in [
    ("digest", chunks_digest), ("comments", chunks_comments),
    ("registry", chunks_registry), ("news_article", chunks_news),
]:
    expected_ids = {c["chunk_id"] for c in chunks}
    actual_ids = checkpoint_ids(source, f"{DAY} {HH}")
    faltantes = expected_ids - actual_ids
    de_mas = actual_ids - expected_ids
    assert not faltantes, f"{source}: {len(faltantes)} chunks sin embeder -- {list(faltantes)[:5]}"
    # de_mas no es necesariamente error (puede incluir vectores de otros ciclos
    # si el filtro de hora fue amplio) -- pero si el conteo es MUY superior al
    # esperado, revisar duplicados reales (ver Paso 2026-08-22, corrida de
    # cycle 14: un batch se reproceso completo por una corrida interrumpida sin
    # checkpoint -- el resultado final salio limpio, pero el gasto de Bedrock
    # se duplico para ese batch. Este chequeo NO detecta gasto duplicado, solo
    # gaps -- si se quiere medir gasto duplicado hay que comparar tokens_in
    # esperados (estimados del texto) contra tokens_in reales en
    # poly-rag-embedding-metrics)
    print(f"{source}: {len(expected_ids)} chunks, {len(actual_ids)} vectores, 0 gaps")
```

Comparacion por **identidad de `chunk_id`**, no solo `len(chunks) == len(vectors)` --
un conteo igual con IDs distintos (duplicados que compensan un gap real, o al reves)
pasaria un chequeo de solo-conteo sin ser detectado.

---

## Paso 3 -- CloudWatch: cero errores reales en los 8 Lambdas de Fase 2

```bash
CYCLE_EPOCH_MS=$(python3 -c "from datetime import datetime; print(int(datetime.fromisoformat('$CYCLE_STARTED_AT').timestamp()*1000))")

for fn in chunk-digest chunk-comments chunk-registry chunk-news-article \
          embed-digest embed-comments embed-registry embed-news-article; do
  n=$(aws logs filter-log-events --log-group-name /aws/lambda/poly-rag-$fn \
      --start-time "$CYCLE_EPOCH_MS" \
      --filter-pattern "?ERROR ?Exception ?Traceback" \
      --query 'length(events)' --output text 2>/dev/null || echo "SIN LOG GROUP")
  echo "poly-rag-$fn: $n eventos de error (debe ser 0)"
done
```

Un Lambda que nunca aparece en `aws logs describe-log-groups` (sin log group) es en
si mismo una senal -- significa que nunca fue invocado en absoluto, lo cual para
`embed_comments`/`embed_registry`/`embed_news_article` es tan grave como un error
real si la cadena deberia haber llegado hasta ahi. Verificar con:

```bash
aws logs describe-log-groups --log-group-name-prefix "/aws/lambda/poly-rag-embed" \
  --query 'logGroups[].logGroupName' --output text
```

Las 4 deben existir con al menos 1 log stream cuyo `lastEventTimestamp` caiga dentro
de la ventana de este ciclo.

---

## Paso 4 -- las metricas de Fase 2 llegaron completas, senal indirecta de que el correo salio

```python
import boto3
metrics = boto3.resource("dynamodb").Table("poly-rag-embedding-metrics")
r = metrics.scan(
    FilterExpression="cycle_started_at = :c",
    ExpressionAttributeValues={":c": cycle_started_at},
)
rows = r["Items"]
sources_seen = {row["source"] for row in rows}
assert sources_seen == {"digest", "comments", "registry", "news_article"} or (
    sources_seen == {"digest", "comments", "news_article"} and n_new_markets == 0
), f"faltan fuentes en embedding_metrics: {sources_seen}"
assert "news_article" in sources_seen, (
    "embed_news_article nunca corrio -- la cadena murio antes del ultimo eslabon, "
    "el correo de metricas de Fase 2 nunca se mando (aunque el de Fase 1 si)"
)
```

No hay forma de leer la bandeja de entrada del usuario desde aqui, asi que este es el
proxy indirecto: `embed_news_article` es lo unico que manda el correo de metricas, y
solo llega ahi si escribio filas en `embedding_metrics` primero (mismo codigo, mismo
paso). Si `registry` no aparece en `sources_seen` **y** `n_new_markets > 0`, es una
falla real -- si `n_new_markets == 0` es aceptable que `embed_registry` no haya
tenido nada que embeder ese ciclo.

---

## Paso 5 -- secuencialidad respetada (sin condicion de carrera de TPM)

**Especifico para el diseno de Fase 2** (ver architecture_canon.md: las 4 Lambdas de
embedding corren en SECUENCIA, no en paralelo, para no recrear la condicion de
carrera del incidente de doble-disparo de News).

```bash
for fn in embed-digest embed-comments embed-registry embed-news-article; do
  aws logs filter-log-events --log-group-name /aws/lambda/poly-rag-$fn \
    --start-time "$CYCLE_EPOCH_MS" --filter-pattern "START RequestId" \
    --query 'events[*].timestamp' --output text
done
```

Cada Lambda deberia tener **exactamente 1** invocacion en la ventana del ciclo (mas
de 1 es senal de una re-invocacion espuria, mismo espiritu que el chequeo de lock
del Paso 2 en el runbook de Fase 1), y los timestamps de arranque deben seguir el
orden `embed_digest < embed_comments < embed_registry < embed_news_article` -- si
dos aparecen con timestamps solapados, la cadena se rompio en algun punto y algo
las esta disparando en paralelo en vez de en secuencia.

---

## Resultado esperado

4 archivos de chunks presentes, conteo de `registry` cruzado y coherente contra
`newly_tracked_markets` de Fase 1, 4 archivos de vectores presentes sin gaps de
`chunk_id`, cero eventos de error en los 8 log groups (los 8 deben existir), fila de
`news_article` presente en `poly-rag-embedding-metrics` para este ciclo, y las 4
invocaciones de embedding en orden estrictamente secuencial sin solape.

Cualquier desviacion no es necesariamente un incidente de invocacion manual (ver
`runbook_manual_invocation_cleanup.md` para eso) -- puede ser un bug nuevo del ciclo
automatico, como los dos que motivaron este runbook. Documentar en tech_debt.md,
igual que se hizo con ambos bugs del 2026-08-22, no descartar como ruido.
