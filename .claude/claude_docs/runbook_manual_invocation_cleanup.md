# Runbook: Remediar un ciclo espurio por invocacion manual

**Que es un "ciclo espurio":** cualquier ejecucion de la cadena de ingestion que NO
haya sido disparada por el cron de EventBridge (00:00 / 12:00 UTC). Tipicamente
ocurre cuando alguien (Claude o el usuario) invoca `poly-rag-ingest-polymarket`
manualmente para "probar" un cambio de codigo, sin considerar que esa Lambda es el
PUNTO DE ENTRADA de toda la cadena: al terminar invoca `ingest_news`, que invoca
`ingest_comments`, que invoca `send_digest`, que manda correo real via SES.

**Origen de este documento:** incidente real del 2026-08-18. Claude invoco
`ingest_polymarket` dos veces para verificar un fix del campo `source` en los
snapshots, sin pedir confirmacion al usuario. Borrar los archivos de S3 NO fue
suficiente -- la mayoria del dano estaba en DynamoDB y en mutaciones de archivos
existentes, invisible desde cualquier listado de S3.

**Factor de amplificacion medido: x12.5.** Dos invocaciones manuales produjeron 25
correos digest. La cascada real, contada desde CloudWatch:

| Etapa | Invocaciones | Por que se multiplica |
|---|---|---|
| `ingest_polymarket` | 4 | 2 manuales + 2 reintentos automaticos de AWS (el primer `aws lambda invoke` murio por timeout del CLIENTE, pero la Lambda siguio corriendo del lado de AWS y el servicio reintento) |
| `ingest_news` | 80 | cada una hace fan-out de ~20 batches (595 markets / BATCH_SIZE 35) |
| `ingest_comments` | 26 | **el bug**: `merge_batch_payloads` es idempotente para ESCRIBIR pero no tiene guarda contra disparar la siguiente etapa varias veces -- con 80 batches concurrentes, 26 de ellos vieron "ya estan todos los archivos" y cada uno invoco a Comments |
| `send_digest` | 25 | uno por cada Comments = **25 correos reales via SES** |

Sin ese bug de doble-disparo (ya documentado en tech_debt.md, "Strict Ingestion
Chaining", nota sobre offset=0 concurrente) habrian sido 4 correos. **Leccion: en
este pipeline el costo de una invocacion manual no es lineal, es multiplicativo** --
razon adicional para no invocar nunca sin confirmacion explicita.

---

## Paso 0 -- DETENER EL SANGRADO (antes de cualquier diagnostico)

Las invocaciones son asincronas (`InvocationType="Event"`), asi que ya estan en la
cola interna de Lambda y NO se pueden cancelar desde afuera. La unica forma de
frenarlas es negarles concurrencia:

```bash
# frena el envio de correos de inmediato (las invocaciones pendientes se rechazan)
aws lambda put-function-concurrency \
  --function-name poly-rag-send-digest \
  --reserved-concurrent-executions 0

# si la cadena sigue corriendo y quieres frenarla completa:
aws lambda put-function-concurrency --function-name poly-rag-ingest-news --reserved-concurrent-executions 0
aws lambda put-function-concurrency --function-name poly-rag-ingest-comments --reserved-concurrent-executions 0
```

**Al terminar la limpieza, REVERTIR** (si no, el cron de las 00:00/12:00 tampoco
correra):

```bash
aws lambda delete-function-concurrency --function-name poly-rag-send-digest
aws lambda delete-function-concurrency --function-name poly-rag-ingest-news
aws lambda delete-function-concurrency --function-name poly-rag-ingest-comments
```

---

## Paso 1 -- IDENTIFICAR la ventana temporal exacta del ciclo espurio

Todo lo demas depende de tener bien este dato. Usar CloudWatch, no adivinar:

```bash
# cuando arranco cada invocacion de la cadena
aws logs filter-log-events \
  --log-group-name /aws/lambda/poly-rag-ingest-polymarket \
  --start-time $(($(date +%s -d '<FECHA HORA UTC>')*1000)) \
  --filter-pattern "START RequestId" \
  --query 'events[*].[timestamp,message]' --output text

# cuantas invocaciones hubo por Lambda (detecta el fan-out descontrolado)
for fn in poly-rag-ingest-polymarket poly-rag-ingest-news poly-rag-ingest-comments poly-rag-send-digest; do
  n=$(aws logs filter-log-events --log-group-name /aws/lambda/$fn \
      --start-time $(($(date +%s -d '<FECHA HORA UTC>')*1000)) \
      --filter-pattern "START RequestId" --query 'length(events)' --output text | head -1)
  echo "$fn: $n invocations"
done
```

Guardar el timestamp de corte (ej. `2026-08-18T21:00:00`). Se usa en TODOS los pasos
siguientes.

---

## Paso 2 -- LOS 9 LUGARES QUE HAY QUE LIMPIAR

**Esta es la lista completa, VALIDADA ejecutandola en el incidente del 2026-08-18.**
Omitir cualquiera deja rastro. El error natural es limpiar solo S3, porque es lo
unico visible en un `aws s3 ls` -- pero **la mayoria del dano vive en DynamoDB y en
mutaciones de archivos existentes, no en archivos nuevos**.

**Los 4 errores que la primera version teorica de este runbook tenia** (encontrados
al ejecutarlo de verdad -- por eso un runbook sin validar no sirve):

| Error | Estimado teorico | Real medido |
|---|---|---|
| Dedup de URLs | ~172 (los del payload final) | **1,302** -- incluye los batches que nunca llegaron al merge |
| Snapshots en markets ya existentes | no contemplado como paso propio | **1,864 snapshots en 466 archivos** |
| Markets resueltos por la corrida | mencionado como "puede aplicar" | **36 markets**, requieren REVERSION no borrado |
| Contadores post-resolucion consumidos | no contemplado | **93 markets** con su ventana quemada en minutos |

### 2.1 S3 -- archivos de ciclo (los 4 prefijos)

```bash
HH=21   # la hora UTC del ciclo espurio
DAY=2026-08-18

aws s3 rm s3://poly-rag-369970405415/polymarket/$DAY/$HH.json
aws s3 rm s3://poly-rag-369970405415/comments/$DAY/$HH.json
aws s3 rm s3://poly-rag-369970405415/digest/$DAY/$HH.json
aws s3 rm s3://poly-rag-369970405415/news/$DAY/$HH.json

# News tambien deja archivos intermedios por batch -- NO OLVIDARLOS
aws s3 ls s3://poly-rag-369970405415/news/$DAY/ | grep "${HH}_batch"
# borrar cada uno (el fan-out puede generar 20+)
```

**Verificar que no quede nada:**
```bash
aws s3 ls s3://poly-rag-369970405415/ --recursive | grep "$DAY/$HH"
# vacio = limpio
```

### 2.2 DynamoDB `poly-rag-market-registry` -- markets nuevos

Markets que entraron al registry SOLO por la corrida espuria:

```bash
aws dynamodb scan --table-name poly-rag-market-registry \
  --filter-expression "first_seen > :t" \
  --expression-attribute-values '{":t":{"S":"2026-08-18T21:00:00"}}' \
  --projection-expression "market_id,first_seen" --output json
```

Borrar cada `market_id` resultante con `delete_item`.

**CUIDADO:** si el ciclo espurio resolvio markets (los marco `resolved`), eso NO se
revierte borrando -- hay que revisar tambien `resolution_date > :t` y decidir si se
revierten a `open`. En el incidente del 2026-08-18 no aplico, pero puede aplicar.

### 2.3 S3 `odds/` -- archivos de los markets nuevos

Cada market nuevo del paso 2.2 tiene su propio `odds/<market_id>.json`, creado por la
corrida espuria (y posiblemente con historia CLOB backfilleada nativa, ver
tech_debt.md "F-lambdas"):

```bash
aws s3 rm s3://poly-rag-369970405415/odds/<market_id>.json
```

**CUIDADO -- caso distinto:** para markets que YA existian antes, la corrida espuria
no creo el archivo, solo le APENDIO un snapshot `source: "cycle"` con el timestamp
espurio. Esos snapshots hay que quitarlos del array sin borrar el archivo:

```python
# leer odds/<id>.json, filtrar snapshots con timestamp dentro de la ventana espuria,
# reescribir el archivo. NO borrar el archivo completo.
```

### 2.4 DynamoDB `poly-rag-architecture-metrics` -- filas de costo/latencia

```bash
aws dynamodb scan --table-name poly-rag-architecture-metrics \
  --filter-expression "#t > :t" \
  --expression-attribute-names '{"#t":"timestamp"}' \
  --expression-attribute-values '{":t":{"S":"2026-08-18T21:00:00"}}' \
  --projection-expression "pk" --output json
```

Borrar cada `pk`. Si no se limpian, las metricas de costo/latencia quedan infladas y
cualquier analisis posterior (ej. costo real por ciclo) sale mal.

### 2.5 DynamoDB `poly-rag-processed-urls` -- dedup de News

**El mas facil de olvidar, y el que causa perdida de datos silenciosa.** Los
articulos que la corrida espuria descargo quedaron marcados como procesados. Si no se
limpian, el proximo ciclo legitimo los saltara -- esas noticias quedan "quemadas",
nunca entran al corpus por un ciclo real.

```bash
aws dynamodb scan --table-name poly-rag-processed-urls \
  --filter-expression "processed_at > :t" \
  --expression-attribute-values '{":t":{"S":"2026-08-18T21:00:00"}}' \
  --projection-expression "url" --output json
```

Borrar cada `url`. Es intencional que el ciclo real los vuelva a bajar.

### 2.6 DynamoDB `poly-rag-processed-comments` -- dedup de Comments

Mismo razonamiento que 2.5. **Confirmado: la tabla SI tiene `processed_at`**, asi que
se filtra igual que las URLs (no hace falta cruzar contra el archivo de S3).
Incidente 2026-08-18: 484 comment_ids.

### 2.7 Registry -- markets RESUELTOS por la corrida espuria (REVERSION, no borrado)

**El paso mas delicado, y el que borrar filas NO resuelve.** Si la corrida espuria
consulto la Gamma API y encontro markets con `closed: true`, los marco resueltos. El
dato es VERDADERO (si resolvieron en Polymarket) pero llego fuera de ciclo, y ademas
consumio su ventana post-resolucion.

```bash
aws dynamodb scan --table-name poly-rag-market-registry \
  --filter-expression "resolution_date > :t" \
  --expression-attribute-values '{":t":{"S":"<CORTE>"}}' \
  --projection-expression "market_id,resolution_date" --output json
```

Revertir CADA UNO a su estado previo (no borrar el item -- el market es legitimo y
existia antes):

```python
reg.update_item(
    Key={'market_id': mid},
    UpdateExpression='SET #s = :open, resolution_date = :null, '
                     'final_outcome = :null, post_resolution_cycles_remaining = :zero',
    ExpressionAttributeNames={'#s': 'status'},
    ExpressionAttributeValues={':open': 'open', ':null': None, ':zero': 0},
)
```

El proximo ciclo legitimo los volvera a detectar como resueltos y les dara su ventana
de 4 ciclos correcta. Incidente 2026-08-18: 36 markets.

### 2.8 Registry -- contadores `post_resolution_cycles_remaining` CONSUMIDOS

**El dano mas invisible de todos.** Cada invocacion de `ingest_news` decrementa el
contador de todo market resuelto que incluya en su busqueda. Con N invocaciones
espurias, la ventana de 48h (4 ciclos) de CADA market resuelto se consume en minutos
-- sin haber capturado nada util, porque las corridas espurias corrieron todas dentro
de la misma hora.

Incidente 2026-08-18: los 93 markets legacy pasaron de 4 a 0. La reparacion fue
re-correr el script one-off (es idempotente y solo re-arma contadores en 0):

```bash
python3 scripts/start_legacy_post_resolution_windows.py --apply
```

**Para markets resueltos DESPUES de ese script**, el contador se restaura como parte
del paso 2.7 (queda en 0 al revertir a `open`, y el proximo ciclo real lo pondra en 4
al re-detectar la resolucion).

### 2.9 S3 `odds/` -- snapshots espurios en markets que YA EXISTIAN

Distinto de 2.3 (que borra archivos completos de markets nuevos). Aqui el archivo es
legitimo y debe conservarse; solo hay que quitarle los snapshots con timestamp dentro
de la ventana espuria:

```python
keys = [o['Key'] for p in s3.get_paginator('list_objects_v2')
        .paginate(Bucket=B, Prefix='odds/') for o in p.get('Contents', [])]
for k in keys:
    d = json.loads(s3.get_object(Bucket=B, Key=k)['Body'].read())
    keep = [s for s in d['snapshots'] if s.get('timestamp', '') < CUT]
    if len(keep) != len(d['snapshots']):
        d['snapshots'] = keep
        s3.put_object(Bucket=B, Key=k, Body=json.dumps(d),
                      ContentType='application/json')
```

Incidente 2026-08-18: **1,864 snapshots removidos de 466 archivos** (4 invocaciones x
466 markets abiertos). Tarda ~3 min sobre ~600 archivos; conviene correrlo en
background.

---

## Paso 3 -- VERIFICAR tabula rasa

```bash
# 1. sin archivos S3 de la hora espuria
aws s3 ls s3://poly-rag-369970405415/ --recursive | grep "$DAY/$HH"

# 2. registry sin items de la ventana
aws dynamodb scan --table-name poly-rag-market-registry \
  --filter-expression "first_seen > :t" \
  --expression-attribute-values '{":t":{"S":"<CORTE>"}}' --select COUNT

# 3. metricas sin filas de la ventana
aws dynamodb scan --table-name poly-rag-architecture-metrics \
  --filter-expression "#t > :t" --expression-attribute-names '{"#t":"timestamp"}' \
  --expression-attribute-values '{":t":{"S":"<CORTE>"}}' --select COUNT

# 4. dedup de URLs sin marcas de la ventana
aws dynamodb scan --table-name poly-rag-processed-urls \
  --filter-expression "processed_at > :t" \
  --expression-attribute-values '{":t":{"S":"<CORTE>"}}' --select COUNT

# 5. contar odds files vs registry items (deben cuadrar)
aws s3 ls s3://poly-rag-369970405415/odds/ | wc -l
aws dynamodb scan --table-name poly-rag-market-registry --select COUNT --output text
```

Los 4 primeros deben dar 0 / vacio. El 5to debe cuadrar entre si.

**Script de verificacion completo (usado y validado el 2026-08-18):** revisa los 6
puntos de una pasada e imprime OK/FALLA por cada uno, mas el conteo final del registry
y la distribucion de contadores post-resolucion. Correrlo hasta que diga
`TABULA RASA CONFIRMADA`:

```python
CUT = '2026-08-18T21:00:00'   # ajustar al incidente
# 1. archivos S3 con la hora espuria en el key           -> esperado 0
# 2. registry items con first_seen >= CUT                -> esperado 0
# 3. registry items con resolution_date >= CUT           -> esperado 0
# 4. metricas con timestamp >= CUT                       -> esperado 0
# 5. processed_urls con processed_at >= CUT              -> esperado 0
# 6. processed_comments con processed_at >= CUT          -> esperado 0
# INFO: total registry + distribucion de status
# INFO: distribucion de post_resolution_cycles_remaining entre los resueltos
#       (debe ser {4: N}, NO {0: N} -- si es 0, falta el paso 2.8)
```

**Estado esperado tras una limpieza exitosa** (incidente 2026-08-18): registry de
vuelta en 595 items (502 open / 93 resolved), identico al previo al incidente, y los
93 contadores en 4.

**No olvidar revertir el Paso 0** (quitar el limite de concurrencia), o el proximo
ciclo automatico no correra:

```bash
aws lambda delete-function-concurrency --function-name poly-rag-send-digest
```

---

## Paso 4 -- LO QUE NO SE PUEDE DESHACER

Ser honesto sobre esto en vez de fingir tabula rasa perfecta:

- **Correos ya enviados via SES.** No hay forma de recuperarlos. El usuario los borra
  a mano de su bandeja.
- **Costo de Bedrock ya gastado.** Cada corrida espuria gasta tokens reales (el filtro
  de verificabilidad del LLM + los resumenes de News + el executive summary del
  digest). Ese gasto ya ocurrio; borrar las metricas solo limpia el REGISTRO del
  gasto, no lo reembolsa. Vale la pena anotar cuanto fue antes de borrar las filas de
  metricas, para saber el costo real del incidente.
- **Rate limits consumidos** contra Gamma API / Google News / outlets. Se recuperan
  solos con el tiempo, pero pueden causar 429s temporales.

---

## Prevencion (mas importante que la remediacion)

**Regla dura, en CLAUDE.md:** NUNCA invocar `poly-rag-ingest-polymarket`,
`poly-rag-ingest-news`, `poly-rag-ingest-comments` ni `poly-rag-send-digest` sin
confirmacion explicita del usuario en ese mismo momento -- incluido "solo para
verificar". No existe invocacion "inofensiva" de estas Lambdas: `ingest_polymarket`
dispara la cadena entera hasta el correo.

**Como verificar un cambio SIN invocar:**
1. `python3 -m py_compile` sobre el handler (sintaxis).
2. Importar el handler localmente y probar funciones puras contra datos reales de
   lectura (asi se verificaron `fetch_clob_price_history`,
   `decrement_post_resolution_counter` y `get_open_markets` el 2026-08-18, sin
   disparar nada).
3. Revisar CloudWatch Logs del PROXIMO ciclo automatico -- si el cambio es correcto,
   se ve ahi sin costo ni riesgo adicional.
4. Si de verdad hace falta invocar: pedirlo explicitamente al usuario, y considerar
   primero poner `send_digest` en concurrencia 0 para que la cadena corra sin mandar
   correo.

**Mejora pendiente (no implementada):** un flag en el payload
(`{"skip_chain": true}`) que permita ejecutar `ingest_polymarket` sin invocar la
siguiente etapa -- convertiria "probar sin efectos secundarios" en algo posible por
diseno en vez de por disciplina. Ver tech_debt.md.
