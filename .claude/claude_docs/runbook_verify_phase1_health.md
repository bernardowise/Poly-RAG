# Runbook: Verificar que un ciclo automatico corrio sano de principio a fin

**Complementario a `runbook_manual_invocation_cleanup.md`, no lo reemplaza.** Ese
runbook prueba AUSENCIA ("no quedo rastro de algo que no debio pasar" -- todos sus
chequeos esperan 0). Este runbook prueba PRESENCIA Y CORRECCION ("el ciclo legitimo
de esta hora escribio exactamente lo que debia escribir"). Son preguntas opuestas;
usar uno para responder al otro da falsos positivos.

**Cuando usarlo:** despues de cada ciclo automatico (00:00 / 12:00 UTC), o
especificamente despues de desplegar un cambio a la cadena de ingestion, para
confirmar que el proximo ciclo real lo probo correctamente -- sin invocar nada
manualmente (ver CLAUDE.md, regla dura sobre invocar Lambdas de produccion).

---

## Paso 0 -- identificar el ciclo a verificar

```bash
DAY=2026-08-20   # fecha UTC del ciclo
HH=00            # hora UTC del ciclo (00 o 12)
```

Todo lo siguiente asume esas dos variables.

---

## Paso 1 -- las 4 etapas existen y son completas

```bash
aws s3 ls s3://poly-rag-369970405415/polymarket/$DAY/$HH.json
aws s3 ls s3://poly-rag-369970405415/news/$DAY/$HH.json
aws s3 ls s3://poly-rag-369970405415/comments/$DAY/$HH.json
aws s3 ls s3://poly-rag-369970405415/digest/$DAY/$HH.json
```

Las 4 deben existir. Si falta alguna, la cadena se corto -- revisar CloudWatch Logs de
la etapa anterior antes de seguir con el resto de este runbook.

**News especificamente debe llegar a `cycle_complete: true`:**

```python
import boto3, json
s3 = boto3.client('s3'); B = 'poly-rag-369970405415'
n = json.loads(s3.get_object(Bucket=B, Key=f'news/{DAY}/{HH}.json')['Body'].read())
print('cycle_complete:', n['metadata']['cycle_complete'])
print('markets_queried:', n['markets_queried'], '| markets_processed:', n['markets_processed'])
# deben ser iguales -- si markets_processed < markets_queried, algun batch nunca
# termino o nunca se mergeo
```

No deben quedar archivos `_batch<offset>.json` huerfanos de este ciclo sin mergear
(el merge los deja existir, eso es normal -- lo anormal es que el archivo FINAL
`$HH.json` no exista mientras los batches si).

---

## Paso 2 -- el lock de encadenamiento se reclamo EXACTAMENTE una vez

**Especifico para el fix del 2026-08-19** (ver tech_debt.md, "Bug de Doble-Disparo").
Antes de este fix, este era precisamente el chequeo que faltaba -- nada detectaba que
mas de un batch de News hubiera encadenado a Comments.

```python
import boto3
d = boto3.resource('dynamodb').Table('poly-rag-cycle-chain-locks')
# el pk es el cycle_started_at exacto -- leer el ISO real del payload de News,
# NO asumir el HH:00:00 nominal (el ciclo puede arrancar unos segundos despues)
cycle_started_at = n['cycle_started_at']   # del paso 1
item = d.get_item(Key={'pk': cycle_started_at}).get('Item')
print('lock existe:', item is not None)
```

Debe existir exactamente un item. Si no lo encuentras con el `cycle_started_at`
exacto, confirma que no haya mas de un valor de `cycle_started_at` circulando para
esta hora (senal de que el fix no esta corriendo o de un ciclo verdaderamente
duplicado).

**Cuenta real de invocaciones de Comments y digest, cruzada contra CloudWatch** (la
prueba mas directa de que el lock funciono):

```bash
for fn in poly-rag-ingest-comments poly-rag-send-digest; do
  n=$(aws logs filter-log-events --log-group-name /aws/lambda/$fn \
      --start-time $(($(date +%s -d "$DAY $HH:00:00 UTC")*1000)) \
      --end-time $(($(date +%s -d "$DAY $HH:00:00 UTC + 30 minutes")*1000)) \
      --filter-pattern "START RequestId" --query 'length(events)' --output text | head -1)
  echo "$fn: $n invocaciones (debe ser 1)"
done
```

Si `ingest_comments` o `send_digest` muestran mas de 1, el lock no esta funcionando --
tratar como incidente y usar `runbook_manual_invocation_cleanup.md`.

---

## Paso 3 -- el registry crecio/cambio de forma coherente

```python
import boto3
from collections import Counter
d = boto3.resource('dynamodb').Table('poly-rag-market-registry')
items = []; r = d.scan()
items += r['Items']
while 'LastEvaluatedKey' in r:
    r = d.scan(ExclusiveStartKey=r['LastEvaluatedKey']); items += r['Items']

nuevos = [i for i in items if i.get('first_seen','').startswith(f'{DAY}T{HH}')]
print('markets nuevos este ciclo:', len(nuevos))
print('  todos con created_at:', all('created_at' in i for i in nuevos))

resueltos = [i for i in items if (i.get('resolution_date') or '').startswith(f'{DAY}T{HH}')]
print('markets resueltos este ciclo:', len(resueltos))
print('  todos con post_resolution_cycles_remaining == 4:',
      all(int(i.get('post_resolution_cycles_remaining', 0)) == 4 for i in resueltos))
```

- Todo market nuevo debe tener `created_at` (nativo desde F-lambdas, sin backfill
  manual).
- Todo market recien resuelto debe arrancar su contador en **4**, no menos -- si
  aparece en 3 o menos, el decrement corrio antes de que el arranque se confirmara
  (orden de escritura sospechoso, investigar).

---

## Paso 4 -- odds: snapshots de ciclo y backfill nativo, ambos con provenance

```python
import boto3, json
from collections import Counter
s3 = boto3.client('s3'); B = 'poly-rag-369970405415'

# de una muestra de markets nuevos del paso 3
for mid in [i['market_id'] for i in nuevos[:5]]:
    try:
        o = json.loads(s3.get_object(Bucket=B, Key=f'odds/{mid}.json')['Body'].read())
        srcs = Counter(s.get('source', 'MISSING') for s in o['snapshots'])
        print(mid, ':', dict(srcs))
    except Exception as e:
        print(mid, ': SIN ARCHIVO odds --', e)
```

- Ningun snapshot deberia salir `MISSING` de `source` (fix nativo desde el
  2026-08-18 -- ver tech_debt.md, "Cycle Snapshots Explicitly Tagged source=cycle").
- Si el market tiene historia previa (creado antes de este ciclo) Y ya tenia
  snapshots de un ciclo anterior, confirmar que esos snapshots viejos SIGAN
  presentes -- el fix del merge (2026-08-19) es justo para que el backfill nativo no
  los borre.

---

## Paso 5 -- News: articulos con clasificacion completa

**Este paso FALLA, no se salta, si el campo falta.** La primera version de este
runbook (2026-08-19) tenia esto como "nota de alcance" en vez de chequeo real --
literalmente decia "mientras no se conecte, este paso se salta". Eso permitia un
runbook 4/4 verde mientras 1,798 articulos (2 ciclos completos) no tenian ninguno
de los dos campos, y el corpus no estaba listo ni para empezar a diseñar chunking.
El usuario lo detecto en vivo, no el runbook. Correccion: un runbook de salud no
debe tener una clausula de "esto no cuenta todavia" -- si el campo es requerido,
su ausencia es una falla, punto.

```python
import boto3, json
s3 = boto3.client('s3'); B = 'poly-rag-369970405415'
n = json.loads(s3.get_object(Bucket=B, Key=f'news/{DAY}/{HH}.json')['Body'].read())
sin_tier = sum(1 for a in n['articles'] if 'temporal_tier' not in a)
sin_status = sum(1 for a in n['articles'] if 'market_status_at_publish' not in a)
print(f'sin temporal_tier: {sin_tier}/{len(n["articles"])}')
print(f'sin market_status_at_publish: {sin_status}/{len(n["articles"])}')
assert sin_tier == 0, f'FALLA: {sin_tier} articulos sin temporal_tier este ciclo'
assert sin_status == 0, f'FALLA: {sin_status} articulos sin market_status_at_publish este ciclo'
```

Cerrado 2026-08-19: `classify_temporal_tier`/`classify_market_status` se copiaron
verbatim de los scripts one-off directo a `lambdas/ingest_news/handler.py` (mismo
patron que las funciones standalone ya preveian), asi que desde el proximo deploy
todo articulo nuevo sale ya clasificado. Si este paso vuelve a fallar en el futuro,
es una regresion real -- no un gap de diseño conocido.

---

## Resultado esperado

Un ciclo sano da: 4 archivos presentes, `cycle_complete: true`,
`markets_queried == markets_processed`, exactamente 1 lock y 1 invocacion de
Comments/digest, markets nuevos con `created_at`, markets resueltos con contador en 4,
snapshots con `source` siempre presente.

Cualquier desviacion no es necesariamente un incidente como el del 2026-08-18 (no
implica invocacion manual) -- puede ser un bug nuevo del propio ciclo automatico.
Documentar en tech_debt.md como se hizo con los bugs encontrados el 2026-08-19, no
descartar como ruido.
