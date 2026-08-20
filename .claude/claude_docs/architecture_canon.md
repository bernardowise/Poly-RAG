# Architecture Canon

**Snapshot del estado actual de la arquitectura de Poly-RAG.** Este documento se
actualiza/sobreescribe conforme la arquitectura evoluciona -- no es una bitacora de
decisiones pasadas (eso vive en session_ledger.md) ni una lista de pendientes (eso
vive en tech_debt.md). Si algo aqui queda obsoleto, se reemplaza, no se acumula.

Ultima actualizacion: 2026-08-18 (Dia 4 -- F-lambdas desplegado: created_at, odds history
retroactivo, y captura post-resolucion ahora nativos en ingest_polymarket/ingest_news)

**Estado del corpus al 2026-08-18** (medido directo contra S3/DynamoDB, no estimado): 6
ciclos completos, registry con 595+ markets (crece cada ciclo -- 9 markets nuevos entraron
solo durante la verificacion de F-lambdas esta misma tarde). Las cifras de 285 items / 4
ciclos que aparecen mas abajo en las notas de limpieza del 2026-08-17 son historicas y
correctas para ESA fecha.

**Registry, campos agregados 2026-08-18 (ver tech_debt.md para el diseno completo de cada
uno):** ademas de los campos ya documentados abajo,
- `created_at` -- fecha REAL de creacion del market en Polymarket (de la misma respuesta
  Gamma que ya se leia, cero costo extra), distinta de `first_seen` (cuando NOSOTROS
  empezamos a trackear). Backfilleado a los 595 markets pre-existentes
  (`scripts/backfill_registry_created_at.py`), y escrito nativamente por
  `upsert_registry_entry` para cualquier market que entre de aqui en adelante.
- `post_resolution_cycles_remaining` -- contador para la captura de noticias
  post-resolucion (ver "Post-Resolution News Capture" en tech_debt.md). Arranca en 0 al
  crear el registro; `mark_registry_resolved` lo pone en 4 en el momento exacto de la
  transicion open->resolved (nunca antes, nunca despues); `ingest_news` lo decrementa cada
  ciclo que el market siga incluido en su busqueda, con guarda contra valores negativos.

**Odds time-series, tras el backfill de historia pre-tracking (ver tech_debt.md, "Odds
History Backfill from Polymarket CLOB"):** 595 archivos, **63,641 snapshots totales**
(62,273 `clob_backfill` + 1,368 `cycle`, tag explicito en ambos -- ver tech_debt.md, "Cycle
Snapshots Explicitly Tagged source=cycle"). Antes del backfill cada market solo tenia
historia desde que entro al registry (1-6 snapshots); ahora 487/595 markets tienen historia
retroactiva hasta su `createdAt` real, hasta 375 puntos diarios en el caso mas profundo
(market creado 2025-07-03). Los 108 restantes simplemente nacieron despues del 2026-08-16 y
no tienen pre-historia que traer.

---

## Data Sources (Ingestion Layer)

Cuatro Lambdas independientes, no un orquestador unico -- una falla en una no tumba
las otras, y los reintentos no desperdician PUT requests de las fuentes que si
funcionaron.

**Criterio real de separacion (aclarado 2026-08-16): aislamiento de fallos, no "una
Lambda por dominio de API".** Polymarket (odds) y Comments comparten el mismo
dominio (`gamma-api.polymarket.com`, sin auth) -- solo endpoints distintos
(`/markets` vs `/comments`) -- y aun asi viven en Lambdas separadas. La razon no es
"APIs distintas ameritan Lambdas distintas": es que odds (la serie de tiempo, el
diferenciador real del proyecto) y comments (una fuente de sentiment mas ruidosa,
con su propio rate limit especifico de 200 req/10s) tienen perfiles de fallo y
criticidad distintos -- un timeout o rate-limit en Comments no debe poder bloquear
la escritura del snapshot de odds de ese ciclo. Fusionarlas ahorraria una Lambda
pero reintroduciria el acoplamiento que la separacion original (Dia 2) buscaba
evitar. `ingest_polymarket` SI prepara el lookup (`comment_entity_type`/
`comment_entity_id`, extraidos de la misma respuesta de `/markets` que ya trae
`events[]`) pero no hace la llamada a `/comments` el mismo -- esa llamada, y su
propio riesgo de fallo, vive enteramente en `ingest_comments`.

**Rediseno 2026-08-15 (ver tech_debt.md, "Ingestion Redesign"):** el filtro estatico
de 3 verticales por keyword (Macro/Geopolitica/Regulatorio-Tech) fue reemplazado por
completo. Ver seccion "Market Registry + Ingestion Redesign" mas abajo para el
diseno vigente -- esta seccion documenta solo el "shape" fisico de cada Lambda
(API, formato, resiliencia), no la logica de filtrado/tagging, que cambio.

**Metadata de linaje (agregado 2026-08-14):** cada JSON escrito a S3 lleva un bloque
`metadata` con `schema_version`, `lambda_name`, `lambda_request_id` (para rastrear el
archivo de vuelta a su ejecucion exacta en CloudWatch Logs), `llm_used` + `llm_model_id`,
`latency_ms`, y `estimated_cost_usd` -- permite auditar linaje/costo de un archivo
individual sin tener que cruzar con la tabla DynamoDB de metricas.

### 1. Polymarket (`poly-rag-ingest-polymarket`)
- **API:** Gamma API (`gamma-api.polymarket.com`), REST/JSON, sin auth
- **Filtro:** `active=true`, top 500 ordenado por `volume24hr` (no `volume` total --
  volume24hr refleja actividad reciente, volume total favorece mercados viejos/muertos)
- **Filtrado/tagging:** ya no es keyword-match -- ver "Market Registry + Ingestion
  Redesign" mas abajo
- **Timeout:** 600s (subido de 60s -- el ciclo paginado + hasta ~25 llamadas Bedrock
  secuenciales necesita mas margen, especialmente en corridas de bootstrap)
- **Output:** `s3://poly-rag-369970405415/polymarket/YYYY-MM-DD/HH.json` (resumen de
  la corrida, no el payload completo de mercados -- ver seccion siguiente)

### 2. News (`poly-rag-ingest-news`)
- **Rediseno 2026-08-16 (ver tech_debt.md, "News Source Redesign"):** reemplaza
  por completo los antiguos 10 feeds RSS curados (BBC/CBC/NYT/CNN/France24) y su
  matching por keywords. Quedaron obsoletos tras auditar el linkeo real (367
  articulos, solo 3 linkeados) y confirmar que el root cause no era el matching
  sino el vocabulario -- el LLM generaba terminos formales de Polymarket
  ("Federal Reserve") en vez de vocabulario periodistico real ("Fed").
- **Fuente:** Google News RSS (`news.google.com/rss/search?q=<question>`), una
  busqueda por cada market abierto en el registry, usando `question` TAL CUAL
  como query (sin keywords generadas, sin LLM en este paso) -- delega el
  problema de sinonimos/parafraseo al motor de relevancia de Google.
- **Excepcion consciente de ToS:** el copyright de la respuesta de este endpoint
  restringe uso a "personal feed reader... personal, non-commercial use" -- un
  pipeline automatizado que alimenta un dataset almacenado esta fuera de ese
  grant. Decision informada y explicita del usuario, documentada con el
  paralelismo al caso Reddit en tech_debt.md.
- **Pipeline de extraccion:** `googlenewsdecoder` resuelve el link ofuscado de
  Google al URL real del articulo (sin headless browser); `trafilatura` extrae
  el texto del cuerpo (`DOWNLOAD_TIMEOUT` bajado de 30s default a 8s tras medir
  que un solo outlet bloqueado podia consumir 30s antes de fallar). Articulos
  que fallan extraccion se DESCARTAN, no caen a fallback de titulo-only -- el
  top 5 por market debe ser cobertura completa, no parcial.
- **Dedup:** por URL exacta (no por similitud de evento), tabla DynamoDB
  `poly-rag-processed-urls` (hash key `url`), sin TTL -- pay-per-request no
  cobra por storage ocioso, y el contenido extraido vive permanente en S3 de
  cualquier forma.
- **Blocklist dinamico de dominios (2026-08-16):** dominios con 5+ fallos
  consecutivos de extraccion (contador reset a 0 en cualquier exito, tabla
  DynamoDB `poly-rag-domain-failures`, hash key `domain`) se saltan sin
  intentar el request -- ej. egamersworld.com, confirmado fallando 100% de
  las veces en produccion. Ver tech_debt.md, "Dynamic Domain Blocklist".
- **Batching (fan-out paralelo, revisado 2026-08-16 mismo dia que el primer
  deploy):** ~21s/market medido end-to-end (search + decode + extract) hace
  que los ~228 markets del registry (~80 min secuenciales) no quepan en una
  sola invocacion bajo el maximo duro de Lambda (900s). La Lambda procesa en
  batches de 35 markets (`BATCH_SIZE`). El diseno original era self-chaining
  (cada batch invoca solo al siguiente) pero la primera corrida real mostro
  ~10-13 min por batch, proyectando ~1.5h para los 7 batches secuenciales --
  demasiado lento. Cambiado a fan-out: la primera invocacion (offset=0)
  dispara TODOS los batches restantes de una vez via `lambda.invoke()`
  asincrono, escalonados `DISPATCH_STAGGER_SECONDS=3` entre cada uno (evita
  un burst simultaneo contra los mismos outlets/Google, sin meter
  infraestructura de proxy/VPN -- evaluado y descartado: no hay forma barata
  de rotar la IP de salida de Lambda por batch, NAT Gateway es una IP fija y
  rompe el budget por si solo, proxies rotativos de terceros agregan costo y
  dependencia externa para lo que en realidad es un tema de concurrencia de
  requests, no de identidad de IP). Cada batch escribe a su PROPIO S3 key
  (`_batch<offset>.json`, no un key compartido -- escritura concurrente al
  mismo key competiria y perderia datos del batch que no gano la carrera) y
  un paso de merge (`merge_batch_payloads`, idempotente) combina todos los
  archivos por-batch en el payload final una vez que todos existen. Sin
  checkpoint persistido -- el offset vive solo en el payload de cada invoke.
  Timeout 900s, memory 512MB, `lambda:InvokeFunction` scoped a su propio ARN.
- **Tagging:** cada articulo se etiqueta con `market_ids` (puede ser [], uno, o
  varios) segun a que busqueda de market perteneciera. Todos los articulos
  extraidos exitosamente se conservan.
- **Output:** `s3://poly-rag-369970405415/news/YYYY-MM-DD/HH.json`, schema
  version v3 (payload de ciclo completo, merge de los `_batch<offset>.json`
  intermedios una vez `cycle_complete: true`; los archivos intermedios se
  borran despues del merge, no persisten)
- **Verificado en produccion (2026-08-16):** primer ciclo completo end-to-end:
  228/228 markets procesados, `cycle_complete: true`, 887 articulos, 0
  duplicados de URL pese a que offset=105 corrio dos veces durante la
  transicion de codigo secuencial a paralelo (dedup via
  `poly-rag-processed-urls` sostuvo). Batches paralelos completaron en
  294s-611s cada uno vs. ~600-800s secuenciales -- ahorro real de tiempo de
  reloj por no esperar a que un batch termine del todo antes de arrancar el
  siguiente. Ver tech_debt.md ("News Source Redesign" update) para la
  narrativa completa, incluyendo el merge manual unico requerido por la
  transicion mid-deploy (no aplica a corridas futuras).

### 3. Comments (`poly-rag-ingest-comments`)
**Reemplaza a Bluesky (2026-08-16, ver tech_debt.md "Comments Source Replaces
Bluesky"):** auditoria de datos reales mostro que la mayoria de los posts de
Bluesky eran bots republicando el propio feed de precios de Polymarket, no
sentiment humano independiente -- señal circular. Los comentarios nativos de
Polymarket (`gamma-api.polymarket.com/comments`, publico, sin auth,
documentado oficialmente) son discusion real de traders.

- **API:** Gamma API, mismo dominio ya usado para market data. Sin auth.
- **Lookup, no busqueda:** a diferencia de News/la Bluesky vieja (texto
  libre), Comments consulta directo por `event_id` o `series_id` -- cada
  market en el registry lleva `comment_entity_type`/`comment_entity_id`
  (poblados por `ingest_polymarket`, sin LLM, directo de `events[]` en la
  respuesta de la Gamma API). No se necesita `search_query` para esto.
- **Agrupacion por entidad:** varios markets pueden compartir el mismo
  event/series -- se agrupan antes de llamar a la API, asi una entidad
  compartida por 9 markets se consulta 1 vez, no 9.
- **Tres niveles de `link_type` (corregido 2026-08-16, mismo dia del primer
  test):**
  - `direct`: comentarios a nivel Event, Y ese event tiene exactamente 1
    market abierto -- 1:1 real.
  - `shared_event`: comentarios a nivel Event, pero el event agrupa VARIOS
    markets (ej. torneo de esports con 1 market por equipo, todos bajo la
    misma seccion de comments) -- encontrado en produccion: 802/1698
    comentarios "direct" en el primer test en realidad tenian multiples
    market_ids, lo cual rompia la promesa 1:1 de "direct" por definicion.
  - `shared_series`: comentarios a nivel Series, compartidos entre TODOS los
    markets de esa serie/liga, a menudo eventos no relacionados entre si
    (verificado: dos partidos de MLB distintos mostraron el mismo hilo de
    comentarios, incluyendo comentarios sobre un tercer partido). Prioridad
    de lookup: Event primero, Series solo si el Event no tiene comentarios.
  - Los tres niveles etiquetan TODOS los market_ids aplicables (no eligen
    uno arbitrario) -- colapsar shared_event/shared_series a un solo
    market_id exageraria la precision real del linkeo.
- **Timeout:** 300s, memory 256MB.
- **Verificado en produccion (2026-08-16):** 295/329 markets con cobertura
  de comentarios (89%), 2506 comentarios reales, 94 llamadas a la API (por
  agrupacion), 0 fallos. Distribucion: 896 direct / 802 shared_event / 808
  shared_series.
- **Output:** `s3://poly-rag-369970405415/comments/YYYY-MM-DD/HH.json`

**Bluesky (descontinuado 2026-08-16), reemplazaba a:** Reddit (descartado --
su Responsible Builder Policy prohibe uso de datos para IA/ML), X/Twitter (sin
tier gratuito viable + prohibicion identica), Truth Social (sin API publica
para individuos). Ninguna de esas exclusiones cambia -- Comments no las
revive, simplemente Bluesky en si dejo de ser la mejor opcion disponible.

### 4. Email Digest (`poly-rag-send-digest`)
- **Proposito:** ultimo eslabon de la cadena estricta (ver "Strict Ingestion
  Chaining" en tech_debt.md) -- consolida el ciclo completo (odds/News/Comments)
  en un artefacto JSON estructurado (`digest/YYYY-MM-DD/HH.json`, pensado para
  ingestion RAG futura) y un email HTML generado A PARTIR de ese JSON, no al reves
- **Contenido sintetizado (rediseño 2026-08-16, "Bespoke Digest Redesign"):**
  `newly_tracked_markets`/`resolved_markets` (con outcome real), `top_volatility`
  (que se MOVIO este ciclo, ranking por delta de precio entre snapshots),
  `world_snapshot` (agregado 2026-08-16 -- que CREE el mercado ahora mismo,
  independiente de si se movio: `top_conviction` = top 5 por `volume24hr` del
  ultimo snapshot, `most_disputed` = top 5 mas cercanos a 50/50 dentro de una
  banda 40-60%, ambos derivados del mismo pase de lectura de S3 que ya usa
  `top_volatility`, sin escaneo extra), `quotes` (verbatim, no parafraseado), y
  `executive_summary` (un llamado Bedrock que ve las 3 fuentes + world_snapshot
  juntas y escribe 2-3 oraciones de narrativa)
- **Envio:** Amazon SES (`ses:SendEmail`), remitente y destinatario verificados
  (`bernardolw@gmail.com`, modo sandbox) -- ver tech_debt.md, "Digest Emails Land
  in Spam", DKIM sin resolver, deliberado
- **Trigger:** invocado directo por `ingest_comments` al terminar (encadenamiento
  estricto, no EventBridge propio -- ver "Strict Ingestion Chaining" en
  tech_debt.md), con `cycle_started_at` heredado de toda la cadena para su propia
  key de S3
- **Degradacion graceful:** si un source payload no esta disponible, reporta "no
  data" para esa fuente en vez de fallar la Lambda completa
- **Permisos:** `s3:GetObject`/`ListBucket` + `s3:PutObject` (agregado en el
  rediseño de digest, antes solo lectura), `dynamodb:GetItem`/`Scan` en el
  registry, `bedrock:InvokeModel` para el executive summary
- **Estado (2026-08-16, world_snapshot):** implementado en el handler, NO
  desplegado a AWS todavia -- pendiente `terraform apply`/deploy del zip

---

## Market Registry + Ingestion Redesign (vigente desde 2026-08-15)

Reemplaza por completo el filtro estatico de 3 verticales por keyword (Macro/
Geopolitica/Regulatorio-Tech). Razonamiento completo, incluyendo los datos reales
de volume24hr que justificaron el cambio de N, en tech_debt.md ("Ingestion
Redesign" entry) -- esta seccion documenta el diseno resultante, no el porque.

**El eje de filtrado correcto no es tema, es verificabilidad.** Un market pasa el
filtro si su outcome se resuelve contra un registro publico citable (comunicado
oficial, conteo certificado, precio de mercado) en vez de juicio humano sobre
evidencia ambigua (rumores, disputas sin registro oficial). Bajo este eje, un
mercado de deportes con marcador oficial es MAS verificable que uno de rumores de
celebridades, aunque el filtro viejo excluia deportes por completo sin evaluar esto.

### Market Registry (DynamoDB: `poly-rag-market-registry`)

Un item por `market_id`, actualizado in-place (metadata, cambia poco -- una vez al
crearse, una vez al resolverse). Campos: `question`, `description`, `end_date`,
`resolution_source`, `status` (open/resolved), `comment_entity_type`,
`comment_entity_id`, `comment_link_type`, `first_seen`, `last_updated`,
`resolution_date`, `final_outcome`.

**Poblado por `ingest_polymarket`:**
1. Trae top 500 candidatos activos por `volume24hr` -- este fetch a la Gamma API
   SIEMPRE es completo cada ciclo, sin excepcion. El "diff" contra el registry no
   evita re-traer el top-500; solo decide que subset de esos candidatos pasa por
   el paso caro (LLM). No hay forma de pedirle a Gamma "solo lo nuevo" sin
   primero tener la lista completa para comparar contra el registry (verificado
   leyendo `fetch_top_markets_by_volume24hr` en el handler, 2026-08-16).
2. Diffea contra el registry (`Scan` completo de DynamoDB cada ciclo) -- solo los
   ids genuinamente nuevos pasan por el LLM (costo escala con tasa de aparicion
   de ids nuevos, no con N -- ver tech_debt.md). Los ids ya conocidos se saltan
   Bedrock por completo.
   **Lo que SI corre cada ciclo para TODO market abierto del top-500, nuevo o
   no:** el snapshot de odds (ver "Odds Time-Series" abajo) -- el diff solo
   protege el paso LLM, no la escritura de la serie de tiempo, que por diseno
   debe ser cada ciclo para todos.
3. Llamada batched a Bedrock (20 markets/batch) que devuelve, por market, SOLO el
   veredicto de verificabilidad (`is_verifiable`) -- simplificado 2026-08-16,
   ya no genera texto de busqueda (ver nota abajo)
4. Solo los verificables entran al registry; el resto se descarta
5. `comment_entity_type`/`comment_entity_id`/`comment_link_type` se extraen
   directo de `events[]` en la respuesta de la Gamma API, sin LLM -- ver
   `get_comment_link` en el handler y la seccion Comments arriba
6. Ids que estaban `open` pero ya no aparecen en el top-500: se consulta
   `markets/{id}` directo en la Gamma API para capturar `closed` + outcome real
   (no se infiere resolucion por ausencia)

**`search_query`/`news_match_terms` removidos (2026-08-16):** el diseño original
(ver tech_debt.md, "Ingestion Redesign") generaba dos representaciones de
busqueda por LLM para que News y Bluesky las consumieran. Ambas quedaron
obsoletas por rediseños posteriores: News usa `question` verbatim contra Google
News RSS (ver "News Source Redesign"), y Comments (que reemplaza a Bluesky) hace
lookup directo por `event_id`/`series_id`, no busqueda de texto. El campo salio
del prompt del LLM y del schema del registry -- ya no se genera ni se guarda.

**Auditoria (Bronze layer):** el prompt completo y la respuesta cruda del LLM se
imprimen a CloudWatch Logs, y las respuestas crudas se guardan tambien en
`metadata.llm_raw_responses` del payload S3 -- permite re-inspeccionar exactamente
que dijo el modelo si un item del registry se ve mal clasificado.

**Robustez:** un batch con JSON malformado/truncado (max_tokens insuficiente,
observado en produccion) se salta sin tumbar la Lambda completa -- esos markets
simplemente se re-evaluan el siguiente ciclo (siguen siendo "nuevos" mientras no
entren al registry).

**Nota (2026-08-16):** el mismo patron de "sigue siendo nuevo cada ciclo hasta
que entre al registry" aplica a un market nuevo en el top-500 que resuelve en
menos de 48h -- el filtro de horizonte minimo (ver tech_debt.md, bug de
`endDate` para deportes) lo descarta ANTES del paso LLM, asi que nunca entra al
registry y se re-evalua (y se re-descarta) cada ciclo mientras siga apareciendo
en el top-500. No es un bug nuevo, es consecuencia directa del bug de horizonte
ya documentado.

### Odds Time-Series (S3: `odds/<market_id>.json`)

Append-only, un archivo por market -- esta ES la diferenciacion real del proyecto
(self-built historical time-series). Cada archivo mezcla DOS origenes distintos de
snapshot, distinguibles por un campo `source` explicito (nunca inferido -- ver
tech_debt.md, "Cycle Snapshots Explicitly Tagged source=cycle"):

- **`source: "cycle"`** -- un snapshot agregado por `ingest_polymarket` cada ciclo
  (12h), nunca sobreescrito. Campos: `timestamp`, `outcomePrices`, `volume`,
  `volume24hr`, `liquidity`. Read-modify-write por ciclo: lee el archivo existente
  (o inicia uno nuevo si no existe -- requiere `s3:ListBucket` a nivel bucket ademas
  de `s3:GetObject`, ver nota IAM abajo), agrega el snapshot, reescribe.
- **`source: "clob_backfill"`** -- historia pre-tracking desde
  `clob.polymarket.com/prices-history`, gratis y sin auth, hasta la fecha de creacion
  real del market. Campos: solo `timestamp`, `outcomePrices`, `source`,
  `backfilled_at` -- SIN `volume`/`volume24hr`/`liquidity` (el endpoint CLOB no los
  expone), lo cual es una segunda senal estructural independiente del campo `source`
  para distinguir el origen. Un cutoff duro impide escribir cualquier punto en o
  despues del momento en que el market entra al registry -- el backfill solo puede
  tocar el pasado, nunca el ciclo trackeado.
  - **Dos caminos, mismo mecanismo (ver tech_debt.md para ambos):** (1) un backfill
    manual one-off (`scripts/backfill_odds_history.py`) corrido 2026-08-18 sobre los
    595 markets ya trackeados en ese momento -- 62,273 snapshots recuperados, dos bugs
    reales encontrados via dry run antes de escribir (alineacion de timestamps entre
    tokens perdiendo ~75% de la historia real, y el endpoint `?id=` de Gamma ocultando
    markets `closed:true`); (2) **desde F-lambdas (2026-08-18, mismo dia), tambien
    corre NATIVAMENTE dentro de `ingest_polymarket`** -- `backfill_odds_history_for_new_market`,
    invocada automaticamente justo despues de que un market entra al registry por
    primera vez. Ningun market nuevo entra ya de aqui en adelante con una serie de
    tiempo vacia. Verificado en produccion real (dos invocaciones live) el mismo dia:
    un market creado 2025-11-11 trajo 255 snapshots backfilleados automaticamente al
    momento de entrar al registry, cero intervencion manual.

**Deliberadamente fuera de alcance (decision explicita del usuario, 2026-08-18):**
recuperar el HISTORIAL DE NOTICIAS que corresponde a esa ventana pre-tracking. Google
News RSS no soporta busqueda por rango de fecha arbitrario, asi que hacerlo seria un
proyecto aparte, no una extension de este. El backfill de odds es "lo mas atras que
vamos a ir" -- ver tech_debt.md para el razonamiento completo.

### Capa semantica de Polymarket: `question`/`description` del registry (cerrado 2026-08-20)

**No es una cuarta fuente -- es la mitad semantica de la MISMA fuente Polymarket** (odds es
la mitad estructurada). El registry (`poly-rag-market-registry`) tiene exactamente dos
campos de texto libre en lenguaje natural: `question` (titulo del market, ej. "Will the
price of Bitcoin be above $66,000 on August 20?") y `description` (contenido exacto aun sin
confirmar contra un item real -- presumiblemente reglas/criterio de resolucion). Todo lo
demas del registry es estructurado (fechas, status, IDs, outcome).

**Por que hace falta:** F1-F5 (filtro estructurado de odds) asumen que ya se tiene el
`market_id` exacto. Pero una pregunta como "dame los markets de Bitcoin" no trae un
`market_id` -- es texto libre. Sin un indice semantico sobre `question`/`description`, no
hay forma de resolver esa pregunta a un `market_id` sin que el usuario ya lo supiera de
antemano. Este indice es el paso 1 (texto libre -> `market_id`(s)) que habilita luego el
paso 2 (filtro exacto F1-F5 sobre odds, o los indices semanticos ya filtrados de News/
Comments).

**Diseño:**
- Unidad de embedding = 1 vector por `market_id`, SIN chunking -- `question`/`description`
  son texto corto por market, no articulos largos que necesiten split.
- Trigger = evento, no cadencia de ciclo: se embede cuando el market entra al registry por
  primera vez (`newly_tracked_markets` de un ciclo), no se re-embede en cada ciclo -- el
  registry mismo dice que estos campos "cambian poco, una vez al crearse, una vez al
  resolverse", asi que solo haria falta re-embeder si `description`/`question` cambian tras
  la resolucion (caso raro, no verificado si aplica en la practica).
- Metadata: `market_id`, `status`, `end_date`/`resolution_date`.
- **Decidido 2026-08-20: `question` y `description` van COMBINADOS en un solo embedding**
  (`f"{question}\n\n{description}"`), no separados. Razon: el proposito de este indice es
  resolver texto libre -> `market_id`, y una pregunta tipica del usuario va a coincidir con
  el tema del market (lo que ya esta en `question`, corto y semanticamente denso) mas que con
  el vocabulario tecnico de las reglas de resolucion (`description`, mas largo). Separar en
  dos embeddings arriesgaba que el vector de `description` compitiera/diluyera el ranking sin
  aportar señal real, y duplicaba vectores por market sin necesidad -- combinado, `question`
  sigue dominando la señal semantica mientras `description` aporta contexto extra sin ser una
  entidad separada que mantener. Mantiene el diseño de `chunk_registry` simple: sigue siendo
  1 vector por `market_id`, no 2.

### Linkeo News/Comments -> market_id

- **News (rediseñado 2026-08-16, ver "News Source Redesign" en tech_debt.md):**
  `news_match_terms` y el AND-match ya NO se usan -- cada articulo se etiqueta
  con `market_ids: [market_id]` segun a que busqueda de Google News (una por
  market, usando `question` tal cual) perteneciera. El linkeo es 1:1 por
  construccion (la busqueda ya es especifica al market), no un match posterior
  contra texto libre.
  - **Campos temporales agregados 2026-08-18 (ver tech_debt.md para el diseño
    completo de ambos):** `temporal_tier` (`"3.1"`/`"3.2"`/`"3.3"`/`"too_old"`/
    `"unknown_market"`) -- clasifica `pubDate` del articulo contra `created_at` y
    `first_seen` del market; y `market_status_at_publish` (`"open"`/`"closed"`/
    `"unknown_market"`) -- si el market seguia abierto o ya habia resuelto al
    momento de publicarse el articulo, calculado desde `resolution_date`, NO desde
    el status ACTUAL del market. Son dos ejes independientes a proposito (mismo
    error evitado que el two-tier `link_type` de Comments) -- la combinacion
    `temporal_tier == "3.3" AND market_status_at_publish == "open"` es la señal de
    mayor confianza para "esto puede explicar un movimiento de odds real."
    Retro-tageados sobre los 3,315 articulos existentes
    (`scripts/tag_news_temporal_tier.py`, `scripts/tag_news_market_status.py`);
    aun NO se calculan nativamente en `ingest_news` para articulos nuevos --
    ambas funciones de clasificacion quedaron escritas standalone para copiarse
    directo al handler cuando se decida hacerlo (no forma parte de F-lambdas,
    que solo cubrio la AMPLIACION de que markets se buscan, no el tagging de
    lo que se encuentra).
  - **Captura post-resolucion (F-lambdas, 2026-08-18):** `get_open_markets` ya no
    filtra solo `status == open` -- tambien incluye markets `resolved` con
    `post_resolution_cycles_remaining > 0` (ver seccion de campos del registry
    arriba). Verificado en produccion: 502 open + 93 resolved (los 93 legacy,
    arrancados manualmente una sola vez) = 595 exacto.
- **Comments (reemplaza a Bluesky, 2026-08-16):** lookup directo por
  `comment_entity_type`/`comment_entity_id`, no busqueda. La precision del
  linkeo varia por diseño -- ver seccion Comments arriba para los 3 niveles
  (`direct`, `shared_event`, `shared_series`) y por que no todo comentario es
  1:1 con un solo market.

### Retrieval (Dia 4, en diseño 2026-08-18)

**El modelo de dos capas (Layer 1 linkeado / Layer 2 ventana temporal ambiental)
esta DEPRECADO**, junto con `retrieval/time_window.py` que lo implementaba (archivo
borrado 2026-08-18). Razon: el rediseño de News a busqueda-por-market dejo a TODOS
los articulos linkeados a exactamente un `market_id` por construccion (verificado
con datos reales: 2,638 articulos en 4 ciclos, 100% linkeados, cero sin linkear) --
el pool ambiental que Layer 2 existia para alcanzar quedo vacio. Ver tech_debt.md,
"Known Limitation: Explicit ID-Linkage", seccion DEPRECATED, para el razonamiento
completo y que limitacion real SIGUE abierta.

**Modelo vigente: un solo camino -- filtro por metadata + ranking semantico.**
El retrieval filtra por metadata del chunk (`market_id`, timestamp/ciclo, source) y
rankea por similitud semantica dentro de ese subconjunto. El tiempo NO desaparece:
sigue siendo central (la pregunta "por que se movio este market entre el ciclo 3 y
el 4" es inherentemente acotada en tiempo), pero baja de ser una CAPA arquitectonica
a ser un campo mas del envelope de metadata, aplicado como filtro.

**Corpus real medido (2026-08-18 12:00 UTC, los 6 ciclos completos):**

| | Total | Chars | ~Tokens | Mediana | Nota |
|---|---|---|---|---|---|
| Articulos (News) | 3,315 | 20.9M | ~5.22M | 3,974 chars | 100% con body_text, max 240K chars |
| Comentarios | 12,711 | 1.04M | ~261K | 44 chars | 47% bajo 40 chars |

**Linkeo verificado sobre el corpus completo:** los 3,315 articulos llevan EXACTAMENTE
un `market_id` cada uno, cero sin linkear -- confirma sobre 6 ciclos lo que motivo
deprecar el modelo de dos capas. Los comentarios son lo opuesto: mediana de 2
`market_id` por comentario, media 3.8, maximo 49 (efecto de `shared_series`), con
distribucion 4,362 `direct` / 4,031 `shared_event` / 4,318 `shared_series`.

**El corpus es ACUMULATIVO, pero News y Comments crecen de forma MUY distinta -- y eso
cambia el diseño de embedding por fuente:**
- **News crece de verdad cada ciclo** (443 -> 666 -> 744 -> 785 -> 643 articulos): son
  articulos nuevos, dedupeados por URL, ~600-800 por ciclo de forma sostenida.
- **Comments se aplano casi por completo** (2,589 -> 2,831 -> 3,077 -> 3,441 -> 429 ->
  344). NO es una falla: `markets_with_comments` siguio alto (438, 494) y
  `entities_queried` normal (109, 126), sin fallos. Los primeros ciclos ingirieron el
  back-catalogue historico completo de cada entidad; ya en estado estable, la tabla
  `poly-rag-processed-comments` solo deja pasar comentarios genuinamente nuevos. El
  volumen real de estado estable son ~350-450 comentarios/ciclo, no ~3,000.

Esto obliga a que el embedding sea incremental y automatizado dentro de la cadena de
ingestion (solo chunks nuevos, nunca re-embedear el corpus), no un backfill manual --
el backfill sobre los ciclos ya existentes es un bootstrap de una sola vez, separado
del camino de estado estable, mismo patron que tuvo el bootstrap del registry. El
costo recurrente de embedding lo domina News (~5M tokens acumulados y creciendo
~700 articulos/ciclo), no Comments (~261K tokens totales, y de crecimiento lento).

**Chunking de News, cerrado 2026-08-20 (ver tech_debt.md, Dia 6, para las alternativas
diferidas a A/B test):** unidad de embedding = parrafo (split de `body_text` por parrafo,
no por tamano fijo de tokens ni por articulo completo). Mecanismo de "vecino" = indice de
parrafo, no deteccion semantica ni por oracion -- dado un chunk con `paragraph_index = i`
del mismo `article_id`, el vecino anterior/siguiente es una consulta directa por
`paragraph_index` +/- 1, sin re-parsear texto. Al momento de retrieval, la busqueda
semantica ocurre a nivel parrafo (precision), pero el contexto devuelto al LLM es el
parrafo ganador MAS sus vecinos inmediatos (parent-child retrieval) -- no el articulo
completo, para mantener costo de tokens acotado sin importar el largo del articulo
(mediana ~4,000 chars, maximo 240K chars).

**Metadata por chunk:**
- Heredado del articulo padre (identico en todos los chunks de un mismo articulo):
  `market_id`, `temporal_tier`, `market_status_at_publish`, `pubDate`, `source` (outlet),
  y `article_id` -- decision 2026-08-20: se usa la `url` del articulo como `article_id`,
  no un id sintetico nuevo, porque ya es unica por diseno (es la misma dedup key de
  `poly-rag-processed-urls`) y no requiere mantener un mapeo articulo->id aparte.
- Propio del chunk: `chunk_id` (`{article_id}#{paragraph_index}`), `paragraph_index`
  (0-indexed), `paragraph_count` (total de parrafos del articulo, para saber si el chunk
  es el primero/ultimo y no tiene vecino de ese lado), y el texto del parrafo (lo que se
  embede).

**Chunking de Comments, cerrado 2026-08-20:** demasiado cortos para embeder
individualmente (mediana 44 chars, 47% bajo 40 chars). Unidad de chunk = comentarios
del mismo `link_type` concatenados hasta un tope de tokens (overflow a multiples
chunks igual que un articulo largo de News), NUNCA por ventana de tiempo abierta
(rechazado -- un chunk que sigue creciendo cada ciclo mientras el market este abierto
rompe el patron "solo lo nuevo, nunca re-embedear", mismo problema que ya tuvo el
read-modify-write de `odds/<market_id>.json`). La unidad depende de `link_type`, no es
uniforme:

```
if link_type == "direct":
    unidad de chunk = market_id           # 1:1 real, nada que compartir
elif link_type in ("shared_event", "shared_series"):
    unidad de chunk = comment_entity_id   # 1 solo stream para TODOS los markets
                                           # que comparten esa entidad
```

**Por que por entidad y no por market en los casos compartidos:** un comment
`shared_series` puede aplicar a hasta 49 `market_ids` (dato real del corpus, ver
seccion Comments arriba) -- chunkear por market_id habria significado re-embeder el
mismo texto hasta 49 veces por el mismo contenido. En vez de eso, el chunk vive UNA
sola vez anclado a la entidad compartida (Event o Series), y los markets que la
comparten simplemente apuntan al mismo chunk -- sin duplicar embedding, storage, ni
arriesgar que dos markets terminen leyendo copias distintas del mismo stream.

**Indice reusado, no nuevo:** el lookup `market_id -> comment_entity_id` ya existe --
es el mismo campo `comment_entity_type`/`comment_entity_id` que `ingest_polymarket`
ya escribe en el registry (ver seccion Comments arriba), sin LLM, extraido de
`events[]` en la respuesta de la Gamma API. El retrieval, dado un `market_id`, hace
lookup de su `comment_entity_id` en el registry y busca chunks por esa entidad --
no requiere una tabla de indexacion nueva.

**Tres streams resultantes, mismo mecanismo para el bootstrap one-off y el
incremental por ciclo** (unico cambio entre ambos: el alcance de comentarios que
entra -- todo el historico vs. solo lo nuevo del ciclo, mismo patron ya usado en
bootstrap del registry/odds vs. estado estable):
- `comments_direct` -- 1 chunk por `market_id` (+overflow si excede el tope de tokens)
- `comments_shared_event` -- 1 chunk por `comment_entity_id` tipo Event
- `comments_shared_series` -- 1 chunk por `comment_entity_id` tipo Series

**Metadata por chunk:** `link_type`, `comment_entity_type`, `comment_entity_id` (o
`market_id` en el caso `direct`), `cycle_started_at` (o rango de ciclos si el chunk
viene del bootstrap one-off), `chunk_id` (`{comment_entity_id o market_id}#{ciclo}`),
texto = comentarios concatenados de esa unidad para ese alcance.

**Chunking/embedding de Digest (Capa 0), cerrado 2026-08-20 -- quinta fuente de
embedding, simetrica con Polymarket/News/Comments.** Reemplaza el diseño anterior
(lookup estructurado directo sobre los campos JSON del digest) -- decision explicita
del usuario: el `_digest` completo se embede como texto, no solo se consulta como
data estructurada, para que Capa 0 sea una fuente de retrieval real, buscable por
lenguaje natural (ej. "que paso este ciclo"), no solo alcanzable via un `market_id`
ya resuelto de antemano.

- **Input:** el JSON de `digest/YYYY-MM-DD/HH.json` (la fuente de verdad, escrita
  ANTES del correo -- ver seccion Email Digest arriba), nunca el HTML del correo
  (que trae markup/estilos sin valor para el embedding).
- **Conversion a texto, template deterministico, sin llamada LLM nueva:** una
  funcion (`digest_to_text(digest_data)`) arma un solo bloque de texto narrativo por
  ciclo a partir de TODOS los campos (`newly_tracked_markets`, `resolved_markets`,
  `top_volatility`, `world_snapshot`, `quotes`, `executive_summary`) -- mismo
  formato que el mock en texto plano ya construido y mostrado al usuario. Codigo
  puro (f-strings/template), consistente con la disciplina del proyecto de no
  agregar llamadas Bedrock nuevas cuando la conversion es mecanica y sin
  ambiguedad -- `executive_summary` es el unico campo ya generado por LLM, el resto
  es JSON estructurado 100% fiel (ver tech_debt.md, "Digest Fidelity Audit").
- **Unidad de embedding = 1 digest completo = 1 chunk, sin split por parrafo** -- un
  digest es corto (resumen de un ciclo, no un articulo largo), fragmentarlo seria
  sobre-ingenieria para su tamano real.
- **Metadata del chunk:** `cycle_started_at`, `digest_s3_key`, `market_ids_mentioned`
  (lista de todos los `market_id` que aparecen en `newly_tracked`/`resolved`/
  `top_volatility`/`world_snapshot` de ese digest -- permite filtrar "digests que
  mencionaron el market X" sin depender solo de similitud semantica).
- **Bootstrap + incremental, mismo mecanismo que las otras 3 fuentes:** ~8-10
  digests historicos a la fecha (bootstrap trivial por volumen), 1 nuevo por ciclo
  en adelante -- mismo trigger encadenado descrito abajo.

**Fase de embedding, desacoplada de la cadena de ingesta (decision 2026-08-20):**
corrige la nota anterior de este documento que decia "el embedding tiene que ser...
automatizado dentro de la cadena de ingestion" -- eso queda OBSOLETO. El proyecto
tiene dos fases separadas: Fase 1 = ingesta (EventBridge -> polymarket -> news ->
comments -> digest, sin cambios en su logica de negocio), Fase 2 = embedding
(proceso independiente que lee lo que Fase 1 ya escribio en S3, nunca modifica
Fase 1).

**Trigger de Fase 2, decidido 2026-08-20: encadenado, ultimo eslabon de la cadena
existente.** `send_digest` invoca Fase 2 al terminar, mismo mecanismo
(`lambda.invoke()`, `cycle_started_at` heredado) que ya usa cada eslabon de la
cadena hoy (polymarket -> news -> comments -> digest) -- la cadena simplemente
crece un eslabon mas (digest -> embedding), no un mecanismo nuevo. "Desacoplado"
aqui es logico (Fase 2 no comparte codigo/responsabilidad con Fase 1, se puede
tocar sin riesgo de romper la cadena de ingesta), no temporal -- no hay delay
agendado ni cron aparte. Fase 2 corre despues del envio del correo (ultimo paso,
no bloquea nada previo), pero en la misma corrida de la cadena.

**Arquitectura de Lambdas de Fase 2, decidida 2026-08-20, revisada el mismo dia a
4 niveles -- aislamiento por fuente, por modelo, Y por store, no una Lambda por
combinacion completa.** Explicitamente rechazado: 1 Lambda por cada una de las 6
combinaciones de corpus (2 chunking x 3 embedding) -- cada una repetiria la lectura
de las 4 fuentes de S3 sin ganar aislamiento real (un fallo leyendo News no se
aisla mejor por tener 6 Lambdas leyendolo igual). En vez de eso, aislamiento por
los ejes que realmente varian:

1. **`poly-rag-embed-orchestrator`** (Lambda mama) -- invocada por `send_digest`
   al terminar (ver trigger arriba). Dispara las 5 Lambdas de chunking en
   paralelo via `lambda.invoke()` asincrono, mismo patron de fan-out ya usado por
   `ingest_news` para sus batches (ver seccion News arriba, "Batching").
2. **5 Lambdas de chunking, una por fuente (no por combinacion):**
   `chunk_registry`, `chunk_news_paragraph`, `chunk_news_article`,
   `chunk_comments`, `chunk_digest`. Cada una lee su propia fuente de S3
   (registry, `news/YYYY-MM-DD/HH.json`, `comments/YYYY-MM-DD/HH.json`,
   `digest/YYYY-MM-DD/HH.json`), produce chunks de texto, escribe a su propio
   prefijo S3 (ej. `chunks/news_paragraph/<cycle>.json`). Terminan
   independientemente entre si -- un fallo en `chunk_news_article` no bloquea ni
   se entera `chunk_comments`. Las dos variantes de News NO son alternativas
   donde se elige una -- ambas corren siempre, en paralelo, cada una produciendo
   su propio conjunto de chunks.
3. **Paso de merge + 6 Lambdas de embedding, SOLO texto -> vector, store-agnostic**
   (mismo patron que `merge_batch_payloads` de News) -- detecta cuando las 5
   Lambdas de chunking terminaron, arma 2 corpus completos (`registry + comments
   + digest + news_paragraph` vs. `registry + comments + digest +
   news_article`), y dispara 3 Lambdas de embedding (`embed_titan`,
   `embed_cohere`, `embed_voyage`) por cada uno de los 2 corpus -- 6 invocaciones
   totales. **Deliberadamente NO escriben a ningun vector store directamente** --
   cada una calcula sus vectores y los persiste a S3
   (`vectors/<variante>/<modelo>.json`), igual que cualquier otra Lambda de
   Fase 1 escribe su output a S3 antes que nada mas pase. Cada Lambda de
   embedding aisla fallos por MODELO -- si Cohere esta caido o Voyage falla por
   su API key, Titan sigue funcionando sin bloquearse.
4. **Lambdas de escritura a store, separadas y desacopladas del calculo del
   vector -- decision explicita 2026-08-20, ver tech_debt.md.** El paso de
   "guardar el vector en un store" es la unica parte de Fase 2 que NO es
   agnostica (cada store tiene su propia API/modelo de organizacion: namespaces
   en Pinecone, collections en Qdrant, archivos Lance en S3 para LanceDB -- ver
   tech_debt.md, "Vector Store Choice", para el detalle de cada uno). Aislarlo en
   su propia Lambda (`write_to_pinecone` hoy; `write_to_qdrant`/
   `write_to_lancedb` en Dia 6) significa que agregar un store nuevo es agregar
   codigo nuevo, CERO cambios a las 3 Lambdas de embedding ya probadas -- y si el
   store falla o se cambia de proveedor, los vectores ya calculados (el trabajo
   caro) siguen intactos en S3, reintentar la escritura no repite el embedding.

Consistente con el patron de cadena estricta ya establecido en Fase 1
(`cycle_started_at` heredado en cada paso) -- aqui el fan-out es simplemente mas
ancho (5 en paralelo, luego 6), no un mecanismo nuevo.

**Modelo de embeddings, decidido 2026-08-20 (ver tech_debt.md, "Embedding Model
Choice", para el pricing/disponibilidad verificados via AWS CLI real):** Amazon
Titan Embeddings v2 como default de produccion (sin friccion, ya autorizado en la
cuenta, mas barato). Cohere Embed v4 y Voyage AI (`voyage-finance-2`) quedan como
comparacion A/B en Dia 6, no descartados -- ver tech_debt.md para el razonamiento
completo, incluyendo la correccion de que excluir Voyage por "romper un patron
IAM-only" nunca fue una regla real del proyecto.

**Vector store, decision inicial + secuencia para Dia 6 (2026-08-20, ver
tech_debt.md, "Vector Store Choice"):** Databricks Vector Search descartado
(limite real de 1 endpoint activo por cuenta en el Free Edition, incompatible con
necesitar 6 indices consultables en paralelo). Se construye con **Pinecone** para
arrancar Dia 4 bloque F mas rapido -- Qdrant y LanceDB se clonan del corpus ya
vectorizado en Dia 6 (operacion de infraestructura, no un re-embedding, dado que
mover vectores ya calculados a otro backend no repite el trabajo caro). OpenSearch
Serverless sigue descartado por costo (~$700/mes, incompatible con el budget).

### Verificado en produccion

**Bootstrap del registry (2026-08-15, tras el fix de horizonte minimo de 48h):**
228 markets trackeados, 228 archivos de odds en S3, 100% con el schema final
de ese momento.

**Registry actualizado (2026-08-16, tras simplificar el LLM y agregar campos
de comments):** 333 markets tras un ciclo nuevo, backfill one-off corrido
contra los 329 markets sin `comment_entity_type` (los que predataban el
rediseño) -- 187 direct, 105 shared_series a nivel de link potencial, 37 sin
comentarios en ningun nivel, 0 errores.

**News (2026-08-16, tras el rediseño a Google News RSS + fan-out paralelo):**
228/228 markets procesados, 887 articulos, `cycle_complete: true`. Reemplaza la
cifra vieja de 367 articulos/3 linkeados del diseño de 10-feeds-RSS +
`news_match_terms`, que causo el rediseño completo (ver tech_debt.md, "News
Source Redesign").

**Comments (2026-08-16, reemplaza a Bluesky):** 295/329 markets con cobertura
(89%), 2506 comentarios, 94 llamadas a la API (agrupadas por entidad
compartida), 0 fallos. Distribucion final tras el fix de 3 niveles: 896
`direct` / 802 `shared_event` / 808 `shared_series`. Bluesky (492 markets
consultados, 689 posts, 0 fallos, ultima corrida 2026-08-15) fue destruido de
AWS el mismo dia que se verifico Comments -- ver tech_debt.md, "Comments
Source Replaces Bluesky".

---

## LLM Enrichment (Ingestion) -- Canon (trial cerrado 2026-08-16)

**Estado:** ya no es un trial opcional -- es canon. Decision cerrada explicitamente
por el usuario 2026-08-16: el rediseno de ingestion (filtro de verificabilidad +
Comments) convirtio al LLM en el filtro de calidad mismo, no un resumen opcional
encima de un pipeline que funcionaria igual sin el -- ademas, con las nuevas fuentes,
el LLM enrichment es un input directo del RAG (Day 4/5), no solo una conveniencia de
lectura humana. Queda pendiente en tech_debt.md una pasada de optimizacion
especificamente para consumo RAG (no para email humano) -- deliberadamente diferida
hasta que se diseñe la capa de retrieval, no un pendiente urgente hoy.

- **Modelo:** Claude Sonnet 4.5 via Bedrock (`us.anthropic.claude-sonnet-4-5-20250929-v1:0`,
  inference profile -- Sonnet 5 aun no disponible sin contacto a AWS Sales)
- **Patron:** una sola llamada batched a Bedrock por corrida de cada Lambda
  (resume hasta 20 items), NO una llamada por item individual -- controla costo
- **Auth:** IAM (boto3 bedrock-runtime), mismas credenciales que S3/DynamoDB --
  sin API key separada de Anthropic
- **Toggle:** variable de entorno `USE_LLM_ENRICHMENT` (true/false) por Lambda,
  sigue existiendo tecnicamente pero ya no es una decision pendiente -- es config,
  no un experimento

**Costo real medido, primera corrida canonica limpia (2026-08-16, ciclo de las
12:00 UTC, automatico end-to-end, las 4 Lambdas, Comments ya en el mix en vez de
Bluesky):**

| Fuente | Costo/corrida |
|---|---|
| Polymarket | $0.032607 |
| News (11 batches) | $0.103491 |
| Comments | $0.004965 |
| send_digest (executive summary) | $0.006357 |
| **Total ciclo completo** | **$0.147420** |

Proyectado a cadencia de 12h (2x/dia, ~30 dias): **~$8.85/mes**. Sube frente a la
cifra historica pre-rediseno (~$1.10/mes) principalmente porque News ahora genera su
propio resumen LLM POR BATCH (hasta 11 llamadas en un ciclo con fan-out completo),
no una sola llamada por corrida como el diseno viejo de 10-feeds-RSS. Sigue dentro
del buffer de $120 en creditos promocionales y de la disciplina de "gastar
deliberadamente, no miseria" de CLAUDE.md, pero es una cifra real a vigilar, no la
que se asumia originalmente.

**Costo del bootstrap del rediseno (2026-08-15):** ~500 candidatos, ~25 llamadas
Bedrock batched (20/batch), estimado ~$0.35-0.40 -- costo de arranque UNA VEZ, no
recurrente. Costo de estado-estable (ciclos despues del bootstrap) escala con la
tasa de aparicion de ids nuevos por ciclo, no con el tamano del pool candidato
(500) -- ver tech_debt.md para el razonamiento. Aun no medido empiricamente cuantos
ids nuevos aparecen por ciclo en la practica.

---

## Infrastructure Inventory

| Recurso | Nombre | Proposito |
|---|---|---|
| S3 bucket | `poly-rag-369970405415` | Storage crudo, particionado `<source>/YYYY-MM-DD/HH.json`, mas `odds/<market_id>.json` (serie de tiempo append-only). Versionado activado 2026-08-17 (ver nota abajo) |
| DynamoDB table | `poly-rag-architecture-metrics` | Costo/latencia/tokens por invocacion, pay-per-request, PITR activado 2026-08-17 |
| DynamoDB table | `poly-rag-market-registry` | Market registry (metadata + comment_entity_type/id/link_type), pay-per-request, hash_key `market_id`, PITR activado 2026-08-17 -- agregada 2026-08-15, campos de comments agregados 2026-08-16 |
| DynamoDB table | `poly-rag-processed-urls` | Dedup de URLs de articulos de News, pay-per-request, hash_key `url`, sin TTL, PITR activado 2026-08-17 -- agregada 2026-08-15 |
| DynamoDB table | `poly-rag-processed-comments` | Dedup de comment_id de Comments, pay-per-request, hash_key `comment_id`, sin TTL, PITR activado 2026-08-17 -- agregada 2026-08-17, ver seccion Comments |
| DynamoDB table | `poly-rag-domain-failures` | Blocklist dinamico de dominios para News, pay-per-request, hash_key `domain`, PITR activado 2026-08-17 -- agregada 2026-08-16 |
| IAM role | `poly-rag-ingest-lambda-role` | Execution role de las Lambdas de ingestion, permisos: S3 PutObject/GetObject/ListBucket (ListBucket agregado 2026-08-15 -- sin el, GetObject sobre una key inexistente devuelve AccessDenied opaco en vez de NoSuchKey, rompiendo el patron read-modify-write de odds), DynamoDB PutItem en metrics + GetItem/PutItem/UpdateItem/Scan/Query en el registry + tablas de News/Comments, Bedrock InvokeModel scoped al modelo especifico |
| IAM policy | `PolyRAG-BudgetBreach-Deny` | Guardrail: bloquea Bedrock/Lambda/S3-writes/DynamoDB-writes si el gasto cruza budget de $10 |
| IAM role | `PolyRAG-BudgetsActionRole` | Permite a AWS Budgets adjuntar la Deny policy automaticamente |
| AWS Budget | $5/mes | Alertas en 20% ($1) y 100% ($5) |
| AWS Budget | $10 | Threshold del guardrail Deny automatico |
| EventBridge rule | `poly-rag-ingest-polymarket-schedule` | Cron `0 0,12 * * ? *` (00:00 y 12:00 UTC), UNICO trigger de EventBridge del pipeline principal desde el "Strict Ingestion Chaining" (2026-08-16, ver tech_debt.md) -- target: Lambda de Polymarket (timeout 600s desde 2026-08-15). News, Comments y send_digest ya NO tienen su propio EventBridge rule; se invocan encadenados directo via `lambda.invoke()` |
| EventBridge rule | `poly-rag-watchdog-schedule` | Cron cada 10 min, target: `poly-rag-watchdog-ingest-news` -- detecta un ciclo de News atorado (batches faltantes 20+ min despues de iniciado) y reintenta solo los offsets faltantes |

**Region:** us-east-1 (N. Virginia) exclusivamente -- consistencia obligatoria,
Bedrock model access y otros recursos son regionales.

**Automatizacion:** las 3 Lambdas corren solas cada 12h via EventBridge (reglas
independientes, no una sola regla compartida -- consistente con el principio de
3 Lambdas aisladas). Cada regla tiene su propio `lambda:InvokeFunction` permission
scoped por `SourceArn`, y su propio target -- pausar/ajustar una fuente no afecta
a las otras dos.

---

## Infrastructure as Code (Terraform)

**Estado:** toda la infraestructura de arriba (S3, DynamoDB, IAM, las 4 Lambdas,
EventBridge) esta en Terraform (`terraform/`) desde 2026-08-14 -- las primeras 3
Lambdas de ingestion se importaron desde recursos ya vivos (cero destroy/recreate);
la 4a Lambda (send_digest) se creo directamente via `terraform apply`, siendo la
primera pieza de infra desplegada nativamente por Terraform en vez de CLI suelto.

- **Provider:** `hashicorp/aws` ~> 5.0, region us-east-1
- **Estructura:** `providers.tf`, `s3.tf`, `dynamodb.tf`, `iam_ingest_lambda.tf`,
  `iam_send_digest.tf`, `lambdas.tf`, `eventbridge.tf` -- un archivo por dominio
- **State file:** `terraform.tfstate` gitignored (contiene ARNs/IDs de cuenta) --
  vive solo local, no versionado. Nota tech-debt: sin backend remoto (S3+DynamoDB
  lock) todavia, aceptable para un solo desarrollador, revisar si esto se vuelve
  colaborativo
- **Secrets:** ninguno actualmente -- las credenciales de Bluesky (`BLUESKY_HANDLE`,
  `BLUESKY_APP_PASSWORD`, gestionadas fuera de Terraform state via
  `lifecycle { ignore_changes }`) dejaron de aplicar cuando Bluesky se destruyo
  (2026-08-16); Comments usa la Gamma API publica, sin auth
- **De aqui en adelante:** cambios de infraestructura (nueva Lambda, ajustar cron,
  nuevos permisos IAM) se hacen editando `.tf` + `terraform apply`, no mas comandos
  sueltos de `aws` CLI para crear/modificar recursos

---

## Pendiente para completar el ciclo de ingestion

- Confirmado (2026-08-16): las 4 Lambdas corren solas via EventBridge, cadena
  estricta completa -- el ciclo automatico de las 12:00 UTC del 16 corrio
  end-to-end sin intervencion manual (News 367/367, cycle_complete true), primera
  corrida canonica limpia con Comments ya en el mix en vez de Bluesky
- Cerrado (2026-08-16): trial LLM-en-ingestion -> canon, ver seccion LLM
  Enrichment. Queda pendiente en tech_debt.md una pasada de optimizacion del
  formato de enrichment especificamente para consumo RAG, deliberadamente
  diferida hasta el diseño de la capa de retrieval (Day 4/5)
- Medir en la practica la tasa real de ids nuevos por ciclo (determina el costo de
  estado-estable real, ver seccion "Market Registry + Ingestion Redesign")
- Limpieza pendiente de `poly-rag-architecture-metrics`: filas de `ingest_bluesky`
  (fuente ya no existe) y ruido de desarrollo del 2026-08-16 (reintentos/debugging
  antes de la primera corrida canonica) -- diferido hasta confirmar 1-2 ciclos
  canonicos limpios consecutivos, para no perder evidencia de depuracion si algo
  falla en el proximo ciclo
- **Cerrado (2026-08-17):** `odds_old_2026-08-15/` ya no existia en el bucket (se
  habia limpiado en algun momento previo a esta sesion, confirmado via listado
  directo -- 0 objetos). Nota vieja removida.
- **Cerrado (2026-08-17), limpieza mayor del registry y del bucket:** el usuario
  identifico que 329 de 629 registry items eran residuo del pipeline pre-2026-08-16
  (linkeados al diseño viejo de Bluesky + News-por-keywords, con campos
  `search_query`/`news_match_terms` obsoletos) -- no datos falsos, pero producto de
  un pipeline ya muerto. Borrados permanentemente: 329 registry items + sus 329
  `odds/<market_id>.json` correspondientes + `test/README.md` (prueba trivial del
  Dia 1) + el prefijo `bluesky/` completo (7 objetos, restos de la fuente
  descontinuada). Registry: 629 -> 300 items (250 open / 50 resolved), 0 campos
  legacy remanentes. Antes de borrar, se activo **S3 bucket versioning** y
  **DynamoDB PITR en las 5 tablas** (deliberadamente en ese orden -- versionar
  primero hace el borrado recuperable via una restauracion deliberada, no
  irreversible, mientras que activar despues del borrado no habria protegido nada
  de lo que se acababa de borrar). Decision explicita del usuario: "mas higienico
  aunque si sea recuperable" -- el objetivo no era destruccion forense sino que
  estos datos dejaran de alimentar el pipeline vivo y el futuro corpus RAG.
- **Extension (2026-08-17, mismo dia):** el usuario noto que ciclos crudos de
  `news/` y `polymarket/` anteriores al rediseño del 15 de agosto seguian en el
  bucket -- misma naturaleza obsoleta que los 329 markets (evidencia de disenos
  muertos: filtro de 3 verticales, 10-feeds-RSS sin extraccion de texto real) pero
  fuera del alcance original de esa limpieza (esos payloads son historial de
  CICLO, no datos de registry/odds ligados a un market_id). Confirmado con
  evidencia directa (campos `feed`/`verticals` en News, sin `url` real -- el
  pipeline de extraccion `googlenewsdecoder`/`trafilatura` aun no existia) antes
  de borrar. Borrados: 5 archivos `news/` (13 y 14 de agosto, mas 08-15 00/12/16h)
  + 8 archivos `polymarket/` (13 y 14 de agosto, mas 08-15 00/04/12/15/16h) = 13
  objetos. El archivo mas viejo que queda en `news/` ahora es `2026-08-15/22.json`
  (23:39 UTC, ya con Google News); en `polymarket/`, `2026-08-16/00.json`. Bucket
  total tras esta segunda pasada: 379 objetos.
- **Extension (2026-08-17), definicion de "ciclo completo" y purga a nivel
  snapshot individual:** desde `eda_mio` (notebook de EDA propio del usuario en
  Databricks), se definio ciclo completo como las 4 etapas (polymarket, news,
  comments, digest) con su archivo FINAL presente bajo la misma fecha/hora
  (excluye explicitamente los `_batchN.json` intermedios de News). Resultado real
  a la fecha: solo 4 ciclos completos -- 2026-08-16 01:00 y 12:00 UTC, 2026-08-17
  00:00 y 12:00 UTC. Borrados 15 archivos de ciclo sueltos que no pertenecian a
  ninguno de los 4 (restos de debugging: comments/digest del 16 de agosto,
  batches huerfanos de News, un `polymarket/2026-08-16/00.json` que nunca llego a
  completar cadena). Extendido el mismo criterio a `odds/<market_id>.json`
  (perspectiva del usuario: un snapshot debe estar asociado al ciclo que lo
  genero -- el timestamp del snapshot coincide exacto con el momento en que
  corrio esa invocacion de `ingest_polymarket`, el mismo usado para su propio
  archivo de ciclo, asi que la asociacion es real, no una aproximacion). De 483
  snapshots totales, 4 no pertenecian a ninguno de los 4 ciclos completos (los 4
  del mismo timestamp `2026-08-16T00:31:01`, la invocacion incompleta ya
  borrada) -- removidos. Un market (`1088487`) quedo con 0 snapshots validos tras
  el filtro (su unico snapshot era ese mismo timestamp) -- su archivo se borro
  por completo, no se dejo vacio, por decision explicita del usuario. Estado
  final: 299 markets con odds, 479 snapshots, todos asociados a uno de los 4
  ciclos completos. Bucket total: 363 objetos.
- **Extension final (2026-08-17), aplicado tambien al registry:** desde
  `eda_mio_2`, construyendo una celda de "saldo" (entraron/salieron por ciclo)
  para el registry, se detecto que el saldo corrido no cuadraba con el total
  real (saldo=210 vs. 250 open reales) -- causa raiz: 15 registry items no
  habian entrado por ninguno de los 4 ciclos completos, sino por otras
  invocaciones de `ingest_polymarket` (debugging del 16 de agosto, timestamps
  00:31/01:07/01:08-09/01:34, ninguno coincide con los 4 ciclos reales).
  Reconciliado con evidencia exacta (los `market_id` reales de
  `newly_tracked_markets` en cada payload de ciclo, no aproximacion por hora --
  un primer intento de reconciliar por prefijo de hora fallo porque varias
  invocaciones de debug caian en la misma hora que un ciclo real). Mismo
  criterio aplicado que al resto del dia: si no entro por un ciclo completo
  real, no debe existir. Borrados los 15 registry items + 14 archivos
  `odds/<market_id>.json` correspondientes (uno, `1088487`, ya no tenia
  archivo -- se habia borrado en la pasada anterior por quedar en 0
  snapshots). Estado final verificado: registry = 285 items, EXACTO igual a
  los 285 market_ids que entraron via los 4 ciclos completos (237 open / 48
  resolved). Bucket total: 349 objetos.
- Explorar paginacion `/markets/keyset` de la Gamma API para conocer el tamano
  real del universo de mercados activos (hoy solo se confirmo un piso de ~2,100
  via offset, que se cae mas alla de eso)
- Fix del bug de horizonte minimo de 48h para mercados deportivos (usa endDate,
  que no refleja la hora real del partido -- ver tech_debt.md)

Ver sprint_plan.md (gerdau/) para el resto del roadmap (Databricks Dia 3 --
Delta Lake/Unity Catalog iniciados 2026-08-17, tablas `workspace.poly_rag.market_registry`/
`odds_snapshots` creadas y verificadas, falta demostrar time travel real (solo existe
version 0 por ahora) y la lectura conceptual de DAMA-DMBOK/Data Mesh; RAG retrieval
Dia 4, synthesis agent Dia 5).
