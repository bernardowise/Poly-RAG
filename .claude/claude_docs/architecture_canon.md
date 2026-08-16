# Architecture Canon

**Snapshot del estado actual de la arquitectura de Poly-RAG.** Este documento se
actualiza/sobreescribe conforme la arquitectura evoluciona -- no es una bitacora de
decisiones pasadas (eso vive en session_ledger.md) ni una lista de pendientes (eso
vive en tech_debt.md). Si algo aqui queda obsoleto, se reemplaza, no se acumula.

Ultima actualizacion: 2026-08-16 (Comments reemplaza a Bluesky)

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

Append-only, un archivo por market, un snapshot agregado cada ciclo (nunca
sobreescrito) -- esta ES la diferenciacion real del proyecto (self-built historical
time-series). Cada snapshot: `timestamp`, `outcomePrices`, `volume`, `volume24hr`,
`liquidity`. Read-modify-write por ciclo: lee el archivo existente (o inicia uno
nuevo si no existe -- requiere `s3:ListBucket` a nivel bucket ademas de
`s3:GetObject`, ver nota IAM abajo), agrega el snapshot, reescribe.

### Linkeo News/Comments -> market_id (Layer 1, alta confianza)

- **News (rediseñado 2026-08-16, ver "News Source Redesign" en tech_debt.md):**
  `news_match_terms` y el AND-match ya NO se usan -- cada articulo se etiqueta
  con `market_ids: [market_id]` segun a que busqueda de Google News (una por
  market, usando `question` tal cual) perteneciera. El linkeo es 1:1 por
  construccion (la busqueda ya es especifica al market), no un match posterior
  contra texto libre.
- **Comments (reemplaza a Bluesky, 2026-08-16):** lookup directo por
  `comment_entity_type`/`comment_entity_id`, no busqueda. La precision del
  linkeo varia por diseño -- ver seccion Comments arriba para los 3 niveles
  (`direct`, `shared_event`, `shared_series`) y por que no todo comentario es
  1:1 con un solo market.

### Retrieval por Ventana Temporal (Layer 2, contextual -- `retrieval/time_window.py`)

**Limitacion reconocida del linkeo explicito:** solo captura correlacion DIRECTA
(el texto menciona literalmente los terminos del market). Una noticia ambiental
("sentimiento cripto se deteriora") puede mover el precio de un market de Bitcoin
sin nunca mencionarlo explicitamente -- nunca quedaria linkeada. Ver tech_debt.md,
"Known Limitation: Explicit ID-Linkage", para el razonamiento completo.

**Mitigacion (disponible ya, sin esperar a embeddings del Dia 4):** todo item de
News/Bluesky ya lleva timestamp de ingesta sin importar si matcheo algun market.
`retrieval/time_window.py` responde "que se ingirio en el mundo mientras este
market se movia" via filtro de rango de fechas sobre el raw storage existente --
cero ML, disponible hoy. Combina Layer 1 (linkeado, alta confianza, mostrado
primero) con Layer 2 (ventana temporal completa, señal mas ruidosa pero real,
contexto secundario). El ranking semantico DENTRO de esa ventana ruidosa (no el
descubrimiento de la ventana en si) es trabajo del Dia 4 (RAG/embeddings).

Verificado con datos reales (2026-08-15): para un market con 2 articulos
linkeados, la ventana de 24h trajo 609 articulos + 839 posts adicionales sin
linkear -- confirma que la mayoria de la senal ambiental se perderia sin esta
segunda capa.

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
| S3 bucket | `poly-rag-369970405415` | Storage crudo, particionado `<source>/YYYY-MM-DD/HH.json`, mas `odds/<market_id>.json` (serie de tiempo append-only) |
| DynamoDB table | `poly-rag-architecture-metrics` | Costo/latencia/tokens por invocacion, pay-per-request |
| DynamoDB table | `poly-rag-market-registry` | Market registry (metadata + comment_entity_type/id/link_type), pay-per-request, hash_key `market_id` -- agregada 2026-08-15, campos de comments agregados 2026-08-16 |
| DynamoDB table | `poly-rag-processed-urls` | Dedup de URLs de articulos de News, pay-per-request, hash_key `url`, sin TTL -- agregada 2026-08-15 |
| DynamoDB table | `poly-rag-domain-failures` | Blocklist dinamico de dominios para News, pay-per-request, hash_key `domain` -- agregada 2026-08-16 |
| IAM role | `poly-rag-ingest-lambda-role` | Execution role de las Lambdas de ingestion, permisos: S3 PutObject/GetObject/ListBucket (ListBucket agregado 2026-08-15 -- sin el, GetObject sobre una key inexistente devuelve AccessDenied opaco en vez de NoSuchKey, rompiendo el patron read-modify-write de odds), DynamoDB PutItem en metrics + GetItem/PutItem/UpdateItem/Scan/Query en el registry + tablas de News, Bedrock InvokeModel scoped al modelo especifico |
| IAM policy | `PolyRAG-BudgetBreach-Deny` | Guardrail: bloquea Bedrock/Lambda/S3-writes/DynamoDB-writes si el gasto cruza budget de $10 |
| IAM role | `PolyRAG-BudgetsActionRole` | Permite a AWS Budgets adjuntar la Deny policy automaticamente |
| AWS Budget | $5/mes | Alertas en 20% ($1) y 100% ($5) |
| AWS Budget | $10 | Threshold del guardrail Deny automatico |
| EventBridge rule | `poly-rag-ingest-polymarket-schedule` | Cron `0 0,12 * * ? *` (00:00 y 12:00 UTC), target: Lambda de Polymarket (timeout 600s desde 2026-08-15) |
| EventBridge rule | `poly-rag-ingest-news-schedule` | Mismo cron, target: Lambda de News (timeout 900s desde el rediseño a fan-out paralelo, 2026-08-16) |
| EventBridge rule | `poly-rag-ingest-comments-schedule` | Mismo cron, target: Lambda de Comments (timeout 300s) -- reemplaza `poly-rag-ingest-bluesky-schedule`, destruida 2026-08-16 |

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
- `odds_old_2026-08-15/` en S3: 700 archivos del bootstrap con schema obsoleto,
  movidos ahi en vez de borrados -- desechar oficialmente en unos dias una vez
  confirmada la calidad del nuevo bootstrap
- Explorar paginacion `/markets/keyset` de la Gamma API para conocer el tamano
  real del universo de mercados activos (hoy solo se confirmo un piso de ~2,100
  via offset, que se cae mas alla de eso)
- Fix del bug de horizonte minimo de 48h para mercados deportivos (usa endDate,
  que no refleja la hora real del partido -- ver tech_debt.md)

Ver sprint_plan.md (gerdau/) para el resto del roadmap (Databricks Dia 3 --
Delta Lake/Unity Catalog aun no iniciados, RAG retrieval Dia 4, synthesis agent
Dia 5).
