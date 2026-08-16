# Architecture Canon

**Snapshot del estado actual de la arquitectura de Poly-RAG.** Este documento se
actualiza/sobreescribe conforme la arquitectura evoluciona -- no es una bitacora de
decisiones pasadas (eso vive en session_ledger.md) ni una lista de pendientes (eso
vive en tech_debt.md). Si algo aqui queda obsoleto, se reemplaza, no se acumula.

Ultima actualizacion: 2026-08-16

---

## Data Sources (Ingestion Layer)

Tres Lambdas independientes, no un orquestador unico -- una falla en una fuente no
tumba las otras dos, y los reintentos no desperdician PUT requests de las fuentes
que si funcionaron.

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

### 3. Bluesky (`poly-rag-ingest-bluesky`)
- **API:** AT Protocol, endpoint `app.bsky.feed.searchPosts`
- **Auth:** requerida -- `com.atproto.server.createSession` con app password
  (no el password de la cuenta), re-autentica cada invocacion
- **Endpoint correcto:** `bsky.social` (el PDS) para AMBOS createSession y
  searchPosts -- `public.api.bsky.app` devuelve 403 en searchPosts especificamente
  aunque otros endpoints de lectura ahi si funcionan
- **Query strategy:** ya no son 3 queries fijas por vertical -- una busqueda
  searchPosts por CADA market abierto en el registry (cobertura completa, no un
  top-N), usando el `search_query` de ese market. Retry/backoff en 429 (rate
  limit no medido empiricamente a este volumen, se prefiere backoff sobre
  adivinar un N seguro)
- **Timeout:** 600s (subido de 60s -- 500+ llamadas HTTP secuenciales externas)
- **Escala real observada (2026-08-15):** 492 markets consultados, 689 posts,
  0 fallos, 83s de duracion real -- muy por debajo del timeout, sin rate limiting
- **Output:** `s3://poly-rag-369970405415/bluesky/YYYY-MM-DD/HH.json`

**Reemplaza a:** Reddit (descartado -- su Responsible Builder Policy prohibe uso
de datos para IA/ML), X/Twitter (sin tier gratuito viable + prohibicion identica),
Truth Social (sin API publica para individuos).

### 4. Email Digest (`poly-rag-send-digest`)
- **Proposito:** consolida los 3 llm_summary mas recientes (uno por fuente) y los
  manda por correo -- pieza permanente de infraestructura, independiente de si el
  LLM-en-ingestion trial se mantiene o revierte (ver seccion LLM Enrichment)
- **Envio:** Amazon SES (`ses:SendEmail`), remitente y destinatario verificados
  (`bernardolw@gmail.com`, modo sandbox)
- **Trigger:** EventBridge, 5 min despues de cada ciclo de ingestion (00:05 y 12:05
  UTC) -- da tiempo a que las 3 Lambdas de ingestion terminen de escribir a S3
- **Degradacion graceful:** si `llm_summary` es null (trial revertido) o no hay
  objeto S3 reciente, reporta "no summary available" / "no data" por fuente en vez
  de fallar la Lambda completa
- **Permisos:** solo lectura de S3 (`s3:GetObject`, `s3:ListBucket`) -- distinto
  del role de las 3 Lambdas de ingestion, que solo tienen escritura

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
`resolution_source`, `status` (open/resolved), `search_query`, `news_match_terms`,
`first_seen`, `last_updated`, `resolution_date`, `final_outcome`.

**Poblado por `ingest_polymarket`:**
1. Trae top 500 candidatos activos por `volume24hr`
2. Diffea contra el registry -- solo los ids genuinamente nuevos pasan por el LLM
   (costo escala con tasa de aparicion de ids nuevos, no con N -- ver tech_debt.md)
3. Llamada batched a Bedrock (20 markets/batch) que devuelve, por market: el
   veredicto de verificabilidad Y dos representaciones de busqueda derivadas (ver
   abajo) -- un solo llamado LLM, sin costo adicional de Bedrock
4. Solo los verificables entran al registry; el resto se descarta
5. Ids que estaban `open` pero ya no aparecen en el top-500: se consulta
   `markets/{id}` directo en la Gamma API para capturar `closed` + outcome real
   (no se infiere resolucion por ausencia)

**Dos representaciones de busqueda, no una (correccion 2026-08-15, mismo dia):**
un primer intento con keywords sueltas (ej. `["Elon Musk", "tweets", "August
2026"]`) perdia senal si se buscaban por separado. News y Bluesky matchean texto
de formas distintas, asi que se generan dos campos en el mismo llamado LLM:
- **`search_query`** (string combinado de texto libre): para Bluesky's searchPosts,
  que acepta queries estilo motor de busqueda
- **`news_match_terms`** (lista corta de 1-3 frases distintivas): para News, que
  no tiene API de busqueda -- descarga el texto RSS completo y hace grep, asi que
  el match requiere logica AND (todos los terminos deben co-ocurrir en el mismo
  articulo) contra terminos suficientemente especificos

**Auditoria (Bronze layer):** el prompt completo y la respuesta cruda del LLM se
imprimen a CloudWatch Logs, y las respuestas crudas se guardan tambien en
`metadata.llm_raw_responses` del payload S3 -- permite re-inspeccionar exactamente
que dijo el modelo si un item del registry se ve mal clasificado.

**Robustez:** un batch con JSON malformado/truncado (max_tokens insuficiente,
observado en produccion) se salta sin tumbar la Lambda completa -- esos markets
simplemente se re-evaluan el siguiente ciclo (siguen siendo "nuevos" mientras no
entren al registry).

### Odds Time-Series (S3: `odds/<market_id>.json`)

Append-only, un archivo por market, un snapshot agregado cada ciclo (nunca
sobreescrito) -- esta ES la diferenciacion real del proyecto (self-built historical
time-series). Cada snapshot: `timestamp`, `outcomePrices`, `volume`, `volume24hr`,
`liquidity`. Read-modify-write por ciclo: lee el archivo existente (o inicia uno
nuevo si no existe -- requiere `s3:ListBucket` a nivel bucket ademas de
`s3:GetObject`, ver nota IAM abajo), agrega el snapshot, reescribe.

### Linkeo News/Bluesky -> market_id (Layer 1, alta confianza)

- **News (rediseñado 2026-08-16, ver "News Source Redesign" en tech_debt.md):**
  `news_match_terms` y el AND-match ya NO se usan -- cada articulo se etiqueta
  con `market_ids: [market_id]` segun a que busqueda de Google News (una por
  market, usando `question` tal cual) perteneciera. El linkeo es 1:1 por
  construccion (la busqueda ya es especifica al market), no un match posterior
  contra texto libre.
- **Bluesky:** sin cambios -- un `searchPosts` por market abierto usando su
  `search_query`; cada post resultante lleva `market_ids: [ese market]`.

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
(`search_query` + `news_match_terms` presentes en todos).

**Bluesky (2026-08-15):** 492 markets consultados, 689 posts, 0 fallos.

**News (2026-08-16, tras el rediseño a Google News RSS + fan-out paralelo):**
228/228 markets procesados, 887 articulos, `cycle_complete: true`. Reemplaza la
cifra vieja de 367 articulos/3 linkeados del diseño de 10-feeds-RSS +
`news_match_terms`, que causo el rediseño completo (ver tech_debt.md, "News
Source Redesign").

---

## LLM Enrichment (Ingestion) -- Trial en curso

**Estado:** trial activo desde 2026-08-13, evaluando 3-4 dias antes de decidir
mantener/revertir. Ver tech_debt.md y session_ledger.md para el razonamiento
completo detras de esta decision.

- **Modelo:** Claude Sonnet 4.5 via Bedrock (`us.anthropic.claude-sonnet-4-5-20250929-v1:0`,
  inference profile -- Sonnet 5 aun no disponible sin contacto a AWS Sales)
- **Patron:** una sola llamada batched a Bedrock por corrida de cada Lambda
  (resume hasta 20 items), NO una llamada por item individual -- controla costo
- **Auth:** IAM (boto3 bedrock-runtime), mismas credenciales que S3/DynamoDB --
  sin API key separada de Anthropic
- **Toggle:** variable de entorno `USE_LLM_ENRICHMENT` (true/false) por Lambda,
  permite comparar con/sin LLM en la misma tabla de metricas

**Costo real medido, filtro viejo por vertical (2026-08-13/14, pre-rediseno):**

| Fuente | Items/corrida | Costo/corrida |
|---|---|---|
| Polymarket | ~22 | ~$0.0064 |
| News | ~93 | ~$0.0057 |
| Bluesky | ~50 | ~$0.0063 |

Total ciclo completo: ~$0.018. Proyectado a cadencia de 12h: ~$1.10/mes.

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
| DynamoDB table | `poly-rag-market-registry` | Market registry (metadata + search_query + news_match_terms), pay-per-request, hash_key `market_id` -- agregada 2026-08-15 |
| IAM role | `poly-rag-ingest-lambda-role` | Execution role de las 3 Lambdas, permisos: S3 PutObject/GetObject/ListBucket (ListBucket agregado 2026-08-15 -- sin el, GetObject sobre una key inexistente devuelve AccessDenied opaco en vez de NoSuchKey, rompiendo el patron read-modify-write de odds), DynamoDB PutItem en metrics + GetItem/PutItem/UpdateItem/Scan/Query en el registry, Bedrock InvokeModel scoped al modelo especifico |
| IAM policy | `PolyRAG-BudgetBreach-Deny` | Guardrail: bloquea Bedrock/Lambda/S3-writes/DynamoDB-writes si el gasto cruza budget de $10 |
| IAM role | `PolyRAG-BudgetsActionRole` | Permite a AWS Budgets adjuntar la Deny policy automaticamente |
| AWS Budget | $5/mes | Alertas en 20% ($1) y 100% ($5) |
| AWS Budget | $10 | Threshold del guardrail Deny automatico |
| EventBridge rule | `poly-rag-ingest-polymarket-schedule` | Cron `0 0,12 * * ? *` (00:00 y 12:00 UTC), target: Lambda de Polymarket (timeout 600s desde 2026-08-15) |
| EventBridge rule | `poly-rag-ingest-news-schedule` | Mismo cron, target: Lambda de News (timeout 60s, sin cambio) |
| EventBridge rule | `poly-rag-ingest-bluesky-schedule` | Mismo cron, target: Lambda de Bluesky (timeout 600s desde 2026-08-15 -- 500+ llamadas HTTP externas por ciclo) |

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
- **Secrets:** las credenciales de Bluesky (`BLUESKY_HANDLE`, `BLUESKY_APP_PASSWORD`)
  NO estan en el codigo Terraform ni en el state -- se gestionan manualmente via CLI,
  con `lifecycle { ignore_changes }` en el recurso Lambda para que Terraform no
  intente sobreescribirlas a vacio en cada apply
- **De aqui en adelante:** cambios de infraestructura (nueva Lambda, ajustar cron,
  nuevos permisos IAM) se hacen editando `.tf` + `terraform apply`, no mas comandos
  sueltos de `aws` CLI para crear/modificar recursos

---

## Pendiente para completar el ciclo de ingestion

- Confirmado (2026-08-15): las 4 Lambdas corren solas via EventBridge -- el ciclo
  automatico de las 12:00 UTC del 15 corrio sin intervencion manual
- Cierre del trial LLM-en-ingestion (3-4 dias de datos, ver seccion LLM Enrichment)
  -- el rediseno de ingestion hace que el LLM ahora sea el filtro de calidad mismo,
  no solo un resumen opcional, lo cual refuerza el caso para mantenerlo
- Medir en la practica la tasa real de ids nuevos por ciclo (determina el costo de
  estado-estable real, ver seccion "Market Registry + Ingestion Redesign")
- `odds_old_2026-08-15/` en S3: 700 archivos del bootstrap con schema obsoleto
  (keywords en vez de search_query/news_match_terms), movidos ahi en vez de
  borrados -- desechar oficialmente en unos dias una vez confirmada la calidad
  del nuevo bootstrap
- Explorar paginacion `/markets/keyset` de la Gamma API para conocer el tamano
  real del universo de mercados activos (hoy solo se confirmo un piso de ~2,100
  via offset, que se cae mas alla de eso)

Ver sprint_plan.md (gerdau/) para el resto del roadmap (Databricks Dia 3 --
Delta Lake/Unity Catalog aun no iniciados, RAG retrieval Dia 4, synthesis agent
Dia 5).
