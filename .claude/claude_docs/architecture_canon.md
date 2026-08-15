# Architecture Canon

**Snapshot del estado actual de la arquitectura de Poly-RAG.** Este documento se
actualiza/sobreescribe conforme la arquitectura evoluciona -- no es una bitacora de
decisiones pasadas (eso vive en session_ledger.md) ni una lista de pendientes (eso
vive en tech_debt.md). Si algo aqui queda obsoleto, se reemplaza, no se acumula.

Ultima actualizacion: 2026-08-14

---

## Data Sources (Ingestion Layer)

Tres Lambdas independientes, no un orquestador unico -- una falla en una fuente no
tumba las otras dos, y los reintentos no desperdician PUT requests de las fuentes
que si funcionaron.

**Metadata de linaje (agregado 2026-08-14):** cada JSON escrito a S3 lleva un bloque
`metadata` con `schema_version`, `lambda_name`, `lambda_request_id` (para rastrear el
archivo de vuelta a su ejecucion exacta en CloudWatch Logs), `llm_used` + `llm_model_id`,
`latency_ms`, y `estimated_cost_usd` -- permite auditar linaje/costo de un archivo
individual sin tener que cruzar con la tabla DynamoDB de metricas.

### 1. Polymarket (`poly-rag-ingest-polymarket`)
- **API:** Gamma API (`gamma-api.polymarket.com`), REST/JSON, sin auth
- **Filtro:** `active=true`, top 100 ordenado por volumen
- **Tagging:** por vertical via keyword match con word-boundary regex, mas
  exclusion explicita de mercados deportivos (ver `SPORTS_EXCLUSION_PATTERNS`
  en el handler)
- **Output:** `s3://poly-rag-369970405415/polymarket/YYYY-MM-DD/HH.json`

### 2. News (`poly-rag-ingest-news`)
- **Fuentes:** 10 feeds RSS curados (BBC World/Business, CBC Business/TopStories,
  NYT World/Opinion/Technology, CNN TopStories/World, France24 English)
- **Formato:** XML, parseado con `xml.etree.ElementTree` (sin dependencias externas)
- **Resiliencia:** fallo por-feed aislado -- si uno cae, los otros 9 siguen
- **Tagging:** mismo esquema de keywords por vertical que Polymarket
- **Output:** `s3://poly-rag-369970405415/news/YYYY-MM-DD/HH.json`

### 3. Bluesky (`poly-rag-ingest-bluesky`)
- **API:** AT Protocol, endpoint `app.bsky.feed.searchPosts`
- **Auth:** requerida -- `com.atproto.server.createSession` con app password
  (no el password de la cuenta), re-autentica cada invocacion
- **Endpoint correcto:** `bsky.social` (el PDS) para AMBOS createSession y
  searchPosts -- `public.api.bsky.app` devuelve 403 en searchPosts especificamente
  aunque otros endpoints de lectura ahi si funcionan
- **Query strategy:** una busqueda por vertical (1 keyword representativa cada
  una), no un filtro OR multi-keyword como Polymarket/News -- limitacion de la API
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

## Vertical Taxonomy

Poly-RAG no ingiere todos los mercados/noticias indiscriminadamente. Se filtran y
etiquetan al momento de ingestion en 3 verticales, elegidas por alta volatilidad,
abundante texto correlacionable en la web, y reglas de resolucion estrictas --
propiedades que las hacen buen material de RAG frente a mercados de pop-culture
ruidosos que dependen de chisme y texto no estructurado.

**Arquitectura:** pipeline unico, dato etiquetado -- no storage fisicamente separado
por vertical. Mercados/articulos frecuentemente abarcan mas de una vertical (ej. un
mercado de regulacion de IA es tanto geopolitica como regulatorio-tech), asi que una
separacion rigida forzaria clasificaciones arbitrarias y logica de filtrado duplicada
3 veces. En cambio, cada item ingerido lleva un campo `verticals` (array -- un
mercado/articulo puede pertenecer a mas de una), y tanto Polymarket como News caen en
el mismo esquema de particion S3 (`s3://bucket/<source>/YYYY-MM-DD/HH.json`) sin
importar la vertical.

Los filtros hacen match contra `question`/`description` de Polymarket y
titulo/descripcion de articulos de noticias, con regex de word-boundary (no substring
plano -- ver bug de "sec" matcheando "second" corregido 2026-08-13).

### 1. Macro / Central Banks
Alta correlacion con noticias financieras; se mueve con decisiones de la Fed, datos
de CPI, etc.

Keywords: `fed`, `federal reserve`, `interest rate`, `inflation`, `cpi`,
`unemployment`, `recession`, `gdp`, `trump`, `truth social`

Nota: Trump/Truth Social incluidos aqui (y cross-tagged en geopolitica) porque sus
posts mueven mercados directo y rapido -- aranceles, nominaciones de la Fed, etc. --
no solo como figura politica sino como fuente de noticias que mueve mercados por si
misma.

### 2. Geopolitics / Elections
Alta volatilidad por eventos de noticias en tiempo real -- un tweet o declaracion
diplomatica puede mover precios bruscamente.

Keywords: `election`, `president`, `war`, `ceasefire`, `sanctions`, `tariff`, `nato`,
`invasion`, `trump`, `putin`, `xi`, `ukraine`, `taiwan`, `china`

### 3. Regulatory / Tech
Nicho especialista -- reglas de resolucion largas y tecnicas (estatus de ensayos FDA,
progreso de casos antitrust) donde el valor de resumen de un RAG es mas alto.

Keywords: `fda`, `antitrust`, `lawsuit`, `sec`, `regulation`, `approval`, `ban`,
`ai regulation`, `google`, `apple`, `meta`, `openai`, `anthropic`, `spacex`

### Out of scope

Mercados/articulos que no matchean ninguna vertical (pop culture, deportes, premios
de entretenimiento, etc.) no se ingieren. Razon: pobre material de RAG -- la
resolucion depende de chisme/criterio subjetivo, y hay poco texto estructurado y
correlacionable en la web contra que hacer retrieval.

---

## News Feed Inventory

10 feeds RSS curados, alineados aproximadamente por vertical, en vez de un feed
generico filtrado post-hoc. Confirmados funcionando (via curl con User-Agent de
navegador -- varios feeds rechazan el UA default de curl, ej. CNBC devuelve 403):

| Feed | URL | Vertical(es) |
|---|---|---|
| BBC World | `feeds.bbci.co.uk/news/world/rss.xml` | Geopolitics |
| BBC Business | `feeds.bbci.co.uk/news/business/rss.xml` | Macro |
| CBC Business | `cbc.ca/webfeed/rss/rss-business` | Macro |
| CBC Top Stories | `cbc.ca/webfeed/rss/rss-topstories` | Geopolitics |
| NYT World | `rss.nytimes.com/services/xml/rss/nyt/World.xml` | Geopolitics |
| NYT Opinion | `rss.nytimes.com/services/xml/rss/nyt/Opinion.xml` | Macro + Geopolitics (sentiment editorial) |
| NYT Technology | `rss.nytimes.com/services/xml/rss/nyt/Technology.xml` | Regulatory/Tech |
| CNN Top Stories | `rss.cnn.com/rss/cnn_topstories.rss` | Geopolitics |
| CNN World | `rss.cnn.com/rss/cnn_world.rss` | Geopolitics |
| France 24 English | `france24.com/en/rss` | Geopolitics |

**Gap conocido:** el peso de verticales esta cargado hacia geopolitica y ligero en
regulatorio/tech (NYT Technology es la unica fuente dedicada ahi) -- revisar si
mercados regulatorio-tech quedan desatendidos por el matching de noticias en el Dia 4.

**Nota de licencia:** el aviso de copyright RSS de CBC dice "FOR PERSONAL USE ONLY"
-- aceptable para este proyecto personal de aprendizaje; requeriria revision si el
proyecto se volviera publico/comercial.

**Descartado:** Latinus (medio mexicano) evaluado pero sin feed RSS estandar
accesible (variantes `/rss` y `/feed/` devuelven 404 o redirigen a HTML, no XML).
Cobertura en espanol sigue siendo un gap; candidatos futuros (El Financiero, Reforma)
sin evaluar aun.

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

**Costo real medido (2026-08-13/14):**

| Fuente | Items/corrida | Costo/corrida |
|---|---|---|
| Polymarket | ~22 | ~$0.0064 |
| News | ~93 | ~$0.0057 |
| Bluesky | ~50 | ~$0.0063 |

Total ciclo completo: ~$0.018. Proyectado a cadencia de 12h: ~$1.10/mes.

---

## Infrastructure Inventory

| Recurso | Nombre | Proposito |
|---|---|---|
| S3 bucket | `poly-rag-369970405415` | Storage crudo, particionado `<source>/YYYY-MM-DD/HH.json` |
| DynamoDB table | `poly-rag-architecture-metrics` | Costo/latencia/tokens por invocacion, pay-per-request |
| IAM role | `poly-rag-ingest-lambda-role` | Execution role de las 3 Lambdas, permisos minimos (S3 PutObject, DynamoDB PutItem, Bedrock InvokeModel scoped al modelo especifico) |
| IAM policy | `PolyRAG-BudgetBreach-Deny` | Guardrail: bloquea Bedrock/Lambda/S3-writes/DynamoDB-writes si el gasto cruza budget de $10 |
| IAM role | `PolyRAG-BudgetsActionRole` | Permite a AWS Budgets adjuntar la Deny policy automaticamente |
| AWS Budget | $5/mes | Alertas en 20% ($1) y 100% ($5) |
| AWS Budget | $10 | Threshold del guardrail Deny automatico |
| EventBridge rule | `poly-rag-ingest-polymarket-schedule` | Cron `0 0,12 * * ? *` (00:00 y 12:00 UTC), target: Lambda de Polymarket |
| EventBridge rule | `poly-rag-ingest-news-schedule` | Mismo cron, target: Lambda de News |
| EventBridge rule | `poly-rag-ingest-bluesky-schedule` | Mismo cron, target: Lambda de Bluesky |

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

- Confirmacion de CloudWatch Logs en corridas automaticas (primera ejecucion via
  EventBridge aun no observada -- las pruebas hasta ahora fueron invocacion manual)
- Cierre del trial LLM-en-ingestion (3-4 dias de datos, ver seccion LLM Enrichment)

Ver sprint_plan.md (gerdau/) para el resto del roadmap (Databricks Dia 3, RAG
retrieval Dia 4, synthesis agent Dia 5).
