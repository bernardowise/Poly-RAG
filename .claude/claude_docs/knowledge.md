# Knowledge

Technical concepts (RAG, NLP, LLM deployment, hallucinations, reranking, etc.) explained
during sessions, archived here for reference. Entries added by /knowledge, most recent last.

## 2026-08-13 — AWS vs GCP storage/database service mapping

AWS and GCP split storage into equivalent tiers, but the naming doesn't map 1:1 across
providers:

| Concept | AWS | GCP |
|---|---|---|
| Object storage (raw files) | S3 | GCS |
| NoSQL key-value/document DB | DynamoDB | Firestore / Bigtable |
| Data warehouse (SQL analytics) | Redshift / Athena | BigQuery |

DynamoDB is AWS-native NoSQL, not a GCP service. BigQuery's actual AWS equivalent is
Redshift/Athena (analytical SQL over large datasets), not DynamoDB.

This split also underpins the Lakehouse / ELT pattern: raw data lands first in object
storage (S3/GCS) as-is (Extract + Load), and transformation (Transform) happens after,
as-needed, rather than before loading like classic ETL. Delta Lake (Databricks) adds
versioning/traceability on top of that raw-to-queryable transform step.

**Contexto:** Comparando el stack de Poly-RAG (S3, DynamoDB) contra el vocabulario de GCP
que el usuario ya conocia (GCS, BigQuery), durante el Dia 1 del sprint de AWS/Databricks.

## 2026-08-13 — NoSQL: que relaja realmente (no es "sin estructura", es "sin schema fijo")

La etiqueta "NoSQL" confunde porque suena a "datos no estructurados", pero JSON SI es
estructurado (tiene llaves, tipos, jerarquia). Lo que NoSQL relaja es otra cosa: el
**schema rigido y uniforme entre registros** que exige SQL.

En una tabla SQL, cada fila debe tener exactamente las mismas columnas. En una base NoSQL
tipo documento (ej. DynamoDB), cada item puede tener campos distintos entre si -- util
cuando, por ejemplo, un mercado de Polymarket tiene 5 campos y otro tiene 8 porque es un
tipo de evento distinto.

Tampoco es cierto que NoSQL no soporte queries -- si las soporta, pero mas limitadas que
SQL. DynamoDB es rapidisimo para "dame el item con esta key exacta", pero torpe para
"todos los items donde el campo X contenga la palabra Y" (eso lo resuelve mejor SQL o un
motor de busqueda). El trade-off real: se sacrifica flexibilidad de consulta a cambio de
velocidad y escala masiva a bajo costo.

Formato real de las fuentes de datos del proyecto:
- Polymarket Gamma API -> JSON via REST (array de objetos: question, outcomes, volume, etc.)
- RSS de noticias -> XML (RSS es un dialecto de XML), con texto libre no estructurado
  dentro de campos como `<description>`, que es lo que alimenta el pipeline de NLP/sentiment.

**Contexto:** El usuario pregunto por que usar una NoSQL DB si "puedes abrir el archivo y
ya" -- llevo a explicar el trade-off real de NoSQL (schema flexible, no ausencia de
estructura) y el formato real de los feeds de Polymarket/RSS que va a ingerir el proyecto.

## 2026-08-13 — AWS pricing: por request vs por volumen, y auto-shutdown via Budget Actions

Cuando se pregunto si pullear datos frecuente (poco volumen, muchas veces) cuesta igual
que pullear infrecuente (mucho volumen, pocas veces), la respuesta depende del servicio:

- **S3**: cobra dos cosas por separado -- (1) por request (PUT/GET, cobrado por llamada,
  casi sin importar el tamano del payload dentro de rangos normales) y (2) por storage
  (GB totales guardados, sin importar cuantas escrituras se hicieron). Por eso pullear
  1MB cada hora (24 requests/dia) sale MAS caro en requests que pullear 24MB una vez al
  dia (1 request/dia), aunque el volumen final almacenado sea el mismo.
- **Lambda**: cobra por invocacion + duracion x memoria. Aqui si importa el tamano del
  payload (mas datos = mas tiempo procesando), pero no es una funcion lineal de
  "frecuencia" sino de "trabajo total hecho".
- **Bedrock**: cobra por tokens procesados, sin importar cuantas llamadas se hicieron --
  1 llamada de 10K tokens cuesta igual que 10 llamadas de 1K tokens cada una.

Conclusion practica: para cadencias de ingestion tipo Poly-RAG (Polymarket/noticias/Reddit
cada pocas horas), es mas barato agrupar/batchear escrituras (menos requests, mismo o mas
volumen por request) que pullear frecuente con payloads chicos -- el limite de 2,000 PUT/mes
del free tier de S3 se agota mucho antes que el limite de 5GB de storage.

Sobre proteccion de gasto: AWS Budgets soporta "Budget Actions" -- una alerta puede
disparar automaticamente una accion, no solo un email. No existe un kill-switch universal
instantaneo que apague todo (S3/Lambda/Bedrock) al llegar a un monto, porque servicios
como S3 no tienen "on/off" (el storage ya escrito sigue generando costo aunque se corte
acceso). Lo mas cercano y realista: una Budget Action que, al llegar a un umbral, adjunte
una IAM policy tipo Deny al usuario/rol que bloquea llamadas activas de gasto (Bedrock
InvokeModel, Lambda Invoke, escrituras a S3/DynamoDB) -- detiene el sangrado sin borrar
nada ya guardado.

**Contexto:** El usuario pregunto si se puede configurar un auto-shutdown de recursos AWS
al llegar a $10 de consumo (por una mala experiencia previa gastando $50 en creditos de
GCP en una sola corrida), y si pullear distintas fuentes de datos con distinta frecuencia
tiene distinto costo -- relevante para disenar la cadencia de ingestion del Dia 2 del sprint.
