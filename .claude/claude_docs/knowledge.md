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

## 2026-08-14 — Databricks: clusters, notebooks, y donde encaja vs. el pipeline de ingestion

Un **cluster** en Databricks es un grupo de maquinas (nodos) corriendo Apache Spark --
diseñado para computo distribuido sobre datasets que no caben o son lentos de procesar
en una sola maquina. El trabajo se reparte en paralelo entre nodos. El patron es el
mismo sin importar el tamaño del dataset (unos KB de prueba o TBs reales) -- el codigo
no cambia, solo escala el cluster.

La analogia correcta NO es "Excel version IA" -- es mas preciso pensarlo como **Jupyter/
Colab + Spark + una capa de gobernanza encima**, alojado en la nube. El notebook corre
en el navegador contra el cluster remoto (como Colab, no como Jupyter local). Lo que
Colab/Jupyter no tienen y Databricks si:

- **Unity Catalog** -- gobernanza (quien puede leer que tabla, linaje de datos)
- **Delta Lake** -- versionado transaccional de las tablas (transaction log, time travel)

**Donde encaja en Poly-RAG:** Databricks es la capa de exploracion/aprendizaje del
sprint (Dia 3), NO reemplaza el pipeline de ingestion en Lambda que ya corre en
produccion cada 12h (eso sigue siendo el ELT real del proyecto). Meter Databricks
dentro del pipeline live agregaria costo fuera del presupuesto de ~$1/mes -- por ahora
es una pieza aislada para demostrar fluidez conceptual en la entrevista, no integrada
al ciclo de datos automatico.

**Contexto:** El usuario comparaba Databricks con Excel y con el flujo tradicional de
abrir Jupyter/Colab e iniciar una sesion de Spark, al arrancar el Dia 3 del sprint
(Databricks Free Edition) despues de completar la automatizacion de ingestion en AWS.

## 2026-08-14 — Paralelismo de datos (Spark) vs paralelismo de computo (entrenamiento DL)

Son dos tipos de paralelismo distintos, no solo dos herramientas distintas:

**Spark -- paralelismo de datos:** distribuye filas entre nodos CPU. El problema que
resuelve no es "esto es dificil de calcular" sino "hay demasiadas filas para que una
sola maquina las procese en tiempo razonable" (ej. filtrar/agregar 500GB de logs). La
operacion por fila es barata; el volumen es lo que exige distribuir. Por eso corre en
CPU normal, sin GPU.

**Entrenamiento DL -- paralelismo de computo matematico:** el problema es que cada
operacion individual es cara -- multiplicar matrices gigantes (los pesos de la red)
contra un batch, repetidamente por cada epoch (backpropagation). GPU/TPU tienen miles
de nucleos disenados especificamente para multiplicacion de matrices en paralelo
masivo; una CPU podria hacerlo pero ordenes de magnitud mas lento. CUDA es la capa de
software de NVIDIA que le da a frameworks (PyTorch/TensorFlow) acceso directo a esos
nucleos. Confirmado: es posible necesitar GPU con un dataset chico si el modelo tiene
muchos parametros -- el peso esta en la operacion, no solo en el volumen de datos.

**Donde entrenar DL en AWS:** SageMaker (Training Jobs especificamente) -- computo
efimero con GPU, se apaga al terminar, pagas solo ese tiempo. Pricing real: ml.g4dn.xlarge
(1 GPU T4, similar a Colab free) ~$0.74/hr; ml.p3.2xlarge (1 GPU V100) ~$3.82/hr;
ml.p4d.24xlarge (8x A100, nivel LLM real) decenas de USD/hr. El free tier de AWS NO
incluye instancias GPU -- entrenar DL en AWS romperia un presupuesto de $5-10/mes en
minutos, no meses.

**Databricks tambien soporta DL training, no solo Spark** -- clusters con GPU
configurables, integracion nativa con MLflow para trackear experimentos. La plataforma
se expandio de "Spark as a service" a cubrir todo el ciclo de ML. El Free Edition
especificamente NO tiene GPU salvo verificacion via LinkedIn (y aun asi limitado) --
no apto para entrenamiento real, solo para el tier de pago con clusters GPU explicitos.

**Contexto:** El usuario pregunto por primeros principios por que entrenar DL requiere
GPU/TPU/CUDA mientras Spark no, y si Databricks esta restringido a Spark o tambien
soporta DL training -- relevante porque Poly-RAG explicitamente NO entrena modelos
propios (usa Bedrock/Sonnet ya entrenado), asi que esto es contexto conceptual para la
entrevista, no algo que el proyecto vaya a implementar.

## 2026-08-14 — Indexacion: raw storage vs estructurado (SQL) vs semantico (vectorial/RAG)

Guardar datos en S3 (o cualquier storage) NO es lo mismo que indexarlos. "Raw storage"
es tener archivos individuales (ej. un JSON por corrida de Lambda) que solo se pueden
leer si sabes exactamente cual abrir -- no hay forma de "preguntarle" nada al conjunto
completo sin leer archivo por archivo.

**Indexar** significa construir una estructura auxiliar que permite consultar sin leer
todo lo crudo. Hay dos tipos distintos, que responden preguntas distintas:

1. **Indexacion estructurada (SQL-consultable):** convierte "un monton de archivos
   sueltos" en "una tabla" -- permite queries tipo `SELECT summary FROM digests WHERE
   source = 'news' AND fecha > X`. Se resuelve con Delta Lake + Unity Catalog
   (Databricks) o, nativo a AWS, Glue Catalog + Athena. Es la "T" (Transform) del
   patron ELT -- el paso que falta despues de Extract+Load.

2. **Indexacion semantica (vectorial, para RAG):** permite preguntar en lenguaje
   natural (ej. "que se dijo sobre inflacion la semana pasada?") y que el sistema
   encuentre contenido semanticamente relevante, no por coincidencia exacta de
   palabra. Requiere convertir texto en embeddings (vectores numericos) guardados en
   un vector store (DynamoDB con busqueda vectorial, OpenSearch, etc.)

**Orden logico:** primero estructurado, despues semantico -- no tiene sentido generar
embeddings sobre datos crudos desorganizados si aun no se sabe que campos importan.
Se necesita la tabla limpia (Tipo 1) antes de decidir que embedear (Tipo 2). Esto
conecta directamente el Dia 3 (Databricks/Delta Lake) con el Dia 4 (RAG/retrieval) del
sprint -- el primero construye el insumo estructurado que el segundo necesita.

**Contexto:** El usuario pregunto como se indexan los llm_summary que las Lambdas de
ingestion generan y guardan en S3 (distinto del correo digest que ya recibe) --
confirmando que hoy solo estan en raw storage, sin indexar de ninguna forma, justo
antes de arrancar el Dia 3 del sprint.
