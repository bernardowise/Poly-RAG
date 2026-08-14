# Session Ledger

Log of work sessions, most recent first. Each entry added by /end.

## 2026-08-14

- Dia 2 del sprint completado: 3 Lambdas de ingestion desplegadas y verificadas con datos reales (Polymarket, News, Bluesky), cada una escribiendo a S3 y registrando metricas de costo/latencia en DynamoDB (tabla poly-rag-architecture-metrics)
- Decision de alcance ajustada en vivo: 3 Lambdas independientes (no orquestador unico) para aislar fallos y proteger el limite de PUT requests en reintentos
- Reddit descartado como fuente (su Responsible Builder Policy prohibe explicitamente usar datos para IA/ML); X y Truth Social tambien evaluados y descartados; reemplazados por Bluesky (AT Protocol)
- Bug real encontrado y corregido en produccion: searchPosts de Bluesky requiere auth contra bsky.social (el PDS), no public.api.bsky.app como indicaba la investigacion inicial -- corregido tras 403s reales
- Filtro de verticales (Macro/Geopolitica/Regulatorio-Tech) definido con keywords especificas, mas exclusion explicita de mercados deportivos tras detectar falsos positivos por matching de substring (ej. sec matcheando dentro de second)
- Decision arquitectonica: LLM (Bedrock/Claude Sonnet 4.5) trial en ingestion para las 3 fuentes, midiendo costo/latencia real por 3-4 dias antes de decidir si se mantiene -- una sola llamada batched por corrida, no una por item, para controlar costo
- Costo real medido: ~0.018 USD por ciclo completo de las 3 fuentes (~1.10 USD/mes proyectado a cadencia de 12h), dentro del presupuesto
- Bloqueos resueltos en el camino: Sonnet 5 no disponible (se uso 4.5 via inference profile), suscripcion AWS Marketplace con delay de propagacion en cuenta nueva

## 2026-08-13

- Sprint de entrevista (gerdau/sprint_plan.md) Dia 1 cerrado: cuenta AWS creada desde cero, MFA en root, budget de $5/mes con alertas 20%/100%, usuario IAM admin (dejando de usar root), AWS CLI v2 instalada y configurada
- Confirmados $120 en creditos promocionales (100 signup + 20 por completar actividad de Budgets) -- CLAUDE.md actualizado para tratar el budget como disciplina real, no colchon para gastar libre
- Guardrail adicional (no estaba en el plan original): policy IAM Deny (PolyRAG-BudgetBreach-Deny) + role (PolyRAG-BudgetsActionRole) armados via CLI para bloquear Bedrock/Lambda/S3-writes/DynamoDB-writes automaticamente si el gasto cruza un budget separado de $10
- Primer contacto practico con S3: bucket creado (poly-rag-369970405415), upload/list confirmado via CLI
- Bloque 5 (Lambda hello-world) abandonado deliberadamente -- se prefirio ir directo a Lambda con trabajo real en Dia 2 en vez de un paso aislado
- Bloque 6 (Bedrock vs SageMaker) cubierto conceptualmente sin necesidad de profundizar mas -- diferencia clara: Bedrock para modelos fundacionales via API (nuestro caso), SageMaker para entrenar modelos propios desde cero (fuera de scope de Poly-RAG)
- Conceptos nuevos archivados en knowledge.md: mapeo AWS/GCP de storage, NoSQL (schema flexible no ausencia de estructura), pricing por request vs volumen, mecanismo de Budget Actions
- Siguiente: Dia 2 -- ingestion real de Polymarket via Lambda + S3 + EventBridge
