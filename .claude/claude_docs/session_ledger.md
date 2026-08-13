# Session Ledger

Log of work sessions, most recent first. Each entry added by /end.

## 2026-08-13

- Sprint de entrevista (gerdau/sprint_plan.md) Dia 1 cerrado: cuenta AWS creada desde cero, MFA en root, budget de $5/mes con alertas 20%/100%, usuario IAM admin (dejando de usar root), AWS CLI v2 instalada y configurada
- Confirmados $120 en creditos promocionales (100 signup + 20 por completar actividad de Budgets) -- CLAUDE.md actualizado para tratar el budget como disciplina real, no colchon para gastar libre
- Guardrail adicional (no estaba en el plan original): policy IAM Deny (PolyRAG-BudgetBreach-Deny) + role (PolyRAG-BudgetsActionRole) armados via CLI para bloquear Bedrock/Lambda/S3-writes/DynamoDB-writes automaticamente si el gasto cruza un budget separado de $10
- Primer contacto practico con S3: bucket creado (poly-rag-369970405415), upload/list confirmado via CLI
- Bloque 5 (Lambda hello-world) abandonado deliberadamente -- se prefirio ir directo a Lambda con trabajo real en Dia 2 en vez de un paso aislado
- Bloque 6 (Bedrock vs SageMaker) cubierto conceptualmente sin necesidad de profundizar mas -- diferencia clara: Bedrock para modelos fundacionales via API (nuestro caso), SageMaker para entrenar modelos propios desde cero (fuera de scope de Poly-RAG)
- Conceptos nuevos archivados en knowledge.md: mapeo AWS/GCP de storage, NoSQL (schema flexible no ausencia de estructura), pricing por request vs volumen, mecanismo de Budget Actions
- Siguiente: Dia 2 -- ingestion real de Polymarket via Lambda + S3 + EventBridge
