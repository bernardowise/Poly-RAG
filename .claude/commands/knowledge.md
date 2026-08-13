---
description: Agrega el concepto que se acaba de explicar en la conversacion a knowledge.md
---

Toma el concepto tecnico (RAG, NLP, despliegue de LLMs, hallucinations, reranking, etc.)
que se acaba de explicar en el mensaje inmediato anterior de esta conversacion, y agregalo
como una entrada nueva al final de .claude/claude_docs/knowledge.md.

Formato de la entrada:

```markdown
## YYYY-MM-DD — <nombre del concepto>

<explicacion clara, en tus propias palabras, del concepto>

**Contexto:** <que parte de la conversacion/proyecto lo disparo>
```

Usa la fecha actual real. No reescribas ni borres entradas anteriores. Si el mensaje
anterior no contiene un concepto tecnico claro que valga la pena archivar, dile al
usuario que no encontraste nada que agregar en vez de inventar contenido.

Este comando tambien se dispara proactivamente (sin que el usuario tenga que invocarlo):
cada vez que en la conversacion se explique un concepto nuevo de RAG/NLP/LLM/despliegue,
agregalo a este mismo archivo sin esperar a que el usuario escriba /knowledge.
