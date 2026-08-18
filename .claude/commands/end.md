---
description: Cierra un bloque de trabajo (no necesariamente la sesion completa) — agrega un resumen a .claude/claude_docs/session_ledger.md
---

**Cuando se invoca este comando:** a criterio del usuario, cada vez que considera que
se completo un task/bloque de trabajo -- NO solo al final de la sesion de Claude Code.
Puede invocarse varias veces en el mismo dia/sesion si se cierran varios tasks distintos
(cada invocacion genera su propia entrada bajo el mismo encabezado `## YYYY-MM-DD` si ya
existe uno para hoy, o crea el encabezado si es el primero del dia). Tambien se usa como
ultimo recurso justo antes de compactar el contexto de la conversacion, para no perder
el resumen de lo hecho antes de que el historial se comprima.

Escribe un resumen breve (3-6 bullets) de lo que se hizo en el bloque de trabajo que se
esta cerrando (no necesariamente toda la sesion desde el inicio -- solo lo relevante
desde el ultimo /end o desde que arranco la conversacion si es el primero).

Si ya existe una entrada `## YYYY-MM-DD` para hoy en session_ledger.md, agrega los
bullets nuevos al FINAL de esa entrada existente (no crees un segundo encabezado con
la misma fecha). Si no existe, crea una nueva entrada:

## YYYY-MM-DD

- bullet 1
- bullet 2
...

**La fecha del encabezado va en HORA LOCAL DEL USUARIO (UTC-6, Ciudad de Mexico), NO en
UTC** -- ver "Timezone Convention" en CLAUDE.md. El ledger es la bitacora de las sesiones
de trabajo del usuario, asi que su dia debe coincidir con el dia del usuario. Ojo con la
trampa real: entre las 18:00 y 23:59 hora local, UTC ya avanzo al dia SIGUIENTE, asi que
usar la fecha por defecto del entorno archivaria una sesion de la noche bajo el dia de
manana. Obten la fecha local explicitamente:

    TZ=America/Mexico_City date +%F

Esto aplica SOLO a la fecha del encabezado del ledger. Cualquier timestamp de datos que se
mencione DENTRO de un bullet (ciclos de ingestion, particiones de S3, corridas de
EventBridge) se queda en UTC, que es canon para todo lo del corpus/pipeline -- si un bullet
menciona ambos, deja explicito cual es cual.

No reescribas ni borres entradas anteriores.

**Ademas, antes de cerrar el bloque:** revisa si algo de lo que se hizo en este bloque
de trabajo dejo desactualizado `.claude/claude_docs/tech_debt.md` (una fuente de datos
reemplazada, un debt resuelto, un mitigation ya aplicado, un riesgo que ya no aplica).
Si encuentras una inconsistencia, corrigela directamente en tech_debt.md como parte de
este mismo /end -- no esperes a que el usuario lo pida. Si no hay nada que actualizar,
no toques el archivo.
