# Infra Design -- Arquitectura de Claude Code para este repo

Este documento describe como esta configurado Claude Code **especificamente para
Poly-RAG** -- que vive dentro de `.claude/`, que hace cada pieza, y como encajan entre
si. No es un tour generico de las features de Claude Code (eso vive en la
documentacion oficial); es el mapa real de este repo, mantenido a mano conforme
`.claude/` evoluciona. Si algo aqui queda obsoleto, se corrige en el momento -- no es
una bitacora (eso es `session_ledger.md`).

---

## CLAUDE.md (raiz del repo)

El punto de entrada -- se carga automaticamente en cada sesion, sin que nadie lo pida.
Define proposito del proyecto, stack, filosofia de arquitectura (agentic-first,
budget-consciente), que esta fuera de scope, convenciones de desarrollo, el mapa de
`claude_docs/` (tabla que apunta a cada archivo de este directorio y su proposito), y
las reglas de git/commit (nunca commitear en automatico, formato de mensaje via
`/commit-msg`). Es la unica fuente de reglas que se tratan como **override** de
cualquier comportamiento por defecto -- todo lo demas en `.claude/` es tooling que
sirve a esas reglas, no reglas nuevas por si mismo.

---

## `.claude/claude_docs/` -- documentacion viva del proyecto

Ocho piezas, cada una con un rol distinto y sin solapamiento (ver la tabla completa en
CLAUDE.md):

- **`architecture_canon.md`** -- snapshot del estado actual de la arquitectura de
  Poly-RAG (fuentes de datos, Lambdas, LLM enrichment, inventario AWS). Se sobreescribe
  in-place, nunca acumula historia.
- **`tech_debt.md`** -- items abiertos: limitaciones conocidas, alternativas
  rechazadas, decisiones con trade-off sin resolver. Estructura Issue/Debt/Mitigation/
  Revisit por entrada. Las entradas se cierran o se reemplazan, no se archivan aparte.
- **`session_ledger.md`** -- log cronologico inmutable, mas reciente primero, un
  encabezado `## YYYY-MM-DD` por dia con bullets agregados via `/end` cada vez que se
  cierra un bloque de trabajo (no solo al final de la sesion completa).
- **`knowledge.md`** -- archivo de conceptos tecnicos explicados durante sesiones
  (RAG, NLP, AWS/GCP, Spark, etc.), agregado via `/knowledge` o proactivamente.
- **`hooks.md`** -- documenta los hooks activos de este repo. Su seccion "Active
  Hooks" se **regenera automaticamente** desde `settings.json` (ver mas abajo) -- no
  se edita a mano esa parte.
- **`infra_design.md`** -- este archivo.
- **`memory_mirror/`** -- espejo git-tracked del memory store interno de Claude Code
  para este proyecto (ver seccion Memoria mas abajo). Vacio a la fecha de esta
  reescritura (2026-08-16) -- el mecanismo de sync existe y esta activo, pero no se ha
  guardado memoria persistente todavia en esta sesion de trabajo.
- **`gerdau/`** -- materiales de prep para la entrevista tecnica (sprint plan de 7
  dias, JD). **Gitignorado explicitamente** (`.gitignore:1`) -- es sobre el proceso de
  entrevista del usuario, no sobre Poly-RAG como producto, y nunca se trackea en git.
- **`new_skills.md`** (no listado en la tabla de CLAUDE.md pero vive aqui) -- tracking
  personal de skills reales aprendidas construyendo el proyecto, para CV/LinkedIn.
  Igual que `gerdau/`, es sobre el usuario, no sobre el proyecto, y **tambien esta
  gitignorado** (`.gitignore:7`) -- correccion 2026-08-17, la version anterior de este
  documento decia erroneamente que si estaba trackeado.

---

## `.claude/commands/` -- slash commands (skills invocables por nombre)

14 comandos, cada uno un archivo `.md` con frontmatter de skill. Agrupados por lo que
realmente hacen:

**Arranque y contexto de sesion:**
- `/start` -- lee CLAUDE.md completo, carga el contexto inicial del proyecto
- `/catchup` -- resume en que se quedo la conversacion usando SOLO el contexto ya
  presente (no relee archivos)
- `/debrief` -- muestra las 5 entradas mas recientes de `session_ledger.md`

**Cierre y documentacion:**
- `/end` -- cierra un bloque de trabajo (a discrecion del usuario, no solo al final de
  la sesion), agrega entrada a `session_ledger.md`. Revisa tambien si algo dejo
  `tech_debt.md` desactualizado y lo corrige en el mismo paso.
- `/knowledge` -- archiva un concepto tecnico recien explicado en `knowledge.md`
- `/canonize` -- propone donde archivar algo que acaba de pasar en la conversacion
  como canon del proyecto -- espera aprobacion antes de escribir
- `/cv` -- agrega una skill real (hecha con las manos, no solo mencionada) a
  `new_skills.md`

**Git:**
- `/commit-msg` -- redacta el mensaje de commit siguiendo el formato canon (sin
  comillas, `type(scope): resumen`, limite 500 caracteres). Nunca ejecuta `git commit`
  -- solo entrega el texto y se detiene, consistente con la regla de CLAUDE.md de que
  el usuario decide cuando y si commitear.
- `/checkout` -- cambia de rama en el mismo workspace, verificando primero que no haya
  cambios sin commitear (evita perder trabajo por accidente)

**Modos de comportamiento:**
- `/brainstorming` -- modo conservador: solo informacion/investigacion/recomendaciones,
  no implementa ni edita archivos hasta que se pida explicitamente ejecutar
- `/plain` -- responde solo con el resultado pedido, sin preambulo ni resumen --
  modo determinista para cuando el usuario quiere output crudo
- `/acknowledge` -- ingiere contexto (URL, archivo, texto pegado) sin producir
  resumenes ni analisis extra -- para cuando el usuario solo quiere que Claude "lea y
  quede enterado", no que reaccione

**Utilidad:**
- `/keep-alive` -- imprime el comando de keep-alive para que el USUARIO lo pegue en su
  propia terminal -- explicitamente no lo ejecuta Claude
- `/hello` -- saludo trivial, sin logica real

---

## `.claude/settings.json` -- hooks activos

Dos hooks `FileChanged`, ambos con handler tipo shell command:

1. **Sync de memoria** (`.claude/scripts/sync_memory.sh`) -- dispara cuando cambia
   algo en `.claude/claude_docs/memory_mirror/**` O en el memory store interno de
   Claude Code (`/home/codespace/.claude/projects/-workspaces-Poly-RAG/memory/**`).
   El script hace un `rsync -a --update` **bidireccional** entre ambos directorios --
   semantica de union: nunca borra, solo agrega/actualiza en cualquiera de los dos
   sentidos. Efecto practico: lo que Claude recuerda entre sesiones queda tambien
   versionado en git (via `memory_mirror/`), y lo que se edite a mano en el mirror
   dentro del repo se propaga de vuelta al store interno.
2. **Regeneracion de docs de hooks** (`.claude/scripts/update_hooks_docs.sh`) --
   dispara cuando cambia `.claude/settings.json` mismo. Parsea el JSON con `jq` y
   reescribe la seccion "Active Hooks" de `hooks.md`, preservando a mano la intro, la
   filosofia de diseño, y la seccion de candidatos futuros (solo esa seccion se
   regenera, el resto del archivo es contenido escrito por humano). Es literalmente
   auto-documentacion: este mismo hook es la razon por la que `hooks.md` nunca queda
   desactualizado respecto a `settings.json` -- si agregas un hook nuevo, la doc se
   reescribe sola al guardar.

No hay hooks de `PreToolUse`/`PostToolUse`/`UserPromptSubmit` en este repo todavia --
`hooks.md` los lista como "candidatos futuros", no implementados.

---

## `.claude/skills/` y `.databricks/` -- skills de Databricks (agregado 2026-08-16)

29 skills de Databricks instaladas via `databricks aitools install --skills-only`
(el flag `--plugin` normal fallo: requiere el binario `claude` en PATH, que no existe
en este entorno de Claude Agent SDK -- `--skills-only` evita esa dependencia,
escribiendo archivos crudos en vez de instalar un plugin).

**Estructura de dos capas, deliberada:**
- `.databricks/aitools/skills/` -- el contenido REAL (3.7MB: `SKILL.md` por skill,
  referencias, assets). Fuente de verdad unica.
- `.claude/skills/` -- symlinks (32KB, solo punteros) apuntando a la carpeta de
  arriba. Es lo que Claude Code realmente lee para descubrir e invocar skills.

No es duplicacion real (confirmado explicitamente en conversacion con el usuario,
2026-08-16) -- son punteros, no copias. Ambas carpetas quedan trackeadas en git (no
gitignoradas) a peticion explicita del usuario, para que el corpus self-referencial
(RAG sobre la evolucion de este mismo repo, ver tech_debt.md "Self-Referential Corpus
Not Yet Built") tenga acceso al contenido real de las skills, no solo a punteros
rotos -- si solo se trackeara `.claude/skills/` (los symlinks) sin `.databricks/`
(el contenido), cualquier clon fresco del repo (o un proceso de ingestion leyendo del
historial de git) encontraria enlaces simbolicos apuntando a nada.

Cubren, entre otras cosas, Unity Catalog, Delta Lake/pipelines, jobs, SQL warehouses,
serverless migration, vector search -- relevante directo para el trabajo pendiente de
Day 3 del sprint (Delta Lake + Unity Catalog, ver `gerdau/sprint_plan.md`).

Se invocan automaticamente por Claude segun el nombre/descripcion calce con la tarea
en curso -- el usuario no las abre ni las llama a mano, igual que los slash commands
de `.claude/commands/` pero con descubrimiento automatico en vez de invocacion
explicita por `/nombre`.

---

## Lo que NO existe en este repo (a proposito de no asumir)

- **`.claude/rules/`** -- no hay reglas scoped por directorio. Todo el gobierno de
  comportamiento vive en CLAUDE.md (global al repo) mas los slash commands (modos
  activables por el usuario).
- **`.claude/agents/`** -- no hay subagentes custom definidos para este proyecto.
  Cuando se delega trabajo (ver ejemplos en `session_ledger.md`), se usan los agent
  types genericos del sistema (Explore, general-purpose), no agentes propios de
  Poly-RAG.
- **MCP servers** -- ninguno configurado. Todo el acceso a AWS (S3, DynamoDB, Bedrock,
  Lambda) pasa por el CLI de `aws` invocado via Bash, no por un servidor MCP dedicado.
  El acceso a Databricks es igual: CLI (`databricks workspace`, `databricks jobs`,
  etc.) via Bash, no MCP.
- **Plugins** -- ninguno instalado como bundle empaquetado (el intento de
  `databricks aitools install` en modo plugin completo fallo por la ausencia del
  binario `claude` en PATH; se resolvio con skills crudas en su lugar, ver arriba).

---

## Memoria (mecanismo, no contenido)

El sistema de memoria persistente de Claude Code para este proyecto vive en
`/home/codespace/.claude/projects/-workspaces-Poly-RAG/memory/` (fuera del repo,
especifico de esta maquina/Codespace) y se espeja hacia `claude_docs/memory_mirror/`
(dentro del repo, git-tracked) via el hook de sync descrito arriba. A la fecha de esta
reescritura (2026-08-16) ambos directorios estan vacios -- el mecanismo esta armado y
activo, pero no se ha escrito ninguna memoria persistente todavia en el trabajo hecho
hasta ahora. Distinto de `claude_docs/*.md`: la memoria es contexto que se recupera
automaticamente en sesiones futuras sin que el usuario tenga que pedirlo; los docs de
`claude_docs/` se leen quando son relevantes al trabajo en curso, tipicamente via
`/start` o lectura directa.
