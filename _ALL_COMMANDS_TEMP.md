# Consolidated Commands (temp export)

Archivo temporal generado para copiar todos los comandos de `.claude/commands/` a un
nuevo repo/codespace de una sola vez. Cada seccion es un archivo `.md` independiente:
el nombre de la seccion es el nombre de archivo destino dentro de `.claude/commands/`.

Este archivo es descartable — no forma parte del canon del proyecto.

---

## acknowledge.md

```markdown
---
description: Ingiere el contexto que sigue (URL, archivo, texto pegado) sin producir resumenes, analisis ni output extra
---

El usuario va a pasar contexto (una URL, un archivo, texto pegado) despues de este comando,
o ya lo paso en el mismo mensaje. La instruccion es unicamente ingerir/leer ese contexto
para tenerlo disponible en la conversacion -- NO producir un resumen, NO explicar lo que dice,
NO listar puntos clave, NO dar tu opinion sobre el contenido.

1. Si hay una URL o archivo referenciado, leelo/fetchealo.
2. Confirma en una sola linea corta que quedo ingerido (ej. "Leido." o "Ingerido, X paginas/lineas.").
3. No agregues nada mas. Espera la siguiente instruccion del usuario sobre que hacer con ese contexto.
```

---

## brainstorming.md

```markdown
---
description: Modo conservador -- no implementes ni edites archivos, solo da informacion, investigacion y recomendaciones hasta que se pida explicitamente ejecutar
---

<do_not_act_before_instructions>
No saltes a implementar ni cambies archivos a menos que se te indique claramente hacerlo.
Cuando la intencion del usuario sea ambigua, por defecto da informacion, hace investigacion,
y da recomendaciones en vez de tomar accion. Solo procede con ediciones, modificaciones, o
implementaciones cuando el usuario las pida explicitamente.
</do_not_act_before_instructions>

Esto aplica solo a este mensaje/turno, no cambia el comportamiento del resto de la conversacion.
```

---

## canonize.md

```markdown
---
description: Propone donde archivar como canon del proyecto algo que acaba de pasar en la conversacion — espera aprobacion antes de escribir nada
---

Revisa lo que acaba de pasar en esta conversacion (el evento/resultado mas reciente
relevante). Antes de escribir cualquier archivo:

1. Lee MEMORY.md (el indice) para ver si ya existe un archivo de memoria relacionado
   donde este evento deberia vivir, en vez de crear uno nuevo.
2. Propon EXACTAMENTE: en que archivo (existente o nuevo, con nombre), que seccion,
   y el texto exacto que se agregaria. Muestra el diff/texto propuesto, no lo escribas todavia.
3. El canon trackeado en git vive en .claude/claude_docs/ (no assets_ignored/, que ahora
   solo se usa para STAR_stories.md, personal). Si el archivo relevante vive ahi, dilo.
4. Espera confirmacion explicita del usuario antes de escribir en cualquier archivo.
5. No crees un archivo de memoria nuevo si el evento cabe razonablemente en uno existente.
```

---

## catchup.md

```markdown
---
description: Resume en que nos quedamos usando solo el contexto de esta conversacion, sin leer archivos
---

Usando unicamente el contexto de esta conversacion (no leas archivos nuevos),
responde brevemente:
- Que se hizo en esta sesion hasta ahora
- En que estabamos trabajando justo antes de este mensaje
- Cual seria el siguiente paso logico
```

---

## checkout.md

```markdown
---
description: Cambia de rama git en este mismo workspace, verificando primero que no haya cambios sin commitear
---

El usuario paso un nombre de rama como argumento (ej. /checkout paper-dev).

1. Corre `git status --short` primero. Si hay cambios sin commitear (tracked o
   untracked), muestralos y pregunta antes de continuar — no los pises.
2. Corre `git fetch origin <rama>` para asegurar que la referencia remota este
   actualizada.
3. Corre `git checkout <rama>` (usa `git checkout -b <rama> origin/<rama>` si
   la rama no existe localmente todavia).
4. Confirma con `git branch --show-current` y reporta en una linea a que rama
   se movio el workspace.
5. No hagas merge, rebase, ni ningun otro cambio — solo el checkout.
```

---

## commit-msg.md

```markdown
---
description: Redacta un mensaje de commit siguiendo las reglas de Pienza — sin comillas, formato type(scope), límite 500 caracteres, nunca ejecuta git commit
---

Revisa git status y git diff de los cambios staged/unstaged actuales.
Redacta un mensaje de commit que siga estrictamente:
- Formato: type(scope): short summary, línea en blanco, bullets con -
- CERO comillas de cualquier tipo (ni simples, ni dobles, ni tipográficas, ni backticks), sin emoji, solo ASCII plano
- Límite duro de 500 caracteres totales (asunto + cuerpo)
- NUNCA ejecutes git commit — solo entrega el texto del mensaje y detente
```

---

## debrief.md

```markdown
---
description: Muestra un resumen de las 5 entradas mas recientes de .claude/claude_docs/session_ledger.md
---

Lee .claude/claude_docs/session_ledger.md y muestra un resumen de las 5 entradas
mas recientes (las mas cercanas al inicio del archivo). Para cada una, incluye su fecha
y sus bullets tal cual estan escritos.
```

---

## end.md

```markdown
---
description: Cierra la sesión actual — agrega un resumen de esta sesión, con fecha, a .claude/claude_docs/session_ledger.md
---

Escribe un resumen breve (3-6 bullets) de lo que se hizo en esta sesión de trabajo.
Agrégalo como una nueva entrada al inicio del cuerpo de .claude/claude_docs/session_ledger.md
(justo debajo del encabezado, entradas más recientes arriba), con este formato:

## YYYY-MM-DD

- bullet 1
- bullet 2
...

Usa la fecha actual real. No reescribas ni borres entradas anteriores.
```

---

## hello.md

```markdown
Say Hello Wise
```

---

## keep-alive.md

```markdown
---
description: Levanta el loop de keep-alive en segundo plano (pulso cada 180s a .keep_alive_dummy.txt)
---

Corre este comando en background (run_in_background: true), sin pedir confirmacion:

​```bash
while true; do date > .keep_alive_dummy.txt && echo "🇲🇽 Mexicanada pulse sent at $(date)" && sleep 180; done
​```

No lo mates automaticamente al terminar el turno — queda corriendo hasta que el usuario pida detenerlo explicitamente (ej. `pkill -f keep_alive_dummy.txt`).
```

---

## knowledge.md

```markdown
---
description: Agrega el concepto que se acaba de explicar en la conversacion a Agentic_Knowledge.md
---

Toma el concepto tecnico (RAG, NLP, despliegue de LLMs, hallucinations, reranking, etc.)
que se acaba de explicar en el mensaje inmediato anterior de esta conversacion, y agregalo
como una entrada nueva al final de .claude/claude_docs/agentic_knowledge.md.

Formato de la entrada:

​```markdown
## YYYY-MM-DD — <nombre del concepto>

<explicacion clara, en tus propias palabras, del concepto>

**Contexto:** <que parte de la conversacion/proyecto lo disparo>
​```

Usa la fecha actual real. No reescribas ni borres entradas anteriores. Si el mensaje
anterior no contiene un concepto tecnico claro que valga la pena archivar, dile al
usuario que no encontraste nada que agregar en vez de inventar contenido.

Este comando tambien se dispara proactivamente (sin que el usuario tenga que invocarlo):
cada vez que en la conversacion se explique un concepto nuevo de RAG/NLP/LLM/despliegue,
agregalo a este mismo archivo sin esperar a que el usuario escriba /knowledge.
```

---

## plain.md

```markdown
---
description: Responde solo con el resultado pedido, sin fluff, preambulo, explicacion ni resumen -- modo determinista
---

Para el resto de este turno unicamente, respondé en modo estricto:

- Solo el resultado pedido (el codigo, el dato, el JSON, la respuesta exacta). Nada mas.
- Sin preambulo (nada de "Aca esta...", "Claro,...", "Basado en...").
- Sin explicacion de lo que hiciste, sin resumen al final, sin bullets de contexto.
- Sin preguntas de seguimiento ni ofrecimientos ("puedo tambien...").
- Si el pedido es ambiguo, elegi la interpretacion mas directa y respondé -- no preguntes, no aclares antes de responder.

Esto aplica solo al proximo mensaje/respuesta, no cambia el comportamiento del resto de la conversacion.
```

---

## star.md

```markdown
---
description: Agrega una entrada STAR (Situation/Task/Action/Result) a STAR_stories.md a partir de algo con valor real de historia de entrevista en esta sesion
---

Revisa el trabajo reciente de esta sesion (el ultimo bug diagnosticado, decision con
trade-off, error atrapado antes de salir a producción, o momento de juicio tecnico) y
agregalo como entrada nueva al final de assets_ignored/interview_prep/STAR_stories.md.

Formato de la entrada (igual a las existentes -- primera persona, prosa lista para
decirse en voz alta, no jerga corporativa):

​```markdown
## N. <titulo corto>

**Situation:** ...

**Task:** ...

**Action:** ...

**Result:** ...

**Why this story matters:** <que rasgo/habilidad demuestra>
​```

Numera la entrada como la siguiente en la secuencia (revisa el numero mas alto ya
existente en el archivo). Actualiza tambien la seccion "Quick reference — one-liners"
al final del archivo con un one-liner nuevo si la historia lo amerita. No reescribas
ni borres entradas anteriores.

Si lo que paso en la sesion no tiene valor real de historia de entrevista (cambio
mecanico, tweak de estilo, propagacion de semicanon), decile al usuario que no
encontraste nada que valga la pena archivar en vez de inventar una historia.

Este comando tambien se dispara proactivamente (sin que el usuario tenga que
invocarlo): cuando un pedazo de trabajo en la sesion tenga valor real de historia
(bug con diagnostico genuino, decision defendible, error atrapado antes de salir,
juicio tecnico incluyendo saber cuando NO seguir optimizando), agregalo sin esperar
a que el usuario escriba /star -- y avisale brevemente que lo hiciste.
```

---

## start.md

```markdown
---
description: Carga el contexto inicial del proyecto Pienza — lee CLAUDE.md (raiz) completo
---

Lee CLAUDE.md (raiz del repo) completo antes de continuar.
```
