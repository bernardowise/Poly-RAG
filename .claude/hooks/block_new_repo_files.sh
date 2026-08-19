#!/usr/bin/env bash
# PreToolUse hook: exige autorizacion del usuario antes de CREAR archivos nuevos
# dentro del repo.
#
# POR QUE EXISTE
# --------------
# Peticion explicita del usuario (2026-08-19): "empiezan a aparecer archivitos por
# todos lados y pierdo el hilo". Caso concreto: se creo un directorio `evals/` con un
# documento de handoff sin preguntar. El usuario lo pidio borrar.
#
# El problema no es que los archivos sean malos -- es que el usuario deja de tener
# control sobre la forma de su propio repo. Cada archivo nuevo es una decision de
# arquitectura chiquita (donde vive, como se llama, si se trackea en git) y esas
# decisiones son suyas, no mias.
#
# QUE BLOQUEA
# -----------
# Solo la CREACION de archivos que no existen todavia, y solo dentro de
# /workspaces/Poly-RAG. NO bloquea:
#   - editar/sobrescribir un archivo que ya existe (eso no genera desorden nuevo)
#   - escribir en el scratchpad de la sesion (/tmp/...), que es efimero por diseno
#
# COMO AUTORIZAR (solo el usuario, deliberadamente)
# --------------------------------------------------
# Cuando el usuario aprueba explicitamente un archivo nuevo, se crea via Bash:
#   POLYRAG_ALLOW_NEW_FILE=1 cat > ruta/al/archivo <<'EOF'
#   ...
#   EOF
#
# Ese camino existe a proposito y no es una trampa: la intencion del hook no es hacer
# imposible crear archivos, es que NUNCA ocurra por inercia. Bloquear la ruta por
# defecto (Write) obliga a un acto distinto y consciente, que es justo la pausa que el
# usuario pidio.

set -uo pipefail

INPUT="$(cat)"

FILE_PATH="$(printf '%s' "$INPUT" | jq -r '.tool_input.file_path // empty' 2>/dev/null)"

[ -z "$FILE_PATH" ] && exit 0

# Fuera del repo (scratchpad, /tmp, etc.) -- no es asunto de este hook.
case "$FILE_PATH" in
  /workspaces/Poly-RAG/*) ;;
  *) exit 0 ;;
esac

# El archivo ya existe -> es una edicion, no genera desorden nuevo.
[ -e "$FILE_PATH" ] && exit 0

REL="${FILE_PATH#/workspaces/Poly-RAG/}"

cat >&2 <<MSG
BLOQUEADO: crear un archivo nuevo en el repo requiere autorizacion del usuario.

Archivo: $REL

No lo crees por tu cuenta. Preguntale primero al usuario, explicando:
  - que archivo quieres crear y donde
  - para que sirve
  - si deberia trackearse en git o ir al .gitignore

Si el usuario lo autoriza explicitamente, creelo con:
  POLYRAG_ALLOW_NEW_FILE=1 cat > $REL <<'EOF'
  ...
  EOF

Si lo que necesitas es un archivo temporal de trabajo, usa el scratchpad de la
sesion en /tmp -- ese no pasa por este hook.

Ver CLAUDE.md, "Nunca crear archivos nuevos en el repo sin autorizacion".
MSG
exit 2
