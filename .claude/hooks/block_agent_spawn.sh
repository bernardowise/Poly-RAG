#!/usr/bin/env bash
# PreToolUse hook: bloquea lanzar subagents (tool Agent) sin autorizacion explicita.
#
# POR QUE EXISTE
# ---------------
# 2026-08-20: el usuario reporto consumo de tokens fuera de lo esperado, con datos
# reales de uso (96% del consumo de 24h vino de sesiones subagent-heavy, 92% con
# >150k de contexto). Cada Agent() dispara su propia cadena de requests -- puede
# ser barato o caro segun el modelo/alcance, y ese costo no siempre es obvio antes
# de lanzarlo. Mismo patron que el bloqueo de Lambdas: una regla en CLAUDE.md
# depende del juicio del modelo en el momento; este hook no.
#
# REGLA (doble seguro, incluso si el USUARIO lo sugiere primero):
# antes de cualquier Agent(), Claude debe preguntar en el chat -- tarea, por que
# un subagente (no un tool directo), y una estimacion aproximada de costo/tokens
# -- y esperar confirmacion explicita. Solo entonces el campo `description` del
# tool call debe llevar el prefijo "AUTORIZADO: " (ver mas abajo). Que la idea
# haya sido del usuario no exime la confirmacion -- el punto es frenar el impulso
# de spawnear, no adivinar quien lo propuso primero.
#
# COMO DESBLOQUEAR: el campo `description` del Agent() debe empezar exactamente
# con "AUTORIZADO: " (ej. "AUTORIZADO: explorar bug X, ~15k tokens est.").

set -uo pipefail

INPUT="$(cat)"

DESCRIPTION="$(printf '%s' "$INPUT" | jq -r '.tool_input.description // empty' 2>/dev/null)"

case "$DESCRIPTION" in
  "AUTORIZADO: "*) exit 0 ;;
esac

cat >&2 <<'MSG'
BLOQUEADO: intento de lanzar un subagent (Agent) sin autorizacion explicita.

Antes de spawnear, pregunta al usuario en el chat:
  1. que tarea vas a delegar y por que necesita un subagente (no un tool directo)
  2. una estimacion aproximada de costo/tokens de esa llamada
  3. espera confirmacion explicita -- aunque la idea haya sido del usuario, sigue
     pidiendo el "si" antes de disparar

Solo despues de esa confirmacion, reintenta el Agent() con el campo `description`
empezando exactamente con "AUTORIZADO: " (ej. "AUTORIZADO: <tarea>, ~Nk tokens est.").

Ver CLAUDE.md ("Nunca lanzar subagents sin autorizacion explicita").
MSG
exit 2
