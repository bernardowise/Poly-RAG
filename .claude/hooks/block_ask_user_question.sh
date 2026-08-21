#!/usr/bin/env bash
# PreToolUse hook: bloquea el tool AskUserQuestion incondicionalmente.
#
# POR QUE EXISTE
# ---------------
# 2026-08-21: el usuario pidio explicitamente dejar de recibir preguntas de
# opcion multiple estructuradas -- patron ya senalado varias veces en sesiones
# anteriores ("que te dije de las preguntas de opcion multiple?"). A diferencia
# de los otros hooks de este repo (Lambda invoke, Agent spawn, archivos nuevos),
# este NO tiene escape hatch condicional -- es un bloqueo total, sin prefijo
# de autorizacion que lo desbloquee. El usuario decidio explicitamente que
# quiere el bloqueo incondicional, no un freno con excepcion.
#
# QUE HACER EN VEZ DE AskUserQuestion:
# cuando haga falta una decision del usuario, preguntar en texto plano dentro
# de la respuesta normal -- sin el formato de opciones estructuradas.

cat >&2 <<'MSG'
BLOQUEADO: AskUserQuestion esta deshabilitado por decision explicita del usuario
(2026-08-21) -- sin excepcion, sin prefijo de autorizacion que lo desbloquee.

Si necesitas una decision del usuario, pregunta en texto plano dentro de tu
respuesta normal, no con este tool.
MSG
exit 2
