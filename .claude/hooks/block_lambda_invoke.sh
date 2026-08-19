#!/usr/bin/env bash
# PreToolUse hook: bloquea invocaciones manuales a las Lambdas de produccion.
#
# POR QUE EXISTE
# --------------
# Incidente 2026-08-18: Claude invoco `poly-rag-ingest-polymarket` dos veces para
# "verificar" un fix de codigo. Esa Lambda es el punto de entrada de toda la cadena
# (polymarket -> news -> comments -> send_digest -> correo real via SES), y un bug de
# doble-disparo en el fan-out de News amplifico x12.5: 25 correos al usuario, ademas
# de contaminar 9 lugares entre S3 y DynamoDB (ver
# .claude/claude_docs/runbook_manual_invocation_cleanup.md).
#
# La regla ya esta escrita en CLAUDE.md, pero una regla depende del juicio del modelo
# en el momento. Este hook NO depende de eso: bloquea el comando antes de que corra,
# sin importar que tan buena parezca la razon. Esa es justamente la peticion del
# usuario -- "una barrera mecanica que ni tu puedas saltarte".
#
# COMO DESBLOQUEAR (solo el usuario, deliberadamente):
#   POLYRAG_ALLOW_LAMBDA_INVOKE=1 aws lambda invoke --function-name ...
# Requiere escribir la variable a mano, que es exactamente la pausa que se busca.

set -uo pipefail

INPUT="$(cat)"

# El payload del hook trae el comando en .tool_input.command (Bash).
COMMAND="$(printf '%s' "$INPUT" | jq -r '.tool_input.command // empty' 2>/dev/null)"

[ -z "$COMMAND" ] && exit 0

# Escape hatch explicito del usuario.
case "$COMMAND" in
  *POLYRAG_ALLOW_LAMBDA_INVOKE=1*) exit 0 ;;
esac

# Solo nos importan invocaciones reales, no lecturas (get-function, describe, logs).
if ! printf '%s' "$COMMAND" | grep -Eq 'lambda[[:space:]]+invoke|lambda\.invoke|invoke_?[fF]unction'; then
  exit 0
fi

PROTECTED='poly-rag-ingest-polymarket|poly-rag-ingest-news|poly-rag-ingest-comments|poly-rag-send-digest|poly-rag-watchdog-ingest-news'

if printf '%s' "$COMMAND" | grep -Eq "$PROTECTED"; then
  cat >&2 <<'MSG'
BLOQUEADO: invocacion manual a una Lambda de produccion de Poly-RAG.

Estas Lambdas encadenan hasta send_digest y mandan CORREO REAL al usuario.
El 2026-08-18, dos invocaciones manuales se convirtieron en 25 correos (x12.5)
y contaminaron 9 lugares entre S3 y DynamoDB.

Verifica el cambio SIN invocar:
  1. python3 -m py_compile lambdas/<x>/handler.py
  2. importar el handler y probar funciones puras (solo lectura)
  3. esperar el ciclo automatico (00:00/12:00 UTC) y leer CloudWatch Logs

Si el USUARIO lo autoriza explicitamente, el comando se antepone con:
  POLYRAG_ALLOW_LAMBDA_INVOKE=1 <comando>

Ver CLAUDE.md ("NUNCA invocar Lambdas de produccion") y
.claude/claude_docs/runbook_manual_invocation_cleanup.md
MSG
  exit 2   # exit 2 = bloquear la herramienta y devolver stderr al modelo
fi

exit 0
