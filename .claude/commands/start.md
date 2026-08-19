---
description: Carga el contexto completo del proyecto — CLAUDE.md, README.md y todo .claude/claude_docs/
---

Carga el contexto completo del proyecto antes de continuar. Lee, en este orden y
completos (no fragmentos, no `head`):

1. **`CLAUDE.md`** — instrucciones del proyecto. Normalmente en la raiz del repo,
   pero si no esta ahi, localizalo antes de leerlo:
   `find . -maxdepth 3 -iname 'CLAUDE.md' -not -path './node_modules/*'`
   Si hay mas de uno, leelos todos (raiz + los de subdirectorio).
2. **`README.md`** — misma logica de busqueda si no esta en la raiz.
3. **`.claude/claude_docs/`**, con una excepcion: `session_ledger.md` y
   `tech_debt.md` ya son demasiado largos para leer completos de tajo (1800+ lineas
   cada uno) y crecen en DIRECCIONES OPUESTAS, asi que "lo reciente" no se lee igual
   en los dos. El resto del directorio (`architecture_canon.md`, `knowledge.md`,
   `hooks.md`, `infra_design.md`, subdirectorios) se lee completo, igual que antes.
   Enumeralo primero (`find .claude/claude_docs -type f | sort`) para no omitir nada.

   - **`session_ledger.md`** — lo mas reciente esta AL INICIO del archivo (bitacora,
     mas reciente primero). Lee solo los primeros 3 bloques `## YYYY-MM-DD`:
     ```
     awk '/^## /{c++} c==4{exit} {print}' .claude/claude_docs/session_ledger.md
     ```
   - **`tech_debt.md`** — es append-only, asi que lo mas reciente esta AL FINAL del
     archivo (orden opuesto al ledger). Lee las ultimas 6 entradas:
     ```
     tac .claude/claude_docs/tech_debt.md | awk '/^## /{c++} c==7{exit} {print}' | tac
     ```
     **Ademas**, un item viejo puede seguir siendo deuda critica sin estar entre las
     6 mas recientes -- por diseño, cualquier item asi debe llevar un marcador
     explicito en su titulo (`PRIORIDAD`, `CRITICAL`). Busca esos marcadores en TODO
     el archivo, sin importar la posicion, y lee completa cualquier entrada que
     encuentres asi, aunque ya haya quedado fuera del tail de arriba:
     ```
     grep -n "PRIORIDAD\|CRITICAL\|CRITICO" .claude/claude_docs/tech_debt.md
     ```

Si necesitas mas contexto historico de cualquiera de los dos (una entrada vieja
especifica, no solo lo reciente), usa `grep`/`sed` dirigido en vez de leer el
archivo completo.

Leelos en paralelo cuando se pueda.

Al terminar, no produzcas un resumen largo: confirma en 3-5 lineas que archivos
cargaste (incluyendo cuantas entradas de ledger/tech_debt, y si el grep de
prioridad encontro algo) y cual es el estado actual del proyecto segun
`architecture_canon.md` y la entrada mas reciente de `session_ledger.md`.
