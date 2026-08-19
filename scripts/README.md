# scripts/ -- herramientas one-off, no parte de la cadena de 12h

Todo lo que vive aqui corre MANUALMENTE, una vez o pocas veces, nunca automatico via
EventBridge ni encadenado desde una Lambda. Ver CLAUDE.md, "Nunca crear archivos
nuevos en el repo sin autorizacion" para el porque de la separacion entre esta
carpeta y `.claude/hooks/` (que son handlers de hooks, no herramientas del
proyecto).

Cada script tiene su propio docstring con el diseno completo -- esta tabla es un
indice, no un reemplazo de leerlos.

| Script | Que hace | Corrido | Estado |
|---|---|---|---|
| `tag_cycle_snapshots.py` | Etiqueta `source: "cycle"` en los snapshots de odds ya existentes (adicion pura, ningun valor cambia) | 2026-08-18, antes del backfill de CLOB | Terminado. Los snapshots nuevos ya llevan `source` nativo desde el mismo dia (fix en `ingest_polymarket`) |
| `backfill_odds_history.py` | Recupera historia de precios pre-tracking desde la API CLOB de Polymarket para los markets ya trackeados, hasta su `created_at` real | 2026-08-18, una vez sobre los 595 markets de ese momento | Terminado. Markets nuevos desde entonces traen su historia via el backfill nativo en `ingest_polymarket` (F-lambdas) |
| `backfill_registry_created_at.py` | Backfillea el campo `created_at` (fecha real de creacion en Polymarket) en el registry, distinto de `first_seen` | 2026-08-18, una vez sobre los 595 markets de ese momento | Terminado. Markets nuevos ya traen `created_at` nativo desde F-lambdas |
| `tag_news_temporal_tier.py` | Clasifica cada articulo en tier 3.1/3.2/3.3/too_old/unknown_market segun su `pubDate` vs `created_at`/`first_seen` del market | 2026-08-18, sobre los 3,315 articulos existentes en ese momento | Sigue siendo necesario correrlo de nuevo -- la logica NO esta conectada a `ingest_news` todavia, asi que los articulos de ciclos posteriores al 18 no tienen estos campos. Ver tech_debt.md, "News Temporal Tiers" |
| `tag_news_market_status.py` | Clasifica `market_status_at_publish` (open/closed) por articulo, usando `resolution_date` del market, no su status actual | 2026-08-18, mismo lote que el anterior | Mismo estado que `tag_news_temporal_tier.py` -- pendiente de conectarse a la Lambda, sigue haciendo falta re-correrlo periodicamente mientras tanto |
| `start_legacy_post_resolution_windows.py` | Arranca manualmente el contador `post_resolution_cycles_remaining` en 4 para los 93 markets que ya estaban resueltos antes de que el mecanismo de captura post-resolucion existiera | 2026-08-18 (dos veces: la corrida original, y otra vez tras limpiar el incidente de las 21:00 UTC que le consumio el contador) | **Terminado, pero se conserva deliberadamente (decision del usuario, 2026-08-19)** -- no se borra ni cuando el contador de los 93 llegue a 0 por el mecanismo automatico normal. Queda como registro de que 93 market_ids se atraparon manualmente y por que, no como herramienta reusable. Lista de IDs congelada a proposito -- ver su propio docstring antes de pensar en re-correrlo con una lista distinta |

## Convenciones que todos siguen

- **Dry-run por defecto**, se necesita `--apply` explicito para escribir.
- **Aditivos, nunca destructivos** -- ningun script borra datos existentes, solo
  agrega campos o snapshots.
- **Idempotentes** -- correr dos veces no duplica ni corrompe (verificado en cada
  uno antes de aplicarlo la primera vez).
- El "cuando corrio" completo, con numeros verificados y el razonamiento detras de
  cada decision de diseno, vive en `.claude/claude_docs/tech_debt.md` -- esta tabla
  es solo el resumen de una linea para orientarse rapido.
