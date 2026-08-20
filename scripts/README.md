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
| `bootstrap_chunk_corpus.py` | Chunkea el corpus completo existente (registry, News x2 variantes parrafo/articulo, Comments, Digest) en texto listo para embeder, sin llamar a ningun modelo todavia -- paso 1 del bootstrap de Fase 2 (embedding), ver architecture_canon.md | 2026-08-20, dry-run validado contra el corpus real (913 registry / 109,511 news_paragraph / 6,695 news_article / 629 comments / 10 digest chunks) | Dry-run validado, `--apply` (escritura real a `chunks/` en S3) aun pendiente de correr. Dos bugs reales encontrados y corregidos en el dry-run antes de escribir: separador de parrafo real es `\n` no `\n\n`, y `comment_entity_id` no vive en el payload del comentario (requiere lookup al registry) |
| `purge_orphan_comments.py` | Borra comentarios cuyo(s) `market_ids` ya no existen en el registry (huerfanos de la limpieza del 17 de agosto que nunca toco `comments/*.json`) -- **unico script de este directorio que borra datos, no solo agrega** | 2026-08-20, aplicado sobre los 4 ciclos afectados (16-17 de agosto) | Terminado y aplicado -- 3,080 comentarios huerfanos removidos (22 markets `direct` + 38 markets `shared_event`/`shared_series`), 8,858 comentarios legitimos intactos, `comment_count` recalculado por archivo. Verificado post-purge: 0 huerfanos en todo el corpus de Comments |

## Convenciones que todos siguen

- **Dry-run por defecto**, se necesita `--apply` explicito para escribir.
- **Aditivos por defecto, con una unica excepcion deliberada y documentada**
  (`purge_orphan_comments.py`, 2026-08-20) -- borra comentarios huerfanos que
  referencian market_ids ya purgados del registry, nunca datos legitimos.
  Todo lo demas en este directorio sigue siendo estrictamente aditivo: ningun
  otro script borra datos existentes, solo agrega campos o snapshots.
- **Idempotentes** -- correr dos veces no duplica ni corrompe (verificado en cada
  uno antes de aplicarlo la primera vez).
- El "cuando corrio" completo, con numeros verificados y el razonamiento detras de
  cada decision de diseno, vive en `.claude/claude_docs/tech_debt.md` -- esta tabla
  es solo el resumen de una linea para orientarse rapido.
