# 2026-09-17 — Nuevos datos derivados del export en /api/upload-stats

## What changed
`POST /api/upload-stats` ahora devuelve 12 campos adicionales: `rewatchStats`, `reviewTextStats`, `ratingExtremes`, `runtimeExtremes`, `watchSpan`, `franchiseStats`, `studioStats`, `industryTotals`, `ratingComparison`, `favoriteFilms`, `customLists`, `daysActive`. Contrato actualizado en `swagger/stats.yaml` en el mismo cambio.

## Why
Inspección directa del export real del usuario mostró campos que ya se descargaban (JSON de TMDB, `diary.csv → Rewatch`, `reviews.csv → Review`, `profile.csv → Favorite Films`/`Date Joined`, `lists/*.csv`) pero nunca se exponían en la respuesta.

## How
Toda la lógica nueva vive en 3 servicios pequeños para no seguir haciendo crecer `stats.service.js` (ya cerca del techo de tamaño de `standards/code-quality.md`):
- `src/services/movieExtras.service.js`: deriva `collection`/`budget`/`revenue`/`voteAverage`/`studios` del JSON de TMDB ya solicitado (sin llamadas nuevas) y agrega franquicias, estudios, totales de industria y comparación de rating tuyo vs. TMDB.
- `src/services/diaryExtras.service.js`: rewatch explícito (columna `Rewatch`), estadísticas de texto de reseñas, extremos de rating y runtime, primer/último visionado.
- `src/services/profileExtras.service.js`: resuelve `Favorite Films` (links `boxd.it` a páginas de película, patrón `/film/slug/` distinto al de perfiles de usuario) y parsea listas personalizadas.
- `src/utils/csvHelper.js`: nuevo `parseListCsvBuffer` para el formato de dos tablas CSV que usa `lists/*.csv` (metadata + películas separadas por línea en blanco).
- `src/utils/concurrency.js`: se extrajo `mapWithConcurrency` (antes vivía inline en `stats.service.js` desde el fix de performance del mismo día) para reusarlo en la resolución de `Favorite Films`.

Validado corriendo `buildStatsFromZipBuffer` contra un ZIP real de export (no solo sintaxis).

## Promoted knowledge
Ninguno en `guides/` — el patrón de servicio pequeño por dominio de dato queda como precedente en el propio código para el siguiente lote de stats.

## Follow-ups
- [ ] Frontend (`letterboxd-stats`) todavía no consume ninguno de estos campos nuevos — son datos disponibles en la API sin UI.
- [ ] `keywords`/temas de TMDB quedó descartado por ahora: requeriría una llamada extra por película (`/movie/{id}/keywords`), justo lo que el fix de performance del mismo día buscaba reducir.
