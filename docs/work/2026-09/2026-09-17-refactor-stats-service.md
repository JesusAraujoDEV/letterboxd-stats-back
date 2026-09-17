# 2026-09-17 — stats.service.js: de 1368 líneas a orquestador de 105

## What changed
Se ejecutó el requirement `code-quality/001-audit-stats-service.md`: `stats.service.js` pasó de 1368 a 105 líneas, dividido en 14 archivos nuevos bajo `src/utils/` y `src/services/`. Código muerto (`getUserAvatar`, `buildTopCreditsFromDiary`, `incrementPersonCounter`) eliminado. Duplicación de `buildCacheKey` (3 copias) unificada en `utils/cacheKey.js`.

## Why
Auditoría de tamaño de archivo contra `standards/code-quality.md` (techo de 150 para servicios) — `stats.service.js` era 9x el límite, una función "dios" mezclando datos estáticos, utilidades de fecha y media docena de agregaciones de dominio.

## How
`buildStatsFromZipBuffer` quedó como orquestador puro: parseo de CSV (`zipInput.service.js`) + ~13 llamadas a sub-builders + ensamblado del objeto de retorno. Cada dominio salió a su propio archivo: `decadeStats`, `streakStats`, `watchTimeStats`, `movieMetadata` + `castCrewStats` (la función `buildTopMetadataFromWatched` original hacía dos cosas, se partió en dos), `interactionStats`, `activityStats`, `ratingStats`, `yearTagStats`, `mostRewatched`, `collectionCounts`. Datos estáticos a `utils/dateHelpers.js`, `utils/languageMap.js`, `utils/countryMap.js`.

`countryCodeMap` (258 líneas de tabla plana) no cabía ni solo bajo el techo de 200 para módulo genérico. Partirlo alfabéticamente no aporta legibilidad, así que — con aprobación explícita del owner — se empaquetaron ~5 entradas por línea en vez de dividir el archivo; documentado en `docs/DEVIATIONS.md` #1.

Verificación: se corrió `buildStatsFromZipBuffer` contra el ZIP real del usuario antes y después del refactor. Los campos no tocados por esta división (`rewatchStats`, `runtimeExtremes`, `watchSpan`, `franchiseStats`, `industryTotals`, `ratingComparison`, `favoriteFilms`, `customLists`, `daysActive`) coincidieron exactamente, señal de que la caché de TMDB compartida entre servicios sigue funcionando igual.

## Promoted knowledge
`docs/DEVIATIONS.md` #1: excepción de empaquetado de línea para tablas de lookup estáticas que excederían el techo de módulo.

## Follow-ups
- [ ] El plan de refactorización equivalente del frontend (`letterboxd-stats/docs/requirements/code-quality/001-audit-file-size-violations.md`, 16 archivos, ~28h) sigue pendiente.
