# 001 — Audit: stats.service.js excede el techo de tamaño 9x

- **Status:** Delivered
- **Plan:** code-quality ([README](README.md))
- **Date:** 2026-09-17
- **Author role:** SYS
- **Branch:** (on In progress: `req/code-quality-001-audit-stats-service`)
- **Depends on:** None

## Context

`src/services/stats.service.js` tiene 1368 líneas contra un techo de 150 para "Service/store" en `standards/code-quality.md`. Es un archivo "dios": mezcla datos estáticos (mapas de idioma/país), utilidades de fecha puras, y media docena de agregaciones de dominio distintas, todo detrás de una única función orquestadora (`buildStatsFromZipBuffer`, ~344 líneas) y una función de metadata que hace dos cosas a la vez (`buildTopMetadataFromWatched`, ~230 líneas). Los fixes de performance y los nuevos campos de export (2026-09-17) ya se sacaron a servicios nuevos (`movieExtras`, `diaryExtras`, `profileExtras`) para no seguir agravando esto — este requirement es la continuación: partir lo que ya existía.

## Goal

- Ningún archivo bajo `src/` supera su techo de `standards/code-quality.md`, salvo excepciones documentadas en `docs/DEVIATIONS.md`.
- `buildStatsFromZipBuffer` queda como orquestador delgado: parseo de CSV del zip + llamadas a sub-builders + ensamblado del objeto de respuesta. Sin lógica de agregación inline.
- Ningún comportamiento observable cambia (mismo JSON de respuesta, mismos tests manuales del ZIP real deben seguir pasando).
- Código muerto confirmado (`getUserAvatar`, `buildTopCreditsFromDiary`) eliminado, no migrado.
- La duplicación de `buildCacheKey` (hoy repetida en `stats.service.js`, `movieExtras.service.js`, `diaryExtras.service.js`) se unifica en un solo util.

## Areas to investigate

Líneas exactas al momento de este audit (`git blame`/`wc` pueden haber corrido desde entonces — releer antes de tocar):

| Símbolo | Líneas actuales | Destino propuesto | Riesgo |
|---|---|---|---|
| `daysOfWeek`, `monthsOfYear`, `getISOWeekNumber`, `parseWatchedDate` | 114–157 (~65) | `src/utils/dateHelpers.js` | Bajo — puras, sin dependencias |
| `languageCodeMap` | 192–379 (~188) | `src/utils/languageMap.js` | Bajo — dato estático, mover tal cual |
| `countryCodeMap` | 380–634 (~255) | `src/utils/countryMap.js` | Bajo, pero el archivo solo (255 líneas) ya excede el techo de 200 para "Generic module". Es una tabla de lookup plana — partirla alfabéticamente no aporta legibilidad. Decisión recomendada: documentar excepción en `docs/DEVIATIONS.md` como "dato de mapeo estático, no lógica", igual que la excepción ya existente para `src/components/ui` en el frontend. |
| `buildCacheKey` (dup. en 3 archivos) | 635–642 aquí + copias en `movieExtras.service.js`/`diaryExtras.service.js` | `src/utils/cacheKey.js` (`buildTmdbCacheKey`) | Medio — hay que actualizar 3 import sites |
| `letterboxdLinkCache`, `resolveLetterboxdLink` | 641–679 (~39) | `src/services/interactionStats.service.js` | Medio — es llamada desde el loop de comentarios dentro de `buildStatsFromZipBuffer`; ese loop también se extrae en el mismo paso |
| `getUserAvatar` | 680 (1) | **Eliminar** — confirmado sin callers | Ninguno |
| `incrementPersonCounter` + parte final de `buildTopMetadataFromWatched` (agregación de actores/directores) | ~682–697 + ~830–927 (~100) | `src/services/castCrewStats.service.js` (`buildCastCrewStats(allMovies)`) | Medio — requiere que `buildTopMetadataFromWatched` devuelva `allMovies` primero y que `buildStatsFromZipBuffer` haga una llamada adicional encadenada |
| Resto de `buildTopMetadataFromWatched` (enriquecimiento TMDB + género/país/idioma) | ~698–830 (~130) | `src/services/movieMetadata.service.js` (`buildMovieMetadata(...)`) | Medio — misma función, mitad del contenido |
| `buildTopCreditsFromDiary` | 928–986 (~59) | **Eliminar** — confirmado sin callers | Ninguno |
| `buildTotalHoursWatched` | 987–1023 (~37) | `src/services/watchTimeStats.service.js` | Bajo |
| `buildTopDecades` | 21–129 (~109) | `src/services/decadeStats.service.js` | Bajo |
| `calculateLongestStreak` | 158–191 (~34) | `src/services/streakStats.service.js` | Bajo |
| Bloque de actividad (`activityByYear`, `watchedYearMap` → `activityStats`, `watchedYearStats`) dentro de `buildStatsFromZipBuffer` | ~90 líneas inline | `src/services/activityStats.service.js` (`buildActivityStats(diaryRows)`) | Medio — lógica no está en función propia hoy, hay que extraerla primero |
| Bloque de rating (`ratingDistribution`, `averageRating`, `averageRatingByReleaseYear`) inline | ~50 líneas inline | `src/services/ratingStats.service.js` (`buildRatingStats(ratingsRows)`) | Medio |
| Bloque de años/tags (`topYears`, `moviesByReleaseYear`, `topTags`) inline | ~40 líneas inline | `src/services/yearTagStats.service.js` | Bajo |
| `mostRewatchedMovies` (por duplicado de título + poster) inline | ~25 líneas inline | `src/services/mostRewatched.service.js` (`buildMostRewatchedMovies(diaryRows)`) — **nombre distinto** a `diaryExtras.service.js#buildRewatchStats` (esa es sobre la columna `Rewatch` explícita; son conceptos distintos, no fusionar) | Bajo |
| Loop de comentarios/interacciones (`interactionsMap`, `topInteractedUsers`, resolución de posters top-10) inline | ~90 líneas inline | `src/services/interactionStats.service.js` (junto con `resolveLetterboxdLink`) | Medio — usa `mapWithConcurrency`, ya extraído a `utils/concurrency.js` |
| Contadores de colección (`totalWatchlist/Reviews/Comments`, `deleted*`, `liked*`) inline | ~60 líneas inline | `src/services/collectionCounts.service.js` | Bajo |

Después de extraer todo lo anterior, `buildStatsFromZipBuffer` debería quedar en ~120–150 líneas: el `Promise.all` de parseo de CSV, ~15 llamadas a sub-builders, y el ensamblado del objeto de retorno.

## Expected deliverable

- ~11 archivos nuevos bajo `src/utils/` y `src/services/` (ver tabla), cada uno bajo su techo.
- `stats.service.js` reducido a orquestador (~120–150 líneas).
- 2 símbolos muertos eliminados.
- `docs/DEVIATIONS.md` con la excepción de `countryCodeMap` (y `languageCodeMap` si se decide igual).
- Mismo JSON de salida verificado corriendo `buildStatsFromZipBuffer` contra un ZIP real antes/después (ya existe un ZIP de prueba usado en el fix de performance del mismo día).

## Estimation

| Milestone | Est. hours | Started | Finished | Actual hours | Notes |
|-----------|-----------|---------|----------|--------------|-------|
| Eliminar código muerto (`getUserAvatar`, `buildTopCreditsFromDiary`) | 0.25 | | | | Riesgo cero, hacer primero |
| Extraer `dateHelpers.js`, `languageMap.js`, `countryMap.js` + excepción en DEVIATIONS.md | 1 | | | | Puramente mecánico |
| Unificar `buildCacheKey` → `utils/cacheKey.js` (3 call sites) | 0.5 | | | | |
| Extraer `decadeStats.service.js`, `streakStats.service.js`, `watchTimeStats.service.js` | 1 | | | | Funciones ya aisladas, solo mover |
| Extraer `mostRewatched.service.js`, `yearTagStats.service.js`, `collectionCounts.service.js` | 1.5 | | | | Lógica hoy inline en `buildStatsFromZipBuffer`, hay que aislarla primero |
| Extraer `activityStats.service.js`, `ratingStats.service.js` | 1.5 | | | | Ídem |
| Extraer `interactionStats.service.js` (+ `resolveLetterboxdLink`) | 1.5 | | | | Usa `mapWithConcurrency`; verificar que la concurrencia se preserva |
| Partir `buildTopMetadataFromWatched` → `movieMetadata.service.js` + `castCrewStats.service.js` | 2.5 | | | | El más delicado: dos funciones deben encadenarse en el orden correcto |
| Verificación final: correr contra ZIP real, comparar JSON byte a byte con el actual | 1 | | | | Bloqueante para cerrar el requirement |
| **Total** | **9.75** | | | | |

## Changes

- 2026-09-17: la división final usó 14 archivos nuevos (no 11): además de los previstos se sumó `zipInput.service.js` (parseo de CSV del zip + perfil, para que el propio `stats.service.js` bajara de 150 líneas) y `mostRewatched.service.js` se separó de lo que iba a ser parte de otro archivo. `languageCodeMap` no necesitó excepción (188 líneas, bajo 200); `countryCodeMap` sí — se empaquetaron varias entradas por línea en vez de partir el archivo (ver `docs/DEVIATIONS.md` #1). Verificado corriendo el pipeline completo contra el ZIP real del usuario: los campos no tocados por este refactor (`rewatchStats`, `runtimeExtremes`, `watchSpan`, `franchiseStats`, `industryTotals`, `ratingComparison`, `favoriteFilms`, `customLists`, `daysActive`) coinciden byte a byte con la corrida previa al refactor.
