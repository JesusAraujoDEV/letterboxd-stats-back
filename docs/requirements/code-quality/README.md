# code-quality — Plan

Reducir los archivos que violan `standards/code-quality.md` (techos de tamaño) a algo mantenible. Este plan cubre el backend; el frontend tiene su propio plan equivalente en `letterboxd-stats/docs/requirements/code-quality/`.

## Alcance detectado

Barrido de `src/**/*.js` (excluyendo `node_modules`) contra la tabla de techos (Service/store 150, Generic module 200). Único archivo fuera de límite:

| Archivo | Líneas | Techo aplicable | Exceso |
|---|---|---|---|
| `src/services/stats.service.js` | 1368 | 150 (service) | 9x |

Todo lo demás (`movieExtras.service.js` 74, `diaryExtras.service.js` 94, `profileExtras.service.js` 51, `csvHelper.js` 53, `tmdbHelper.js` 64, `concurrency.js` 14, `server.js` 41, `stats.controller.js` 17, `stats.routes.js` 16) está dentro de techo.

## Índice de requirements

| # | Slug | Qué cubre |
|---|------|-----------|
| 001 | audit-stats-service | Hallazgos completos y plan de división de `stats.service.js` |

## Orden recomendado

Ejecutar 001 en fases (detalladas dentro del propio requirement): primero eliminación de código muerto y extracción de datos/utilidades puras (riesgo bajo, sin tocar call sites), después los agregados independientes, al final la división de las dos funciones "dios" (`buildTopMetadataFromWatched`, `buildStatsFromZipBuffer`) que sí cambian call sites.
