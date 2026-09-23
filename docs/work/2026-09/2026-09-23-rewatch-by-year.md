# 2026-09-23 — rewatchByYear en /api/upload-stats

## What changed
Se agregó el campo `rewatchByYear` a la respuesta de `POST /api/upload-stats`: por cada año de visionado devuelve `totalWatches`, `firstWatches`, `rewatches` y sus porcentajes. Se creó el servicio `rewatchByYear.service.js`, se exportó `isRewatchRow` desde `diaryExtras.service.js` para reusarlo y se documentó el campo en `swagger/stats.yaml`.

## Why
El frontend necesita mostrar, por año, la proporción entre películas vistas por primera vez y rewatches. La lógica de detección de rewatch ya existía pero no estaba expuesta ni agrupada por año.

## How
`buildRewatchByYear(diaryRows)` agrupa las filas de `diary.csv` por año (extraído de `Watched Date`, fallback `Date`, validado con `/^[0-9]{4}$/`), cuenta rewatches con el `isRewatchRow` exportado y calcula porcentajes con 1 decimal. El resultado se ordena por año descendente y se conecta en `stats.service.js`.

## Promoted knowledge
None — el contrato del campo queda documentado en `swagger/stats.yaml`, fuente viva del contrato de la API.

## Follow-ups
- [ ] None
