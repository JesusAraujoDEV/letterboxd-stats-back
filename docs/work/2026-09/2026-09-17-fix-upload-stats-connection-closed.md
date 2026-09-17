# 2026-09-17 — Fix ERR_CONNECTION_CLOSED en /api/upload-stats

## What changed
`POST /api/upload-stats` cerraba la conexión sin respuesta (`net::ERR_CONNECTION_CLOSED` en el navegador) al subir exports grandes de Letterboxd. Se añadió límite de tamaño en la subida, timeouts a las llamadas externas, concurrencia limitada en las resoluciones de enlaces `boxd.it` y en la búsqueda de posters de comentarios, y caché negativa para TMDB.

## Why
Diagnóstico (roles `performance-reliability` y `security-compliance` vía crew): el proceso probablemente moría por OOM (`multer.memoryStorage()` sin límite de tamaño + caché TMDB completa con créditos por película) y, de forma agravante, `resolveLetterboxdLink` hacía un `fetch HEAD` secuencial por cada comentario con enlace `boxd.it`, sin caché ni timeout — con historiales grandes esto tardaba varios minutos y no tenía techo si TMDB o Letterboxd colgaban una respuesta.

## How
- `src/routes/stats.routes.js`: límite de 20MB en multer + respuesta 413 explícita (antes: sin límite).
- `src/utils/tmdbHelper.js`: `AbortSignal.timeout(5000)` en cada llamada TMDB + log de aviso en 429 (antes: sin timeout, un cuelgue de TMDB colgaba la request entera).
- `src/services/stats.service.js`:
  - Nuevo helper local `mapWithConcurrency` (sin dependencia nueva) para acotar concurrencia a 8.
  - `resolveLetterboxdLink`: timeout de 4s + caché por URL (`letterboxdLinkCache`, módulo-scope) + resolución concurrente en vez de secuencial en el bucle de `commentsRows`.
  - Bucle de posters de comentarios de los top-10 usuarios: de secuencial a concurrente con el mismo helper.
  - `detailsCache` de TMDB (usado en `buildTopMetadataFromWatched` y `buildTotalHoursWatched`): ahora cachea también resultados `null` (caché negativa), evitando re-consultar TMDB en cada rewatch de una película no resoluble.

## Promoted knowledge
Ninguno nuevo en `guides/` todavía — el criterio de timeouts/concurrencia queda documentado solo en el código (comentarios `ponytail:`). Si se repite el patrón en otro endpoint, promover a una guía de "llamadas externas con presupuesto" en `docs/guides/`.

## Follow-ups
- [ ] Verificar en logs/reinicio del contenedor en Dokploy si la causa era efectivamente OOM (no confirmado en runtime, solo por diagnóstico estático).
- [ ] P1 pendiente, no implementado: procesamiento asíncrono (`202` + `jobId` + polling) para eliminar por completo la dependencia del timeout del proxy inverso.
- [ ] P1 pendiente: caché TMDB a nivel de proceso (hoy es por-request, se pierde entre uploads).
- [ ] Nota de proceso: este cambio quedó empaquetado por accidente dentro del commit `95a2eea` ("docs: add comprehensive documentation structure...") hecho por el subagente `crew-installer`, que también hizo `git push` a `origin/main` sin confirmación previa del usuario. Revisar si conviene un commit separado de corrección de historial o dejarlo documentado aquí como excepción.
