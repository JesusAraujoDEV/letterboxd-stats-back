# 0001 — Persist the owner's Letterboxd library in Postgres

- **Status**: Accepted
- **Date**: 2026-10-30
- **Owner role**: system-architect
- **Affects**: `src/services`, `src/utils`, `src/routes`, deployment env, locked decisions in `AGENTS.md`

## Context

Fondo Mi Box (Android TV app) needs the owner's films, watchlist and recommendations from a stable API, without Letterboxd or TMDB secrets on the device. The API was stateless ("no database"), so it could not serve that data nor accumulate changes between exports.

## Decision

Persist one person's library in the `letterboxd` schema of the personal Postgres (`jesulovesdatabases`), fed by a ZIP import plus a daily read of the public RSS feed.

The API stays a single Express service. The film key is the Letterboxd slug, the only identifier available in both sources. The ZIP is the authoritative snapshot (it resets flags and diary up to its newest date); the feed only adds. Recommendations are precomputed after each sync and read from a table. The scheduler lives in-process. Library endpoints require a bearer token and fail closed when it is not configured. The schema is applied idempotently at boot from `src/db/schema.sql`.

The stateless `POST /api/upload-stats` flow is unchanged and does not use the database.

## Considered alternatives

- **Separate service for the library** — a new repo and deployment. Rejected because it would duplicate the TMDB and ZIP code for a one-person workload.
- **TMDB search by title and year for ZIP rows** — no scraping needed. Rejected because it can match the wrong film; the `boxd.it` page states the exact TMDB id.
- **Computing recommendations per request** — always fresh. Rejected because each request would need dozens of TMDB calls.
- **External cron (Dokploy schedule)** — visible in the panel. Rejected because it needs an extra endpoint and token path; the hourly in-process check also recovers runs missed during a restart.

## Consequences

- **Positive**: the TV app gets one authenticated API; history accumulates without manual uploads.
- **Negative**: the service now depends on Postgres for library routes. The feed cannot see old rating changes, deletions or the watchlist, so those wait for the next ZIP. Uploading a stale ZIP rolls those fields back until a fresher one is imported.
- **Neutral**: single-user by design. Multi-user would need a user key on every table.

## Migration notes

Replaces the "sin base de datos" locked decision in `AGENTS.md`. Requires `DATABASE_URL`, `LIBRARY_API_TOKEN` and `LETTERBOXD_USERNAME` in the deployment.
