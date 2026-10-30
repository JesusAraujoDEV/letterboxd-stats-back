# 001 — Personal library API

- **Status:** In progress
- **Plan:** library-sync ([README](README.md))
- **Date:** 2026-10-30
- **Author role:** SYS
- **Branch:** `req/library-sync-001-personal-library`
- **Depends on:** [ADR 0001](../../decisions/0001-persist-personal-library-in-postgres.md)

## Context

The owner wants their Letterboxd data in the `jesulovesdatabases` Postgres, updated daily, and exposed to Fondo Mi Box. TMDB and database secrets must stay on the server.

## Goal

- A ZIP import loads the full history into the `letterboxd` schema.
- The public feed adds new activity daily at 03:00 Caracas time.
- `GET /api/recommendations`, `/api/films`, `/api/watchlist` and `/api/health` respond only with a valid bearer token.
- `/api/recommendations` includes reason, availability country and an optional Netflix id.

## Areas to investigate

- Whether the production server can reach Letterboxd film pages and the feed.

## Expected deliverable

Code, schema, OpenAPI contract, updated `AGENTS.md`, deployed service, initial data load.

## Estimation

| Milestone | Est. hours | Started | Finished | Actual hours | Notes |
|-----------|-----------|---------|----------|--------------|-------|
| Schema, import and feed sync | 3 | 2026-10-30 15:22 -0400 | | | |
| Recommendations and API | 2 | | | | |
| Contract and docs | 1 | | | | |
| Deploy and initial load | 1 | | | | |

## Changes

- None.
