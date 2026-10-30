# 2026-10-30 — Personal library stored in Postgres and served with a token

## What changed
The API now keeps the owner's Letterboxd library in the `letterboxd` schema of `jesulovesdatabases`. It adds a ZIP import, a daily feed sync, precomputed TMDB recommendations and token-protected read endpoints. The first import loaded 519 films, 279 diary entries and 30 recommendations.

## Why
Fondo Mi Box needs the owner's data from an API, with Letterboxd and TMDB access kept on the server.

## How
Film identity uses the Letterboxd slug, read from the `boxd.it` redirect for ZIP rows and from the item link for the feed. The scheduler runs in-process and the schema is applied at boot. The initial load went through the deployed API over HTTPS because Postgres on its public port has TLS off.

## Promoted knowledge
`AGENTS.md`, `README.md`, `swagger/library.yaml` and ADR 0001.

## Follow-ups
- [ ] Close the public Postgres port or enable TLS.
- [ ] Configure backups for `DatajesusBase`.
- [ ] Confirm the first scheduled feed run at 03:00 Caracas time on 2026-10-31.
