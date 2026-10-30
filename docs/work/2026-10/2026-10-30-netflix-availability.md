# 2026-10-30 — Recommendations limited to Netflix Venezuela and USA

## What changed
`/api/recommendations` now returns only titles on Netflix in Venezuela, the USA or both, with `netflixCountries` and `netflixId`. The first refresh produced 28 recommendations, 21 of them with a Netflix id.

## Why
The owner watches Netflix in both countries through a VPN, and Fondo Mi Box needs the title id for the click.

## How
JustWatch answers both countries and the id in one query, matched by `tmdbId`. TMDB watch providers cover failures and titles JustWatch does not find, without an id.

## Promoted knowledge
`README.md`, `swagger/library.yaml` and ADR 0002.

## Follow-ups
- [ ] Confirm the first scheduled feed run at 03:00 Caracas time on 2026-10-31.
- [ ] Close the public Postgres port or enable TLS.
