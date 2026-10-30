# 0002 — Use JustWatch for Netflix availability

- **Status**: Accepted
- **Date**: 2026-10-30
- **Owner role**: system-architect
- **Affects**: `src/services`, `src/utils`, `/api/recommendations` contract

## Context

Fondo Mi Box should only suggest titles the owner can open on Netflix, in Venezuela or the USA (the owner switches with a VPN), and it needs the Netflix title id for the click.

## Decision

Read Netflix availability and the title id from the JustWatch GraphQL API, and fall back to TMDB watch providers when JustWatch fails or does not find the title.

A JustWatch result counts only when its `tmdbId` matches the candidate, because the query searches by title. The TMDB fallback gives countries without an id, so `netflixId` can be `null`. Recommendations not on Netflix in either country are discarded. This replaces the `country` and `providers` fields with `netflixCountries`.

## Considered alternatives

- **TMDB only** — official and stable. Rejected because it has no Netflix title id.
- **Letterboxd availability endpoint** — the source the owner pointed to. Rejected because Cloudflare answered 403 from a plain HTTP client, and it could not be verified for two countries.

## Consequences

- **Positive**: one query answers both countries and returns the id for the click.
- **Negative**: the JustWatch API is unofficial and undocumented, so it may change without notice. If it breaks, recommendations keep working from TMDB but lose `netflixId`.
- **Neutral**: at most 150 candidates are checked per sync to reach 30 recommendations.

## Migration notes

`src/db/schema.sql` drops `country` and `providers` from `letterboxd.recommendations` and adds `netflix_countries`. `RECOMMENDATIONS_COUNTRY` is no longer used.
