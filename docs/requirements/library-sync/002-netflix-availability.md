# 002 — Netflix availability in recommendations

- **Status:** Delivered
- **Plan:** library-sync ([README](README.md))
- **Date:** 2026-10-30
- **Author role:** SYS
- **Branch:** `req/library-sync-002-netflix-availability`
- **Depends on:** [001](001-personal-library-api.md), [ADR 0002](../../decisions/0002-use-justwatch-for-netflix-availability.md)

## Context

Recommendations listed subscription platforms for one country, and `netflixId` was always null. The owner watches Netflix in Venezuela and the USA.

## Goal

- `/api/recommendations` only returns titles on Netflix in Venezuela, the USA or both.
- Each item states `netflixCountries` and, when known, `netflixId`.
- A JustWatch failure falls back to TMDB without stopping the sync.

## Areas to investigate

- How many candidates JustWatch matches by `tmdbId`.

## Expected deliverable

Code, schema change, updated OpenAPI contract, deployed service, refreshed recommendations.

## Estimation

| Milestone | Est. hours | Started | Finished | Actual hours | Notes |
|-----------|-----------|---------|----------|--------------|-------|
| JustWatch and TMDB availability | 1.5 | 2026-10-30 15:44 -0400 | 2026-10-30 15:45 -0400 | 0.02 | Executed together with the next milestone; the time is the shared total |
| Contract, docs and tests | 0.5 | 2026-10-30 15:44 -0400 | 2026-10-30 15:45 -0400 | 0.02 | Shared total |
| Deploy and verification | 0.5 | 2026-10-30 15:45 -0400 | 2026-10-30 15:47 -0400 | 0.03 | Times from the Dokploy deployment and the import finish in `/api/health` |

## Changes

- None.
