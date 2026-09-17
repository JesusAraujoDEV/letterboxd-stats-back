# Accepted deviations from the crew documentation standard

This project was audited against the `crew` plugin documentation standard (taxonomy in [`AGENTS.md`](AGENTS.md), circuit in [`guides/delivery-circuit.md`](guides/delivery-circuit.md)). The deviations below were reviewed by the project owner and **deliberately kept**.

> **Binding for all agents:** a deviation recorded here is a decision, not a defect. Do not "fix" it, do not flag it again in audits, do not treat the standard as overriding it. To revisit one, raise it with the owner — never unilaterally.

## Registry

| # | Deviation | Standard says | This project does | Rationale | Decided | Date |
|---|-----------|---------------|-------------------|-----------|---------|------|
| 1 | `src/utils/countryMap.js` — flat static ISO country-code → name lookup table would need 258 lines at one entry per line | Files over the 200-line "Generic module" ceiling must be split | Kept as one file, packed 5 entries per line (~57 lines) instead of splitting alphabetically | It's data, not logic; an alphabetical split adds no readability, it only exists to satisfy a line count — packing entries per line honors both the spirit and the mechanical `guard-code-quality.js` hook | Jesús Araujo (owner) | 2026-09-17 |

## Convention

- One row per deviation; keep rationale to one line, link a fuller doc if needed.
- Added only as the outcome of a `DOC` audit conversation with the owner — never unilaterally by an agent.
- Removing a row requires the owner's explicit decision (the project converged to the standard, or the deviation was superseded).
- If this file is empty, the project follows the standard fully.
