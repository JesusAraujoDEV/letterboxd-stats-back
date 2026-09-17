# Requirements — Technical work items

High-level technical work defined by an architect role (`SYS`, `DA`, `INFRA`, `FE`, ...): refactors, platform capabilities, infrastructure, technical debt. Work that does not pass through functional stories. The technical half of the backlog; [`../stories/`](../stories/README.md) is the functional half.

## Structure

```
docs/requirements/
└── <plan>/                   # kebab-case English; groups related requirements
    ├── README.md             # plan context + requirement index + recommended order
    └── NNN-slug.md           # one requirement; numbering restarts per plan
```

**Kind lives in the slug.** Verification work (auditing an area against a spec or quality bar) is `NNN-audit-slug.md` — same template, same lifecycle, the expected deliverable is the findings report. No kind folders, no kind field: the filename is the taxonomy.

## Lifecycle

```
Draft → Ready → In progress → Delivered → Closed
```

Same semantics as stories, minus functional validation: a requirement is verified by its **expected deliverable** (does the artifact exist and behave as specified?), typically by the authoring architect role or `SC` (spec-compliance). From Closed on, the file is immutable.

## Rules

- A requirement describes work to do, not the decision taken. If executing it produces a decision with trade-offs, that decision is an ADR in `decisions/` and the requirement links it.
- The estimation table is mandatory before implementation starts (see [`../AGENTS.md`](../AGENTS.md#estimation-discipline-mandatory)).
- Branch convention: `req/<plan>-NNN-slug`.

## Requirement template

```markdown
# NNN — Short title

- **Status:** Draft | Ready | In progress | Delivered | Closed
- **Plan:** <plan> ([README](README.md))
- **Date:** YYYY-MM-DD
- **Author role:** SYS | DA | INFRA | ...
- **Branch:** (on In progress: `req/<plan>-NNN-slug`)
- **Depends on:** (requirements, stories, or ADRs that must land first; "None" if none)

## Context

(What motivates this work. 2-4 paragraphs max.)

## Goal

(What must be true when this is done — phrased as verifiable outcomes, not activities.)

## Areas to investigate

(Open questions, files to read, patterns to validate. What the implementing agent must resolve.)

## Expected deliverable

(Artifacts this produces: code, schema, config, docs, ADR.)

## Estimation

| Milestone | Est. hours | Started | Finished | Actual hours | Notes |
|-----------|-----------|---------|----------|--------------|-------|
| | | | | | |

(Filled by the evaluating agent BEFORE implementation; Started/Finished recorded during execution as `YYYY-MM-DD HH:MM -ZZ:ZZ` — format and discipline in [`../AGENTS.md`](../AGENTS.md#estimation-discipline-mandatory).)

## Changes

- (Only if the goal changes after In progress: date, what, why.)
```
