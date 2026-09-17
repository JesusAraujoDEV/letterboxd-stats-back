# Architectural Decision Records (ADRs)

This folder records architectural decisions: what was decided, why, what alternatives were considered, and what trade-offs were accepted.

## Why ADRs

The code shows *what* was built. Git history shows *when*. ADRs capture *why* — the constraints, alternatives, and rationale that aren't visible in the diff.

Read these when:
- A pattern in the code seems non-obvious.
- You're about to revisit a decision and need the original context.
- You're onboarding and want to understand the project's shape.

## Convention

- One file per decision: `NNNN-kebab-case-title.md`. Sequential numbering, never reused.
- Use [`0000-template.md`](0000-template.md) as the starting point.
- Append-only: to change a decision, write a new ADR that supersedes the old one and update the old one's `Status` to `Superseded by NNNN`.
- Keep them short. One ADR = one decision. If you need three, write three ADRs.

## Status values

- **Proposed** — under discussion, not yet adopted.
- **Accepted** — adopted and in force.
- **Superseded by NNNN** — replaced by a newer ADR.
- **Deprecated** — no longer in force but the rationale remains historically relevant.

State lives in the file header and changes in place — a Proposed ADR is born in this folder and **never moves** when accepted. There is no separate "pending decisions" folder: work to be done is a story or requirement; a decision (in any state) is an ADR here.

## When to write an ADR

Yes:
- Picking one technology or pattern over named alternatives.
- A non-obvious trade-off that future-you will question.
- A constraint that affects multiple modules.

No:
- Trivial choices (file naming, formatter config) — those go in `standards/`.
- Pure implementation details — those belong next to the code.
- Tutorials — those belong in `docs/guides/`.

## Index

Add new ADRs to this list with their status:

| # | Title | Status |
|---|-------|--------|
| - | - | - |
