# Documentation — Criteria for Agents and Contributors

Prescriptive guide: **when, where, and why** to write documentation in this project's `docs/` tree. Every agent that writes any artifact must know this file.

## Foundational rules

1. **Folder = nature, state = field.** Folders are named by what the artifact *is* (a story, a requirement, a decision), never by what state it is in. State lives inside the file (`Status:` field). Files never move between folders when their state changes.
2. **Single source of truth.** Each fact lives in exactly one file. Other artifacts link to it; they never duplicate it. An external tracker, if used, holds only state + assignee + a link to the file here.
3. **Every significant task leaves a trace.** If a change affects behavior, structure, conventions, or decisions, it gets recorded in the corresponding layer. When in doubt, record it.
4. **Accepted deviations are intentional.** If this project diverges from this standard, the divergence is recorded in [`DEVIATIONS.md`](DEVIATIONS.md). Agents respect recorded deviations — they are decisions, not defects to fix.

## Where to write — decision criteria

### `docs/briefs/` — Executive decision requests

High-level documents for a non-technical sponsor (CEO, CTO, client) to validate an initiative before backlog work starts. Written by `PROD`; decided by the sponsor (ownership map). Hard cap 800 words, one explicit ask, immutable after decision. Boundary rule: if only a developer understands the sentence, it goes to the plan, not the brief. Conventions: [`briefs/README.md`](briefs/README.md).

**Not here:** plans, estimation breakdowns, technical detail (→ `requirements/`/`stories/`); decisions already taken (→ `decisions/`).

### `docs/stories/` — Functional work items

Units of user-observable behavior with acceptance criteria, written by functional analysis (`FA` role). Grouped by feature (`stories/<feature>/NNN-slug.md`). Kind lives in the slug — a defect is `NNN-bug-slug.md`; no kind folders, no kind field. Template, lifecycle, and the embedded estimation table: [`stories/README.md`](stories/README.md).

**Not here:** technical decisions (→ `decisions/`), technical work items (→ `requirements/`).

### `docs/requirements/` — Technical work items

High-level technical work defined by an architect role (`SYS`, `DA`, `INFRA`, ...) that does not pass through functional stories: refactors, infrastructure, platform capabilities, technical debt. Grouped by plan (`requirements/<plan>/NNN-slug.md`). Kind lives in the slug — verification work is `NNN-audit-slug.md`; no kind folders, no kind field. Template and estimation table: [`requirements/README.md`](requirements/README.md).

**Not here:** the decision itself (when implementing a requirement produces a decision with trade-offs, that decision is an ADR in `decisions/`; the requirement links it).

### `docs/decisions/` — Architecture Decision Records

Decisions with viable alternatives, hard to revert, or affecting multiple modules. Status (`Proposed` / `Accepted` / `Superseded` / `Deprecated`) lives in the file header — a proposed decision is born here and never moves. Conventions: [`decisions/README.md`](decisions/README.md).

### `docs/proposals/` — Ownerless ideas

Improvement detected during work, with no owner and no date yet. Matures into a story/requirement (when someone owns it), becomes an ADR (when it is decided), or is discarded with a recorded reason.

### `docs/guides/` — Living behavior docs

How something cross-cutting works **today**. Updated in the same change that alters the behavior. The delivery process itself is one of these: [`guides/delivery-circuit.md`](guides/delivery-circuit.md).

**Builder/agent-facing only.** End-user or product documentation (user guides, manuals, help content) is outside this standard's scope and must not live here — give it its own location (e.g. `docs/product/` or the product's site) so agents never confuse audience.

### `docs/work/` — Historical log (evidence, never truth)

What was done, when, and why — one immutable entry per significant change. **A work entry expires the day it is written**: it is evidence of rationale, never a source of current behavior. Any knowledge in a work entry that is still true must be promoted to a living guide; agents must never cite `work/` as current truth. Conventions: [`work/README.md`](work/README.md).

### `docs/glossary/` — Domain terms

Plain-language entries for domain jargon visible in the product. Source of truth for UI tooltips.

### `docs/DEVIATIONS.md` — Accepted deviations from this standard

When a documentation audit (`DOC` role) finds a divergence and the owner decides to keep it, it is recorded here with rationale and date. Binding for all agents.

## Estimation discipline (mandatory)

Every story and requirement carries an **estimation table** (milestones, estimated human-hours, real start/end timestamps, actual hours). Any agent that evaluates a work item (`UX`, `SYS`, `DA`, or any other) must fill the estimation table **before** implementation starts. The agent that executes records real start/finish per milestone. This is how the team measures the cost of each agentic iteration — do not skip it, do not estimate retroactively.

Timestamps are timezone-stamped: `YYYY-MM-DD HH:MM -ZZ:ZZ`, taken from the clock (`date "+%Y-%m-%d %H:%M %z"`), never reconstructed from memory. Closure with an incomplete table is invalid and hook-enforced.

## Completeness check (when closing a task)

- [ ] Did observable behavior change? → changelog entry (project-specific location).
- [ ] Did you implement a story/requirement? → state, branch, and estimation table updated in its file.
- [ ] Did you make a decision with trade-offs? → ADR in `decisions/`.
- [ ] Did cross-cutting behavior change? → update/create the guide.
- [ ] Did you close a significant iteration? → entry in `work/YYYY-MM/`.
- [ ] Did the doc structure change? → update this file + `INDEX.md`.

## See also

- [INDEX.md](INDEX.md) — documentation index
- [MAINTAINING.md](MAINTAINING.md) — lifecycle rules, drift prevention
- [guides/delivery-circuit.md](guides/delivery-circuit.md) — the full delivery circuit
- [DEVIATIONS.md](DEVIATIONS.md) — accepted deviations registry
