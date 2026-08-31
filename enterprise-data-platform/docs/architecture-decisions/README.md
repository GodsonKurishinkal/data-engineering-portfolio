# Architecture Decision Records (ADRs)

> Documenting the "why" behind technical decisions, not just the "what"

## What are ADRs?

Architecture Decision Records capture significant technical decisions made during system design and evolution. They provide context for future maintainers (including future-you) about why certain paths were chosen over alternatives.

## ADR Template

Each ADR follows this structure:

```markdown
# ADR-XXX: Title

## Status
Accepted / Proposed / Deprecated / Superseded by ADR-XXX

## Context
What is the issue we're seeing that motivates this decision?

## Decision
What is the change we're proposing/making?

## Consequences
What becomes easier or more difficult because of this change?

## Alternatives Considered
What other options were evaluated? Why were they rejected?
```

## Index of ADRs

| ID | Title | Status | Date |
|----|-------|--------|------|
| [ADR-001](001-polars-over-pandas.md) | Choose Polars over Pandas for ETL | Accepted | 2024-03 |
| [ADR-002](002-duckdb-for-analytics.md) | Use DuckDB for analytical queries | Accepted | 2024-04 |
| [ADR-003](003-medallion-architecture.md) | Adopt Medallion (Bronze/Silver/Gold) architecture | Accepted | 2024-02 |
| [ADR-004](004-parquet-hive-partitioning.md) | Use Parquet with Hive partitioning (not Delta) | Accepted | 2024-02 |
| [ADR-005](005-config-driven-pipelines.md) | Configuration-driven pipeline design | Accepted | 2024-03 |
| [ADR-006](006-rpa-for-legacy-systems.md) | Use RPA for legacy system extraction | Accepted | 2024-05 |
| [ADR-007](007-task-scheduler-orchestration.md) | Windows Task Scheduler for orchestration | Accepted | 2024-03 |

## When to Write an ADR

Write an ADR when:
- Choosing between multiple viable technologies
- Making decisions that affect system structure
- Setting patterns/standards for the team
- Changing a previous architectural decision

Don't write an ADR for:
- Trivial implementation details
- Temporary workarounds
- Standard library choices

## Numbering

ADR numbers are **global to this repository** and never reused. Case studies on
the portfolio site cite these numbers directly; a decision without a file here
does not get an ADR number.
