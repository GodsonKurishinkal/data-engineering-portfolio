# ADR-007: Windows Task Scheduler for Orchestration

## Status
**Accepted** (March 2024)

## Context

50+ pipelines need to run on a schedule, in a defined order, with failures
visible to a human before the business notices.

**Constraints:**
- Single Windows host; no cluster, no cloud budget approved
- No dedicated platform engineer to operate an orchestrator
- Daily batch cadence, a handful of hourly jobs, no sub-hourly requirement
- Dependencies are shallow: extract → transform → build Gold

An early attempt started with **Airflow plus Spark** and was abandoned after two
weeks: the orchestration stack required more operational attention than the
pipelines it was orchestrating.

## Decision

Use **Windows Task Scheduler** — already present, already paid for, already
monitored by IT — with structured logging to a pipeline log table and Teams
alerts on failure.

Observability is supplied by the platform rather than the scheduler: every run
writes a record (pipeline, status, row count, duration, error), and a small
Streamlit board reads that table to give the DAG-style view Task Scheduler
does not provide.

## Consequences

**Easier:**
- Zero additional infrastructure to run, patch, or explain to IT.
- Scheduling is inspectable by anyone on the ops team without new tooling.
- The whole platform survives a host rebuild with no orchestrator state to restore.

**Harder:**
- **No DAG visualisation** out of the box — solved with the Streamlit status board.
- **No native backfill semantics** — backfills are an explicit partition
  overwrite ([ADR-004](004-parquet-hive-partitioning.md)).
- **Weak dependency expression** — ordering is encoded as start times plus
  guard checks, not as a true dependency graph. This is the real cost, and it is
  the constraint that will eventually force a migration.
- **Windows-bound** — the orchestration layer is not portable.

## Alternatives Considered

| Alternative | Why rejected |
|-------------|--------------|
| **Apache Airflow** | Tried and abandoned. Heavier to operate than the platform itself at this scale, with no dedicated owner. |
| **Prefect / Dagster** | Better ergonomics than Airflow, but still a service to run and a dependency to keep current, for a shallow daily DAG. |
| **Cron on a Linux VM** | No Linux host available; would have added a machine to justify. |
| **Orchestrator-in-Python** | Writing a scheduler is not the problem worth solving here. |

## Revisit If

Dependencies stop being shallow, the pipeline count crosses roughly 100, or the
platform moves to a cloud runtime — at which point managed orchestration comes
with the platform and this ADR is superseded.
