# ADR-005: Configuration-Driven Pipeline Design

## Status
**Accepted** (March 2024)

## Context

The first pipelines were written as standalone scripts. By the fifth source they
had visibly diverged: each script had its own logging, its own retry logic, its
own idea of what "incremental" meant, and its own copy of the connection
handling.

**Problems this created:**

1. **Behavioural drift** — two pipelines doing the same thing differently.
2. **Change cost** — adding a quality rule meant editing N scripts.
3. **Unreviewable diffs** — a schedule change looked like a code change.
4. **Onboarding cost** — reading a pipeline meant reading all of it.

**Requirements:**
- New source live in hours, not days
- Load mode, schema and quality rules visible without reading Python
- Consistent logging, retry and alerting across every pipeline
- Changes reviewable as text

## Decision

Every pipeline is a **YAML configuration plus an Abstract Base Class**. The
engine reads the config and runs the pipeline; the subclass supplies only what is
genuinely source-specific.

```yaml
pipeline:
  name: "inventory_daily"
  source:
    type: "oracle_obi"
    query: "SELECT * FROM vw_inventory_snapshot WHERE snapshot_date = :d"
  destination:
    layer: "bronze"
    format: "parquet"
    partition_by: ["extract_date"]
  quality:
    required: ["sku_id", "location_id", "on_hand_qty"]
    rules:
      - "on_hand_qty >= 0"
      - "extract_date <= today"
  load_mode: "INCREMENTAL"
  schedule: "0 6 * * *"
```

The ABC trio — `BaseExtractor`, `BaseTransformer`, and the loader pair — owns
retries, structured logging, extraction metadata and failure alerting. A concrete
extractor implements `extract()` and nothing else.

## Consequences

**Easier:**
- 50+ pipelines behave identically in every respect that isn't the query.
- Quality rules and load modes are declarative and diffable.
- A behavioural change ships as a YAML diff a reviewer can read in 10 seconds.
- Cross-cutting improvements (retry backoff, screenshot-on-failure) land once.

**Harder:**
- **Steeper ramp** for a new contributor: you must learn the framework before you
  can add a pipeline. Mitigated with templates and reviewed PRs.
- **Config sprawl risk** — YAML that grows conditionals is a signal the logic
  belongs in code. We hold the line at declarative-only.
- **Debugging indirection** — a stack trace points at the engine, not the
  pipeline. Mitigated by putting the pipeline name in every log record.

## Alternatives Considered

| Alternative | Why rejected |
|-------------|--------------|
| **One script per pipeline** | The status quo we were escaping. Does not scale past ~5 sources. |
| **dbt** | Excellent for the transformation layer, but does not cover extraction, and our sources include RPA ([ADR-006](006-rpa-for-legacy-systems.md)). Still the right answer for the semantic layer above Gold. |
| **Airflow with a DAG factory** | Solves orchestration, not consistency, and brings a runtime heavier than the platform ([ADR-007](007-task-scheduler-orchestration.md)). |
| **A code-generation step** | Generated pipelines drift from their templates the moment anyone edits the output. |
