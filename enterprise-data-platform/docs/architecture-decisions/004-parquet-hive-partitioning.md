# ADR-004: Use Parquet with Hive Partitioning (not Delta Lake)

## Status
**Accepted** (February 2024)

## Context

The medallion layers need a physical storage format. Bronze must be an immutable
historical record we can reprocess from; Silver and Gold must be fast to scan
from both Polars and DuckDB.

**Constraints:**

1. **No Spark runtime.** Delta Lake's reference implementation assumes Spark, and
   no cluster budget was approved ([ADR-001](001-polars-over-pandas.md)).
2. **Single-machine processing** on a Windows host, 32 GB RAM.
3. **Two readers.** Polars for transforms, DuckDB for ad-hoc analytics
   ([ADR-002](002-duckdb-for-analytics.md)).
4. **Daily batch cadence** — no streaming or concurrent-writer requirement.

## Decision

Store every layer as **Parquet, Hive-partitioned by `extract_date`**.

```
/lakehouse/bronze/{source}/{table}/extract_date=YYYY-MM-DD/part-0.parquet
```

**Why Parquet + Hive partitioning:**

- **Columnar and compressed** — roughly 80% smaller than the equivalent CSV.
- **Predicate pushdown** — `scan_parquet()` and DuckDB both prune partitions from
  the path before reading a byte.
- **Zero lock-in** — plain files. Any engine that reads Parquet reads this
  lakehouse, today or in five years.
- **Partition-level reprocessing** — a bad extract is one directory to overwrite.

## Consequences

**Easier:**
- Backfills are an explicit partition overwrite, easy to reason about and audit.
- Both readers work against the same files with no export step.
- Storage costs stay trivial; quarantined rows are near-free to keep.

**Harder:**
- **No time travel.** There is no version history beyond the partitions themselves.
- **No ACID guarantees.** A crashed write can leave a partial partition; we
  mitigate with write-to-temp-then-rename in `ParquetWriter`.
- **No schema evolution enforcement** at the storage layer — that responsibility
  moved up into `SchemaEnforcer`.
- **Small-file risk** on high-frequency sources; compaction is manual.

## Alternatives Considered

| Alternative | Why rejected |
|-------------|--------------|
| **Delta Lake** | Requires a Spark runtime we do not have. The ACID and time-travel guarantees are genuinely better, but not at the cost of the whole platform. |
| **Apache Iceberg** | Same catalogue and engine overhead; the Python-only story was immature at the time of the decision. |
| **SQL Server tables** | Row-based storage, licence cost per volume, and no cheap immutable Bronze. |
| **CSV** | 5× the storage, no types, no pushdown. |

## Migration Path

If concurrent writers, streaming ingestion, or true time travel become
requirements, migrate to Delta or Iceberg. Because the physical layout is already
partitioned Parquet, that migration is a table-format wrapper over existing
files rather than a re-extraction.
