# ADR-001: Choose Polars over Pandas for ETL Processing

## Status
**Accepted** (March 2024)

## Context

Our ETL pipelines process 30,000+ SKUs daily across multiple source systems (ERP, WMS, a BI reporting server, POS). The existing Pandas-based pipelines had several issues:

1. **Memory pressure**: large DataFrames caused out-of-memory errors on analyst workstations
2. **Single-threaded bottleneck**: GIL prevented parallel processing within transformations
3. **Slow aggregations**: GroupBy operations on large tables were slow
4. **CSV intermediate files**: Pandas I/O forced us to use CSV, adding overhead

**Requirements:**
- Process the daily data within the batch window
- Run on single-machine infrastructure (no cluster budget approved)
- Maintain SQL-like expressiveness for analyst handoff
- First-class Parquet support (our target storage format)

## Decision

Replace Pandas with **Polars** for all new ETL development. Migrate critical existing pipelines.

**Polars advantages for our use case:**
- **Lazy evaluation**: Build query plan, execute once, with lower peak memory
- **Multi-threaded by default**: Utilizes all CPU cores without GIL constraints
- **Native Parquet**: `scan_parquet()` with predicate pushdown
- **Expressive API**: Method chaining feels like SQL
- **Rust backend**: Compiled performance, not interpreted Python

## Benchmarks

Polars was faster and used less memory than Pandas on our workloads. The benchmark is not published here, so no figures are quoted.

## Consequences

### Positive
- Batch processing time dropped (my own measurement at work: about 4 hours to about 30 minutes; not reproducible from this repository)
- No more OOM errors on analyst workstations
- Code is more readable (`.pipe()` chains vs. nested assignments)
- Parquet read/write is seamless

### Negative
- **Learning curve**: Team needed 2 weeks to adjust to new API
- **Ecosystem gaps**: Some niche libraries assume Pandas (workaround: `.to_pandas()`)
- **Stack Overflow scarcity**: Fewer community answers than Pandas (improving rapidly)

### Neutral
- Need to maintain Pandas interop for legacy dashboards (trivial with `.to_pandas()`)

## Alternatives Considered

### 1. Apache Spark (PySpark)
- ✅ Distributed processing capability
- ❌ **Overkill**: Our data fits on one machine; cluster overhead not justified
- ❌ **Latency**: Spark startup time (~30s) exceeds some of our pipeline runtimes
- ❌ **Cost**: Would require EMR/Databricks spend we haven't budgeted

**Verdict:** Keep Spark as migration path if we hit 50GB+ daily volume

### 2. Dask
- ✅ Pandas-compatible API
- ❌ **Slower than Polars** on our workloads
- ❌ **Less mature** lazy evaluation
- ❌ **Debugging complexity** with task graphs

**Verdict:** Polars was the better fit

### 3. Vaex
- ✅ Memory-mapped for huge files
- ❌ **API inconsistencies** between versions
- ❌ **Development has slowed** (fewer releases)
- ❌ **Community smaller** than Polars

**Verdict:** Polars has more momentum and better ergonomics

### 4. Stick with Pandas (optimize)
- ✅ No migration cost
- ❌ **Fundamental GIL limitation** can't be optimized away
- ❌ **Memory model** requires full DataFrame in RAM

**Verdict:** Optimizing Pandas helped a little; switching engine helped much more

## Implementation Notes

```python
# Migration pattern: Pandas → Polars

# BEFORE (Pandas)
df = pd.read_csv("inventory.csv")
result = df.groupby("location")["quantity"].sum()

# AFTER (Polars - Lazy)
result = (
    pl.scan_csv("inventory.csv")
    .group_by("location")
    .agg(pl.col("quantity").sum())
    .collect()
)

# For Parquet (our actual approach)
result = (
    pl.scan_parquet("lakehouse/bronze/inventory/**/*.parquet")
    .filter(pl.col("extract_date") >= "2024-01-01")
    .group_by("location")
    .agg(pl.col("quantity").sum().alias("total_qty"))
    .collect()
)
```

## References

- [Polars Documentation](https://pola.rs/)
- [Polars vs Pandas Benchmark (H2O)](https://h2oai.github.io/db-benchmark/)

## Revision History

| Date | Change | Author |
|------|--------|--------|
| 2024-03-15 | Initial decision | Godson K. |
| 2024-06-01 | Added 3-month retrospective | Godson K. |
| 2026-11-10 | Removed benchmark table: the benchmark is not published here | Godson K. |
