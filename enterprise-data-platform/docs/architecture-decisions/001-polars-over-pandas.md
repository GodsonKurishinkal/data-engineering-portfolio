# ADR-001: Choose Polars over Pandas for ETL Processing

## Status
**Accepted** (March 2024)

## Context

Our ETL pipelines process 10,000+ SKUs daily across multiple source systems (ERP, CRM, WMS, OBI). The existing Pandas-based pipelines had several issues:

1. **Memory pressure**: DataFrames exceeding 500MB caused OOM errors on 16GB workstations
2. **Single-threaded bottleneck**: GIL prevented parallel processing within transformations
3. **Slow aggregations**: GroupBy operations on 2M+ rows took 15+ seconds
4. **CSV intermediate files**: Pandas I/O forced us to use CSV, adding overhead

**Requirements:**
- Process 2-5GB daily data within 30-minute batch window
- Run on single-machine infrastructure (no cluster budget approved)
- Maintain SQL-like expressiveness for analyst handoff
- First-class Parquet support (our target storage format)

## Decision

Replace Pandas with **Polars** for all new ETL development. Migrate critical existing pipelines.

**Polars advantages for our use case:**
- **Lazy evaluation**: Build query plan, execute once (60% memory reduction)
- **Multi-threaded by default**: Utilizes all CPU cores without GIL constraints
- **Native Parquet**: `scan_parquet()` with predicate pushdown
- **Expressive API**: Method chaining feels like SQL
- **Rust backend**: Compiled performance, not interpreted Python

## Benchmarks (Our Data)

Tested on actual production datasets:

| Operation | Pandas | Polars | Improvement |
|-----------|--------|--------|-------------|
| Read CSV (1.2GB) | 52s | 9s | **5.8x faster** |
| GroupBy + Agg | 18s | 1.4s | **12.9x faster** |
| Join (2 tables, 1M rows each) | 11s | 1.1s | **10x faster** |
| Filter + Select | 3.2s | 0.2s | **16x faster** |
| Memory (same operation) | 4.8GB | 1.2GB | **4x less** |

## Consequences

### Positive
- Batch processing now completes in 30 minutes (was 4 hours)
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
- ❌ **Slower than Polars** in our benchmarks (7x slower on groupby)
- ❌ **Less mature** lazy evaluation
- ❌ **Debugging complexity** with task graphs

**Verdict:** Polars outperformed on every metric we tested

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

**Verdict:** Optimizing Pandas yielded 20% improvement; Polars gave 500%+

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
- [Internal benchmark notebook](/notebooks/polars-pandas-benchmark.ipynb)

## Revision History

| Date | Change | Author |
|------|--------|--------|
| 2024-03-15 | Initial decision | Godson K. |
| 2024-06-01 | Added 3-month retrospective metrics | Godson K. |
