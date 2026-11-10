# ADR-002: Use DuckDB for Analytical Queries

## Status
**Accepted**

## Context

Our Gold layer contains 10+ fact tables and 6+ dimension tables in star schema format. Analysts need to run complex analytical queries for:

1. **Ad-hoc analysis**: Exploratory queries during investigations
2. **Dashboard backends**: Streamlit apps querying aggregated data
3. **Report generation**: Scheduled reports with complex joins
4. **Data validation**: Quality checks across dimension/fact relationships

**Previous state:**
- Analysts exported Parquet to Excel, limiting analysis to 1M rows
- Complex queries required writing to staging SQL Server tables (slow)
- No way to query across multiple Parquet files efficiently

**Requirements:**
- Query Parquet files directly without ETL to database
- Answer analytical queries over the Gold layer interactively
- Support standard SQL (analysts know SQL, not Python)
- Embedded solution (no server infrastructure)
- Low operational overhead

## Decision

Use **DuckDB** as the analytical query engine for Gold layer consumption.

**Key capabilities:**
- **Zero-copy Parquet reads**: Queries Parquet without loading into memory
- **Vectorized execution**: Column-at-a-time processing
- **Embedded**: Single file, no server process, starts instantly
- **Full SQL support**: Window functions, CTEs, complex joins
- **Python integration**: `duckdb.sql()` returns Polars/Pandas DataFrames

## Benchmarks

DuckDB was faster than the staging SQL Server database on our Gold-layer queries. The benchmark is not published here, so no figures are quoted.

## Consequences

### Positive
- **Analysts unblocked**: Can query entire Gold layer from laptops
- **No infrastructure**: Zero server costs, zero maintenance
- **Portability**: Database file can be shared/copied

### Negative
- **Concurrent writes**: Limited support (fine for read-heavy analytics)
- **No user management**: Single-user model (security via file permissions)
- **Learning curve**: Team needed to understand embedded DB paradigm

### Neutral
- Still maintain SQL Server for OLTP workloads (different use case)
- DuckDB file regenerated daily during Gold layer build

## Alternatives Considered

### 1. PostgreSQL / SQL Server (Copy Data)
- ✅ Standard RDBMS, familiar tooling
- ❌ **ETL overhead**: Must copy Parquet → DB tables daily
- ❌ **Storage duplication**: Pay for data twice
- ❌ **Latency**: Write time delays query availability
- ❌ **Cost**: Server licenses/hosting

**Verdict:** Too much operational overhead for analytics-only workload

### 2. Apache Spark SQL
- ✅ Handles massive datasets
- ❌ **Startup latency**: 30+ seconds before first query
- ❌ **Cluster requirement**: Need infrastructure
- ❌ **Overkill**: Our Gold layer fits on one machine

**Verdict:** Right tool for petabyte scale, not our scale

### 3. Presto / Trino
- ✅ Federated query engine
- ❌ **Server requirement**: Coordinator + workers
- ❌ **Complexity**: Kubernetes or dedicated infra
- ❌ **Overkill**: Single data source (Parquet files)

**Verdict:** Federated querying not needed; we control all sources

### 4. ClickHouse
- ✅ Blazing fast analytics
- ❌ **Server process**: Must run and maintain
- ❌ **Data ingestion**: Need to load data into CH format
- ❌ **Operational overhead**: Backups, monitoring, upgrades

**Verdict:** Great for production analytics services, heavy for local dev

### 5. Direct Parquet with Polars
- ✅ Already using Polars
- ❌ **SQL familiarity**: Analysts prefer SQL over DataFrame API
- ❌ **Complex joins**: Star schema joins cleaner in SQL

**Verdict:** Polars for ETL, DuckDB for analytics—different tools for different jobs

## Implementation Pattern

```python
# Pattern: DuckDB on Gold Layer Parquet

import duckdb

# Create connection to Gold layer
conn = duckdb.connect("lakehouse/gold/analytics.duckdb")

# Register Parquet files as views
conn.execute("""
    CREATE OR REPLACE VIEW fact_inventory AS 
    SELECT * FROM read_parquet('lakehouse/gold/facts/fact_inventory/**/*.parquet')
""")

conn.execute("""
    CREATE OR REPLACE VIEW dim_product AS 
    SELECT * FROM read_parquet('lakehouse/gold/dimensions/dim_product/*.parquet')
""")

# Analyst-friendly SQL query
result = conn.execute("""
    SELECT 
        p.category,
        p.brand,
        SUM(i.quantity_on_hand) as total_stock,
        SUM(i.quantity_on_hand * i.unit_cost) as stock_value
    FROM fact_inventory i
    JOIN dim_product p ON i.product_key = p.product_key
    WHERE i.date_key >= 20240101
    GROUP BY p.category, p.brand
    ORDER BY stock_value DESC
    LIMIT 100
""").fetchdf()  # Returns Pandas DataFrame

# Or return Polars
result_polars = conn.execute("...").pl()
```

## Cost Analysis

| Item | SQL Server (Alternative) | DuckDB (Chosen) |
|------|-------------------------|-----------------|
| Server license | Recurring licence fee | None |
| Storage | Separate database storage | None (uses existing Parquet) |
| Maintenance | A server to patch and back up | None |

**Net effect:** no licence or separate storage cost, plus analyst productivity gains

## References

- [DuckDB Documentation](https://duckdb.org/docs/)
- [DuckDB vs Pandas Benchmark](https://duckdb.org/2021/05/14/sql-on-pandas.html)
- [Parquet Integration Guide](https://duckdb.org/docs/data/parquet/overview)
