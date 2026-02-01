# ADR-003: Adopt Medallion (Bronze/Silver/Gold) Architecture

## Status
**Accepted** (February 2024)

## Context

We needed an architecture for organizing data as it flows from source systems to analytics consumption. The existing state was:

1. **No separation of concerns**: Raw and transformed data mixed in same directories
2. **No data lineage**: Impossible to trace issues back to source
3. **Reprocessing nightmares**: Schema changes required re-extracting from source
4. **Quality issues discovered late**: Bad data reached reports before detection

**Requirements:**
- Clear separation between raw inputs and processed outputs
- Ability to reprocess without re-extracting from source
- Incremental quality improvement at each stage
- Support for both batch and (future) streaming patterns
- Self-documenting structure

## Decision

Adopt the **Medallion Architecture** with three distinct layers:

```
Bronze (Raw)     →    Silver (Validated)    →    Gold (Business-Ready)
   ↓                       ↓                          ↓
Immutable raw        Cleaned, deduplicated      Star schema models
Full audit trail     Schema enforced            Optimized for queries
Source format        Standard types             Dimensional models
```

### Layer Specifications

#### Bronze Layer
- **Purpose**: Raw data landing zone, immutable historical record
- **Contents**: Exact copy of source data with minimal additions
- **Transformations**: None (except adding metadata columns)
- **Schema**: Inferred from source (flexible)
- **Retention**: Indefinite (archive to cold storage after 90 days)
- **Path Pattern**: `/lakehouse/bronze/{source}/{table}/extract_date={YYYY-MM-DD}/`

**Added Metadata:**
```python
_source_system: str       # e.g., "erp", "wms"
_extract_timestamp: datetime
_extract_date: str        # Partition key
_row_hash: str            # MD5 for deduplication
_file_name: str           # Source file if applicable
```

#### Silver Layer
- **Purpose**: Validated, cleaned, standardized data
- **Contents**: Business entities with enforced schema
- **Transformations**: Type casting, null handling, deduplication, validation
- **Schema**: Strictly enforced (fail on mismatch)
- **Retention**: Latest + 30 days history
- **Path Pattern**: `/lakehouse/silver/{domain}/{entity}/`

**Quality Gates:**
1. Schema validation (required columns, data types)
2. Null checks (configurable by column)
3. Referential integrity (foreign key validation)
4. Business rules (range checks, pattern matching)

#### Gold Layer
- **Purpose**: Business-ready analytical models
- **Contents**: Dimensional models (facts + dimensions)
- **Transformations**: Joins, aggregations, surrogate keys, SCD handling
- **Schema**: Star schema (Kimball methodology)
- **Retention**: Latest + 90 days history
- **Path Pattern**: `/lakehouse/gold/{facts|dimensions}/{table}/`

## Consequences

### Positive
- **Debuggability**: Can trace any Gold issue back to Bronze
- **Reprocessing**: Schema change? Reprocess from Bronze, not source
- **Quality visibility**: Know exactly where quality issues originate
- **Flexibility**: Bronze schema can evolve without breaking downstream
- **Self-service**: Analysts can query any layer appropriate to their need

### Negative
- **Storage overhead**: Data exists in 3 copies (mitigated by compression)
- **Complexity**: More pipelines to maintain than single-hop ETL
- **Latency**: 3 stages vs 1 adds processing time

### Trade-offs We Accept
- ~3x storage cost for auditability (storage is cheap, data loss is expensive)
- Additional pipeline complexity for reduced debugging time
- Slightly higher latency for much higher reliability

## Alternatives Considered

### 1. Lambda Architecture (Batch + Streaming)
- ✅ Real-time + batch capabilities
- ❌ **Complexity**: Two codepaths for same logic
- ❌ **Consistency**: Hard to keep batch and stream in sync
- ❌ **Overkill**: We don't have real-time requirements yet

**Verdict:** Keep streaming as future enhancement, not initial architecture

### 2. Single-Hop ETL (Direct to Star Schema)
- ✅ Simpler pipeline
- ✅ Less storage
- ❌ **No recovery**: Source system issue = re-extract everything
- ❌ **No debugging**: Can't see what data looked like before transformation
- ❌ **Tight coupling**: Schema changes break everything

**Verdict:** Short-term savings, long-term pain. Rejected.

### 3. Data Vault 2.0
- ✅ Excellent for enterprise-scale MDM
- ✅ Full historical tracking
- ❌ **Complexity**: Hubs, links, satellites add modeling overhead
- ❌ **Query complexity**: Analysts need joins for simple queries
- ❌ **Overkill**: We're not doing MDM across hundreds of sources

**Verdict:** Right for enterprise MDM, wrong for our use case

### 4. Lakehouse (Delta Lake / Iceberg)
- ✅ ACID transactions on data lake
- ✅ Time travel built-in
- ❌ **Infrastructure**: Requires Spark/distributed compute
- ❌ **Cost**: Cluster costs exceed our budget
- ❌ **Overkill**: Our volume doesn't need distributed processing

**Verdict:** Medallion pattern without Delta/Iceberg dependencies. Can add later.

## Implementation Notes

### Directory Structure

```
/lakehouse/
├── bronze/
│   ├── erp/
│   │   ├── inventory/
│   │   │   ├── extract_date=2024-01-15/
│   │   │   │   └── data.parquet
│   │   │   └── extract_date=2024-01-16/
│   │   │       └── data.parquet
│   │   └── sales/
│   │       └── ...
│   ├── wms/
│   └── crm/
├── silver/
│   ├── inventory/
│   │   ├── current_stock/
│   │   └── stock_movements/
│   └── sales/
│       └── transactions/
└── gold/
    ├── facts/
    │   ├── fact_inventory/
    │   ├── fact_sales/
    │   └── fact_orders/
    └── dimensions/
        ├── dim_product/
        ├── dim_location/
        └── dim_time/
```

### Layer Transition Validation

```python
# Bronze → Silver: Schema validation
def validate_bronze_to_silver(bronze_df: pl.DataFrame, silver_schema: dict) -> bool:
    for col, expected_type in silver_schema.items():
        if col not in bronze_df.columns:
            raise SchemaViolation(f"Missing required column: {col}")
        # Additional type checking...
    return True

# Silver → Gold: Referential integrity
def validate_silver_to_gold(silver_df: pl.DataFrame, dim_tables: dict) -> bool:
    for fk_col, dim_table in dim_tables.items():
        orphans = silver_df.filter(
            ~pl.col(fk_col).is_in(dim_table["key_col"])
        )
        if len(orphans) > 0:
            raise IntegrityViolation(f"Orphan records for {fk_col}")
    return True
```

## References

- [Databricks: Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)
- [Delta Lake: Multi-Hop Architecture](https://docs.delta.io/latest/delta-medallion.html)
- [Kimball Group: Dimensional Modeling](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/)

## Revision History

| Date | Change | Author |
|------|--------|--------|
| 2024-02-15 | Initial decision | Godson K. |
| 2024-05-01 | Added storage retention policies | Godson K. |
