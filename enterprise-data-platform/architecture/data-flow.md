# Data Flow

> How data is meant to move from source systems to analytics.

This is a design description. The component code is in `etl_framework/` and
`data_quality/`. The pipelines, schedules and alerting described here are not in
this repository, so no run times, row counts or success rates are quoted.

## Source systems

| System | Type | Extraction method |
|--------|------|-------------------|
| ERP | Relational database | `DatabaseExtractor` (ODBC) |
| WMS | Legacy system with no API | `RPAExtractor` (Selenium) |
| POS | REST API | `APIExtractor` |
| BI reporting server | Report exports | File export, then load |

## Flow

```
SOURCE → EXTRACT → BRONZE → VALIDATE → SILVER → MODEL → GOLD → CONSUME
```

| Step | What happens | Component |
|------|--------------|-----------|
| Extract | Pull from the source as it is | `etl_framework/extractors/` |
| Bronze | Land raw data as date-partitioned Parquet with metadata columns | `etl_framework/loaders/parquet_writer.py` |
| Validate | Schema check, business rules, statistical outliers | `etl_framework/transformers/schema_enforcer.py`, `data_quality/` |
| Silver | Clean, standardise, deduplicate | `etl_framework/transformers/data_cleaner.py` |
| Model | Join to dimensions, add surrogate keys | Not in this repository |
| Gold | Fact and dimension tables | Not in this repository |
| Consume | Power BI, Streamlit, ad-hoc SQL through DuckDB | `etl_framework/loaders/duckdb_loader.py` for the DuckDB part |

## Example: inventory

1. **Extract** the day's inventory snapshot from the ERP with `DatabaseExtractor`.
2. **Land in Bronze** at `/lakehouse/bronze/erp/inventory/extract_date=YYYY-MM-DD/`,
   adding `_source_system`, `_extract_timestamp` and `_row_hash`.
3. **Validate**:
   - schema: expected columns and types
   - nulls: item code and location are required
   - ranges: quantity is not negative, unit cost is positive
   - outliers: values far from the mean, and sudden changes from the previous day
4. **Silver**: standardise item and location codes, compute available quantity,
   keep the latest row per item and location.
5. **Gold**: join to the product, location and time dimensions and write
   `fact_inventory`.

## Example: WMS extraction by browser automation

The WMS has no API, so a Selenium bot does what a person would do:

1. Log in with credentials read from the environment.
2. Open the stock movement report.
3. Set the date range and filters.
4. Export to Excel and wait for the download.
5. Check the file: not empty, required columns present.
6. Read it with Polars, add metadata columns and write to Bronze.

On failure the bot takes a screenshot, retries, and raises if the retries run out.
The bot does no parsing or cleaning; that happens downstream.

## Ordering

Bronze extractions have no dependencies on each other. Silver runs after all
Bronze loads are complete, and Gold runs after Silver. Dashboards refresh after Gold.

```
Bronze (ERP, WMS, POS, BI reports)  →  Silver  →  Gold  →  Dashboards, alerts
```

## Retry settings

```python
RETRY_CONFIG = {
    "database_extractor": {
        "max_retries": 3,
        "backoff_factor": 2,
        "retry_on": ["ConnectionError", "TimeoutError"],
    },
    "api_extractor": {
        "max_retries": 5,
        "backoff_factor": 1,
        "retry_on": ["RateLimitError", "ServiceUnavailable"],
    },
    "rpa_extractor": {
        "max_retries": 2,
        "backoff_factor": 30,
        "retry_on": ["ElementNotFound", "PageLoadTimeout"],
    },
}
```

These are illustrative values, not constants read by the code.

## Related documentation

- [Medallion architecture](medallion-architecture.md): layer specifications
- [Architecture decisions](../docs/architecture-decisions/README.md)
