# Medallion Architecture Specification

> Bronze → Silver → Gold data layers for progressive data refinement

## Overview

The Medallion architecture provides a structured approach to data management, organizing data into three distinct layers based on quality and readiness for consumption.

---

## 🥉 Bronze Layer

### Purpose
Raw data landing zone. Captures data exactly as received from source systems.

### Characteristics
| Attribute | Value |
|-----------|-------|
| **Quality** | Raw, unprocessed |
| **Schema** | Source-native (may vary) |
| **Retention** | Indefinite (immutable) |
| **Partitioning** | By `extract_date` |
| **Format** | Parquet |

### Directory Structure
```
/lakehouse/bronze/
├── erp/
│   ├── inventory/
│   │   ├── extract_date=2026-01-01/
│   │   │   └── data.parquet
│   │   └── extract_date=2026-01-02/
│   │       └── data.parquet
│   ├── sales/
│   └── products/
├── wms/
│   ├── stock_movements/
│   └── locations/
├── crm/
│   └── customers/
└── obi/
    └── reports/
```

### Metadata Columns
Every Bronze table includes:
```python
{
    "_source_system": str,      # e.g., "erp", "wms"
    "_extract_timestamp": datetime,
    "_extract_date": date,      # Partition key
    "_file_name": str,          # Source file reference
    "_row_hash": str,           # MD5 hash for dedup
}
```

### Key Principles
1. **Never transform** - Data stays exactly as extracted
2. **Never delete** - Bronze is the audit trail
3. **Always partition by date** - Enables efficient reprocessing
4. **Capture metadata** - Track lineage from day one

---

## 🥈 Silver Layer

### Purpose
Cleaned, validated, and standardized data. Single source of truth for each entity.

### Characteristics
| Attribute | Value |
|-----------|-------|
| **Quality** | Cleaned, validated |
| **Schema** | Enforced, consistent |
| **Retention** | Rolling (configurable) |
| **Partitioning** | By entity-specific keys |
| **Format** | Parquet |

### Directory Structure
```
/lakehouse/silver/
├── inventory/
│   ├── current_stock/
│   └── stock_movements/
├── sales/
│   ├── transactions/
│   └── returns/
├── products/
│   ├── master/
│   └── hierarchy/
├── customers/
│   └── profiles/
└── suppliers/
    └── master/
```

### Transformations Applied
```python
# 1. Schema Enforcement
df = df.cast({
    "sku_code": pl.Utf8,
    "quantity": pl.Int64,
    "price": pl.Float64,
    "transaction_date": pl.Date,
})

# 2. Null Handling
df = df.with_columns([
    pl.col("quantity").fill_null(0),
    pl.col("price").fill_null(pl.col("price").median()),
])

# 3. Deduplication
df = df.unique(subset=["sku_code", "location_id", "date"], keep="last")

# 4. Standardization
df = df.with_columns([
    pl.col("sku_code").str.to_uppercase().str.strip_chars(),
    pl.col("location_code").str.replace_all(r"[^A-Z0-9]", ""),
])
```

### Data Quality Checks
```python
SILVER_VALIDATION_RULES = {
    "inventory.current_stock": {
        "not_null": ["sku_code", "location_id", "quantity"],
        "positive": ["quantity", "unit_cost"],
        "unique": ["sku_code", "location_id"],
        "referential": {
            "sku_code": "products.master.sku_code",
            "location_id": "locations.master.location_id",
        }
    }
}
```

---

## 🥇 Gold Layer

### Purpose
Business-ready dimensional models. Optimized for analytics and reporting.

### Characteristics
| Attribute | Value |
|-----------|-------|
| **Quality** | Analytics-ready |
| **Schema** | Star schema |
| **Retention** | Historical (dimension SCD) |
| **Partitioning** | By date dimensions |
| **Format** | Parquet |

### Directory Structure
```
/lakehouse/gold/
├── facts/
│   ├── fact_sales/
│   ├── fact_inventory/
│   ├── fact_orders/
│   ├── fact_replenishment/
│   └── fact_forecast/
└── dimensions/
    ├── dim_product/
    ├── dim_location/
    ├── dim_time/
    ├── dim_supplier/
    ├── dim_customer/
    └── dim_channel/
```

### Star Schema Design

```
                    ┌─────────────┐
                    │ dim_product │
                    └──────┬──────┘
                           │
┌─────────────┐     ┌──────┴──────┐     ┌──────────────┐
│dim_location │─────│ fact_sales  │─────│ dim_customer │
└─────────────┘     └──────┬──────┘     └──────────────┘
                           │
                    ┌──────┴──────┐
                    │  dim_time   │
                    └─────────────┘
```

### Fact Table: fact_inventory

```sql
CREATE TABLE fact_inventory (
    -- Keys
    inventory_key       BIGINT PRIMARY KEY,
    product_key         BIGINT REFERENCES dim_product,
    location_key        BIGINT REFERENCES dim_location,
    date_key            INT REFERENCES dim_time,
    
    -- Measures
    quantity_on_hand    INT,
    quantity_reserved   INT,
    quantity_in_transit INT,
    quantity_available  INT,  -- Calculated: on_hand - reserved
    
    -- Costs
    unit_cost           DECIMAL(10,2),
    total_value         DECIMAL(15,2),
    
    -- Metadata
    last_updated        TIMESTAMP
);
```

### Dimension Table: dim_product (SCD Type 2)

```sql
CREATE TABLE dim_product (
    -- Surrogate Key
    product_key         BIGINT PRIMARY KEY,
    
    -- Natural Key
    sku_code            VARCHAR(50),
    
    -- Attributes
    product_name        VARCHAR(200),
    category_l1         VARCHAR(100),
    category_l2         VARCHAR(100),
    category_l3         VARCHAR(100),
    brand               VARCHAR(100),
    supplier_code       VARCHAR(50),
    unit_of_measure     VARCHAR(20),
    
    -- ABC-XYZ Classification
    abc_class           CHAR(1),      -- A, B, C
    xyz_class           CHAR(1),      -- X, Y, Z
    
    -- SCD Type 2 Columns
    effective_from      DATE,
    effective_to        DATE,
    is_current          BOOLEAN
);
```

---

## Data Flow Example

### Inventory Pipeline

```
ERP.INVENTORY_SNAPSHOT
        │
        ▼ [Extract]
┌─────────────────────────────────────────────┐
│ BRONZE: erp/inventory/extract_date=2026-01-01 │
│                                              │
│ • Raw columns as-is                         │
│ • Added: _source_system, _extract_timestamp │
│ • Partitioned by extract_date               │
└─────────────────────────────────────────────┘
        │
        ▼ [Transform: Clean + Validate]
┌─────────────────────────────────────────────┐
│ SILVER: inventory/current_stock             │
│                                              │
│ • Schema enforced                           │
│ • Nulls handled                             │
│ • Duplicates removed                        │
│ • Anomalies flagged                         │
└─────────────────────────────────────────────┘
        │
        ▼ [Transform: Model]
┌─────────────────────────────────────────────┐
│ GOLD: facts/fact_inventory                  │
│                                              │
│ • Joined with dimensions                    │
│ • Surrogate keys assigned                   │
│ • Measures calculated                       │
│ • Ready for analytics                       │
└─────────────────────────────────────────────┘
```

---

## Reprocessing Strategy

### Scenario: Fix data quality issue discovered on Jan 5

```python
# 1. Bronze layer is untouched (immutable)
# 2. Reprocess Silver from Bronze for affected dates

affected_dates = ["2026-01-01", "2026-01-02", "2026-01-03", "2026-01-04"]

for date in affected_dates:
    # Read from Bronze
    bronze_df = read_bronze("erp/inventory", extract_date=date)
    
    # Apply FIXED transformation logic
    silver_df = transform_to_silver(bronze_df, version="v2")
    
    # Overwrite Silver partition
    write_silver("inventory/current_stock", silver_df, mode="overwrite")

# 3. Rebuild Gold from corrected Silver
rebuild_gold_fact("fact_inventory", from_date="2026-01-01")
```

---

## Storage Estimates

| Layer | Daily Ingest | Retention | Total Storage |
|-------|--------------|-----------|---------------|
| Bronze | 5 GB | Forever | ~1.8 TB/year |
| Silver | 2 GB | 2 years | ~1.5 TB |
| Gold | 500 MB | 5 years | ~900 GB |

---

## Related Documentation

- [Data Flow](data-flow.md) - End-to-end pipeline documentation
- [Performance Benchmarks](../docs/performance-benchmarks.md) - Processing times
- [Deployment Guide](../docs/deployment-guide.md) - Production setup
