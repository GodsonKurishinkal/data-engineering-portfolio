# Data Flow Specification

> End-to-end pipeline documentation from source systems to analytics

## Source Systems

### Overview

```
┌──────────────────────────────────────────────────────────────────┐
│                       SOURCE SYSTEMS                              │
├───────────────┬───────────────┬───────────────┬─────────────────┤
│     ERP       │     WMS       │     CRM       │     OBI         │
│   (Oracle)    │  (Legacy WMS) │  (REST API)   │  (BI Reports)   │
├───────────────┼───────────────┼───────────────┼─────────────────┤
│ • Inventory   │ • Stock Moves │ • Customers   │ • Daily Reports │
│ • Sales       │ • Locations   │ • Orders      │ • KPI Summaries │
│ • Products    │ • Receipts    │ • Feedback    │ • Forecasts     │
│ • Suppliers   │ • Shipments   │               │                 │
└───────────────┴───────────────┴───────────────┴─────────────────┘
```

### Connection Details

| System | Type | Extraction Method | Frequency | SLA |
|--------|------|-------------------|-----------|-----|
| ERP | Oracle DB | JDBC (pyodbc) | Daily 6:00 AM | 30 min |
| WMS | Legacy System | RPA (Selenium) | Daily 5:00 AM | 60 min |
| CRM | REST API | HTTP Client | Hourly | 5 min |
| OBI | BI Platform | SFTP Export | Daily 7:00 AM | 15 min |

---

## Pipeline Architecture

### High-Level Flow

```
SOURCE → EXTRACT → BRONZE → VALIDATE → SILVER → MODEL → GOLD → CONSUME
   │         │        │         │          │        │       │       │
   │         │        │         │          │        │       │       └─→ Dashboards
   │         │        │         │          │        │       │           Power BI
   │         │        │         │          │        │       │           Streamlit
   │         │        │         │          │        │       │
   │         │        │         │          │        │       └─→ Fact Tables
   │         │        │         │          │        │           Dimension Tables
   │         │        │         │          │        │
   │         │        │         │          │        └─→ Joins & Aggregations
   │         │        │         │          │            Surrogate Keys
   │         │        │         │          │
   │         │        │         │          └─→ Cleaned Data
   │         │        │         │              Schema Enforced
   │         │        │         │
   │         │        │         └─→ Quality Checks
   │         │        │             Anomaly Detection
   │         │        │
   │         │        └─→ Raw Parquet Files
   │         │            Partitioned by Date
   │         │
   │         └─→ Extractors (DB, API, RPA)
   │
   └─→ ERP, WMS, CRM, OBI
```

---

## Detailed Pipeline Flows

### 1. Inventory Pipeline

```
┌─────────────────────────────────────────────────────────────────────┐
│                     INVENTORY PIPELINE                               │
│                     Schedule: Daily 6:00 AM                          │
└─────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────┐
│ STEP 1: EXTRACTION                                                   │
│                                                                      │
│ Source: ERP.INV_SNAPSHOT                                            │
│ Method: DatabaseExtractor                                            │
│ Query:                                                               │
│   SELECT item_code, location, qty_on_hand, qty_reserved,            │
│          unit_cost, last_count_date                                  │
│   FROM inventory_snapshot                                            │
│   WHERE snapshot_date = CURRENT_DATE                                 │
│                                                                      │
│ Records: ~500,000 rows                                              │
│ Duration: ~2 minutes                                                 │
└─────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────┐
│ STEP 2: BRONZE LANDING                                               │
│                                                                      │
│ Path: /lakehouse/bronze/erp/inventory/extract_date=2026-01-15/      │
│ Format: Parquet (snappy compression)                                 │
│ Size: ~45 MB                                                         │
│                                                                      │
│ Added Metadata:                                                      │
│   • _source_system = "erp"                                          │
│   • _extract_timestamp = "2026-01-15T06:02:31Z"                     │
│   • _row_hash = MD5(item_code + location)                           │
└─────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────┐
│ STEP 3: VALIDATION                                                   │
│                                                                      │
│ Rules Applied:                                                       │
│   ✓ Schema validation (expected columns, data types)                │
│   ✓ Null checks (item_code, location required)                      │
│   ✓ Range validation (qty >= 0, unit_cost > 0)                      │
│   ✓ Referential integrity (item_code exists in products)            │
│                                                                      │
│ Anomaly Detection:                                                   │
│   ✓ Statistical outliers (qty > 3σ from mean)                       │
│   ✓ Sudden spikes (>200% change from yesterday)                     │
│   ✓ Missing expected records                                         │
│                                                                      │
│ Output: 12 records flagged, 0 rejected                              │
└─────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────┐
│ STEP 4: SILVER TRANSFORMATION                                        │
│                                                                      │
│ Transformations:                                                     │
│   • Standardize item_code (uppercase, trim)                         │
│   • Convert location codes to standard format                       │
│   • Calculate available_qty = on_hand - reserved                    │
│   • Handle nulls (fill with 0 or median)                            │
│   • Deduplicate (keep latest per item+location)                     │
│                                                                      │
│ Path: /lakehouse/silver/inventory/current_stock/                    │
│ Format: Parquet                                                      │
│ Duration: ~45 seconds                                                │
└─────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────┐
│ STEP 5: GOLD MODELING                                                │
│                                                                      │
│ Operations:                                                          │
│   • Join with dim_product (get product_key, attributes)             │
│   • Join with dim_location (get location_key, region)               │
│   • Join with dim_time (get date_key)                               │
│   • Calculate total_value = qty * unit_cost                         │
│   • Generate surrogate inventory_key                                 │
│                                                                      │
│ Path: /lakehouse/gold/facts/fact_inventory/                         │
│ Indexes: product_key, location_key, date_key                        │
│ Duration: ~30 seconds                                                │
└─────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────┐
│ STEP 6: CONSUMPTION                                                  │
│                                                                      │
│ Downstream Systems:                                                  │
│   • Power BI refresh triggered                                       │
│   • Replenishment engine notified                                    │
│   • Anomaly alerts sent (12 items flagged)                          │
│                                                                      │
│ Total Pipeline Duration: 4 minutes 12 seconds                       │
└─────────────────────────────────────────────────────────────────────┘
```

---

### 2. Sales Pipeline

```
SOURCE: ERP.SALES_TRANSACTIONS
SCHEDULE: Every 2 hours (6 AM - 10 PM)

Flow:
┌──────────┐    ┌─────────┐    ┌──────────┐    ┌──────────┐    ┌───────────┐
│  ERP     │───▶│ Bronze  │───▶│ Validate │───▶│  Silver  │───▶│   Gold    │
│ (Oracle) │    │ /sales/ │    │ (checks) │    │ /sales/  │    │fact_sales │
└──────────┘    └─────────┘    └──────────┘    └──────────┘    └───────────┘
     │               │               │               │               │
     │               │               │               │               │
 ~50K rows       Parquet         Schema          Cleaned       Star Schema
 per batch       append          + Range         + Dedup       + Dimensions
```

### 3. WMS Pipeline (RPA-based)

```
SOURCE: Legacy WMS (No API)
SCHEDULE: Daily 5:00 AM
METHOD: Selenium RPA Bot

Flow:
┌──────────────────────────────────────────────────────────────────────┐
│                        RPA EXTRACTION FLOW                            │
└──────────────────────────────────────────────────────────────────────┘

[1] AUTHENTICATE
    └─▶ Navigate to WMS login page
    └─▶ Enter credentials (from vault)
    └─▶ Handle MFA if required

[2] NAVIGATE
    └─▶ Go to Reports → Stock Movement Report
    └─▶ Wait for page load (explicit waits)

[3] CONFIGURE REPORT
    └─▶ Set date range (yesterday)
    └─▶ Select all locations
    └─▶ Select all movement types

[4] EXPORT
    └─▶ Click "Export to Excel"
    └─▶ Wait for download (~2-3 minutes)
    └─▶ Move file to staging directory

[5] VALIDATE
    └─▶ Check file size (>0 bytes)
    └─▶ Check row count (expected range)
    └─▶ Check required columns present

[6] INGEST TO BRONZE
    └─▶ Read Excel with Polars
    └─▶ Add metadata columns
    └─▶ Write to Bronze layer

Error Handling:
    └─▶ Screenshot on failure
    └─▶ Retry logic (3 attempts)
    └─▶ Alert on final failure
```

---

## Orchestration

### Daily Schedule

```
TIME        PIPELINE              DEPENDENCY              STATUS
───────────────────────────────────────────────────────────────────
05:00 AM    WMS Stock Movements   None                    ●
05:30 AM    WMS Locations         None                    ●
06:00 AM    ERP Inventory         None                    ●
06:00 AM    ERP Products          None                    ●
06:30 AM    ERP Sales (batch 1)   None                    ●
07:00 AM    OBI Reports           None                    ●
07:30 AM    Silver Processing     All Bronze complete     ◐
08:00 AM    Gold Modeling         Silver complete         ◐
08:30 AM    Anomaly Reports       Gold complete           ○
09:00 AM    Dashboard Refresh     Gold complete           ○
```

Legend: ● Running  ◐ Pending  ○ Scheduled

### Dependency Graph

```
                    ┌─────────────┐
                    │   Bronze    │
                    │   Layer     │
                    └──────┬──────┘
                           │
        ┌─────────┬────────┼────────┬─────────┐
        ▼         ▼        ▼        ▼         ▼
    ┌───────┐ ┌───────┐ ┌───────┐ ┌───────┐ ┌───────┐
    │  ERP  │ │  ERP  │ │  ERP  │ │  WMS  │ │  OBI  │
    │  INV  │ │ SALES │ │ PROD  │ │ STOCK │ │REPORTS│
    └───┬───┘ └───┬───┘ └───┬───┘ └───┬───┘ └───┬───┘
        │         │        │         │         │
        └─────────┴────────┼─────────┴─────────┘
                           ▼
                    ┌─────────────┐
                    │   Silver    │
                    │ Validation  │
                    └──────┬──────┘
                           │
                           ▼
                    ┌─────────────┐
                    │    Gold     │
                    │  Modeling   │
                    └──────┬──────┘
                           │
            ┌──────────────┼──────────────┐
            ▼              ▼              ▼
     ┌───────────┐  ┌───────────┐  ┌───────────┐
     │  Power BI │  │ Replenish │  │  Alerts   │
     │  Refresh  │  │  Engine   │  │  System   │
     └───────────┘  └───────────┘  └───────────┘
```

---

## Error Handling

### Retry Strategy

```python
RETRY_CONFIG = {
    "database_extractor": {
        "max_retries": 3,
        "backoff_factor": 2,  # Exponential: 2s, 4s, 8s
        "retry_on": ["ConnectionError", "TimeoutError"],
    },
    "api_extractor": {
        "max_retries": 5,
        "backoff_factor": 1,  # Linear: 1s, 2s, 3s, 4s, 5s
        "retry_on": ["RateLimitError", "ServiceUnavailable"],
    },
    "rpa_extractor": {
        "max_retries": 2,
        "backoff_factor": 30,  # 30s, 60s
        "retry_on": ["ElementNotFound", "PageLoadTimeout"],
    },
}
```

### Alerting Rules

| Severity | Condition | Action |
|----------|-----------|--------|
| Critical | Pipeline failure (all retries exhausted) | PagerDuty + Teams |
| High | Data quality threshold breach (>5% invalid) | Teams + Email |
| Medium | Anomaly count > 50 | Email + Log |
| Low | Extraction time > SLA | Log only |

---

## Monitoring Metrics

### Pipeline Health Dashboard

```
┌─────────────────────────────────────────────────────────────────────┐
│                     PIPELINE HEALTH - 2026-01-15                     │
├─────────────────────────────────────────────────────────────────────┤
│                                                                      │
│  Extraction Success Rate                                             │
│  ██████████████████████████████████████████████████ 98.5%           │
│                                                                      │
│  Avg Pipeline Duration          Records Processed Today              │
│  ┌────────────────────────┐     ┌─────────────────────────────────┐ │
│  │      4.2 minutes       │     │       2,847,291 records         │ │
│  │      (Target: 5 min)   │     │       (+12% vs yesterday)       │ │
│  └────────────────────────┘     └─────────────────────────────────┘ │
│                                                                      │
│  Data Quality Score             Anomalies Detected                   │
│  ┌────────────────────────┐     ┌─────────────────────────────────┐ │
│  │        99.2%           │     │          47 items               │ │
│  │    ▲ 0.3% from week    │     │    (within normal range)        │ │
│  └────────────────────────┘     └─────────────────────────────────┘ │
│                                                                      │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Related Documentation

- [Medallion Architecture](medallion-architecture.md) - Layer specifications
- [Performance Benchmarks](../docs/performance-benchmarks.md) - Timing details
- [Reliability Metrics](../docs/reliability-metrics.md) - SLA tracking
