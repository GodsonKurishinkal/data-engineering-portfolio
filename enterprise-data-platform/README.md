# ETL framework components

A Python library of ETL building blocks for a Bronze/Silver/Gold Parquet
lakehouse: extractors, a schema enforcer, loaders and data-quality checks.
Built on Polars and DuckDB.

This is a component library, not a running platform. Read "What is not included"
before relying on it.

## What is included

| Package | Contents |
|---------|----------|
| `etl_framework/extractors/` | `BaseExtractor`, plus database (ODBC), REST API and Selenium implementations |
| `etl_framework/transformers/` | `BaseTransformer`, `SchemaEnforcer`, `DataCleaner` |
| `etl_framework/loaders/` | Hive-partitioned Parquet writer, DuckDB loader |
| `data_quality/validation_rules/` | Rule-based validation engine |
| `data_quality/anomaly_detection/` | Outlier detection: z-score, modified z-score, IQR, percentile, spike and volume checks |

About 5,000 lines of source and 33 tests.

## What is not included

- No pipeline entry point, no sample data and no pipeline configs. Nothing here
  runs end to end.
- No Gold-layer build, forecasting, dashboard or alerting code. The design
  documents mention them; the code is not in this repository.
- No lock file. Dependencies are declared with lower bounds only.

## Layout

```
enterprise-data-platform/
├── etl_framework/
│   ├── extractors/
│   ├── transformers/
│   └── loaders/
├── data_quality/
│   ├── validation_rules/
│   └── anomaly_detection/
├── tests/
├── architecture/                  # layer specification, data flow (design notes)
├── docs/architecture-decisions/   # ADR-001 to ADR-007
├── Makefile
└── pyproject.toml
```

## Install and run the tests

Python 3.10 or newer.

```bash
cd enterprise-data-platform
python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -e ".[dev]"
pytest tests/
```

Expected result: 33 passed.

## Test and coverage status

```bash
pytest tests/ --cov=etl_framework --cov=data_quality
```

| Scope | Coverage |
|-------|----------|
| Whole package | 20% |
| `schema_enforcer.py` | 81% |
| `anomaly_detector.py` | 60% |
| The two modules above together (what CI reports) | 67% |
| Extractors, loaders, `validation_engine.py` | 0% |

CI runs the tests on Python 3.10 and 3.12 for every pull request
(`.github/workflows/tests.yml`).

## Known gaps

- **Extractors and loaders are untested.** They need a live database, API or
  browser session, and no stand-in is provided.
- **The validation engine is untested**, and the third detector tier
  (volatility) has no tests.
- **`make lint` and `make check` fail.** Ruff and mypy both report errors, and
  black would reformat most files. CI does not run them.
- **No end-to-end run.** The pieces compose in principle; that is not
  demonstrated here.
- **`rpa_extractor.py` uses Selenium only.** The `rpa` extra also lists
  PyAutoGUI, which the code does not import.

## Using the pieces

The tested path is schema enforcement followed by outlier detection. The tests
are the best usage examples:

- `tests/test_schema_enforcer.py`
- `tests/test_anomaly_detection.py`

## Design documents

- [Architecture decision records](docs/architecture-decisions/README.md):
  seven ADRs, each with context, alternatives and cost
- [Medallion architecture](architecture/medallion-architecture.md): layer specification
- [Data flow](architecture/data-flow.md): how the components are meant to fit together

## License

MIT. See [LICENSE](LICENSE).
