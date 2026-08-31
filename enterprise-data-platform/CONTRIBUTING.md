# Working on this codebase

This is a reference implementation, not a staffed open-source project. There is
no review queue and no roadmap to contribute to. What follows is how to run it
locally and the conventions the code holds itself to — useful if you are reading
the source, forking it, or evaluating it.

Questions and corrections are welcome by
[email](mailto:godson.kurishinkal@gmail.com) or as a GitHub issue.

---

## Local setup

**Prerequisites:** Python 3.10+, Git. Optionally the ODBC Driver 17 for SQL
Server and an Oracle client if you intend to exercise `DatabaseExtractor`
against a real source.

```bash
git clone https://github.com/GodsonKurishinkal/data-engineering-portfolio.git
cd data-engineering-portfolio/enterprise-data-platform

python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate

pip install -e ".[dev]"        # runtime + ruff, mypy, black
```

Optional extras, installed the same way: `.[rpa]` for the Selenium/PyAutoGUI
bots, `.[viz]` for Streamlit, `.[ml]` for the forecasting stack, `.[all]` for
everything.

Verify:

```bash
python -c "import etl_framework; print(etl_framework.__version__)"
```

---

## Layout

```
etl_framework/
  extractors/    BaseExtractor + database, API and RPA implementations
  transformers/  BaseTransformer + schema enforcement and cleaning
  loaders/       Hive-partitioned Parquet writes, DuckDB query access
data_quality/
  validation_rules/    Tiers 1–2: schema checks and business rules
  anomaly_detection/   Tier 3: statistical outlier detection
docs/architecture-decisions/   ADR-001 … ADR-007
```

Each subpackage's `__init__.py` is the public surface — if a name is not in
`__all__`, treat it as internal.

---

## Conventions

**Everything inherits from a base class.** A new extractor subclasses
`BaseExtractor` and implements `extract()`. Retry, structured logging,
extraction metadata and failure alerting are inherited, never reimplemented.
The same holds for transformers and loaders. See
[ADR-005](docs/architecture-decisions/005-config-driven-pipelines.md).

**Behaviour is declared, not coded.** Schema, quality rules and load mode belong
in a pipeline's YAML config. If a config starts growing conditionals, that logic
belongs in Python instead.

**Types are not optional.** `mypy` runs in strict mode. Every public function is
annotated, including the return type.

**Polars, not Pandas,** for anything in the transform path — see
[ADR-001](docs/architecture-decisions/001-polars-over-pandas.md). Pandas remains
acceptable at the edges where a library forces it.

**Errors are typed.** Raise `ExtractionError`, `TransformationError`,
`SchemaViolationError` or `WriteError` rather than bare exceptions, so callers
can distinguish a bad source from a bad row.

**Quality checks never silently pass.** Tier 1 blocks the batch, Tier 2 flags and
quarantines, Tier 3 alerts and logs. A check that cannot decide should raise.

---

## Before you commit

```bash
make format     # black + ruff --fix
make check      # ruff, mypy, and a black --check pass
```

Both run against `etl_framework/` and `data_quality/`.

**There is no test suite yet.** That is a known gap, and the honest reason the
repository carries no coverage badge. If you are adding code here, a focused
`pytest` suite over the pure functions — `SchemaEnforcer`, `DataCleaner`, and the
statistical detectors — is the most useful thing you could contribute.

---

## Recording a decision

Anything that changes the shape of the system gets an ADR in
`docs/architecture-decisions/`, following the Context · Decision · Consequences ·
Alternatives structure the existing seven use.

ADR numbers are **global to this repository and never reused**. Take the next
free number, add a row to the index in that directory's `README.md`, and link the
file from anywhere that references the decision.
