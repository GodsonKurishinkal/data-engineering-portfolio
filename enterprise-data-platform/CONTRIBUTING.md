# Contributing to Enterprise Data Platform

Thank you for your interest in contributing! This document provides guidelines and workflows for contributing to the Enterprise Data Platform.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Workflow](#development-workflow)
- [Coding Standards](#coding-standards)
- [Testing Guidelines](#testing-guidelines)
- [Pipeline Development](#pipeline-development)
- [Documentation](#documentation)
- [Pull Request Process](#pull-request-process)

---

## Code of Conduct

Be respectful, professional, and constructive. We're all here to build better data infrastructure.

---

## Getting Started

### Prerequisites

- Python 3.10+
- Git
- (Optional) Database drivers: ODBC Driver 17 for SQL Server, Oracle Client

### Setup

```bash
# Clone the repository
git clone https://github.com/GodsonKurishinkal/data-engineering-portfolio.git
cd data-engineering-portfolio/enterprise-data-platform

# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # Linux/Mac
# .venv\Scripts\activate   # Windows

# Install development dependencies
make install-dev

# Verify setup
make test-fast
```

### Project Structure

```
enterprise-data-platform/
├── etl-framework/           # Core ETL library
│   ├── extractors/          # Data extraction modules
│   ├── transformers/        # Data transformation modules
│   └── loaders/             # Data loading modules
├── architecture/            # Architecture documentation
├── data-quality/            # Data quality rules and checks
├── docs/                    # Technical documentation
├── tests/                   # Test suites
└── notebooks/               # Jupyter notebooks for exploration
```

---

## Development Workflow

### Branch Naming

```
feature/TICKET-description    # New features
bugfix/TICKET-description     # Bug fixes
docs/description              # Documentation updates
refactor/description          # Code refactoring
test/description              # Test additions
```

### Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

**Types:**
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation
- `style`: Formatting (no code change)
- `refactor`: Code restructuring
- `test`: Adding tests
- `chore`: Maintenance tasks

**Examples:**
```
feat(extractors): add Oracle database extractor
fix(validators): handle null values in schema check
docs(adr): add ADR-003 for medallion architecture decision
test(transformers): add unit tests for data cleaner
```

---

## Coding Standards

### Python Style

We use [Ruff](https://github.com/astral-sh/ruff) for linting and [Black](https://github.com/psf/black) for formatting.

```bash
# Format code
make format

# Check linting
make lint

# Run all quality checks
make check
```

### Type Hints

All public functions must have type hints:

```python
# Good
def extract_data(
    source: str,
    query: str,
    batch_size: int = 10_000,
) -> pl.DataFrame:
    """Extract data from source system."""
    ...

# Bad
def extract_data(source, query, batch_size=10_000):
    ...
```

### Docstrings

Use Google-style docstrings:

```python
def calculate_metrics(
    df: pl.DataFrame,
    group_cols: list[str],
    value_col: str,
) -> pl.DataFrame:
    """
    Calculate aggregated metrics grouped by specified columns.
    
    Args:
        df: Input DataFrame with raw data.
        group_cols: Columns to group by.
        value_col: Column to aggregate.
    
    Returns:
        DataFrame with aggregated metrics including sum, mean, and count.
    
    Raises:
        ValueError: If group_cols contains columns not in df.
    
    Example:
        >>> result = calculate_metrics(df, ["region"], "sales")
        >>> print(result.columns)
        ['region', 'sum', 'mean', 'count']
    """
    ...
```

### Logging

Use structured logging with appropriate levels:

```python
import logging

logger = logging.getLogger(__name__)

# Levels:
logger.debug("Detailed debugging info")
logger.info("General operational messages")
logger.warning("Something unexpected but not critical")
logger.error("Error that prevented operation")
logger.critical("System is unusable")

# Include context:
logger.info(
    "Extraction complete",
    extra={
        "source": source_name,
        "records": record_count,
        "duration_seconds": duration,
    }
)
```

---

## Testing Guidelines

### Test Structure

```
tests/
├── unit/                    # Unit tests (fast, isolated)
│   ├── extractors/
│   ├── transformers/
│   └── loaders/
├── integration/             # Integration tests (slower, need resources)
├── fixtures/                # Shared test fixtures
└── conftest.py              # pytest configuration
```

### Writing Tests

```python
import pytest
import polars as pl
from etl_framework.transformers import DataCleaner


class TestDataCleaner:
    """Tests for DataCleaner transformer."""
    
    @pytest.fixture
    def sample_data(self) -> pl.DataFrame:
        """Sample DataFrame for testing."""
        return pl.DataFrame({
            "id": [1, 2, 2, 3],
            "value": [10.0, None, 20.0, 30.0],
            "name": ["  Alice ", "Bob", "Bob", "Charlie"],
        })
    
    def test_removes_duplicates(self, sample_data: pl.DataFrame):
        """Test that duplicate rows are removed."""
        cleaner = DataCleaner(dedupe_columns=["id"])
        result = cleaner.clean(sample_data)
        
        assert len(result) == 3
        assert result["id"].to_list() == [1, 2, 3]
    
    def test_handles_null_values(self, sample_data: pl.DataFrame):
        """Test null value handling with fill strategy."""
        cleaner = DataCleaner(null_strategy="fill_zero")
        result = cleaner.clean(sample_data)
        
        assert result["value"].null_count() == 0
    
    @pytest.mark.slow
    def test_large_dataset_performance(self):
        """Test performance on large dataset."""
        large_df = pl.DataFrame({
            "id": range(1_000_000),
            "value": [float(i) for i in range(1_000_000)],
        })
        
        cleaner = DataCleaner()
        result = cleaner.clean(large_df)
        
        assert len(result) == 1_000_000
```

### Running Tests

```bash
# All tests
make test

# With coverage
make test-cov

# Fast tests only (skip slow/integration)
make test-fast

# Specific test file
pytest tests/unit/extractors/test_database_extractor.py -v

# Specific test
pytest tests/unit/extractors/test_database_extractor.py::TestDatabaseExtractor::test_connection -v
```

### Test Coverage

Maintain minimum 80% coverage. Check coverage report:

```bash
make test-cov
open coverage_html/index.html
```

---

## Pipeline Development

### Creating a New Pipeline

1. **Define configuration** in `config/pipelines/`:

```yaml
# config/pipelines/new_source.yaml
pipeline:
  name: new_source_daily
  description: Extract data from new source system
  owner: your.email@company.com
  
source:
  type: database
  connection: ${NEW_SOURCE_CONN}
  query: |
    SELECT * FROM source_table
    WHERE modified_date > :last_run

destination:
  layer: bronze
  table: new_source_data
  partition_by: [extract_date]
  format: parquet

schedule:
  cron: "0 6 * * *"
  timezone: Asia/Dubai

alerts:
  on_failure: [slack, email]
  on_success: [log]
```

2. **Implement extractor** (if new source type):

```python
# etl-framework/extractors/new_source_extractor.py
from .base_extractor import BaseExtractor, ExtractorConfig

class NewSourceExtractor(BaseExtractor):
    def _connect(self) -> None:
        # Implementation
        pass
    
    def _extract(self) -> pl.DataFrame:
        # Implementation
        pass
    
    def _disconnect(self) -> None:
        # Implementation
        pass
```

3. **Add tests**:

```python
# tests/unit/extractors/test_new_source_extractor.py
class TestNewSourceExtractor:
    def test_extraction(self):
        # Test implementation
        pass
```

4. **Document** in ADR if significant design decision.

### Pipeline Checklist

Before merging a new pipeline:

- [ ] Configuration file created and validated
- [ ] Extractor implemented (or using existing)
- [ ] Transformers defined for Silver layer
- [ ] Gold layer models identified
- [ ] Data quality rules defined
- [ ] Unit tests added (>80% coverage)
- [ ] Integration test added
- [ ] Documentation updated
- [ ] ADR written (if new pattern)
- [ ] Alerts configured
- [ ] Runbook created

---

## Documentation

### When to Document

- New features or significant changes
- Architecture decisions (write an ADR)
- API changes
- Configuration options
- Troubleshooting guides

### Documentation Locations

| Type | Location |
|------|----------|
| API docs | Docstrings in code |
| Architecture | `docs/architecture/` |
| ADRs | `docs/architecture-decisions/` |
| Guides | `docs/guides/` |
| Runbooks | `docs/runbooks/` |

### Writing ADRs

Create ADR in `docs/architecture-decisions/`:

```markdown
# ADR-XXX: Title

## Status
Proposed / Accepted / Deprecated / Superseded by ADR-XXX

## Context
What is the issue motivating this decision?

## Decision
What is the change being proposed?

## Consequences
What becomes easier or harder?

## Alternatives Considered
What other options were evaluated?
```

---

## Pull Request Process

### Before Submitting

1. **Ensure all checks pass:**
   ```bash
   make check
   make test
   ```

2. **Update documentation** if needed

3. **Write meaningful commit messages**

4. **Rebase on main** if behind:
   ```bash
   git fetch origin
   git rebase origin/main
   ```

### PR Template

```markdown
## Summary
Brief description of changes

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Breaking change
- [ ] Documentation update

## Testing
- [ ] Unit tests added/updated
- [ ] Integration tests added/updated
- [ ] Manual testing performed

## Documentation
- [ ] README updated
- [ ] ADR written (if applicable)
- [ ] Docstrings added/updated

## Checklist
- [ ] Code follows project style guidelines
- [ ] Self-review completed
- [ ] Changes generate no new warnings
- [ ] Tests pass locally
```

### Review Process

1. At least 1 approval required
2. All CI checks must pass
3. No unresolved conversations
4. Branch must be up to date with main

---

## Questions?

Open an issue or reach out:
- Email: godson.kurishinkal@gmail.com
- LinkedIn: [linkedin.com/in/godsonkurishinkal](https://linkedin.com/in/godsonkurishinkal)

Thank you for contributing!
