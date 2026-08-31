# ============================================================================
# Enterprise Data Platform — Makefile
# ============================================================================
# Every target here runs against what is actually in this repository.
# Usage: make <target>
# ============================================================================

.PHONY: help install install-dev install-all lint format check clean

help:
	@echo "Enterprise Data Platform — development commands"
	@echo ""
	@echo "Setup:"
	@echo "  make install      Install runtime dependencies"
	@echo "  make install-dev  Install runtime + dev tooling"
	@echo "  make install-all  Install everything (dev + rpa + viz + ml)"
	@echo ""
	@echo "Quality:"
	@echo "  make lint         ruff + mypy over etl_framework/ and data_quality/"
	@echo "  make format       black + ruff --fix"
	@echo "  make check        lint plus a black --check pass"
	@echo ""
	@echo "Housekeeping:"
	@echo "  make clean        Remove caches and build artifacts"

# ============================================================================
# Setup
# ============================================================================

install:
	pip install -e .

install-dev:
	pip install -e ".[dev]"

install-all:
	pip install -e ".[all]"

# ============================================================================
# Code Quality
# ============================================================================

SRC = etl_framework data_quality

lint:
	@echo "Running ruff..."
	ruff check $(SRC)
	@echo "Running mypy..."
	mypy $(SRC)

format:
	@echo "Running black..."
	black $(SRC)
	@echo "Running ruff fix..."
	ruff check --fix $(SRC)

check: lint
	@echo "Running black check..."
	black --check $(SRC)

# ============================================================================
# Housekeeping
# ============================================================================

clean:
	rm -rf build/ dist/ *.egg-info/
	rm -rf .pytest_cache/ .ruff_cache/ .mypy_cache/
	rm -rf coverage_html/ .coverage
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
