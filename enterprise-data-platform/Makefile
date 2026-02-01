# ============================================================================
# Enterprise Data Platform - Makefile
# ============================================================================
# Common development, testing, and deployment commands
# Usage: make <target>
# ============================================================================

.PHONY: help install install-dev lint format test test-cov clean run docs

# Default target
help:
	@echo "Enterprise Data Platform - Development Commands"
	@echo ""
	@echo "Setup:"
	@echo "  make install      Install production dependencies"
	@echo "  make install-dev  Install all dependencies (including dev)"
	@echo "  make install-all  Install everything (dev + rpa + viz + ml)"
	@echo ""
	@echo "Quality:"
	@echo "  make lint         Run linters (ruff + mypy)"
	@echo "  make format       Auto-format code (black + ruff)"
	@echo "  make check        Run all quality checks"
	@echo ""
	@echo "Testing:"
	@echo "  make test         Run tests"
	@echo "  make test-cov     Run tests with coverage report"
	@echo "  make test-fast    Run tests excluding slow/integration"
	@echo ""
	@echo "Development:"
	@echo "  make run          Run example pipeline"
	@echo "  make docs         Build documentation"
	@echo "  make clean        Remove build artifacts"
	@echo ""
	@echo "Pipeline:"
	@echo "  make pipeline-inventory  Run inventory pipeline"
	@echo "  make pipeline-sales      Run sales pipeline"
	@echo "  make pipeline-all        Run all pipelines"

# ============================================================================
# Setup
# ============================================================================

install:
	pip install -e .

install-dev:
	pip install -e ".[dev]"
	pre-commit install

install-all:
	pip install -e ".[all]"
	pre-commit install

# ============================================================================
# Code Quality
# ============================================================================

lint:
	@echo "Running ruff..."
	ruff check etl-framework/ tests/
	@echo "Running mypy..."
	mypy etl-framework/

format:
	@echo "Running black..."
	black etl-framework/ tests/
	@echo "Running ruff fix..."
	ruff check --fix etl-framework/ tests/

check: lint
	@echo "Running black check..."
	black --check etl-framework/ tests/

# ============================================================================
# Testing
# ============================================================================

test:
	pytest tests/ -v

test-cov:
	pytest tests/ --cov=etl_framework --cov-report=term-missing --cov-report=html

test-fast:
	pytest tests/ -v -m "not slow and not integration"

test-integration:
	pytest tests/ -v -m "integration"

# ============================================================================
# Development
# ============================================================================

run:
	@echo "Running example pipeline..."
	python -m etl_framework.run --pipeline example_pipeline --dry-run

docs:
	@echo "Building documentation..."
	cd docs && mkdocs build

docs-serve:
	@echo "Serving documentation..."
	cd docs && mkdocs serve

# ============================================================================
# Pipeline Commands
# ============================================================================

pipeline-inventory:
	@echo "Running inventory pipeline..."
	python -m etl_framework.run --pipeline inventory_daily

pipeline-sales:
	@echo "Running sales pipeline..."
	python -m etl_framework.run --pipeline sales_hourly

pipeline-all:
	@echo "Running all pipelines..."
	python -m etl_framework.run --all

pipeline-status:
	@echo "Checking pipeline status..."
	python -m etl_framework.status

# ============================================================================
# Data Quality
# ============================================================================

validate-bronze:
	@echo "Validating Bronze layer..."
	python -m etl_framework.validate --layer bronze

validate-silver:
	@echo "Validating Silver layer..."
	python -m etl_framework.validate --layer silver

validate-gold:
	@echo "Validating Gold layer..."
	python -m etl_framework.validate --layer gold

validate-all: validate-bronze validate-silver validate-gold

# ============================================================================
# Cleanup
# ============================================================================

clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf .pytest_cache/
	rm -rf .ruff_cache/
	rm -rf .mypy_cache/
	rm -rf coverage_html/
	rm -rf .coverage
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

clean-data:
	@echo "WARNING: This will delete all lakehouse data!"
	@read -p "Are you sure? [y/N] " confirm && [ "$$confirm" = "y" ]
	rm -rf lakehouse/bronze/*
	rm -rf lakehouse/silver/*
	rm -rf lakehouse/gold/*

# ============================================================================
# Docker (Future)
# ============================================================================

docker-build:
	docker build -t enterprise-data-platform .

docker-run:
	docker run -it --rm enterprise-data-platform

# ============================================================================
# Benchmarks
# ============================================================================

benchmark:
	@echo "Running performance benchmarks..."
	python -m benchmarks.run_all

benchmark-polars:
	@echo "Running Polars vs Pandas benchmark..."
	python -m benchmarks.polars_vs_pandas
