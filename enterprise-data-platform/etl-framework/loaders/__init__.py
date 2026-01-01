"""
ETL Framework - Loaders Module

Provides data loading utilities for writing to various targets
including Parquet files and DuckDB databases.

Author: Godson Kurishinkal
"""

from .parquet_writer import (
    ParquetWriter,
    ParquetWriterConfig,
    WriteMetadata,
    WriteError,
    write_to_bronze,
    write_to_silver,
    write_to_gold,
)

from .duckdb_loader import (
    DuckDBLoader,
    DuckDBConfig,
    QueryError,
    query_lakehouse,
    create_analytics_db,
)


__all__ = [
    # Parquet Writer
    "ParquetWriter",
    "ParquetWriterConfig",
    "WriteMetadata",
    "WriteError",
    "write_to_bronze",
    "write_to_silver",
    "write_to_gold",
    # DuckDB Loader
    "DuckDBLoader",
    "DuckDBConfig",
    "QueryError",
    "query_lakehouse",
    "create_analytics_db",
]
