"""
ETL Framework — configuration-driven pipeline components for the medallion lakehouse.

Layers:
    extractors/   Database, REST API and RPA sources behind one BaseExtractor
    transformers/ Schema enforcement, cleaning and Silver-layer shaping
    loaders/      Hive-partitioned Parquet writes and DuckDB analytical access

See docs/architecture-decisions/005-config-driven-pipelines.md for the design.

Author: Godson Kurishinkal Antony
"""

__version__ = "1.0.0"
