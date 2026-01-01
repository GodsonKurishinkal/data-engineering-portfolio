"""
ETL Framework - Transformers Module

Provides transformation utilities for Silver and Gold layer processing.

Author: Godson Kurishinkal
"""

from .base_transformer import (
    BaseTransformer,
    TransformerConfig,
    TransformationMetadata,
    TransformationError,
    TransformUtils,
)

from .schema_enforcer import (
    SchemaEnforcer,
    SchemaSpec,
    ColumnSpec,
    SchemaRegistry,
    SchemaViolationError,
    INVENTORY_SCHEMA,
    PRODUCT_SCHEMA,
)

from .data_cleaner import (
    DataCleaner,
    CleaningReport,
    quick_clean,
)


__all__ = [
    # Base classes
    "BaseTransformer",
    "TransformerConfig",
    "TransformationMetadata",
    "TransformationError",
    "TransformUtils",
    # Schema
    "SchemaEnforcer",
    "SchemaSpec",
    "ColumnSpec",
    "SchemaRegistry",
    "SchemaViolationError",
    "INVENTORY_SCHEMA",
    "PRODUCT_SCHEMA",
    # Cleaner
    "DataCleaner",
    "CleaningReport",
    "quick_clean",
]
