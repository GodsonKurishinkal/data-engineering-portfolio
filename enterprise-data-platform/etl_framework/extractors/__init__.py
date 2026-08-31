"""
ETL Framework - Extractors Module

Provides abstract base class and concrete implementations for data extraction
from various source systems including databases, APIs, and legacy systems.

Author: Godson Kurishinkal
"""

from .base_extractor import (
    BaseExtractor,
    ExtractorConfig,
    ExtractionMetadata,
    ExtractionError,
)

from .database_extractor import (
    DatabaseExtractor,
    DatabaseConfig,
    create_sql_server_extractor,
    create_oracle_extractor,
)

from .api_extractor import (
    APIExtractor,
    APIConfig,
    AuthType,
    PaginationType,
    create_rest_api_extractor,
)

from .rpa_extractor import (
    RPAExtractor,
    RPAConfig,
    StepBuilder,
    create_wms_extractor,
)


__all__ = [
    # Base classes
    "BaseExtractor",
    "ExtractorConfig",
    "ExtractionMetadata",
    "ExtractionError",
    # Database
    "DatabaseExtractor",
    "DatabaseConfig",
    "create_sql_server_extractor",
    "create_oracle_extractor",
    # API
    "APIExtractor",
    "APIConfig",
    "AuthType",
    "PaginationType",
    "create_rest_api_extractor",
    # RPA
    "RPAExtractor",
    "RPAConfig",
    "StepBuilder",
    "create_wms_extractor",
]
