"""
Abstract Base Transformer

Provides the interface and common functionality for all data transformers.
Implements the transformation layer of the Medallion architecture.

Author: Godson Kurishinkal
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
import logging

import polars as pl


@dataclass
class TransformationMetadata:
    """Metadata captured during transformation."""
    source_table: str
    target_table: str
    transform_timestamp: datetime
    source_layer: str  # bronze, silver
    target_layer: str  # silver, gold
    records_in: int = 0
    records_out: int = 0
    records_filtered: int = 0
    records_rejected: int = 0
    duration_seconds: float = 0.0
    status: str = "pending"
    error_message: Optional[str] = None
    quality_score: float = 0.0


@dataclass
class TransformerConfig:
    """Base configuration for transformers."""
    source_table: str
    target_table: str
    source_path: Path
    target_path: Path
    source_layer: str = "bronze"
    target_layer: str = "silver"
    
    # Schema settings
    schema: Optional[Dict[str, type]] = None
    required_columns: List[str] = field(default_factory=list)
    
    # Quality settings
    drop_nulls_columns: List[str] = field(default_factory=list)
    fill_null_rules: Dict[str, Any] = field(default_factory=dict)
    
    # Deduplication
    dedupe_columns: List[str] = field(default_factory=list)
    dedupe_keep: str = "last"  # first, last
    
    # Output settings
    partition_by: List[str] = field(default_factory=list)
    output_format: str = "parquet"
    compression: str = "snappy"


class BaseTransformer(ABC):
    """
    Abstract Base Class for all data transformers.
    
    Provides:
    - Schema enforcement
    - Null handling
    - Deduplication
    - Data quality scoring
    - Partition writing
    
    Subclasses must implement:
    - _transform(): Apply business-specific transformations
    """
    
    def __init__(self, config: TransformerConfig):
        self.config = config
        self.logger = logging.getLogger(f"transformer.{config.target_table}")
        self.metadata = TransformationMetadata(
            source_table=config.source_table,
            target_table=config.target_table,
            transform_timestamp=datetime.utcnow(),
            source_layer=config.source_layer,
            target_layer=config.target_layer,
        )
        self._rejected_records: Optional[pl.DataFrame] = None
    
    @abstractmethod
    def _transform(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Apply business-specific transformations.
        
        This is where subclasses implement their specific logic:
        - Column calculations
        - Joins with reference data
        - Business rule applications
        
        Args:
            df: Input DataFrame after standard cleaning
            
        Returns:
            pl.DataFrame: Transformed DataFrame
        """
        pass
    
    def transform(self, df: Optional[pl.DataFrame] = None) -> pl.DataFrame:
        """
        Main transformation method with full pipeline.
        
        Pipeline steps:
        1. Read source data (if not provided)
        2. Validate schema
        3. Handle nulls
        4. Apply business transformations
        5. Deduplicate
        6. Calculate quality score
        
        Args:
            df: Optional input DataFrame (reads from source if not provided)
            
        Returns:
            pl.DataFrame: Transformed data ready for target layer
        """
        start_time = datetime.utcnow()
        
        try:
            # Step 1: Read source data if not provided
            if df is None:
                df = self._read_source()
            
            self.metadata.records_in = len(df)
            self.logger.info(f"Starting transformation: {len(df):,} input records")
            
            # Step 2: Validate and enforce schema
            df = self._validate_schema(df)
            
            # Step 3: Handle nulls
            df = self._handle_nulls(df)
            
            # Step 4: Apply business transformations
            df = self._transform(df)
            
            # Step 5: Deduplicate
            df = self._deduplicate(df)
            
            # Step 6: Calculate quality score
            self._calculate_quality_score(df)
            
            # Update metadata
            self.metadata.records_out = len(df)
            self.metadata.records_filtered = self.metadata.records_in - len(df)
            self.metadata.status = "success"
            self.metadata.duration_seconds = (
                datetime.utcnow() - start_time
            ).total_seconds()
            
            self.logger.info(
                f"Transformation complete: {len(df):,} output records "
                f"({self.metadata.records_filtered:,} filtered) in "
                f"{self.metadata.duration_seconds:.2f}s, "
                f"quality score: {self.metadata.quality_score:.2%}"
            )
            
            return df
            
        except Exception as e:
            self.metadata.status = "failed"
            self.metadata.error_message = str(e)
            self.metadata.duration_seconds = (
                datetime.utcnow() - start_time
            ).total_seconds()
            
            self.logger.error(f"Transformation failed: {str(e)}")
            raise TransformationError(f"Transformation failed: {str(e)}")
    
    def _read_source(self) -> pl.DataFrame:
        """Read data from source layer."""
        source_path = self.config.source_path / self.config.source_table
        
        if not source_path.exists():
            raise TransformationError(f"Source path not found: {source_path}")
        
        # Read all partitions
        if source_path.is_dir():
            return pl.read_parquet(source_path / "**/*.parquet")
        else:
            return pl.read_parquet(source_path)
    
    def _validate_schema(self, df: pl.DataFrame) -> pl.DataFrame:
        """Validate and enforce schema on input data."""
        # Check required columns
        missing_cols = set(self.config.required_columns) - set(df.columns)
        if missing_cols:
            raise TransformationError(f"Missing required columns: {missing_cols}")
        
        # Cast to expected types if schema provided
        if self.config.schema:
            cast_exprs = []
            for col, dtype in self.config.schema.items():
                if col in df.columns:
                    cast_exprs.append(pl.col(col).cast(dtype))
            
            if cast_exprs:
                df = df.with_columns(cast_exprs)
        
        return df
    
    def _handle_nulls(self, df: pl.DataFrame) -> pl.DataFrame:
        """Handle null values according to configuration."""
        # Drop rows with nulls in specified columns
        if self.config.drop_nulls_columns:
            initial_count = len(df)
            df = df.drop_nulls(subset=self.config.drop_nulls_columns)
            dropped = initial_count - len(df)
            if dropped > 0:
                self.logger.info(f"Dropped {dropped:,} rows with null values")
        
        # Fill nulls with specified values
        if self.config.fill_null_rules:
            fill_exprs = []
            for col, fill_value in self.config.fill_null_rules.items():
                if col in df.columns:
                    if callable(fill_value):
                        # Dynamic fill (e.g., median, mean)
                        fill_exprs.append(fill_value(pl.col(col)).alias(col))
                    else:
                        # Static fill
                        fill_exprs.append(pl.col(col).fill_null(fill_value).alias(col))
            
            if fill_exprs:
                df = df.with_columns(fill_exprs)
        
        return df
    
    def _deduplicate(self, df: pl.DataFrame) -> pl.DataFrame:
        """Remove duplicate records based on configuration."""
        if not self.config.dedupe_columns:
            return df
        
        initial_count = len(df)
        
        df = df.unique(
            subset=self.config.dedupe_columns,
            keep=self.config.dedupe_keep,
            maintain_order=True,
        )
        
        removed = initial_count - len(df)
        if removed > 0:
            self.logger.info(f"Removed {removed:,} duplicate records")
        
        return df
    
    def _calculate_quality_score(self, df: pl.DataFrame) -> None:
        """Calculate data quality score based on completeness and validity."""
        if len(df) == 0:
            self.metadata.quality_score = 0.0
            return
        
        # Score based on null percentage
        total_cells = len(df) * len(df.columns)
        null_count = df.null_count().sum_horizontal()[0]
        completeness_score = 1.0 - (null_count / total_cells if total_cells > 0 else 0)
        
        self.metadata.quality_score = completeness_score
    
    def write_to_target(self, df: pl.DataFrame) -> Path:
        """Write transformed data to target layer."""
        target_path = self.config.target_path / self.config.target_table
        target_path.mkdir(parents=True, exist_ok=True)
        
        if self.config.partition_by:
            # Write partitioned
            df.write_parquet(
                target_path,
                partition_by=self.config.partition_by,
                compression=self.config.compression,
            )
        else:
            # Write single file
            output_file = target_path / "data.parquet"
            df.write_parquet(
                output_file,
                compression=self.config.compression,
            )
        
        self.logger.info(f"Written {len(df):,} records to {target_path}")
        return target_path
    
    def get_rejected_records(self) -> Optional[pl.DataFrame]:
        """Return records that were rejected during transformation."""
        return self._rejected_records
    
    def get_metadata(self) -> Dict[str, Any]:
        """Return transformation metadata as dictionary."""
        return {
            "source_table": self.metadata.source_table,
            "target_table": self.metadata.target_table,
            "transform_timestamp": self.metadata.transform_timestamp.isoformat(),
            "source_layer": self.metadata.source_layer,
            "target_layer": self.metadata.target_layer,
            "records_in": self.metadata.records_in,
            "records_out": self.metadata.records_out,
            "records_filtered": self.metadata.records_filtered,
            "records_rejected": self.metadata.records_rejected,
            "duration_seconds": self.metadata.duration_seconds,
            "status": self.metadata.status,
            "error_message": self.metadata.error_message,
            "quality_score": self.metadata.quality_score,
        }


class TransformationError(Exception):
    """Custom exception for transformation failures."""
    pass


# Common transformation utilities
class TransformUtils:
    """Static utility methods for common transformations."""
    
    @staticmethod
    def standardize_string(col_name: str) -> pl.Expr:
        """Standardize string column: uppercase, trim, remove special chars."""
        return (
            pl.col(col_name)
            .str.to_uppercase()
            .str.strip_chars()
            .str.replace_all(r"[^\w\s-]", "")
        )
    
    @staticmethod
    def parse_date(col_name: str, format: str = "%Y-%m-%d") -> pl.Expr:
        """Parse string column to date."""
        return pl.col(col_name).str.to_date(format)
    
    @staticmethod
    def calculate_age_days(date_col: str) -> pl.Expr:
        """Calculate age in days from a date column."""
        return (pl.lit(datetime.now().date()) - pl.col(date_col)).dt.total_days()
    
    @staticmethod
    def bin_numeric(
        col_name: str, 
        bins: List[float], 
        labels: List[str]
    ) -> pl.Expr:
        """Bin numeric values into categories."""
        return pl.col(col_name).cut(bins, labels=labels)
    
    @staticmethod
    def flag_outliers(col_name: str, n_std: float = 3.0) -> pl.Expr:
        """Flag statistical outliers based on standard deviation."""
        mean = pl.col(col_name).mean()
        std = pl.col(col_name).std()
        return (
            (pl.col(col_name) < (mean - n_std * std)) |
            (pl.col(col_name) > (mean + n_std * std))
        ).alias(f"{col_name}_is_outlier")
