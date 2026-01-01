"""
Data Cleaner

Provides comprehensive data cleaning utilities for the Silver layer.
Handles standardization, deduplication, null handling, and data quality fixes.

Author: Godson Kurishinkal
"""

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
import logging
import re

import polars as pl


@dataclass
class CleaningReport:
    """Report of cleaning operations performed."""
    total_records_in: int = 0
    total_records_out: int = 0
    nulls_filled: Dict[str, int] = field(default_factory=dict)
    duplicates_removed: int = 0
    strings_standardized: int = 0
    outliers_flagged: int = 0
    invalid_values_fixed: int = 0
    columns_renamed: Dict[str, str] = field(default_factory=dict)
    columns_dropped: List[str] = field(default_factory=list)


class DataCleaner:
    """
    Comprehensive data cleaning for Silver layer transformations.
    
    Provides:
    - String standardization (case, trimming, special chars)
    - Null handling (fill, drop, interpolate)
    - Deduplication (exact and fuzzy)
    - Data type fixing
    - Column operations (rename, drop, reorder)
    - Value standardization (mappings, lookups)
    
    Example:
        cleaner = DataCleaner()
        
        df = (cleaner
            .load(raw_df)
            .standardize_strings(["name", "code"])
            .fill_nulls({"quantity": 0, "price": "median"})
            .deduplicate(["id", "date"])
            .drop_columns(["temp_col"])
            .clean())
    """
    
    def __init__(self):
        self.logger = logging.getLogger("cleaner")
        self._df: Optional[pl.DataFrame] = None
        self._report = CleaningReport()
        self._operations: List[Tuple[str, Callable]] = []
    
    def load(self, df: pl.DataFrame) -> "DataCleaner":
        """Load DataFrame for cleaning."""
        self._df = df.clone()
        self._report = CleaningReport(total_records_in=len(df))
        self._operations = []
        return self
    
    def clean(self) -> pl.DataFrame:
        """Execute all queued cleaning operations and return cleaned DataFrame."""
        if self._df is None:
            raise ValueError("No DataFrame loaded. Call load() first.")
        
        for op_name, op_func in self._operations:
            self.logger.debug(f"Executing: {op_name}")
            self._df = op_func(self._df)
        
        self._report.total_records_out = len(self._df)
        return self._df
    
    def get_report(self) -> CleaningReport:
        """Get cleaning report."""
        return self._report
    
    # ==================== String Operations ====================
    
    def standardize_strings(
        self, 
        columns: List[str],
        uppercase: bool = True,
        strip: bool = True,
        remove_special: bool = False,
        special_pattern: str = r"[^\w\s-]",
    ) -> "DataCleaner":
        """
        Standardize string columns.
        
        Args:
            columns: Columns to standardize
            uppercase: Convert to uppercase
            strip: Trim whitespace
            remove_special: Remove special characters
            special_pattern: Regex pattern for special chars to remove
        """
        def operation(df: pl.DataFrame) -> pl.DataFrame:
            exprs = []
            for col in columns:
                if col not in df.columns:
                    continue
                
                expr = pl.col(col)
                if strip:
                    expr = expr.str.strip_chars()
                if uppercase:
                    expr = expr.str.to_uppercase()
                if remove_special:
                    expr = expr.str.replace_all(special_pattern, "")
                
                exprs.append(expr.alias(col))
            
            if exprs:
                self._report.strings_standardized += len(exprs)
            
            return df.with_columns(exprs) if exprs else df
        
        self._operations.append(("standardize_strings", operation))
        return self
    
    def clean_phone_numbers(
        self, 
        column: str, 
        country_code: str = "+1",
        output_column: Optional[str] = None,
    ) -> "DataCleaner":
        """Clean and standardize phone numbers."""
        def operation(df: pl.DataFrame) -> pl.DataFrame:
            out_col = output_column or column
            return df.with_columns(
                pl.col(column)
                .str.replace_all(r"[^\d]", "")  # Keep only digits
                .str.strip_chars()
                .map_elements(
                    lambda x: f"{country_code}{x[-10:]}" if x and len(x) >= 10 else None,
                    return_dtype=pl.Utf8
                )
                .alias(out_col)
            )
        
        self._operations.append(("clean_phone_numbers", operation))
        return self
    
    def clean_emails(self, column: str) -> "DataCleaner":
        """Clean and validate email addresses."""
        email_pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
        
        def operation(df: pl.DataFrame) -> pl.DataFrame:
            return df.with_columns(
                pl.when(
                    pl.col(column).str.to_lowercase().str.strip_chars().str.contains(email_pattern)
                )
                .then(pl.col(column).str.to_lowercase().str.strip_chars())
                .otherwise(None)
                .alias(column)
            )
        
        self._operations.append(("clean_emails", operation))
        return self
    
    # ==================== Null Handling ====================
    
    def fill_nulls(
        self, 
        rules: Dict[str, Union[Any, str]],
    ) -> "DataCleaner":
        """
        Fill null values according to rules.
        
        Args:
            rules: Dict mapping column names to fill values or strategies
                   Strategies: "mean", "median", "mode", "forward", "backward", "zero"
        
        Example:
            .fill_nulls({
                "quantity": 0,
                "price": "median",
                "category": "Unknown",
            })
        """
        def operation(df: pl.DataFrame) -> pl.DataFrame:
            exprs = []
            
            for col, fill_value in rules.items():
                if col not in df.columns:
                    continue
                
                if isinstance(fill_value, str):
                    if fill_value == "mean":
                        exprs.append(pl.col(col).fill_null(pl.col(col).mean()).alias(col))
                    elif fill_value == "median":
                        exprs.append(pl.col(col).fill_null(pl.col(col).median()).alias(col))
                    elif fill_value == "mode":
                        mode_val = df[col].mode()[0] if len(df[col].mode()) > 0 else None
                        exprs.append(pl.col(col).fill_null(mode_val).alias(col))
                    elif fill_value == "forward":
                        exprs.append(pl.col(col).forward_fill().alias(col))
                    elif fill_value == "backward":
                        exprs.append(pl.col(col).backward_fill().alias(col))
                    elif fill_value == "zero":
                        exprs.append(pl.col(col).fill_null(0).alias(col))
                    else:
                        exprs.append(pl.col(col).fill_null(fill_value).alias(col))
                else:
                    exprs.append(pl.col(col).fill_null(fill_value).alias(col))
                
                null_count = df[col].null_count()
                if null_count > 0:
                    self._report.nulls_filled[col] = null_count
            
            return df.with_columns(exprs) if exprs else df
        
        self._operations.append(("fill_nulls", operation))
        return self
    
    def drop_nulls(
        self, 
        columns: Optional[List[str]] = None,
        threshold: Optional[float] = None,
    ) -> "DataCleaner":
        """
        Drop rows with null values.
        
        Args:
            columns: Only check these columns (default: all)
            threshold: Drop if more than this fraction of columns are null
        """
        def operation(df: pl.DataFrame) -> pl.DataFrame:
            if threshold:
                # Drop rows where more than threshold% of columns are null
                null_counts = df.select(
                    pl.sum_horizontal([pl.col(c).is_null().cast(pl.Int64) for c in df.columns])
                    .alias("null_count")
                )
                max_nulls = int(len(df.columns) * threshold)
                return df.filter(null_counts["null_count"] <= max_nulls)
            else:
                return df.drop_nulls(subset=columns)
        
        self._operations.append(("drop_nulls", operation))
        return self
    
    # ==================== Deduplication ====================
    
    def deduplicate(
        self, 
        columns: List[str],
        keep: str = "last",
        sort_by: Optional[List[str]] = None,
    ) -> "DataCleaner":
        """
        Remove duplicate records.
        
        Args:
            columns: Columns to check for duplicates
            keep: Which duplicate to keep ("first" or "last")
            sort_by: Sort before deduplication (important for "first"/"last")
        """
        def operation(df: pl.DataFrame) -> pl.DataFrame:
            initial_count = len(df)
            
            if sort_by:
                df = df.sort(sort_by)
            
            df = df.unique(subset=columns, keep=keep, maintain_order=True)
            
            removed = initial_count - len(df)
            self._report.duplicates_removed += removed
            
            return df
        
        self._operations.append(("deduplicate", operation))
        return self
    
    # ==================== Column Operations ====================
    
    def rename_columns(self, mapping: Dict[str, str]) -> "DataCleaner":
        """Rename columns according to mapping."""
        def operation(df: pl.DataFrame) -> pl.DataFrame:
            valid_mapping = {k: v for k, v in mapping.items() if k in df.columns}
            self._report.columns_renamed.update(valid_mapping)
            return df.rename(valid_mapping)
        
        self._operations.append(("rename_columns", operation))
        return self
    
    def drop_columns(self, columns: List[str]) -> "DataCleaner":
        """Drop specified columns."""
        def operation(df: pl.DataFrame) -> pl.DataFrame:
            existing = [c for c in columns if c in df.columns]
            self._report.columns_dropped.extend(existing)
            return df.drop(existing)
        
        self._operations.append(("drop_columns", operation))
        return self
    
    def reorder_columns(self, columns: List[str]) -> "DataCleaner":
        """Reorder columns (unlisted columns go to end)."""
        def operation(df: pl.DataFrame) -> pl.DataFrame:
            ordered = [c for c in columns if c in df.columns]
            remaining = [c for c in df.columns if c not in ordered]
            return df.select(ordered + remaining)
        
        self._operations.append(("reorder_columns", operation))
        return self
    
    def add_column(
        self, 
        name: str, 
        expression: Union[pl.Expr, Any],
    ) -> "DataCleaner":
        """Add a calculated column."""
        def operation(df: pl.DataFrame) -> pl.DataFrame:
            if isinstance(expression, pl.Expr):
                return df.with_columns(expression.alias(name))
            else:
                return df.with_columns(pl.lit(expression).alias(name))
        
        self._operations.append(("add_column", operation))
        return self
    
    # ==================== Value Standardization ====================
    
    def map_values(
        self, 
        column: str, 
        mapping: Dict[Any, Any],
        default: Optional[Any] = None,
    ) -> "DataCleaner":
        """Map values in a column using a lookup dictionary."""
        def operation(df: pl.DataFrame) -> pl.DataFrame:
            return df.with_columns(
                pl.col(column).replace(mapping, default=default)
            )
        
        self._operations.append(("map_values", operation))
        return self
    
    def clip_values(
        self, 
        column: str, 
        min_val: Optional[float] = None,
        max_val: Optional[float] = None,
    ) -> "DataCleaner":
        """Clip numeric values to a range."""
        def operation(df: pl.DataFrame) -> pl.DataFrame:
            expr = pl.col(column)
            if min_val is not None:
                expr = pl.when(expr < min_val).then(min_val).otherwise(expr)
            if max_val is not None:
                expr = pl.when(expr > max_val).then(max_val).otherwise(expr)
            return df.with_columns(expr.alias(column))
        
        self._operations.append(("clip_values", operation))
        return self
    
    def coalesce_columns(
        self, 
        output_column: str,
        source_columns: List[str],
    ) -> "DataCleaner":
        """Create column from first non-null value across source columns."""
        def operation(df: pl.DataFrame) -> pl.DataFrame:
            existing = [c for c in source_columns if c in df.columns]
            if not existing:
                return df
            return df.with_columns(
                pl.coalesce([pl.col(c) for c in existing]).alias(output_column)
            )
        
        self._operations.append(("coalesce_columns", operation))
        return self
    
    # ==================== Type Conversions ====================
    
    def to_date(
        self, 
        column: str, 
        format: str = "%Y-%m-%d",
        strict: bool = False,
    ) -> "DataCleaner":
        """Convert string column to date."""
        def operation(df: pl.DataFrame) -> pl.DataFrame:
            return df.with_columns(
                pl.col(column).str.to_date(format, strict=strict).alias(column)
            )
        
        self._operations.append(("to_date", operation))
        return self
    
    def to_datetime(
        self, 
        column: str, 
        format: str = "%Y-%m-%d %H:%M:%S",
        strict: bool = False,
    ) -> "DataCleaner":
        """Convert string column to datetime."""
        def operation(df: pl.DataFrame) -> pl.DataFrame:
            return df.with_columns(
                pl.col(column).str.to_datetime(format, strict=strict).alias(column)
            )
        
        self._operations.append(("to_datetime", operation))
        return self
    
    def to_numeric(
        self, 
        column: str, 
        dtype: pl.DataType = pl.Float64,
    ) -> "DataCleaner":
        """Convert column to numeric type, handling non-numeric values."""
        def operation(df: pl.DataFrame) -> pl.DataFrame:
            # First clean any non-numeric characters, then cast
            if df[column].dtype == pl.Utf8:
                return df.with_columns(
                    pl.col(column)
                    .str.replace_all(r"[^\d.-]", "")
                    .cast(dtype, strict=False)
                    .alias(column)
                )
            else:
                return df.with_columns(pl.col(column).cast(dtype, strict=False))
        
        self._operations.append(("to_numeric", operation))
        return self
    
    # ==================== Filtering ====================
    
    def filter_rows(self, condition: pl.Expr) -> "DataCleaner":
        """Filter rows based on condition."""
        def operation(df: pl.DataFrame) -> pl.DataFrame:
            return df.filter(condition)
        
        self._operations.append(("filter_rows", operation))
        return self
    
    def remove_outliers(
        self, 
        column: str, 
        method: str = "iqr",
        threshold: float = 1.5,
    ) -> "DataCleaner":
        """
        Remove statistical outliers.
        
        Args:
            column: Numeric column to check
            method: "iqr" (interquartile range) or "zscore"
            threshold: IQR multiplier or z-score threshold
        """
        def operation(df: pl.DataFrame) -> pl.DataFrame:
            initial_count = len(df)
            
            if method == "iqr":
                q1 = df[column].quantile(0.25)
                q3 = df[column].quantile(0.75)
                iqr = q3 - q1
                lower = q1 - threshold * iqr
                upper = q3 + threshold * iqr
                df = df.filter(
                    (pl.col(column) >= lower) & (pl.col(column) <= upper)
                )
            elif method == "zscore":
                mean = df[column].mean()
                std = df[column].std()
                df = df.filter(
                    ((pl.col(column) - mean) / std).abs() <= threshold
                )
            
            removed = initial_count - len(df)
            self._report.outliers_flagged += removed
            
            return df
        
        self._operations.append(("remove_outliers", operation))
        return self


# Convenience function for quick cleaning
def quick_clean(
    df: pl.DataFrame,
    string_columns: Optional[List[str]] = None,
    fill_rules: Optional[Dict[str, Any]] = None,
    dedupe_columns: Optional[List[str]] = None,
) -> pl.DataFrame:
    """Quick one-liner for common cleaning operations."""
    cleaner = DataCleaner().load(df)
    
    if string_columns:
        cleaner.standardize_strings(string_columns)
    
    if fill_rules:
        cleaner.fill_nulls(fill_rules)
    
    if dedupe_columns:
        cleaner.deduplicate(dedupe_columns)
    
    return cleaner.clean()
