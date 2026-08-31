"""
Parquet Writer

Optimized Parquet writer for data lakehouse with partitioning,
compression options, and statistics generation.

Author: Godson Kurishinkal
"""

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
import logging
import json

import polars as pl


@dataclass
class WriteMetadata:
    """Metadata captured during write operations."""
    target_path: str
    target_table: str
    write_timestamp: datetime
    partition_columns: List[str]
    compression: str
    records_written: int = 0
    files_written: int = 0
    bytes_written: int = 0
    duration_seconds: float = 0.0
    status: str = "pending"
    error_message: Optional[str] = None


@dataclass
class ParquetWriterConfig:
    """Configuration for Parquet writer."""
    target_path: Path
    table_name: str
    
    # Partitioning
    partition_by: List[str] = field(default_factory=list)
    max_partitions: int = 10000  # Safety limit
    
    # Compression
    compression: str = "snappy"  # snappy, gzip, lz4, zstd
    compression_level: Optional[int] = None
    
    # File options
    row_group_size: int = 100_000
    statistics: bool = True
    
    # Write mode
    mode: str = "overwrite"  # overwrite, append, error
    
    # Delta handling
    enable_delta: bool = False
    delta_merge_columns: List[str] = field(default_factory=list)


class ParquetWriter:
    """
    Optimized Parquet writer for data lakehouse operations.
    
    Features:
    - Hive-style partitioning
    - Multiple compression options
    - Append and overwrite modes
    - Write statistics tracking
    - Delta merge support (upsert)
    
    Example:
        config = ParquetWriterConfig(
            target_path=Path("/lakehouse/silver"),
            table_name="inventory",
            partition_by=["region", "date"],
            compression="snappy",
        )
        
        writer = ParquetWriter(config)
        writer.write(df)
    """
    
    def __init__(self, config: ParquetWriterConfig):
        self.config = config
        self.logger = logging.getLogger(f"writer.{config.table_name}")
        self.metadata = WriteMetadata(
            target_path=str(config.target_path),
            target_table=config.table_name,
            write_timestamp=datetime.utcnow(),
            partition_columns=config.partition_by,
            compression=config.compression,
        )
    
    def write(self, df: pl.DataFrame) -> Path:
        """
        Write DataFrame to Parquet with configured options.
        
        Args:
            df: DataFrame to write
            
        Returns:
            Path where data was written
        """
        start_time = datetime.utcnow()
        
        try:
            target_dir = self.config.target_path / self.config.table_name
            
            # Handle write mode
            if self.config.mode == "overwrite":
                self._clear_existing(target_dir)
            elif self.config.mode == "error" and target_dir.exists():
                raise WriteError(f"Target already exists: {target_dir}")
            
            # Create target directory
            target_dir.mkdir(parents=True, exist_ok=True)
            
            # Write with or without partitioning
            if self.config.partition_by:
                output_path = self._write_partitioned(df, target_dir)
            else:
                output_path = self._write_single(df, target_dir)
            
            # Update metadata
            self.metadata.records_written = len(df)
            self.metadata.status = "success"
            self.metadata.duration_seconds = (
                datetime.utcnow() - start_time
            ).total_seconds()
            
            # Calculate files and bytes written
            self._calculate_write_stats(target_dir)
            
            self.logger.info(
                f"Written {len(df):,} records to {target_dir} "
                f"({self.metadata.files_written} files, "
                f"{self.metadata.bytes_written / 1024 / 1024:.2f} MB) "
                f"in {self.metadata.duration_seconds:.2f}s"
            )
            
            return output_path
            
        except Exception as e:
            self.metadata.status = "failed"
            self.metadata.error_message = str(e)
            self.metadata.duration_seconds = (
                datetime.utcnow() - start_time
            ).total_seconds()
            
            self.logger.error(f"Write failed: {str(e)}")
            raise WriteError(f"Write failed: {str(e)}")
    
    def _write_partitioned(self, df: pl.DataFrame, target_dir: Path) -> Path:
        """Write data with Hive-style partitioning."""
        # Validate partition columns exist
        missing = set(self.config.partition_by) - set(df.columns)
        if missing:
            raise WriteError(f"Partition columns not found: {missing}")
        
        # Check partition cardinality
        unique_partitions = (
            df.select(self.config.partition_by)
            .unique()
            .height
        )
        
        if unique_partitions > self.config.max_partitions:
            raise WriteError(
                f"Too many partitions ({unique_partitions} > {self.config.max_partitions}). "
                "Consider using fewer partition columns or bucketing."
            )
        
        self.logger.info(
            f"Writing {len(df):,} records to {unique_partitions} partitions"
        )
        
        # Write partitioned
        df.write_parquet(
            target_dir,
            partition_by=self.config.partition_by,
            compression=self.config.compression,
            row_group_size=self.config.row_group_size,
            statistics=self.config.statistics,
        )
        
        return target_dir
    
    def _write_single(self, df: pl.DataFrame, target_dir: Path) -> Path:
        """Write data as single file."""
        output_file = target_dir / "data.parquet"
        
        df.write_parquet(
            output_file,
            compression=self.config.compression,
            row_group_size=self.config.row_group_size,
            statistics=self.config.statistics,
        )
        
        return output_file
    
    def _clear_existing(self, target_dir: Path) -> None:
        """Clear existing files in overwrite mode."""
        if target_dir.exists():
            import shutil
            shutil.rmtree(target_dir)
            self.logger.info(f"Cleared existing data at {target_dir}")
    
    def _calculate_write_stats(self, target_dir: Path) -> None:
        """Calculate files and bytes written."""
        files = list(target_dir.rglob("*.parquet"))
        self.metadata.files_written = len(files)
        self.metadata.bytes_written = sum(f.stat().st_size for f in files)
    
    def merge(
        self, 
        df: pl.DataFrame, 
        on: Optional[List[str]] = None,
    ) -> Path:
        """
        Merge (upsert) new data with existing data.
        
        Performs:
        1. Read existing data
        2. Anti-join to find new records
        3. Update matching records
        4. Union and write
        
        Args:
            df: New data to merge
            on: Columns to match on (default: config.delta_merge_columns)
            
        Returns:
            Path where merged data was written
        """
        merge_columns = on or self.config.delta_merge_columns
        
        if not merge_columns:
            raise WriteError("Merge columns not specified")
        
        target_dir = self.config.target_path / self.config.table_name
        
        if not target_dir.exists():
            # No existing data, just write
            return self.write(df)
        
        self.logger.info(f"Merging on columns: {merge_columns}")
        
        # Read existing data
        existing_df = pl.read_parquet(target_dir / "**/*.parquet")
        initial_count = len(existing_df)
        
        # Create merge key for comparison
        existing_keys = existing_df.select(merge_columns)
        new_keys = df.select(merge_columns)
        
        # Find records to update (exist in both)
        updates = df.join(existing_keys, on=merge_columns, how="inner")
        
        # Find new records (only in new data)
        inserts = df.join(existing_keys, on=merge_columns, how="anti")
        
        # Find unchanged records (only in existing, not in new)
        unchanged = existing_df.join(new_keys, on=merge_columns, how="anti")
        
        # Combine: unchanged + updates + inserts
        merged_df = pl.concat([unchanged, updates, inserts])
        
        self.logger.info(
            f"Merge: {len(inserts)} inserts, {len(updates)} updates, "
            f"{len(unchanged)} unchanged ({initial_count} -> {len(merged_df)} total)"
        )
        
        # Write merged data
        return self.write(merged_df)
    
    def append(self, df: pl.DataFrame) -> Path:
        """
        Append new data to existing data.
        
        Creates new partition files without touching existing files.
        """
        if not self.config.partition_by:
            raise WriteError(
                "Append mode requires partitioning. "
                "Use merge() for single-file tables."
            )
        
        target_dir = self.config.target_path / self.config.table_name
        target_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate unique file suffix to avoid conflicts
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
        
        # Write partitioned with unique file names
        for partition_values, partition_df in df.group_by(self.config.partition_by):
            # Build partition path
            partition_path = target_dir
            for col, val in zip(self.config.partition_by, partition_values):
                partition_path = partition_path / f"{col}={val}"
            
            partition_path.mkdir(parents=True, exist_ok=True)
            
            output_file = partition_path / f"data_{timestamp}.parquet"
            partition_df.write_parquet(
                output_file,
                compression=self.config.compression,
                row_group_size=self.config.row_group_size,
            )
        
        self.metadata.records_written = len(df)
        self.logger.info(f"Appended {len(df):,} records to {target_dir}")
        
        return target_dir
    
    def get_metadata(self) -> Dict[str, Any]:
        """Return write metadata as dictionary."""
        return {
            "target_path": self.metadata.target_path,
            "target_table": self.metadata.target_table,
            "write_timestamp": self.metadata.write_timestamp.isoformat(),
            "partition_columns": self.metadata.partition_columns,
            "compression": self.metadata.compression,
            "records_written": self.metadata.records_written,
            "files_written": self.metadata.files_written,
            "bytes_written": self.metadata.bytes_written,
            "duration_seconds": self.metadata.duration_seconds,
            "status": self.metadata.status,
            "error_message": self.metadata.error_message,
        }


class WriteError(Exception):
    """Custom exception for write failures."""
    pass


# Convenience factory functions
def write_to_bronze(
    df: pl.DataFrame,
    lakehouse_path: Path,
    table_name: str,
    extract_date: Optional[str] = None,
) -> Path:
    """Quick write to Bronze layer with date partitioning."""
    if extract_date is None:
        extract_date = datetime.utcnow().strftime("%Y-%m-%d")
    
    # Add extract_date column if not present
    if "extract_date" not in df.columns:
        df = df.with_columns(pl.lit(extract_date).alias("extract_date"))
    
    config = ParquetWriterConfig(
        target_path=lakehouse_path / "bronze",
        table_name=table_name,
        partition_by=["extract_date"],
        compression="snappy",
        mode="append",
    )
    
    writer = ParquetWriter(config)
    return writer.append(df)


def write_to_silver(
    df: pl.DataFrame,
    lakehouse_path: Path,
    table_name: str,
    partition_by: Optional[List[str]] = None,
) -> Path:
    """Quick write to Silver layer."""
    config = ParquetWriterConfig(
        target_path=lakehouse_path / "silver",
        table_name=table_name,
        partition_by=partition_by or [],
        compression="snappy",
        mode="overwrite",
    )
    
    writer = ParquetWriter(config)
    return writer.write(df)


def write_to_gold(
    df: pl.DataFrame,
    lakehouse_path: Path,
    table_name: str,
    partition_by: Optional[List[str]] = None,
) -> Path:
    """Quick write to Gold layer."""
    config = ParquetWriterConfig(
        target_path=lakehouse_path / "gold",
        table_name=table_name,
        partition_by=partition_by or [],
        compression="zstd",  # Better compression for analytics
        mode="overwrite",
    )
    
    writer = ParquetWriter(config)
    return writer.write(df)
