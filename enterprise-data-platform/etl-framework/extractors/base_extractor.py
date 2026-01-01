"""
Abstract Base Extractor

Provides the interface and common functionality for all data extractors.
All extractors must inherit from this class and implement the abstract methods.

Author: Godson Kurishinkal
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
import hashlib
import logging

import polars as pl


@dataclass
class ExtractionMetadata:
    """Metadata captured during extraction for audit and lineage."""
    source_system: str
    extract_timestamp: datetime
    extract_date: str
    file_name: Optional[str] = None
    record_count: int = 0
    duration_seconds: float = 0.0
    status: str = "pending"
    error_message: Optional[str] = None
    checksum: Optional[str] = None


@dataclass
class ExtractorConfig:
    """Base configuration for all extractors."""
    source_name: str
    target_table: str
    bronze_path: Path
    batch_size: int = 100_000
    timeout_seconds: int = 300
    retry_attempts: int = 3
    retry_delay_seconds: int = 5
    add_metadata: bool = True
    add_row_hash: bool = True
    hash_columns: List[str] = field(default_factory=list)
    extra_config: Dict[str, Any] = field(default_factory=dict)


class BaseExtractor(ABC):
    """
    Abstract Base Class for all data extractors.
    
    Provides:
    - Common metadata injection
    - Row-level hashing for deduplication
    - Retry logic framework
    - Logging infrastructure
    - Bronze layer writing
    
    Subclasses must implement:
    - _connect(): Establish connection to source
    - _extract(): Perform the actual extraction
    - _disconnect(): Clean up connection
    """
    
    def __init__(self, config: ExtractorConfig):
        self.config = config
        self.logger = logging.getLogger(f"extractor.{config.source_name}")
        self.metadata = ExtractionMetadata(
            source_system=config.source_name,
            extract_timestamp=datetime.utcnow(),
            extract_date=datetime.utcnow().strftime("%Y-%m-%d"),
        )
        self._connection = None
    
    @abstractmethod
    def _connect(self) -> None:
        """
        Establish connection to the source system.
        
        Raises:
            ConnectionError: If connection cannot be established
        """
        pass
    
    @abstractmethod
    def _extract(self) -> pl.DataFrame:
        """
        Perform the actual data extraction.
        
        Returns:
            pl.DataFrame: Extracted data as a Polars DataFrame
            
        Raises:
            ExtractionError: If extraction fails
        """
        pass
    
    @abstractmethod
    def _disconnect(self) -> None:
        """Clean up connection resources."""
        pass
    
    def extract(self) -> pl.DataFrame:
        """
        Main extraction method with retry logic and metadata handling.
        
        Returns:
            pl.DataFrame: Extracted data with metadata columns
            
        Raises:
            ExtractionError: If extraction fails after all retries
        """
        start_time = datetime.utcnow()
        attempt = 0
        last_error = None
        
        while attempt < self.config.retry_attempts:
            attempt += 1
            try:
                self.logger.info(
                    f"Extraction attempt {attempt}/{self.config.retry_attempts} "
                    f"for {self.config.source_name}"
                )
                
                # Connect to source
                self._connect()
                
                # Perform extraction
                df = self._extract()
                
                # Disconnect
                self._disconnect()
                
                # Add metadata columns
                if self.config.add_metadata:
                    df = self._add_metadata(df)
                
                # Add row hash for deduplication
                if self.config.add_row_hash:
                    df = self._add_row_hash(df)
                
                # Update metadata
                self.metadata.record_count = len(df)
                self.metadata.status = "success"
                self.metadata.duration_seconds = (
                    datetime.utcnow() - start_time
                ).total_seconds()
                
                self.logger.info(
                    f"Extraction complete: {len(df):,} records in "
                    f"{self.metadata.duration_seconds:.2f}s"
                )
                
                return df
                
            except Exception as e:
                last_error = e
                self.logger.warning(
                    f"Extraction attempt {attempt} failed: {str(e)}"
                )
                
                if attempt < self.config.retry_attempts:
                    import time
                    time.sleep(self.config.retry_delay_seconds * attempt)
                
                try:
                    self._disconnect()
                except Exception:
                    pass
        
        # All retries exhausted
        self.metadata.status = "failed"
        self.metadata.error_message = str(last_error)
        self.metadata.duration_seconds = (
            datetime.utcnow() - start_time
        ).total_seconds()
        
        self.logger.error(
            f"Extraction failed after {self.config.retry_attempts} attempts: "
            f"{str(last_error)}"
        )
        raise ExtractionError(
            f"Extraction failed for {self.config.source_name}: {str(last_error)}"
        )
    
    def _add_metadata(self, df: pl.DataFrame) -> pl.DataFrame:
        """Add standard metadata columns to extracted data."""
        return df.with_columns([
            pl.lit(self.metadata.source_system).alias("_source_system"),
            pl.lit(self.metadata.extract_timestamp).alias("_extract_timestamp"),
            pl.lit(self.metadata.extract_date).alias("_extract_date"),
            pl.lit(self.metadata.file_name).alias("_file_name"),
        ])
    
    def _add_row_hash(self, df: pl.DataFrame) -> pl.DataFrame:
        """Add MD5 hash column for row-level deduplication."""
        hash_cols = self.config.hash_columns or df.columns[:3]
        
        # Concatenate hash columns and compute MD5
        hash_expr = pl.concat_str(
            [pl.col(c).cast(pl.Utf8).fill_null("") for c in hash_cols],
            separator="|"
        )
        
        return df.with_columns(
            hash_expr.map_elements(
                lambda x: hashlib.md5(x.encode()).hexdigest(),
                return_dtype=pl.Utf8
            ).alias("_row_hash")
        )
    
    def write_to_bronze(self, df: pl.DataFrame) -> Path:
        """
        Write extracted data to Bronze layer with date partitioning.
        
        Args:
            df: DataFrame to write
            
        Returns:
            Path: Path where data was written
        """
        # Build partition path
        partition_path = (
            self.config.bronze_path 
            / self.config.target_table 
            / f"extract_date={self.metadata.extract_date}"
        )
        partition_path.mkdir(parents=True, exist_ok=True)
        
        # Write as Parquet
        output_file = partition_path / "data.parquet"
        df.write_parquet(
            output_file,
            compression="snappy",
            statistics=True,
        )
        
        # Compute checksum
        self.metadata.checksum = self._compute_file_checksum(output_file)
        
        self.logger.info(f"Written {len(df):,} records to {output_file}")
        return output_file
    
    def _compute_file_checksum(self, file_path: Path) -> str:
        """Compute MD5 checksum of written file."""
        md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                md5.update(chunk)
        return md5.hexdigest()
    
    def get_metadata(self) -> Dict[str, Any]:
        """Return extraction metadata as dictionary."""
        return {
            "source_system": self.metadata.source_system,
            "extract_timestamp": self.metadata.extract_timestamp.isoformat(),
            "extract_date": self.metadata.extract_date,
            "file_name": self.metadata.file_name,
            "record_count": self.metadata.record_count,
            "duration_seconds": self.metadata.duration_seconds,
            "status": self.metadata.status,
            "error_message": self.metadata.error_message,
            "checksum": self.metadata.checksum,
        }


class ExtractionError(Exception):
    """Custom exception for extraction failures."""
    pass
