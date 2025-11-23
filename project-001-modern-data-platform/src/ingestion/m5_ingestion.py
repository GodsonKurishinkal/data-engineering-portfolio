"""
Bronze Layer Ingestion - M5 Walmart Data
==========================================

This module handles raw data ingestion from M5 Walmart dataset into
Hive-partitioned Parquet files in the bronze layer.

Features:
- Reads raw CSV files from M5 dataset
- Partitions by store_id and date
- Preserves source schema
- Tracks data lineage
- Appends ingestion metadata
"""

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, List
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class M5DataIngestion:
    """
    Ingest M5 Walmart data into bronze layer with Hive partitioning.
    
    Bronze layer characteristics:
    - Raw data preservation
    - Minimal transformation
    - Hive-partitioned by store_id and date
    - Immutable (append-only)
    - Full data lineage
    """
    
    def __init__(
        self,
        source_path: str,
        bronze_path: str,
        compression: str = 'snappy'
    ):
        """
        Initialize M5 data ingestion.
        
        Parameters
        ----------
        source_path : str
            Path to source M5 CSV files
        bronze_path : str
            Path to bronze layer output
        compression : str, default='snappy'
            Parquet compression codec
        """
        self.source_path = Path(source_path)
        self.bronze_path = Path(bronze_path)
        self.compression = compression
        
        # Create bronze directories if they don't exist
        self.bronze_path.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Initialized M5DataIngestion")
        logger.info(f"  Source: {self.source_path}")
        logger.info(f"  Bronze: {self.bronze_path}")
    
    def ingest_sales_data(
        self,
        validation_mode: bool = True,
        max_rows: Optional[int] = None
    ) -> Dict[str, int]:
        """
        Ingest M5 sales data into bronze layer.
        
        Parameters
        ----------
        validation_mode : bool, default=True
            If True, loads validation dataset. If False, loads evaluation dataset.
        max_rows : int, optional
            Maximum rows to ingest (for testing)
        
        Returns
        -------
        dict
            Ingestion statistics
        """
        logger.info("="*70)
        logger.info("BRONZE LAYER INGESTION - M5 SALES DATA")
        logger.info("="*70)
        
        # Load sales data
        sales_file = 'sales_train_validation.csv' if validation_mode else 'sales_train_evaluation.csv'
        sales_path = self.source_path / sales_file
        
        logger.info(f"Loading sales data from: {sales_path}")
        sales_df = pd.read_csv(sales_path, nrows=max_rows)
        
        logger.info(f"Loaded {len(sales_df):,} products")
        
        # Load calendar data
        calendar_path = self.source_path / 'calendar.csv'
        calendar_df = pd.read_csv(calendar_path)
        logger.info(f"Loaded {len(calendar_df):,} calendar days")
        
        # Load prices data
        prices_path = self.source_path / 'sell_prices.csv'
        prices_df = pd.read_csv(prices_path)
        logger.info(f"Loaded {len(prices_df):,} price records")
        
        # Transform to long format
        logger.info("Transforming sales data to long format...")
        sales_long = self._melt_sales_data(sales_df)
        
        # Merge with calendar and prices
        logger.info("Merging with calendar and prices...")
        merged_df = self._merge_datasets(sales_long, calendar_df, prices_df)
        
        # Add metadata
        merged_df['ingestion_timestamp'] = datetime.utcnow()
        merged_df['source_file'] = sales_file
        
        logger.info(f"Final dataset: {len(merged_df):,} records")
        
        # Write to bronze layer with Hive partitioning
        logger.info("Writing to bronze layer (Hive-partitioned)...")
        stats = self._write_hive_partitioned(
            merged_df,
            partition_cols=['store_id', 'date'],
            table_name='sales'
        )
        
        logger.info("✅ Bronze ingestion completed successfully")
        return stats
    
    def _melt_sales_data(self, sales_df: pd.DataFrame) -> pd.DataFrame:
        """
        Transform sales data from wide to long format.
        
        Wide format: id, item_id, d_1, d_2, d_3, ...
        Long format: id, item_id, d, sales
        """
        # Identify columns
        id_cols = ['id', 'item_id', 'dept_id', 'cat_id', 'store_id', 'state_id']
        value_cols = [col for col in sales_df.columns if col.startswith('d_')]
        
        # Melt to long format
        sales_long = pd.melt(
            sales_df,
            id_vars=id_cols,
            value_vars=value_cols,
            var_name='d',
            value_name='sales'
        )
        
        return sales_long
    
    def _merge_datasets(
        self,
        sales: pd.DataFrame,
        calendar: pd.DataFrame,
        prices: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Merge sales, calendar, and price data.
        """
        # Merge sales with calendar
        merged = sales.merge(
            calendar[['d', 'date', 'wm_yr_wk', 'event_name_1', 'event_type_1']],
            on='d',
            how='left'
        )
        
        # Merge with prices
        merged = merged.merge(
            prices[['store_id', 'item_id', 'wm_yr_wk', 'sell_price']],
            on=['store_id', 'item_id', 'wm_yr_wk'],
            how='left'
        )
        
        # Convert date to datetime
        merged['date'] = pd.to_datetime(merged['date'])
        
        return merged
    
    def _write_hive_partitioned(
        self,
        df: pd.DataFrame,
        partition_cols: List[str],
        table_name: str
    ) -> Dict[str, int]:
        """
        Write DataFrame to Hive-partitioned Parquet files.
        
        Structure: bronze/sales/store_id={CA_1}/date={2024-01-01}/*.parquet
        """
        output_path = self.bronze_path / table_name
        
        # Convert to PyArrow table
        table = pa.Table.from_pandas(df)
        
        # Write with Hive partitioning
        pq.write_to_dataset(
            table,
            root_path=str(output_path),
            partition_cols=partition_cols,
            compression=self.compression,
            existing_data_behavior='overwrite_or_ignore'
        )
        
        logger.info(f"✅ Written to: {output_path}")
        logger.info(f"   Partitioning: {' / '.join(partition_cols)}")
        logger.info(f"   Compression: {self.compression}")
        
        # Calculate statistics
        stats = {
            'total_records': len(df),
            'stores': df['store_id'].nunique(),
            'dates': df['date'].nunique(),
            'items': df['item_id'].nunique()
        }
        
        return stats


def main():
    """
    Main entry point for bronze layer ingestion.
    """
    print("="*70)
    print("M5 WALMART DATA - BRONZE LAYER INGESTION")
    print("="*70)
    print()
    
    # Configuration
    SOURCE_PATH = '../../../data-science-portfolio/demand-forecasting-system/data/raw'
    BRONZE_PATH = '../data/bronze'
    
    # Initialize ingestion
    ingestion = M5DataIngestion(
        source_path=SOURCE_PATH,
        bronze_path=BRONZE_PATH
    )
    
    # Run ingestion (with sample for testing)
    stats = ingestion.ingest_sales_data(
        validation_mode=True,
        max_rows=100  # Remove this for full ingestion
    )
    
    # Print statistics
    print()
    print("📊 INGESTION STATISTICS")
    print("-"*70)
    for key, value in stats.items():
        print(f"  {key}: {value:,}")
    print()
    
    print("✅ Bronze layer ingestion completed successfully!")


if __name__ == "__main__":
    main()
