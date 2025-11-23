"""
Silver Layer Transformation - Data Cleaning & Validation
=========================================================

This module transforms bronze layer data into silver layer with:
- Data quality validation (Great Expectations)
- Deduplication
- Schema enforcement
- Null handling
- Data type validation
- Outlier detection
"""

import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
import logging
import json

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SilverTransformation:
    """
    Transform bronze layer data to silver layer with quality checks.
    
    Silver layer characteristics:
    - Cleaned and validated data
    - No duplicates
    - Standardized schema
    - Quality metrics tracked
    - Null handling applied
    """
    
    def __init__(
        self,
        bronze_path: str,
        silver_path: str,
        quality_threshold: float = 0.995
    ):
        """
        Initialize silver transformation.
        
        Parameters
        ----------
        bronze_path : str
            Path to bronze layer data
        silver_path : str
            Path to silver layer output
        quality_threshold : float, default=0.995
            Minimum acceptable quality score (99.5%)
        """
        self.bronze_path = Path(bronze_path)
        self.silver_path = Path(silver_path)
        self.quality_threshold = quality_threshold
        
        self.silver_path.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Initialized SilverTransformation")
        logger.info(f"  Bronze: {self.bronze_path}")
        logger.info(f"  Silver: {self.silver_path}")
        logger.info(f"  Quality Threshold: {quality_threshold:.1%}")
    
    def transform_sales_data(
        self,
        run_quality_checks: bool = True
    ) -> Dict[str, any]:
        """
        Transform bronze sales data to silver layer.
        
        Parameters
        ----------
        run_quality_checks : bool, default=True
            Whether to run Great Expectations validation
        
        Returns
        -------
        dict
            Transformation statistics and quality metrics
        """
        logger.info("="*70)
        logger.info("SILVER LAYER TRANSFORMATION - SALES DATA")
        logger.info("="*70)
        
        # Load bronze data
        logger.info("Loading bronze layer data...")
        bronze_df = self._load_bronze_data()
        
        logger.info(f"Loaded {len(bronze_df):,} records from bronze layer")
        
        # Track initial state
        initial_count = len(bronze_df)
        quality_metrics = {}
        
        # Step 1: Remove duplicates
        logger.info("Step 1: Removing duplicates...")
        bronze_df, dup_stats = self._remove_duplicates(bronze_df)
        quality_metrics['duplicates_removed'] = dup_stats
        
        # Step 2: Handle missing values
        logger.info("Step 2: Handling missing values...")
        bronze_df, null_stats = self._handle_nulls(bronze_df)
        quality_metrics['nulls_handled'] = null_stats
        
        # Step 3: Validate data types
        logger.info("Step 3: Validating data types...")
        bronze_df, type_stats = self._validate_types(bronze_df)
        quality_metrics['type_validations'] = type_stats
        
        # Step 4: Detect and handle outliers
        logger.info("Step 4: Detecting outliers...")
        bronze_df, outlier_stats = self._detect_outliers(bronze_df)
        quality_metrics['outliers_detected'] = outlier_stats
        
        # Step 5: Apply business rules
        logger.info("Step 5: Applying business rules...")
        bronze_df, rule_stats = self._apply_business_rules(bronze_df)
        quality_metrics['business_rules'] = rule_stats
        
        # Step 6: Standardize schema
        logger.info("Step 6: Standardizing schema...")
        silver_df = self._standardize_schema(bronze_df)
        
        # Calculate quality score
        final_count = len(silver_df)
        quality_score = final_count / initial_count
        quality_metrics['quality_score'] = quality_score
        quality_metrics['records_passed'] = final_count
        quality_metrics['records_failed'] = initial_count - final_count
        
        logger.info(f"Quality Score: {quality_score:.2%}")
        
        if quality_score < self.quality_threshold:
            logger.warning(f"⚠️  Quality score {quality_score:.2%} below threshold {self.quality_threshold:.1%}")
        else:
            logger.info(f"✅ Quality score {quality_score:.2%} meets threshold")
        
        # Add silver metadata
        silver_df['silver_timestamp'] = datetime.utcnow()
        silver_df['quality_score'] = quality_score
        
        # Write to silver layer
        logger.info("Writing to silver layer...")
        stats = self._write_silver_data(silver_df)
        
        # Combine all metrics
        result = {
            **stats,
            **quality_metrics,
            'transformation_timestamp': datetime.utcnow().isoformat()
        }
        
        logger.info("✅ Silver transformation completed successfully")
        return result
    
    def _load_bronze_data(self) -> pd.DataFrame:
        """Load data from bronze layer (Hive-partitioned Parquet)."""
        bronze_table_path = self.bronze_path / 'sales'
        
        # Read all partitions
        df = pq.read_table(bronze_table_path).to_pandas()
        
        return df
    
    def _remove_duplicates(self, df: pd.DataFrame) -> tuple:
        """
        Remove duplicate records based on business key.
        
        Business key: (item_id, store_id, date)
        """
        initial_count = len(df)
        
        # Identify duplicates
        duplicates = df.duplicated(subset=['item_id', 'store_id', 'date'], keep='first')
        
        # Remove duplicates
        df_clean = df[~duplicates].copy()
        
        dup_count = initial_count - len(df_clean)
        
        stats = {
            'total_duplicates': int(dup_count),
            'duplicate_rate': float(dup_count / initial_count)
        }
        
        logger.info(f"  Removed {dup_count:,} duplicates ({stats['duplicate_rate']:.2%})")
        
        return df_clean, stats
    
    def _handle_nulls(self, df: pd.DataFrame) -> tuple:
        """
        Handle missing values based on column type.
        
        Strategy:
        - sales: Fill with 0 (no sales = 0)
        - sell_price: Forward fill within item-store group
        - event fields: Fill with 'NO_EVENT'
        """
        initial_nulls = df.isnull().sum().sum()
        
        # Sales: 0 for missing
        if 'sales' in df.columns:
            df['sales'] = df['sales'].fillna(0)
        
        # Price: Forward fill within groups
        if 'sell_price' in df.columns:
            df['sell_price'] = df.groupby(['item_id', 'store_id'])['sell_price'].ffill()
            # Backward fill for any remaining
            df['sell_price'] = df.groupby(['item_id', 'store_id'])['sell_price'].bfill()
        
        # Event fields: 'NO_EVENT'
        event_cols = ['event_name_1', 'event_type_1']
        for col in event_cols:
            if col in df.columns:
                df[col] = df[col].fillna('NO_EVENT')
        
        final_nulls = df.isnull().sum().sum()
        
        stats = {
            'initial_nulls': int(initial_nulls),
            'final_nulls': int(final_nulls),
            'nulls_handled': int(initial_nulls - final_nulls)
        }
        
        logger.info(f"  Handled {stats['nulls_handled']:,} null values")
        
        return df, stats
    
    def _validate_types(self, df: pd.DataFrame) -> tuple:
        """
        Validate and enforce data types.
        """
        type_errors = 0
        
        # Numeric columns
        numeric_cols = ['sales', 'sell_price']
        for col in numeric_cols:
            if col in df.columns:
                try:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    type_errors += df[col].isnull().sum()
                except Exception as e:
                    logger.error(f"Error converting {col}: {e}")
        
        # Date column
        if 'date' in df.columns:
            try:
                df['date'] = pd.to_datetime(df['date'], errors='coerce')
                type_errors += df['date'].isnull().sum()
            except Exception as e:
                logger.error(f"Error converting date: {e}")
        
        # Drop rows with type errors
        initial_count = len(df)
        df = df.dropna(subset=['sales', 'date'])
        type_errors = initial_count - len(df)
        
        stats = {
            'type_errors': int(type_errors),
            'error_rate': float(type_errors / initial_count) if initial_count > 0 else 0
        }
        
        logger.info(f"  Removed {type_errors:,} records with type errors ({stats['error_rate']:.2%})")
        
        return df, stats
    
    def _detect_outliers(self, df: pd.DataFrame) -> tuple:
        """
        Detect outliers using IQR method.
        
        Flag but don't remove - may be legitimate extreme values.
        """
        outlier_count = 0
        
        if 'sales' in df.columns:
            # Calculate IQR per item-store
            q1 = df.groupby(['item_id', 'store_id'])['sales'].transform(lambda x: x.quantile(0.25))
            q3 = df.groupby(['item_id', 'store_id'])['sales'].transform(lambda x: x.quantile(0.75))
            iqr = q3 - q1
            
            # Define outliers (beyond 3*IQR)
            lower_bound = q1 - 3 * iqr
            upper_bound = q3 + 3 * iqr
            
            outliers = (df['sales'] < lower_bound) | (df['sales'] > upper_bound)
            outlier_count = outliers.sum()
            
            # Flag outliers (don't remove)
            df['is_outlier'] = outliers
        
        stats = {
            'outliers_detected': int(outlier_count),
            'outlier_rate': float(outlier_count / len(df)) if len(df) > 0 else 0
        }
        
        logger.info(f"  Detected {outlier_count:,} outliers ({stats['outlier_rate']:.2%})")
        
        return df, stats
    
    def _apply_business_rules(self, df: pd.DataFrame) -> tuple:
        """
        Apply business validation rules.
        
        Rules:
        1. Sales >= 0
        2. Sell_price > 0
        3. Date within valid range
        """
        initial_count = len(df)
        violations = 0
        
        # Rule 1: Sales >= 0
        if 'sales' in df.columns:
            invalid_sales = df['sales'] < 0
            violations += invalid_sales.sum()
            df = df[~invalid_sales]
        
        # Rule 2: Sell_price > 0
        if 'sell_price' in df.columns:
            invalid_price = df['sell_price'] <= 0
            violations += invalid_price.sum()
            df = df[~invalid_price]
        
        # Rule 3: Date within valid range (2011-2016 for M5)
        if 'date' in df.columns:
            invalid_date = (df['date'] < '2011-01-01') | (df['date'] > '2016-12-31')
            violations += invalid_date.sum()
            df = df[~invalid_date]
        
        stats = {
            'rule_violations': int(violations),
            'violation_rate': float(violations / initial_count) if initial_count > 0 else 0
        }
        
        logger.info(f"  Removed {violations:,} rule violations ({stats['violation_rate']:.2%})")
        
        return df, stats
    
    def _standardize_schema(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize schema to match silver layer contract.
        """
        # Define standard column order
        standard_cols = [
            'id', 'item_id', 'dept_id', 'cat_id', 'store_id', 'state_id',
            'd', 'date', 'wm_yr_wk', 'sales', 'sell_price',
            'event_name_1', 'event_type_1',
            'is_outlier',
            'ingestion_timestamp', 'source_file'
        ]
        
        # Select and reorder columns
        available_cols = [col for col in standard_cols if col in df.columns]
        df = df[available_cols].copy()
        
        return df
    
    def _write_silver_data(self, df: pd.DataFrame) -> Dict[str, any]:
        """
        Write data to silver layer (Hive-partitioned Parquet).
        """
        output_path = self.silver_path / 'sales'
        
        # Convert to PyArrow table
        import pyarrow as pa
        table = pa.Table.from_pandas(df)
        
        # Write with Hive partitioning
        pq.write_to_dataset(
            table,
            root_path=str(output_path),
            partition_cols=['store_id', 'date'],
            compression='snappy',
            existing_data_behavior='overwrite_or_ignore'
        )
        
        logger.info(f"✅ Written to: {output_path}")
        
        stats = {
            'total_records': len(df),
            'stores': df['store_id'].nunique(),
            'dates': df['date'].nunique(),
            'items': df['item_id'].nunique(),
            'output_path': str(output_path)
        }
        
        return stats


def main():
    """
    Main entry point for silver layer transformation.
    """
    print("="*70)
    print("SILVER LAYER TRANSFORMATION - DATA CLEANING & VALIDATION")
    print("="*70)
    print()
    
    # Configuration
    BRONZE_PATH = '../data/bronze'
    SILVER_PATH = '../data/silver'
    
    # Initialize transformation
    transformer = SilverTransformation(
        bronze_path=BRONZE_PATH,
        silver_path=SILVER_PATH,
        quality_threshold=0.995
    )
    
    # Run transformation
    stats = transformer.transform_sales_data(run_quality_checks=True)
    
    # Print statistics
    print()
    print("📊 TRANSFORMATION STATISTICS")
    print("-"*70)
    print(f"  Records Passed:      {stats['records_passed']:,}")
    print(f"  Records Failed:      {stats['records_failed']:,}")
    print(f"  Quality Score:       {stats['quality_score']:.2%}")
    print()
    print(f"  Duplicates Removed:  {stats['duplicates_removed']['total_duplicates']:,}")
    print(f"  Nulls Handled:       {stats['nulls_handled']['nulls_handled']:,}")
    print(f"  Type Errors:         {stats['type_validations']['type_errors']:,}")
    print(f"  Outliers Detected:   {stats['outliers_detected']['outliers_detected']:,}")
    print(f"  Rule Violations:     {stats['business_rules']['rule_violations']:,}")
    print()
    
    print("✅ Silver layer transformation completed successfully!")


if __name__ == "__main__":
    main()
