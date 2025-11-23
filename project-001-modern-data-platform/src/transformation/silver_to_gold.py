"""
Gold Layer Feature Engineering
===============================

This module transforms silver layer data into gold layer with:
- Daily sales aggregations
- Weekly demand patterns
- Inventory metrics
- Warehouse performance
- Revenue calculations
- Feature engineering for ML models
"""

import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from datetime import datetime
from typing import Dict, List
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class GoldTransformation:
    """
    Transform silver layer data to gold layer with feature engineering.
    
    Gold layer characteristics:
    - Business-level aggregations
    - Feature-engineered tables
    - ML-ready datasets
    - Denormalized for performance
    """
    
    def __init__(
        self,
        silver_path: str,
        gold_path: str
    ):
        """
        Initialize gold transformation.
        
        Parameters
        ----------
        silver_path : str
            Path to silver layer data
        gold_path : str
            Path to gold layer output
        """
        self.silver_path = Path(silver_path)
        self.gold_path = Path(gold_path)
        
        self.gold_path.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Initialized GoldTransformation")
        logger.info(f"  Silver: {self.silver_path}")
        logger.info(f"  Gold: {self.gold_path}")
    
    def create_gold_tables(self) -> Dict[str, any]:
        """
        Create all gold layer tables.
        
        Returns
        -------
        dict
            Statistics for each gold table
        """
        logger.info("="*70)
        logger.info("GOLD LAYER FEATURE ENGINEERING")
        logger.info("="*70)
        
        # Load silver data
        logger.info("Loading silver layer data...")
        silver_df = self._load_silver_data()
        
        logger.info(f"Loaded {len(silver_df):,} records from silver layer")
        
        stats = {}
        
        # Create gold tables
        logger.info("\nCreating gold layer tables...")
        
        # Table 1: Daily Sales Aggregation
        logger.info("1. Creating daily_sales_agg...")
        stats['daily_sales_agg'] = self._create_daily_sales_agg(silver_df)
        
        # Table 2: Weekly Demand Patterns
        logger.info("2. Creating weekly_demand_patterns...")
        stats['weekly_demand_patterns'] = self._create_weekly_demand_patterns(silver_df)
        
        # Table 3: Inventory Metrics
        logger.info("3. Creating inventory_metrics...")
        stats['inventory_metrics'] = self._create_inventory_metrics(silver_df)
        
        # Table 4: Item Performance
        logger.info("4. Creating item_performance...")
        stats['item_performance'] = self._create_item_performance(silver_df)
        
        # Table 5: Store Performance
        logger.info("5. Creating store_performance...")
        stats['store_performance'] = self._create_store_performance(silver_df)
        
        logger.info("\n✅ All gold tables created successfully")
        
        return stats
    
    def _load_silver_data(self) -> pd.DataFrame:
        """Load data from silver layer."""
        silver_table_path = self.silver_path / 'sales'
        
        df = pq.read_table(silver_table_path).to_pandas()
        
        # Ensure date is datetime
        df['date'] = pd.to_datetime(df['date'])
        
        return df
    
    def _create_daily_sales_agg(self, df: pd.DataFrame) -> Dict:
        """
        Create daily sales aggregation table.
        
        Columns:
        - date, store_id, item_id
        - total_sales, total_revenue
        - avg_price, min_price, max_price
        - days_in_stock (sales > 0)
        """
        logger.info("  Aggregating daily sales...")
        
        # Calculate revenue
        df['revenue'] = df['sales'] * df['sell_price']
        
        # Aggregate
        agg_df = df.groupby(['date', 'store_id', 'item_id']).agg({
            'sales': 'sum',
            'revenue': 'sum',
            'sell_price': ['mean', 'min', 'max']
        }).reset_index()
        
        # Flatten column names
        agg_df.columns = ['date', 'store_id', 'item_id', 'total_sales', 'total_revenue',
                          'avg_price', 'min_price', 'max_price']
        
        # Add derived metrics
        agg_df['days_in_stock'] = (agg_df['total_sales'] > 0).astype(int)
        
        # Add timestamp
        agg_df['created_at'] = datetime.utcnow()
        
        # Write to gold
        output_path = self.gold_path / 'daily_sales_agg'
        self._write_gold_table(agg_df, output_path, partition_cols=['date'])
        
        stats = {
            'total_records': len(agg_df),
            'date_range': f"{agg_df['date'].min()} to {agg_df['date'].max()}",
            'total_revenue': f"${agg_df['total_revenue'].sum():,.2f}"
        }
        
        logger.info(f"  ✅ Created daily_sales_agg: {len(agg_df):,} records")
        
        return stats
    
    def _create_weekly_demand_patterns(self, df: pd.DataFrame) -> Dict:
        """
        Create weekly demand pattern analysis.
        
        Features:
        - Weekly sales statistics
        - Demand variability (CV)
        - Trend indicators
        - Seasonality flags
        """
        logger.info("  Analyzing weekly demand patterns...")
        
        # Add week information
        df['year_week'] = df['date'].dt.strftime('%Y-W%U')
        
        # Aggregate by week
        weekly_df = df.groupby(['year_week', 'store_id', 'item_id']).agg({
            'sales': ['sum', 'mean', 'std', 'min', 'max'],
            'sell_price': 'mean',
            'date': ['min', 'max']
        }).reset_index()
        
        # Flatten columns
        weekly_df.columns = ['year_week', 'store_id', 'item_id',
                            'total_sales', 'avg_daily_sales', 'std_sales', 'min_sales', 'max_sales',
                            'avg_price', 'week_start', 'week_end']
        
        # Calculate demand variability (CV)
        weekly_df['demand_cv'] = weekly_df['std_sales'] / (weekly_df['avg_daily_sales'] + 1e-10)
        weekly_df['demand_cv'] = weekly_df['demand_cv'].fillna(0)
        
        # Flag high variability weeks
        weekly_df['is_high_variability'] = (weekly_df['demand_cv'] > 1.0).astype(int)
        
        # Add timestamp
        weekly_df['created_at'] = datetime.utcnow()
        
        # Write to gold
        output_path = self.gold_path / 'weekly_demand_patterns'
        self._write_gold_table(weekly_df, output_path)
        
        stats = {
            'total_records': len(weekly_df),
            'weeks_analyzed': weekly_df['year_week'].nunique(),
            'avg_demand_cv': f"{weekly_df['demand_cv'].mean():.3f}"
        }
        
        logger.info(f"  ✅ Created weekly_demand_patterns: {len(weekly_df):,} records")
        
        return stats
    
    def _create_inventory_metrics(self, df: pd.DataFrame) -> Dict:
        """
        Create inventory performance metrics.
        
        Metrics:
        - Average inventory (proxy: days with sales)
        - Stockout frequency
        - Service level
        - Inventory turnover (proxy)
        """
        logger.info("  Calculating inventory metrics...")
        
        # Calculate per item-store
        inventory_df = df.groupby(['item_id', 'store_id']).agg({
            'sales': ['sum', 'mean', 'count'],
            'date': ['min', 'max']
        }).reset_index()
        
        # Flatten columns
        inventory_df.columns = ['item_id', 'store_id',
                               'total_sales', 'avg_daily_sales', 'days_observed',
                               'first_date', 'last_date']
        
        # Calculate metrics
        inventory_df['days_span'] = (inventory_df['last_date'] - inventory_df['first_date']).dt.days + 1
        
        # Stockout frequency (days with zero sales / total days)
        zero_sales_df = df[df['sales'] == 0].groupby(['item_id', 'store_id']).size().reset_index(name='zero_sales_days')
        inventory_df = inventory_df.merge(zero_sales_df, on=['item_id', 'store_id'], how='left')
        inventory_df['zero_sales_days'] = inventory_df['zero_sales_days'].fillna(0)
        
        inventory_df['stockout_frequency'] = inventory_df['zero_sales_days'] / inventory_df['days_span']
        
        # Service level (1 - stockout frequency)
        inventory_df['service_level'] = 1 - inventory_df['stockout_frequency']
        
        # Inventory turnover (proxy: total sales / avg daily sales)
        inventory_df['inventory_turnover_proxy'] = inventory_df['total_sales'] / (inventory_df['avg_daily_sales'] * 365 + 1e-10)
        
        # Add timestamp
        inventory_df['created_at'] = datetime.utcnow()
        
        # Write to gold
        output_path = self.gold_path / 'inventory_metrics'
        self._write_gold_table(inventory_df, output_path)
        
        stats = {
            'total_records': len(inventory_df),
            'avg_service_level': f"{inventory_df['service_level'].mean():.2%}",
            'avg_stockout_frequency': f"{inventory_df['stockout_frequency'].mean():.2%}"
        }
        
        logger.info(f"  ✅ Created inventory_metrics: {len(inventory_df):,} records")
        
        return stats
    
    def _create_item_performance(self, df: pd.DataFrame) -> Dict:
        """
        Create item-level performance summary.
        """
        logger.info("  Analyzing item performance...")
        
        # Calculate revenue
        df['revenue'] = df['sales'] * df['sell_price']
        
        # Aggregate by item
        item_df = df.groupby('item_id').agg({
            'sales': ['sum', 'mean'],
            'revenue': 'sum',
            'store_id': 'nunique',
            'date': ['min', 'max']
        }).reset_index()
        
        # Flatten columns
        item_df.columns = ['item_id', 'total_sales', 'avg_daily_sales',
                          'total_revenue', 'num_stores', 'first_date', 'last_date']
        
        # Calculate revenue rank (ABC classification proxy)
        item_df['revenue_rank'] = item_df['total_revenue'].rank(ascending=False, method='dense')
        total_items = len(item_df)
        
        # ABC classification
        item_df['abc_class'] = 'C'
        item_df.loc[item_df['revenue_rank'] <= total_items * 0.2, 'abc_class'] = 'A'
        item_df.loc[(item_df['revenue_rank'] > total_items * 0.2) & 
                    (item_df['revenue_rank'] <= total_items * 0.5), 'abc_class'] = 'B'
        
        # Add timestamp
        item_df['created_at'] = datetime.utcnow()
        
        # Write to gold
        output_path = self.gold_path / 'item_performance'
        self._write_gold_table(item_df, output_path)
        
        stats = {
            'total_items': len(item_df),
            'class_a_items': int((item_df['abc_class'] == 'A').sum()),
            'class_b_items': int((item_df['abc_class'] == 'B').sum()),
            'class_c_items': int((item_df['abc_class'] == 'C').sum())
        }
        
        logger.info(f"  ✅ Created item_performance: {len(item_df):,} items")
        
        return stats
    
    def _create_store_performance(self, df: pd.DataFrame) -> Dict:
        """
        Create store-level performance summary.
        """
        logger.info("  Analyzing store performance...")
        
        # Calculate revenue
        df['revenue'] = df['sales'] * df['sell_price']
        
        # Aggregate by store
        store_df = df.groupby('store_id').agg({
            'sales': ['sum', 'mean'],
            'revenue': 'sum',
            'item_id': 'nunique',
            'date': ['min', 'max']
        }).reset_index()
        
        # Flatten columns
        store_df.columns = ['store_id', 'total_sales', 'avg_daily_sales',
                           'total_revenue', 'num_items', 'first_date', 'last_date']
        
        # Calculate days in operation
        store_df['days_operation'] = (store_df['last_date'] - store_df['first_date']).dt.days + 1
        
        # Revenue per day
        store_df['revenue_per_day'] = store_df['total_revenue'] / store_df['days_operation']
        
        # Add timestamp
        store_df['created_at'] = datetime.utcnow()
        
        # Write to gold
        output_path = self.gold_path / 'store_performance'
        self._write_gold_table(store_df, output_path)
        
        stats = {
            'total_stores': len(store_df),
            'total_revenue': f"${store_df['total_revenue'].sum():,.2f}",
            'avg_revenue_per_store': f"${store_df['total_revenue'].mean():,.2f}"
        }
        
        logger.info(f"  ✅ Created store_performance: {len(store_df):,} stores")
        
        return stats
    
    def _write_gold_table(
        self,
        df: pd.DataFrame,
        output_path: Path,
        partition_cols: List[str] = None
    ):
        """Write DataFrame to gold layer as Parquet."""
        # Convert to PyArrow table
        table = pa.Table.from_pandas(df)
        
        if partition_cols:
            # Write with partitioning
            pq.write_to_dataset(
                table,
                root_path=str(output_path),
                partition_cols=partition_cols,
                compression='snappy',
                existing_data_behavior='overwrite_or_ignore'
            )
        else:
            # Write as single file
            output_path.mkdir(parents=True, exist_ok=True)
            pq.write_table(
                table,
                str(output_path / 'data.parquet'),
                compression='snappy'
            )
        
        logger.info(f"    Written to: {output_path}")


def main():
    """
    Main entry point for gold layer feature engineering.
    """
    print("="*70)
    print("GOLD LAYER FEATURE ENGINEERING")
    print("="*70)
    print()
    
    # Configuration
    SILVER_PATH = '../data/silver'
    GOLD_PATH = '../data/gold'
    
    # Initialize transformation
    transformer = GoldTransformation(
        silver_path=SILVER_PATH,
        gold_path=GOLD_PATH
    )
    
    # Create gold tables
    stats = transformer.create_gold_tables()
    
    # Print statistics
    print()
    print("📊 GOLD LAYER STATISTICS")
    print("-"*70)
    
    for table_name, table_stats in stats.items():
        print(f"\n{table_name}:")
        for key, value in table_stats.items():
            print(f"  {key}: {value}")
    
    print()
    print("✅ Gold layer feature engineering completed successfully!")


if __name__ == "__main__":
    main()
