"""
DuckDB Loader

Loads Parquet data into DuckDB for fast analytical queries.
Supports in-memory and persistent databases with optional Gold layer views.

Author: Godson Kurishinkal
"""

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
import logging

import duckdb
import polars as pl


@dataclass
class DuckDBConfig:
    """Configuration for DuckDB loader."""
    database_path: Optional[Path] = None  # None for in-memory
    read_only: bool = False
    
    # Memory settings
    memory_limit: str = "4GB"
    threads: int = 4
    
    # Extension settings
    load_extensions: List[str] = field(default_factory=lambda: ["parquet", "json"])


class DuckDBLoader:
    """
    DuckDB loader for analytical queries on the data lakehouse.
    
    Features:
    - In-memory or persistent databases
    - Direct Parquet querying
    - View creation from Gold layer
    - SQL analytics support
    - Integration with Polars
    
    Example:
        loader = DuckDBLoader()
        
        # Register Gold layer tables
        loader.register_parquet_table("fact_sales", gold_path / "facts/fact_sales")
        loader.register_parquet_table("dim_product", gold_path / "dimensions/dim_product")
        
        # Query with SQL
        result = loader.query('''
            SELECT dp.category_l1, SUM(fs.quantity) as total_qty
            FROM fact_sales fs
            JOIN dim_product dp ON fs.product_key = dp.product_key
            GROUP BY dp.category_l1
        ''')
    """
    
    def __init__(self, config: Optional[DuckDBConfig] = None):
        self.config = config or DuckDBConfig()
        self.logger = logging.getLogger("duckdb.loader")
        self._conn: Optional[duckdb.DuckDBPyConnection] = None
        self._registered_tables: Dict[str, str] = {}
        
        self._connect()
    
    def _connect(self) -> None:
        """Initialize DuckDB connection."""
        db_path = str(self.config.database_path) if self.config.database_path else ":memory:"
        
        self._conn = duckdb.connect(
            database=db_path,
            read_only=self.config.read_only,
        )
        
        # Configure settings
        self._conn.execute(f"SET memory_limit='{self.config.memory_limit}'")
        self._conn.execute(f"SET threads={self.config.threads}")
        
        # Load extensions
        for ext in self.config.load_extensions:
            try:
                self._conn.execute(f"INSTALL {ext}")
                self._conn.execute(f"LOAD {ext}")
            except Exception as e:
                self.logger.warning(f"Could not load extension {ext}: {e}")
        
        self.logger.info(f"Connected to DuckDB ({db_path})")
    
    def register_parquet_table(
        self, 
        table_name: str, 
        parquet_path: Union[str, Path],
        create_view: bool = True,
    ) -> None:
        """
        Register a Parquet directory/file as a queryable table.
        
        Args:
            table_name: Name for the table
            parquet_path: Path to Parquet file or directory
            create_view: If True, create a VIEW; otherwise use direct path
        """
        path = Path(parquet_path)
        
        if path.is_dir():
            glob_path = str(path / "**/*.parquet")
        else:
            glob_path = str(path)
        
        if create_view:
            # Create a view for cleaner syntax
            self._conn.execute(f"""
                CREATE OR REPLACE VIEW {table_name} AS 
                SELECT * FROM read_parquet('{glob_path}', hive_partitioning=true)
            """)
        
        self._registered_tables[table_name] = glob_path
        self.logger.info(f"Registered table: {table_name} -> {glob_path}")
    
    def register_polars_df(
        self, 
        table_name: str, 
        df: pl.DataFrame,
    ) -> None:
        """Register a Polars DataFrame as a queryable table."""
        # Convert to Arrow for zero-copy sharing
        arrow_table = df.to_arrow()
        self._conn.register(table_name, arrow_table)
        self.logger.info(f"Registered DataFrame as: {table_name}")
    
    def query(
        self, 
        sql: str, 
        params: Optional[Dict[str, Any]] = None,
    ) -> pl.DataFrame:
        """
        Execute SQL query and return results as Polars DataFrame.
        
        Args:
            sql: SQL query string
            params: Optional parameters for parameterized queries
            
        Returns:
            Query results as Polars DataFrame
        """
        try:
            if params:
                result = self._conn.execute(sql, params)
            else:
                result = self._conn.execute(sql)
            
            # Convert to Polars via Arrow
            arrow_table = result.fetch_arrow_table()
            return pl.from_arrow(arrow_table)
            
        except Exception as e:
            self.logger.error(f"Query failed: {str(e)}")
            raise QueryError(f"Query failed: {str(e)}")
    
    def execute(self, sql: str) -> None:
        """Execute SQL statement without returning results."""
        try:
            self._conn.execute(sql)
        except Exception as e:
            self.logger.error(f"Execute failed: {str(e)}")
            raise QueryError(f"Execute failed: {str(e)}")
    
    def load_gold_layer(
        self, 
        gold_path: Path,
        fact_tables: Optional[List[str]] = None,
        dimension_tables: Optional[List[str]] = None,
    ) -> None:
        """
        Load entire Gold layer structure.
        
        Args:
            gold_path: Path to Gold layer root
            fact_tables: List of fact table names to load
            dimension_tables: List of dimension table names to load
        """
        facts_path = gold_path / "facts"
        dims_path = gold_path / "dimensions"
        
        # Load fact tables
        if fact_tables:
            for table in fact_tables:
                table_path = facts_path / table
                if table_path.exists():
                    self.register_parquet_table(table, table_path)
        elif facts_path.exists():
            # Auto-discover fact tables
            for table_path in facts_path.iterdir():
                if table_path.is_dir():
                    self.register_parquet_table(table_path.name, table_path)
        
        # Load dimension tables
        if dimension_tables:
            for table in dimension_tables:
                table_path = dims_path / table
                if table_path.exists():
                    self.register_parquet_table(table, table_path)
        elif dims_path.exists():
            # Auto-discover dimension tables
            for table_path in dims_path.iterdir():
                if table_path.is_dir():
                    self.register_parquet_table(table_path.name, table_path)
        
        self.logger.info(f"Loaded {len(self._registered_tables)} Gold layer tables")
    
    def get_table_info(self, table_name: str) -> pl.DataFrame:
        """Get schema information for a table."""
        return self.query(f"DESCRIBE {table_name}")
    
    def get_table_stats(self, table_name: str) -> Dict[str, Any]:
        """Get statistics for a table."""
        count_result = self.query(f"SELECT COUNT(*) as cnt FROM {table_name}")
        count = count_result["cnt"][0]
        
        return {
            "table_name": table_name,
            "row_count": count,
            "source_path": self._registered_tables.get(table_name),
        }
    
    def list_tables(self) -> List[str]:
        """List all registered tables."""
        return list(self._registered_tables.keys())
    
    def export_to_parquet(
        self, 
        sql: str, 
        output_path: Path,
        compression: str = "snappy",
    ) -> Path:
        """
        Export query results to Parquet file.
        
        Args:
            sql: SQL query
            output_path: Output file path
            compression: Compression codec
            
        Returns:
            Path to written file
        """
        self._conn.execute(f"""
            COPY ({sql}) 
            TO '{output_path}' 
            (FORMAT PARQUET, COMPRESSION {compression})
        """)
        
        self.logger.info(f"Exported query results to {output_path}")
        return output_path
    
    def export_to_csv(
        self, 
        sql: str, 
        output_path: Path,
        header: bool = True,
    ) -> Path:
        """Export query results to CSV file."""
        self._conn.execute(f"""
            COPY ({sql}) 
            TO '{output_path}' 
            (FORMAT CSV, HEADER {header})
        """)
        
        self.logger.info(f"Exported query results to {output_path}")
        return output_path
    
    def create_aggregate_table(
        self,
        table_name: str,
        sql: str,
        materialize: bool = False,
    ) -> None:
        """
        Create an aggregate/summary table.
        
        Args:
            table_name: Name for the aggregate table
            sql: SQL query defining the aggregation
            materialize: If True, create actual table; otherwise create view
        """
        if materialize:
            self._conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS {sql}")
        else:
            self._conn.execute(f"CREATE OR REPLACE VIEW {table_name} AS {sql}")
        
        self.logger.info(f"Created aggregate: {table_name} (materialized={materialize})")
    
    def close(self) -> None:
        """Close DuckDB connection."""
        if self._conn:
            self._conn.close()
            self.logger.info("DuckDB connection closed")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class QueryError(Exception):
    """Custom exception for query failures."""
    pass


# Convenience functions
def query_lakehouse(
    gold_path: Path,
    sql: str,
    tables: Optional[List[str]] = None,
) -> pl.DataFrame:
    """
    Quick function to query the Gold layer.
    
    Example:
        result = query_lakehouse(
            gold_path=Path("/lakehouse/gold"),
            sql="SELECT * FROM fact_sales LIMIT 100"
        )
    """
    with DuckDBLoader() as loader:
        # Auto-register tables mentioned in the query
        if tables:
            for table in tables:
                table_path = gold_path / "facts" / table
                if not table_path.exists():
                    table_path = gold_path / "dimensions" / table
                if table_path.exists():
                    loader.register_parquet_table(table, table_path)
        
        return loader.query(sql)


def create_analytics_db(
    gold_path: Path,
    output_db: Path,
) -> DuckDBLoader:
    """
    Create a persistent DuckDB database from Gold layer.
    
    Useful for sharing analytics database with BI tools.
    """
    config = DuckDBConfig(database_path=output_db)
    loader = DuckDBLoader(config)
    loader.load_gold_layer(gold_path)
    
    return loader
