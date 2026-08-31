"""
Database Extractor

Extracts data from relational databases (SQL Server, Oracle, PostgreSQL).
Supports both full and incremental extraction patterns.

Author: Godson Kurishinkal
"""

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
import logging

import polars as pl

from .base_extractor import BaseExtractor, ExtractorConfig, ExtractionError


@dataclass
class DatabaseConfig(ExtractorConfig):
    """Configuration for database extraction."""
    # Connection settings
    driver: str = "ODBC Driver 17 for SQL Server"
    host: str = ""
    port: int = 1433
    database: str = ""
    username: str = ""
    password: str = ""  # Should be loaded from vault in production
    
    # Extraction settings
    query: Optional[str] = None
    table_name: Optional[str] = None
    columns: List[str] = field(default_factory=list)
    where_clause: Optional[str] = None
    
    # Incremental settings
    incremental: bool = False
    incremental_column: str = "modified_date"
    last_extracted_value: Optional[str] = None
    
    # Performance settings
    fetch_size: int = 10_000
    enable_fast_executemany: bool = True


class DatabaseExtractor(BaseExtractor):
    """
    Extractor for relational databases.
    
    Supports:
    - SQL Server, Oracle, PostgreSQL via pyodbc
    - Full and incremental extraction
    - Custom SQL queries or table-based extraction
    - Chunked reading for large tables
    
    Example:
        config = DatabaseConfig(
            source_name="erp",
            target_table="inventory",
            bronze_path=Path("/lakehouse/bronze"),
            host="erp-db.company.com",
            database="ERP_PROD",
            username="etl_user",
            password=os.getenv("ERP_PASSWORD"),
            query="SELECT * FROM inventory_snapshot WHERE snapshot_date = CURRENT_DATE",
        )
        
        extractor = DatabaseExtractor(config)
        df = extractor.extract()
        extractor.write_to_bronze(df)
    """
    
    def __init__(self, config: DatabaseConfig):
        super().__init__(config)
        self.db_config = config
        self._cursor = None
    
    def _connect(self) -> None:
        """Establish database connection via pyodbc."""
        import pyodbc
        
        connection_string = (
            f"DRIVER={{{self.db_config.driver}}};"
            f"SERVER={self.db_config.host},{self.db_config.port};"
            f"DATABASE={self.db_config.database};"
            f"UID={self.db_config.username};"
            f"PWD={self.db_config.password};"
            "TrustServerCertificate=yes;"
        )
        
        try:
            self._connection = pyodbc.connect(
                connection_string,
                timeout=self.config.timeout_seconds,
            )
            self._connection.setdecoding(pyodbc.SQL_CHAR, encoding='utf-8')
            self._connection.setdecoding(pyodbc.SQL_WCHAR, encoding='utf-8')
            
            if self.db_config.enable_fast_executemany:
                self._cursor = self._connection.cursor()
                self._cursor.fast_executemany = True
            
            self.logger.info(
                f"Connected to {self.db_config.host}/{self.db_config.database}"
            )
            
        except pyodbc.Error as e:
            raise ConnectionError(f"Database connection failed: {str(e)}")
    
    def _extract(self) -> pl.DataFrame:
        """Execute query and return results as Polars DataFrame."""
        query = self._build_query()
        
        self.logger.info(f"Executing query: {query[:200]}...")
        
        try:
            # Execute query
            cursor = self._connection.cursor()
            cursor.execute(query)
            
            # Get column names
            columns = [desc[0] for desc in cursor.description]
            
            # Fetch in chunks for memory efficiency
            all_rows = []
            while True:
                rows = cursor.fetchmany(self.db_config.fetch_size)
                if not rows:
                    break
                all_rows.extend(rows)
                
                if len(all_rows) % 100_000 == 0:
                    self.logger.info(f"Fetched {len(all_rows):,} rows...")
            
            cursor.close()
            
            # Convert to Polars DataFrame
            if not all_rows:
                self.logger.warning("Query returned no results")
                return pl.DataFrame(schema={col: pl.Utf8 for col in columns})
            
            df = pl.DataFrame(
                data=all_rows,
                schema=columns,
                orient="row",
            )
            
            return df
            
        except Exception as e:
            raise ExtractionError(f"Query execution failed: {str(e)}")
    
    def _build_query(self) -> str:
        """Build SQL query based on configuration."""
        if self.db_config.query:
            query = self.db_config.query
            
            # Handle incremental extraction
            if self.db_config.incremental and self.db_config.last_extracted_value:
                query = self._add_incremental_filter(query)
            
            return query
        
        # Build query from table/columns
        if not self.db_config.table_name:
            raise ExtractionError("Either query or table_name must be specified")
        
        columns = ", ".join(self.db_config.columns) if self.db_config.columns else "*"
        query = f"SELECT {columns} FROM {self.db_config.table_name}"
        
        conditions = []
        if self.db_config.where_clause:
            conditions.append(self.db_config.where_clause)
        
        if self.db_config.incremental and self.db_config.last_extracted_value:
            conditions.append(
                f"{self.db_config.incremental_column} > '{self.db_config.last_extracted_value}'"
            )
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        return query
    
    def _add_incremental_filter(self, query: str) -> str:
        """Add incremental filter to existing query."""
        # Simple approach: wrap in subquery
        # In production, use a smarter SQL parser
        return f"""
        SELECT * FROM (
            {query}
        ) base_query
        WHERE {self.db_config.incremental_column} > '{self.db_config.last_extracted_value}'
        """
    
    def _disconnect(self) -> None:
        """Close database connection."""
        try:
            if self._cursor:
                self._cursor.close()
            if self._connection:
                self._connection.close()
            self.logger.info("Database connection closed")
        except Exception as e:
            self.logger.warning(f"Error closing connection: {str(e)}")
    
    def test_connection(self) -> bool:
        """Test database connectivity."""
        try:
            self._connect()
            cursor = self._connection.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            self._disconnect()
            return True
        except Exception as e:
            self.logger.error(f"Connection test failed: {str(e)}")
            return False
    
    def get_table_schema(self, table_name: str) -> pl.DataFrame:
        """Retrieve schema information for a table."""
        query = f"""
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION,
            IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION
        """
        
        self._connect()
        cursor = self._connection.cursor()
        cursor.execute(query)
        
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        
        cursor.close()
        self._disconnect()
        
        return pl.DataFrame(data=rows, schema=columns, orient="row")
    
    def get_row_count(self, table_name: str) -> int:
        """Get approximate row count for a table."""
        # Use fast count method for SQL Server
        query = f"""
        SELECT SUM(p.rows) AS row_count
        FROM sys.partitions p
        JOIN sys.tables t ON p.object_id = t.object_id
        WHERE t.name = '{table_name}'
        AND p.index_id IN (0, 1)
        """
        
        self._connect()
        cursor = self._connection.cursor()
        cursor.execute(query)
        result = cursor.fetchone()
        cursor.close()
        self._disconnect()
        
        return result[0] if result else 0


# Convenience factory functions
def create_sql_server_extractor(
    source_name: str,
    target_table: str,
    bronze_path: Path,
    host: str,
    database: str,
    username: str,
    password: str,
    query: str,
    **kwargs,
) -> DatabaseExtractor:
    """Factory function for SQL Server extraction."""
    config = DatabaseConfig(
        source_name=source_name,
        target_table=target_table,
        bronze_path=bronze_path,
        driver="ODBC Driver 17 for SQL Server",
        host=host,
        port=1433,
        database=database,
        username=username,
        password=password,
        query=query,
        **kwargs,
    )
    return DatabaseExtractor(config)


def create_oracle_extractor(
    source_name: str,
    target_table: str,
    bronze_path: Path,
    host: str,
    database: str,
    username: str,
    password: str,
    query: str,
    **kwargs,
) -> DatabaseExtractor:
    """Factory function for Oracle extraction."""
    config = DatabaseConfig(
        source_name=source_name,
        target_table=target_table,
        bronze_path=bronze_path,
        driver="Oracle in OraClient19Home1",
        host=host,
        port=1521,
        database=database,
        username=username,
        password=password,
        query=query,
        **kwargs,
    )
    return DatabaseExtractor(config)
