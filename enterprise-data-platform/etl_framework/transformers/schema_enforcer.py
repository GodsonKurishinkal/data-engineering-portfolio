"""
Schema Enforcer

Enforces consistent schemas across data layers with type casting,
column validation, and schema evolution support.

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
class ColumnSpec:
    """Specification for a single column."""
    name: str
    dtype: pl.DataType
    nullable: bool = True
    default: Any = None
    description: str = ""
    constraints: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SchemaSpec:
    """Complete schema specification for a table."""
    table_name: str
    columns: List[ColumnSpec]
    primary_key: List[str] = field(default_factory=list)
    version: str = "1.0.0"
    description: str = ""
    
    def to_polars_schema(self) -> Dict[str, pl.DataType]:
        """Convert to Polars schema dict."""
        return {col.name: col.dtype for col in self.columns}
    
    def get_required_columns(self) -> List[str]:
        """Get list of non-nullable columns."""
        return [col.name for col in self.columns if not col.nullable]


class SchemaEnforcer:
    """
    Enforces schema consistency across the data lakehouse.
    
    Features:
    - Type casting with error handling
    - Column validation
    - Default value injection
    - Schema evolution tracking
    - Constraint validation
    
    Example:
        schema = SchemaSpec(
            table_name="inventory",
            columns=[
                ColumnSpec("sku_code", pl.Utf8, nullable=False),
                ColumnSpec("quantity", pl.Int64, default=0),
                ColumnSpec("unit_cost", pl.Float64),
            ],
            primary_key=["sku_code"],
        )
        
        enforcer = SchemaEnforcer(schema)
        df = enforcer.enforce(raw_df)
    """
    
    def __init__(self, schema: SchemaSpec, strict: bool = False):
        """
        Initialize schema enforcer.
        
        Args:
            schema: Schema specification to enforce
            strict: If True, raise errors on schema violations
        """
        self.schema = schema
        self.strict = strict
        self.logger = logging.getLogger(f"schema.{schema.table_name}")
        self._violations: List[Dict] = []
    
    def enforce(self, df: pl.DataFrame) -> pl.DataFrame:
        """
        Enforce schema on DataFrame.
        
        Steps:
        1. Validate required columns exist
        2. Add missing optional columns with defaults
        3. Cast columns to expected types
        4. Validate constraints
        5. Select and order columns per schema
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with enforced schema
        """
        self._violations = []
        
        # Step 1: Validate required columns
        df = self._validate_required_columns(df)
        
        # Step 2: Add missing columns with defaults
        df = self._add_missing_columns(df)
        
        # Step 3: Cast types
        df = self._cast_types(df)
        
        # Step 4: Validate constraints
        df = self._validate_constraints(df)
        
        # Step 5: Select and order columns
        df = self._select_columns(df)
        
        if self._violations:
            self.logger.warning(f"Schema enforcement completed with {len(self._violations)} violations")
        
        return df
    
    def _validate_required_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """Check that all required columns exist."""
        required = self.schema.get_required_columns()
        missing = set(required) - set(df.columns)
        
        if missing:
            msg = f"Missing required columns: {missing}"
            self._violations.append({"type": "missing_column", "details": msg})
            
            if self.strict:
                raise SchemaViolationError(msg)
            
            self.logger.error(msg)
        
        return df
    
    def _add_missing_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """Add missing optional columns with default values."""
        schema_columns = {col.name for col in self.schema.columns}
        existing_columns = set(df.columns)
        
        new_columns = []
        for col_spec in self.schema.columns:
            if col_spec.name not in existing_columns:
                if col_spec.default is not None:
                    new_columns.append(
                        pl.lit(col_spec.default).cast(col_spec.dtype).alias(col_spec.name)
                    )
                elif col_spec.nullable:
                    new_columns.append(
                        pl.lit(None).cast(col_spec.dtype).alias(col_spec.name)
                    )
        
        if new_columns:
            df = df.with_columns(new_columns)
            self.logger.info(f"Added {len(new_columns)} missing columns with defaults")
        
        return df
    
    def _cast_types(self, df: pl.DataFrame) -> pl.DataFrame:
        """Cast columns to expected types."""
        cast_exprs = []
        
        for col_spec in self.schema.columns:
            if col_spec.name not in df.columns:
                continue
            
            current_dtype = df[col_spec.name].dtype
            expected_dtype = col_spec.dtype
            
            if current_dtype != expected_dtype:
                try:
                    cast_exprs.append(
                        pl.col(col_spec.name).cast(expected_dtype).alias(col_spec.name)
                    )
                except Exception as e:
                    msg = f"Failed to cast {col_spec.name} from {current_dtype} to {expected_dtype}: {e}"
                    self._violations.append({"type": "cast_error", "details": msg})
                    
                    if self.strict:
                        raise SchemaViolationError(msg)
        
        if cast_exprs:
            df = df.with_columns(cast_exprs)
        
        return df
    
    def _validate_constraints(self, df: pl.DataFrame) -> pl.DataFrame:
        """Validate column constraints."""
        for col_spec in self.schema.columns:
            if col_spec.name not in df.columns:
                continue
            
            constraints = col_spec.constraints
            
            # Check not null
            if not col_spec.nullable:
                null_count = df[col_spec.name].null_count()
                if null_count > 0:
                    msg = f"Column {col_spec.name} has {null_count} null values but is not nullable"
                    self._violations.append({"type": "null_violation", "details": msg})
            
            # Check min/max
            if "min" in constraints:
                violations = (df[col_spec.name] < constraints["min"]).sum()
                if violations > 0:
                    self._violations.append({
                        "type": "min_violation",
                        "details": f"{col_spec.name}: {violations} values below min {constraints['min']}"
                    })
            
            if "max" in constraints:
                violations = (df[col_spec.name] > constraints["max"]).sum()
                if violations > 0:
                    self._violations.append({
                        "type": "max_violation",
                        "details": f"{col_spec.name}: {violations} values above max {constraints['max']}"
                    })
            
            # Check allowed values
            if "allowed_values" in constraints:
                allowed = set(constraints["allowed_values"])
                actual = set(df[col_spec.name].unique().to_list())
                invalid = actual - allowed
                if invalid:
                    self._violations.append({
                        "type": "value_violation",
                        "details": f"{col_spec.name}: invalid values {invalid}"
                    })
            
            # Check regex pattern
            if "pattern" in constraints:
                pattern = constraints["pattern"]
                violations = (~df[col_spec.name].str.contains(pattern)).sum()
                if violations > 0:
                    self._violations.append({
                        "type": "pattern_violation",
                        "details": f"{col_spec.name}: {violations} values don't match pattern"
                    })
        
        return df
    
    def _select_columns(self, df: pl.DataFrame) -> pl.DataFrame:
        """Select and order columns according to schema."""
        schema_columns = [col.name for col in self.schema.columns]
        available_columns = [c for c in schema_columns if c in df.columns]
        return df.select(available_columns)
    
    def get_violations(self) -> List[Dict]:
        """Return list of schema violations found."""
        return self._violations
    
    def validate(self, df: pl.DataFrame) -> bool:
        """
        Validate DataFrame against schema without modification.
        
        Returns:
            True if valid, False if violations found
        """
        self._violations = []
        
        # Check required columns
        required = self.schema.get_required_columns()
        missing = set(required) - set(df.columns)
        if missing:
            self._violations.append({"type": "missing_column", "columns": list(missing)})
        
        # Check types
        for col_spec in self.schema.columns:
            if col_spec.name in df.columns:
                if df[col_spec.name].dtype != col_spec.dtype:
                    self._violations.append({
                        "type": "type_mismatch",
                        "column": col_spec.name,
                        "expected": str(col_spec.dtype),
                        "actual": str(df[col_spec.name].dtype),
                    })
        
        return len(self._violations) == 0


class SchemaRegistry:
    """
    Central registry for all table schemas.
    
    Enables:
    - Schema versioning
    - Cross-table validation
    - Schema documentation
    """
    
    def __init__(self, registry_path: Optional[Path] = None):
        self.schemas: Dict[str, SchemaSpec] = {}
        self.registry_path = registry_path
        
        if registry_path and registry_path.exists():
            self._load_registry()
    
    def register(self, schema: SchemaSpec) -> None:
        """Register a schema."""
        self.schemas[schema.table_name] = schema
    
    def get(self, table_name: str) -> Optional[SchemaSpec]:
        """Get schema by table name."""
        return self.schemas.get(table_name)
    
    def get_enforcer(self, table_name: str, strict: bool = False) -> SchemaEnforcer:
        """Get a schema enforcer for a table."""
        schema = self.get(table_name)
        if not schema:
            raise ValueError(f"No schema registered for table: {table_name}")
        return SchemaEnforcer(schema, strict=strict)
    
    def _load_registry(self) -> None:
        """Load schemas from registry file."""
        # Implementation for loading from JSON/YAML
        pass
    
    def save_registry(self) -> None:
        """Save schemas to registry file."""
        # Implementation for saving to JSON/YAML
        pass


class SchemaViolationError(Exception):
    """Raised when schema enforcement fails in strict mode."""
    pass


# Pre-defined schemas for common tables
INVENTORY_SCHEMA = SchemaSpec(
    table_name="inventory",
    columns=[
        ColumnSpec("sku_code", pl.Utf8, nullable=False, description="Product SKU"),
        ColumnSpec("location_id", pl.Utf8, nullable=False, description="Warehouse location"),
        ColumnSpec("quantity_on_hand", pl.Int64, default=0, constraints={"min": 0}),
        ColumnSpec("quantity_reserved", pl.Int64, default=0, constraints={"min": 0}),
        ColumnSpec("quantity_available", pl.Int64, default=0),
        ColumnSpec("unit_cost", pl.Float64, constraints={"min": 0}),
        ColumnSpec("last_count_date", pl.Date, nullable=True),
        ColumnSpec("last_movement_date", pl.Date, nullable=True),
    ],
    primary_key=["sku_code", "location_id"],
    description="Current inventory levels by SKU and location",
)

PRODUCT_SCHEMA = SchemaSpec(
    table_name="product",
    columns=[
        ColumnSpec("sku_code", pl.Utf8, nullable=False),
        ColumnSpec("product_name", pl.Utf8, nullable=False),
        ColumnSpec("category_l1", pl.Utf8),
        ColumnSpec("category_l2", pl.Utf8),
        ColumnSpec("category_l3", pl.Utf8),
        ColumnSpec("brand", pl.Utf8),
        ColumnSpec("supplier_code", pl.Utf8),
        ColumnSpec("unit_of_measure", pl.Utf8, default="EA"),
        ColumnSpec("abc_class", pl.Utf8, constraints={"allowed_values": ["A", "B", "C"]}),
        ColumnSpec("xyz_class", pl.Utf8, constraints={"allowed_values": ["X", "Y", "Z"]}),
        ColumnSpec("is_active", pl.Boolean, default=True),
    ],
    primary_key=["sku_code"],
    description="Product master data",
)
