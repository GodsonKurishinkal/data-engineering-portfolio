"""
Validation Rules Engine

Declarative validation rules for data quality checks.
Supports schema, range, referential, and custom validations.

Author: Godson Kurishinkal
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Union
import logging

import polars as pl


class RuleSeverity(Enum):
    """Severity level for validation rules."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class RuleType(Enum):
    """Types of validation rules."""
    NOT_NULL = "not_null"
    UNIQUE = "unique"
    RANGE = "range"
    PATTERN = "pattern"
    ALLOWED_VALUES = "allowed_values"
    REFERENTIAL = "referential"
    CUSTOM = "custom"
    COMPLETENESS = "completeness"
    FRESHNESS = "freshness"


@dataclass
class ValidationResult:
    """Result of a single validation rule execution."""
    rule_name: str
    rule_type: RuleType
    column: Optional[str]
    severity: RuleSeverity
    passed: bool
    total_records: int
    valid_records: int
    invalid_records: int
    invalid_percentage: float
    message: str
    sample_invalid: Optional[List[Any]] = None
    execution_time_ms: float = 0.0


@dataclass
class ValidationReport:
    """Complete validation report for a dataset."""
    table_name: str
    validation_timestamp: datetime
    total_rules: int
    passed_rules: int
    failed_rules: int
    total_records: int
    overall_quality_score: float
    results: List[ValidationResult]
    critical_failures: int = 0
    error_failures: int = 0
    warning_failures: int = 0


@dataclass
class ValidationRule:
    """Definition of a validation rule."""
    name: str
    rule_type: RuleType
    column: Optional[str] = None
    columns: Optional[List[str]] = None
    severity: RuleSeverity = RuleSeverity.ERROR
    params: Dict[str, Any] = field(default_factory=dict)
    description: str = ""
    custom_check: Optional[Callable[[pl.DataFrame], pl.Series]] = None


class ValidationEngine:
    """
    Data quality validation engine.
    
    Features:
    - Declarative rule definitions
    - Multiple rule types (null, unique, range, pattern, etc.)
    - Severity-based failure handling
    - Quality score calculation
    - Detailed reporting
    
    Example:
        engine = ValidationEngine("inventory")
        
        engine.add_rules([
            ValidationRule("sku_not_null", RuleType.NOT_NULL, column="sku_code"),
            ValidationRule("qty_positive", RuleType.RANGE, column="quantity",
                          params={"min": 0}),
            ValidationRule("sku_unique", RuleType.UNIQUE, columns=["sku_code", "location_id"]),
        ])
        
        report = engine.validate(df)
        
        if report.critical_failures > 0:
            raise DataQualityError("Critical validation failures")
    """
    
    def __init__(self, table_name: str):
        self.table_name = table_name
        self.logger = logging.getLogger(f"validation.{table_name}")
        self.rules: List[ValidationRule] = []
        self._reference_data: Dict[str, Set] = {}
    
    def add_rule(self, rule: ValidationRule) -> "ValidationEngine":
        """Add a single validation rule."""
        self.rules.append(rule)
        return self
    
    def add_rules(self, rules: List[ValidationRule]) -> "ValidationEngine":
        """Add multiple validation rules."""
        self.rules.extend(rules)
        return self
    
    def register_reference_data(
        self, 
        ref_name: str, 
        values: Union[Set, List, pl.DataFrame, pl.Series],
        column: Optional[str] = None,
    ) -> "ValidationEngine":
        """
        Register reference data for referential integrity checks.
        
        Args:
            ref_name: Name to reference in rules
            values: Valid values (set, list, DataFrame, or Series)
            column: Column name if values is a DataFrame
        """
        if isinstance(values, pl.DataFrame):
            if column is None:
                raise ValueError("Column must be specified for DataFrame reference")
            self._reference_data[ref_name] = set(values[column].to_list())
        elif isinstance(values, pl.Series):
            self._reference_data[ref_name] = set(values.to_list())
        elif isinstance(values, list):
            self._reference_data[ref_name] = set(values)
        else:
            self._reference_data[ref_name] = values
        
        return self
    
    def validate(self, df: pl.DataFrame) -> ValidationReport:
        """
        Execute all validation rules against the DataFrame.
        
        Args:
            df: DataFrame to validate
            
        Returns:
            ValidationReport with detailed results
        """
        results = []
        total_records = len(df)
        
        for rule in self.rules:
            start_time = datetime.utcnow()
            
            try:
                result = self._execute_rule(rule, df)
                result.execution_time_ms = (
                    datetime.utcnow() - start_time
                ).total_seconds() * 1000
                results.append(result)
                
            except Exception as e:
                self.logger.error(f"Rule execution failed: {rule.name} - {str(e)}")
                results.append(ValidationResult(
                    rule_name=rule.name,
                    rule_type=rule.rule_type,
                    column=rule.column,
                    severity=rule.severity,
                    passed=False,
                    total_records=total_records,
                    valid_records=0,
                    invalid_records=total_records,
                    invalid_percentage=100.0,
                    message=f"Rule execution error: {str(e)}",
                ))
        
        # Calculate summary statistics
        passed_rules = sum(1 for r in results if r.passed)
        failed_rules = len(results) - passed_rules
        critical_failures = sum(1 for r in results if not r.passed and r.severity == RuleSeverity.CRITICAL)
        error_failures = sum(1 for r in results if not r.passed and r.severity == RuleSeverity.ERROR)
        warning_failures = sum(1 for r in results if not r.passed and r.severity == RuleSeverity.WARNING)
        
        # Calculate quality score (weighted by severity)
        quality_score = self._calculate_quality_score(results)
        
        report = ValidationReport(
            table_name=self.table_name,
            validation_timestamp=datetime.utcnow(),
            total_rules=len(self.rules),
            passed_rules=passed_rules,
            failed_rules=failed_rules,
            total_records=total_records,
            overall_quality_score=quality_score,
            results=results,
            critical_failures=critical_failures,
            error_failures=error_failures,
            warning_failures=warning_failures,
        )
        
        self.logger.info(
            f"Validation complete: {passed_rules}/{len(self.rules)} rules passed, "
            f"quality score: {quality_score:.2%}"
        )
        
        return report
    
    def _execute_rule(self, rule: ValidationRule, df: pl.DataFrame) -> ValidationResult:
        """Execute a single validation rule."""
        total_records = len(df)
        
        if rule.rule_type == RuleType.NOT_NULL:
            return self._check_not_null(rule, df)
        
        elif rule.rule_type == RuleType.UNIQUE:
            return self._check_unique(rule, df)
        
        elif rule.rule_type == RuleType.RANGE:
            return self._check_range(rule, df)
        
        elif rule.rule_type == RuleType.PATTERN:
            return self._check_pattern(rule, df)
        
        elif rule.rule_type == RuleType.ALLOWED_VALUES:
            return self._check_allowed_values(rule, df)
        
        elif rule.rule_type == RuleType.REFERENTIAL:
            return self._check_referential(rule, df)
        
        elif rule.rule_type == RuleType.COMPLETENESS:
            return self._check_completeness(rule, df)
        
        elif rule.rule_type == RuleType.CUSTOM:
            return self._check_custom(rule, df)
        
        else:
            raise ValueError(f"Unknown rule type: {rule.rule_type}")
    
    def _check_not_null(self, rule: ValidationRule, df: pl.DataFrame) -> ValidationResult:
        """Check for null values in a column."""
        col = rule.column
        null_count = df[col].null_count()
        valid_count = len(df) - null_count
        
        return ValidationResult(
            rule_name=rule.name,
            rule_type=rule.rule_type,
            column=col,
            severity=rule.severity,
            passed=null_count == 0,
            total_records=len(df),
            valid_records=valid_count,
            invalid_records=null_count,
            invalid_percentage=(null_count / len(df) * 100) if len(df) > 0 else 0,
            message=f"{null_count} null values found" if null_count > 0 else "No null values",
        )
    
    def _check_unique(self, rule: ValidationRule, df: pl.DataFrame) -> ValidationResult:
        """Check for duplicate values."""
        cols = rule.columns or [rule.column]
        
        duplicate_count = len(df) - len(df.unique(subset=cols))
        valid_count = len(df) - duplicate_count
        
        return ValidationResult(
            rule_name=rule.name,
            rule_type=rule.rule_type,
            column=", ".join(cols),
            severity=rule.severity,
            passed=duplicate_count == 0,
            total_records=len(df),
            valid_records=valid_count,
            invalid_records=duplicate_count,
            invalid_percentage=(duplicate_count / len(df) * 100) if len(df) > 0 else 0,
            message=f"{duplicate_count} duplicate records found" if duplicate_count > 0 else "All records unique",
        )
    
    def _check_range(self, rule: ValidationRule, df: pl.DataFrame) -> ValidationResult:
        """Check if values are within a specified range."""
        col = rule.column
        min_val = rule.params.get("min")
        max_val = rule.params.get("max")
        
        conditions = []
        if min_val is not None:
            conditions.append(pl.col(col) >= min_val)
        if max_val is not None:
            conditions.append(pl.col(col) <= max_val)
        
        if conditions:
            combined = conditions[0]
            for c in conditions[1:]:
                combined = combined & c
            
            invalid_count = (~combined).sum()
        else:
            invalid_count = 0
        
        valid_count = len(df) - invalid_count
        
        range_str = f"[{min_val or '-∞'}, {max_val or '∞'}]"
        
        return ValidationResult(
            rule_name=rule.name,
            rule_type=rule.rule_type,
            column=col,
            severity=rule.severity,
            passed=invalid_count == 0,
            total_records=len(df),
            valid_records=valid_count,
            invalid_records=invalid_count,
            invalid_percentage=(invalid_count / len(df) * 100) if len(df) > 0 else 0,
            message=f"{invalid_count} values outside range {range_str}" if invalid_count > 0 else f"All values within {range_str}",
        )
    
    def _check_pattern(self, rule: ValidationRule, df: pl.DataFrame) -> ValidationResult:
        """Check if string values match a regex pattern."""
        col = rule.column
        pattern = rule.params.get("pattern", ".*")
        
        matches = df[col].str.contains(pattern).fill_null(False)
        invalid_count = (~matches).sum()
        valid_count = len(df) - invalid_count
        
        return ValidationResult(
            rule_name=rule.name,
            rule_type=rule.rule_type,
            column=col,
            severity=rule.severity,
            passed=invalid_count == 0,
            total_records=len(df),
            valid_records=valid_count,
            invalid_records=invalid_count,
            invalid_percentage=(invalid_count / len(df) * 100) if len(df) > 0 else 0,
            message=f"{invalid_count} values don't match pattern" if invalid_count > 0 else "All values match pattern",
        )
    
    def _check_allowed_values(self, rule: ValidationRule, df: pl.DataFrame) -> ValidationResult:
        """Check if values are in allowed set."""
        col = rule.column
        allowed = set(rule.params.get("values", []))
        
        is_valid = df[col].is_in(list(allowed))
        invalid_count = (~is_valid).sum()
        valid_count = len(df) - invalid_count
        
        # Get sample of invalid values
        invalid_values = df.filter(~is_valid)[col].unique().head(5).to_list()
        
        return ValidationResult(
            rule_name=rule.name,
            rule_type=rule.rule_type,
            column=col,
            severity=rule.severity,
            passed=invalid_count == 0,
            total_records=len(df),
            valid_records=valid_count,
            invalid_records=invalid_count,
            invalid_percentage=(invalid_count / len(df) * 100) if len(df) > 0 else 0,
            message=f"{invalid_count} invalid values" if invalid_count > 0 else "All values allowed",
            sample_invalid=invalid_values if invalid_count > 0 else None,
        )
    
    def _check_referential(self, rule: ValidationRule, df: pl.DataFrame) -> ValidationResult:
        """Check referential integrity against registered reference data."""
        col = rule.column
        ref_name = rule.params.get("reference")
        
        if ref_name not in self._reference_data:
            raise ValueError(f"Reference data not found: {ref_name}")
        
        valid_values = self._reference_data[ref_name]
        is_valid = df[col].is_in(list(valid_values))
        invalid_count = (~is_valid).sum()
        valid_count = len(df) - invalid_count
        
        return ValidationResult(
            rule_name=rule.name,
            rule_type=rule.rule_type,
            column=col,
            severity=rule.severity,
            passed=invalid_count == 0,
            total_records=len(df),
            valid_records=valid_count,
            invalid_records=invalid_count,
            invalid_percentage=(invalid_count / len(df) * 100) if len(df) > 0 else 0,
            message=f"{invalid_count} orphan records" if invalid_count > 0 else "All references valid",
        )
    
    def _check_completeness(self, rule: ValidationRule, df: pl.DataFrame) -> ValidationResult:
        """Check overall completeness of the dataset."""
        threshold = rule.params.get("threshold", 0.95)  # 95% completeness required
        
        total_cells = len(df) * len(df.columns)
        null_cells = df.null_count().sum_horizontal()[0]
        completeness = 1 - (null_cells / total_cells) if total_cells > 0 else 1
        
        return ValidationResult(
            rule_name=rule.name,
            rule_type=rule.rule_type,
            column=None,
            severity=rule.severity,
            passed=completeness >= threshold,
            total_records=len(df),
            valid_records=int(completeness * len(df)),
            invalid_records=int((1 - completeness) * len(df)),
            invalid_percentage=(1 - completeness) * 100,
            message=f"Completeness: {completeness:.2%} (threshold: {threshold:.0%})",
        )
    
    def _check_custom(self, rule: ValidationRule, df: pl.DataFrame) -> ValidationResult:
        """Execute custom validation function."""
        if rule.custom_check is None:
            raise ValueError("Custom rule must have custom_check function")
        
        # Custom check should return a boolean Series
        is_valid = rule.custom_check(df)
        invalid_count = (~is_valid).sum()
        valid_count = len(df) - invalid_count
        
        return ValidationResult(
            rule_name=rule.name,
            rule_type=rule.rule_type,
            column=rule.column,
            severity=rule.severity,
            passed=invalid_count == 0,
            total_records=len(df),
            valid_records=valid_count,
            invalid_records=invalid_count,
            invalid_percentage=(invalid_count / len(df) * 100) if len(df) > 0 else 0,
            message=rule.description or f"{invalid_count} records failed custom check",
        )
    
    def _calculate_quality_score(self, results: List[ValidationResult]) -> float:
        """Calculate weighted quality score based on rule results."""
        if not results:
            return 1.0
        
        # Weights by severity
        weights = {
            RuleSeverity.CRITICAL: 10,
            RuleSeverity.ERROR: 5,
            RuleSeverity.WARNING: 2,
            RuleSeverity.INFO: 1,
        }
        
        total_weight = 0
        weighted_score = 0
        
        for result in results:
            weight = weights[result.severity]
            total_weight += weight
            
            # Score for each rule: % valid records
            rule_score = result.valid_records / result.total_records if result.total_records > 0 else 0
            weighted_score += weight * rule_score
        
        return weighted_score / total_weight if total_weight > 0 else 1.0


class DataQualityError(Exception):
    """Raised when critical data quality issues are found."""
    pass


# Convenience function for quick validation
def validate_dataframe(
    df: pl.DataFrame,
    not_null_columns: Optional[List[str]] = None,
    unique_columns: Optional[List[str]] = None,
    positive_columns: Optional[List[str]] = None,
) -> ValidationReport:
    """
    Quick validation with common rules.
    
    Example:
        report = validate_dataframe(
            df,
            not_null_columns=["id", "name"],
            unique_columns=["id"],
            positive_columns=["amount"],
        )
    """
    engine = ValidationEngine("quick_validation")
    
    if not_null_columns:
        for col in not_null_columns:
            engine.add_rule(ValidationRule(
                name=f"{col}_not_null",
                rule_type=RuleType.NOT_NULL,
                column=col,
            ))
    
    if unique_columns:
        engine.add_rule(ValidationRule(
            name="unique_check",
            rule_type=RuleType.UNIQUE,
            columns=unique_columns,
        ))
    
    if positive_columns:
        for col in positive_columns:
            engine.add_rule(ValidationRule(
                name=f"{col}_positive",
                rule_type=RuleType.RANGE,
                column=col,
                params={"min": 0},
            ))
    
    return engine.validate(df)
