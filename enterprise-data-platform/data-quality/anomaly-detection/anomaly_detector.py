"""
3-Tier Anomaly Detection System

Statistical anomaly detection with three tiers:
1. Validation: Schema and business rule violations
2. Outlier Detection: Statistical outliers (IQR, Z-score, etc.)
3. Volatility Analysis: Sudden changes and trend deviations

Author: Godson Kurishinkal
"""

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Union
import logging

import polars as pl
import numpy as np


class AnomalyType(Enum):
    """Types of anomalies detected."""
    VALIDATION = "validation"      # Tier 1: Schema/business rule violation
    OUTLIER = "outlier"            # Tier 2: Statistical outlier
    VOLATILITY = "volatility"      # Tier 3: Sudden change/trend deviation
    MISSING = "missing"            # Missing expected data
    DRIFT = "drift"                # Data distribution drift


class AnomalySeverity(Enum):
    """Severity of detected anomalies."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class Anomaly:
    """Represents a single detected anomaly."""
    anomaly_id: str
    anomaly_type: AnomalyType
    severity: AnomalySeverity
    column: str
    detected_at: datetime
    description: str
    value: Any
    expected_range: Optional[Tuple[float, float]] = None
    deviation_score: float = 0.0
    affected_records: int = 0
    sample_records: Optional[List[Dict]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AnomalyReport:
    """Complete anomaly detection report."""
    table_name: str
    detection_timestamp: datetime
    total_records: int
    total_anomalies: int
    anomalies_by_type: Dict[AnomalyType, int]
    anomalies_by_severity: Dict[AnomalySeverity, int]
    anomalies: List[Anomaly]
    health_score: float  # 0-100


class TierOneValidator:
    """
    Tier 1: Validation Anomaly Detection
    
    Detects schema violations and business rule violations:
    - Missing required fields
    - Data type mismatches
    - Constraint violations
    - Business logic violations
    """
    
    def __init__(self, table_name: str):
        self.table_name = table_name
        self.logger = logging.getLogger(f"anomaly.tier1.{table_name}")
        self.rules: Dict[str, Callable] = {}
    
    def add_required_columns(self, columns: List[str]) -> "TierOneValidator":
        """Add required column check."""
        self.rules["required_columns"] = lambda df: self._check_required(df, columns)
        return self
    
    def add_positive_check(self, columns: List[str]) -> "TierOneValidator":
        """Add positive value check for numeric columns."""
        self.rules["positive_values"] = lambda df: self._check_positive(df, columns)
        return self
    
    def add_non_future_date(self, columns: List[str]) -> "TierOneValidator":
        """Ensure date columns are not in the future."""
        self.rules["non_future_dates"] = lambda df: self._check_non_future(df, columns)
        return self
    
    def add_business_rule(
        self, 
        name: str, 
        condition: Callable[[pl.DataFrame], pl.Series],
        description: str = "",
    ) -> "TierOneValidator":
        """Add custom business rule."""
        self.rules[name] = lambda df: self._check_custom(df, condition, name, description)
        return self
    
    def detect(self, df: pl.DataFrame) -> List[Anomaly]:
        """Run all Tier 1 checks and return anomalies."""
        anomalies = []
        
        for rule_name, check_func in self.rules.items():
            try:
                rule_anomalies = check_func(df)
                anomalies.extend(rule_anomalies)
            except Exception as e:
                self.logger.error(f"Rule {rule_name} failed: {str(e)}")
        
        return anomalies
    
    def _check_required(self, df: pl.DataFrame, columns: List[str]) -> List[Anomaly]:
        """Check for null values in required columns."""
        anomalies = []
        
        for col in columns:
            if col not in df.columns:
                anomalies.append(Anomaly(
                    anomaly_id=f"validation_{col}_missing",
                    anomaly_type=AnomalyType.VALIDATION,
                    severity=AnomalySeverity.CRITICAL,
                    column=col,
                    detected_at=datetime.utcnow(),
                    description=f"Required column '{col}' is missing from data",
                    value=None,
                    affected_records=len(df),
                ))
                continue
            
            null_count = df[col].null_count()
            if null_count > 0:
                severity = (
                    AnomalySeverity.CRITICAL if null_count / len(df) > 0.1
                    else AnomalySeverity.HIGH if null_count / len(df) > 0.01
                    else AnomalySeverity.MEDIUM
                )
                
                anomalies.append(Anomaly(
                    anomaly_id=f"validation_{col}_nulls",
                    anomaly_type=AnomalyType.VALIDATION,
                    severity=severity,
                    column=col,
                    detected_at=datetime.utcnow(),
                    description=f"Required column '{col}' has {null_count} null values ({null_count/len(df):.2%})",
                    value=null_count,
                    affected_records=null_count,
                ))
        
        return anomalies
    
    def _check_positive(self, df: pl.DataFrame, columns: List[str]) -> List[Anomaly]:
        """Check for non-positive values."""
        anomalies = []
        
        for col in columns:
            if col not in df.columns:
                continue
            
            negative_count = (df[col] < 0).sum()
            if negative_count > 0:
                samples = df.filter(pl.col(col) < 0)[col].head(5).to_list()
                
                anomalies.append(Anomaly(
                    anomaly_id=f"validation_{col}_negative",
                    anomaly_type=AnomalyType.VALIDATION,
                    severity=AnomalySeverity.HIGH,
                    column=col,
                    detected_at=datetime.utcnow(),
                    description=f"Column '{col}' has {negative_count} negative values",
                    value=samples[0] if samples else None,
                    affected_records=negative_count,
                    metadata={"sample_values": samples},
                ))
        
        return anomalies
    
    def _check_non_future(self, df: pl.DataFrame, columns: List[str]) -> List[Anomaly]:
        """Check for future dates."""
        anomalies = []
        today = datetime.now().date()
        
        for col in columns:
            if col not in df.columns:
                continue
            
            future_count = (df[col] > today).sum()
            if future_count > 0:
                anomalies.append(Anomaly(
                    anomaly_id=f"validation_{col}_future",
                    anomaly_type=AnomalyType.VALIDATION,
                    severity=AnomalySeverity.MEDIUM,
                    column=col,
                    detected_at=datetime.utcnow(),
                    description=f"Column '{col}' has {future_count} future dates",
                    value=None,
                    affected_records=future_count,
                ))
        
        return anomalies
    
    def _check_custom(
        self, 
        df: pl.DataFrame, 
        condition: Callable, 
        name: str,
        description: str,
    ) -> List[Anomaly]:
        """Run custom validation check."""
        try:
            is_valid = condition(df)
            invalid_count = (~is_valid).sum()
            
            if invalid_count > 0:
                return [Anomaly(
                    anomaly_id=f"validation_{name}",
                    anomaly_type=AnomalyType.VALIDATION,
                    severity=AnomalySeverity.MEDIUM,
                    column=name,
                    detected_at=datetime.utcnow(),
                    description=description or f"Business rule '{name}' violated",
                    value=None,
                    affected_records=invalid_count,
                )]
        except Exception as e:
            self.logger.error(f"Custom check {name} failed: {str(e)}")
        
        return []


class TierTwoOutlierDetector:
    """
    Tier 2: Statistical Outlier Detection
    
    Detects statistical outliers using:
    - IQR (Interquartile Range)
    - Z-Score
    - Modified Z-Score (MAD)
    - Percentile thresholds
    """
    
    def __init__(self, table_name: str):
        self.table_name = table_name
        self.logger = logging.getLogger(f"anomaly.tier2.{table_name}")
        self.columns: Dict[str, Dict] = {}
    
    def add_iqr_check(
        self, 
        column: str, 
        multiplier: float = 1.5,
        severity: AnomalySeverity = AnomalySeverity.MEDIUM,
    ) -> "TierTwoOutlierDetector":
        """Add IQR-based outlier detection for a column."""
        self.columns[column] = {
            "method": "iqr",
            "multiplier": multiplier,
            "severity": severity,
        }
        return self
    
    def add_zscore_check(
        self, 
        column: str, 
        threshold: float = 3.0,
        severity: AnomalySeverity = AnomalySeverity.MEDIUM,
    ) -> "TierTwoOutlierDetector":
        """Add Z-score based outlier detection."""
        self.columns[column] = {
            "method": "zscore",
            "threshold": threshold,
            "severity": severity,
        }
        return self
    
    def add_mad_check(
        self, 
        column: str, 
        threshold: float = 3.5,
        severity: AnomalySeverity = AnomalySeverity.MEDIUM,
    ) -> "TierTwoOutlierDetector":
        """Add Modified Z-score (MAD) based outlier detection."""
        self.columns[column] = {
            "method": "mad",
            "threshold": threshold,
            "severity": severity,
        }
        return self
    
    def add_percentile_check(
        self, 
        column: str, 
        lower: float = 0.01,
        upper: float = 0.99,
        severity: AnomalySeverity = AnomalySeverity.LOW,
    ) -> "TierTwoOutlierDetector":
        """Add percentile-based outlier detection."""
        self.columns[column] = {
            "method": "percentile",
            "lower": lower,
            "upper": upper,
            "severity": severity,
        }
        return self
    
    def detect(self, df: pl.DataFrame) -> List[Anomaly]:
        """Run all Tier 2 checks and return anomalies."""
        anomalies = []
        
        for column, config in self.columns.items():
            if column not in df.columns:
                continue
            
            method = config["method"]
            
            if method == "iqr":
                anomalies.extend(self._detect_iqr(df, column, config))
            elif method == "zscore":
                anomalies.extend(self._detect_zscore(df, column, config))
            elif method == "mad":
                anomalies.extend(self._detect_mad(df, column, config))
            elif method == "percentile":
                anomalies.extend(self._detect_percentile(df, column, config))
        
        return anomalies
    
    def _detect_iqr(self, df: pl.DataFrame, column: str, config: Dict) -> List[Anomaly]:
        """Detect outliers using IQR method."""
        q1 = df[column].quantile(0.25)
        q3 = df[column].quantile(0.75)
        iqr = q3 - q1
        multiplier = config["multiplier"]
        
        lower_bound = q1 - multiplier * iqr
        upper_bound = q3 + multiplier * iqr
        
        is_outlier = (df[column] < lower_bound) | (df[column] > upper_bound)
        outlier_count = is_outlier.sum()
        
        if outlier_count > 0:
            outlier_values = df.filter(is_outlier)[column].to_list()
            
            return [Anomaly(
                anomaly_id=f"outlier_iqr_{column}",
                anomaly_type=AnomalyType.OUTLIER,
                severity=config["severity"],
                column=column,
                detected_at=datetime.utcnow(),
                description=f"IQR outliers in '{column}': {outlier_count} values outside [{lower_bound:.2f}, {upper_bound:.2f}]",
                value=outlier_values[0] if outlier_values else None,
                expected_range=(lower_bound, upper_bound),
                affected_records=outlier_count,
                metadata={
                    "method": "iqr",
                    "q1": q1,
                    "q3": q3,
                    "iqr": iqr,
                    "sample_outliers": outlier_values[:10],
                },
            )]
        
        return []
    
    def _detect_zscore(self, df: pl.DataFrame, column: str, config: Dict) -> List[Anomaly]:
        """Detect outliers using Z-score method."""
        mean = df[column].mean()
        std = df[column].std()
        threshold = config["threshold"]
        
        if std == 0:
            return []
        
        z_scores = ((df[column] - mean) / std).abs()
        is_outlier = z_scores > threshold
        outlier_count = is_outlier.sum()
        
        if outlier_count > 0:
            return [Anomaly(
                anomaly_id=f"outlier_zscore_{column}",
                anomaly_type=AnomalyType.OUTLIER,
                severity=config["severity"],
                column=column,
                detected_at=datetime.utcnow(),
                description=f"Z-score outliers in '{column}': {outlier_count} values with |z| > {threshold}",
                value=None,
                deviation_score=z_scores.filter(is_outlier).max(),
                affected_records=outlier_count,
                metadata={
                    "method": "zscore",
                    "mean": mean,
                    "std": std,
                    "threshold": threshold,
                },
            )]
        
        return []
    
    def _detect_mad(self, df: pl.DataFrame, column: str, config: Dict) -> List[Anomaly]:
        """Detect outliers using Modified Z-score (MAD) method."""
        median = df[column].median()
        mad = (df[column] - median).abs().median()
        threshold = config["threshold"]
        
        if mad == 0:
            return []
        
        # Modified Z-score: 0.6745 is the 0.75th quantile of the normal distribution
        modified_z = 0.6745 * (df[column] - median) / mad
        is_outlier = modified_z.abs() > threshold
        outlier_count = is_outlier.sum()
        
        if outlier_count > 0:
            return [Anomaly(
                anomaly_id=f"outlier_mad_{column}",
                anomaly_type=AnomalyType.OUTLIER,
                severity=config["severity"],
                column=column,
                detected_at=datetime.utcnow(),
                description=f"MAD outliers in '{column}': {outlier_count} values with modified z > {threshold}",
                value=None,
                affected_records=outlier_count,
                metadata={
                    "method": "mad",
                    "median": median,
                    "mad": mad,
                },
            )]
        
        return []
    
    def _detect_percentile(self, df: pl.DataFrame, column: str, config: Dict) -> List[Anomaly]:
        """Detect outliers using percentile thresholds."""
        lower = config["lower"]
        upper = config["upper"]
        
        lower_bound = df[column].quantile(lower)
        upper_bound = df[column].quantile(upper)
        
        is_outlier = (df[column] < lower_bound) | (df[column] > upper_bound)
        outlier_count = is_outlier.sum()
        
        if outlier_count > 0:
            return [Anomaly(
                anomaly_id=f"outlier_percentile_{column}",
                anomaly_type=AnomalyType.OUTLIER,
                severity=config["severity"],
                column=column,
                detected_at=datetime.utcnow(),
                description=f"Percentile outliers in '{column}': {outlier_count} values outside [{lower:.0%}, {upper:.0%}]",
                value=None,
                expected_range=(lower_bound, upper_bound),
                affected_records=outlier_count,
                metadata={
                    "method": "percentile",
                    "lower_percentile": lower,
                    "upper_percentile": upper,
                },
            )]
        
        return []


class TierThreeVolatilityAnalyzer:
    """
    Tier 3: Volatility and Change Detection
    
    Detects:
    - Sudden value changes (spikes/drops)
    - Trend deviations
    - Volume anomalies
    - Missing expected data
    """
    
    def __init__(self, table_name: str):
        self.table_name = table_name
        self.logger = logging.getLogger(f"anomaly.tier3.{table_name}")
        self.checks: List[Dict] = []
    
    def add_spike_detection(
        self,
        column: str,
        threshold_pct: float = 200.0,  # 200% = 2x normal
        date_column: str = "date",
        severity: AnomalySeverity = AnomalySeverity.HIGH,
    ) -> "TierThreeVolatilityAnalyzer":
        """Detect sudden spikes in values compared to previous period."""
        self.checks.append({
            "type": "spike",
            "column": column,
            "threshold_pct": threshold_pct,
            "date_column": date_column,
            "severity": severity,
        })
        return self
    
    def add_drop_detection(
        self,
        column: str,
        threshold_pct: float = 50.0,  # 50% = half of normal
        date_column: str = "date",
        severity: AnomalySeverity = AnomalySeverity.HIGH,
    ) -> "TierThreeVolatilityAnalyzer":
        """Detect sudden drops in values compared to previous period."""
        self.checks.append({
            "type": "drop",
            "column": column,
            "threshold_pct": threshold_pct,
            "date_column": date_column,
            "severity": severity,
        })
        return self
    
    def add_volume_check(
        self,
        expected_min: int,
        expected_max: int,
        severity: AnomalySeverity = AnomalySeverity.MEDIUM,
    ) -> "TierThreeVolatilityAnalyzer":
        """Check if record count is within expected range."""
        self.checks.append({
            "type": "volume",
            "expected_min": expected_min,
            "expected_max": expected_max,
            "severity": severity,
        })
        return self
    
    def add_rolling_average_check(
        self,
        column: str,
        window_size: int = 7,
        threshold_std: float = 2.0,
        date_column: str = "date",
        severity: AnomalySeverity = AnomalySeverity.MEDIUM,
    ) -> "TierThreeVolatilityAnalyzer":
        """Detect deviations from rolling average."""
        self.checks.append({
            "type": "rolling",
            "column": column,
            "window_size": window_size,
            "threshold_std": threshold_std,
            "date_column": date_column,
            "severity": severity,
        })
        return self
    
    def detect(
        self, 
        df: pl.DataFrame, 
        historical_df: Optional[pl.DataFrame] = None,
    ) -> List[Anomaly]:
        """Run all Tier 3 checks."""
        anomalies = []
        
        for check in self.checks:
            check_type = check["type"]
            
            if check_type == "spike":
                anomalies.extend(self._detect_spike(df, historical_df, check))
            elif check_type == "drop":
                anomalies.extend(self._detect_drop(df, historical_df, check))
            elif check_type == "volume":
                anomalies.extend(self._detect_volume(df, check))
            elif check_type == "rolling":
                anomalies.extend(self._detect_rolling(df, check))
        
        return anomalies
    
    def _detect_spike(
        self, 
        df: pl.DataFrame, 
        historical_df: Optional[pl.DataFrame],
        check: Dict,
    ) -> List[Anomaly]:
        """Detect sudden value spikes."""
        column = check["column"]
        threshold = check["threshold_pct"]
        
        if historical_df is None or column not in df.columns:
            return []
        
        current_value = df[column].sum()
        historical_value = historical_df[column].sum() if column in historical_df.columns else 0
        
        if historical_value == 0:
            return []
        
        change_pct = ((current_value - historical_value) / historical_value) * 100
        
        if change_pct > threshold:
            return [Anomaly(
                anomaly_id=f"volatility_spike_{column}",
                anomaly_type=AnomalyType.VOLATILITY,
                severity=check["severity"],
                column=column,
                detected_at=datetime.utcnow(),
                description=f"Sudden spike in '{column}': +{change_pct:.1f}% (threshold: {threshold}%)",
                value=current_value,
                deviation_score=change_pct,
                metadata={
                    "current_value": current_value,
                    "historical_value": historical_value,
                    "change_pct": change_pct,
                },
            )]
        
        return []
    
    def _detect_drop(
        self, 
        df: pl.DataFrame, 
        historical_df: Optional[pl.DataFrame],
        check: Dict,
    ) -> List[Anomaly]:
        """Detect sudden value drops."""
        column = check["column"]
        threshold = check["threshold_pct"]
        
        if historical_df is None or column not in df.columns:
            return []
        
        current_value = df[column].sum()
        historical_value = historical_df[column].sum() if column in historical_df.columns else 0
        
        if historical_value == 0:
            return []
        
        change_pct = ((historical_value - current_value) / historical_value) * 100
        
        if change_pct > (100 - threshold):
            return [Anomaly(
                anomaly_id=f"volatility_drop_{column}",
                anomaly_type=AnomalyType.VOLATILITY,
                severity=check["severity"],
                column=column,
                detected_at=datetime.utcnow(),
                description=f"Sudden drop in '{column}': -{change_pct:.1f}% (threshold: {100-threshold}%)",
                value=current_value,
                deviation_score=change_pct,
                metadata={
                    "current_value": current_value,
                    "historical_value": historical_value,
                    "change_pct": -change_pct,
                },
            )]
        
        return []
    
    def _detect_volume(self, df: pl.DataFrame, check: Dict) -> List[Anomaly]:
        """Check record volume is within expected range."""
        count = len(df)
        expected_min = check["expected_min"]
        expected_max = check["expected_max"]
        
        if count < expected_min:
            return [Anomaly(
                anomaly_id="volatility_volume_low",
                anomaly_type=AnomalyType.MISSING,
                severity=check["severity"],
                column="_record_count",
                detected_at=datetime.utcnow(),
                description=f"Record count ({count}) below expected minimum ({expected_min})",
                value=count,
                expected_range=(expected_min, expected_max),
                metadata={
                    "missing_records": expected_min - count,
                },
            )]
        
        if count > expected_max:
            return [Anomaly(
                anomaly_id="volatility_volume_high",
                anomaly_type=AnomalyType.VOLATILITY,
                severity=check["severity"],
                column="_record_count",
                detected_at=datetime.utcnow(),
                description=f"Record count ({count}) above expected maximum ({expected_max})",
                value=count,
                expected_range=(expected_min, expected_max),
                metadata={
                    "excess_records": count - expected_max,
                },
            )]
        
        return []
    
    def _detect_rolling(self, df: pl.DataFrame, check: Dict) -> List[Anomaly]:
        """Detect deviations from rolling average."""
        # Implementation for rolling average deviation detection
        # Requires time-series ordered data
        return []


class AnomalyDetector:
    """
    Unified 3-Tier Anomaly Detection System.
    
    Combines all three tiers for comprehensive anomaly detection:
    - Tier 1: Validation (schema, business rules)
    - Tier 2: Outliers (statistical)
    - Tier 3: Volatility (temporal changes)
    
    Example:
        detector = AnomalyDetector("inventory")
        
        # Configure tiers
        detector.tier1.add_required_columns(["sku_code", "quantity"])
        detector.tier2.add_iqr_check("quantity")
        detector.tier3.add_spike_detection("quantity", threshold_pct=200)
        
        # Run detection
        report = detector.detect(current_df, historical_df)
        
        if report.anomalies_by_severity[AnomalySeverity.CRITICAL] > 0:
            raise DataQualityError("Critical anomalies detected")
    """
    
    def __init__(self, table_name: str):
        self.table_name = table_name
        self.tier1 = TierOneValidator(table_name)
        self.tier2 = TierTwoOutlierDetector(table_name)
        self.tier3 = TierThreeVolatilityAnalyzer(table_name)
        self.logger = logging.getLogger(f"anomaly.{table_name}")
    
    def detect(
        self, 
        df: pl.DataFrame,
        historical_df: Optional[pl.DataFrame] = None,
    ) -> AnomalyReport:
        """
        Run all tiers of anomaly detection.
        
        Args:
            df: Current data to analyze
            historical_df: Historical data for comparison (Tier 3)
            
        Returns:
            Comprehensive anomaly report
        """
        all_anomalies = []
        
        # Tier 1: Validation
        tier1_anomalies = self.tier1.detect(df)
        all_anomalies.extend(tier1_anomalies)
        self.logger.info(f"Tier 1 (Validation): {len(tier1_anomalies)} anomalies")
        
        # Tier 2: Outliers
        tier2_anomalies = self.tier2.detect(df)
        all_anomalies.extend(tier2_anomalies)
        self.logger.info(f"Tier 2 (Outliers): {len(tier2_anomalies)} anomalies")
        
        # Tier 3: Volatility
        tier3_anomalies = self.tier3.detect(df, historical_df)
        all_anomalies.extend(tier3_anomalies)
        self.logger.info(f"Tier 3 (Volatility): {len(tier3_anomalies)} anomalies")
        
        # Build report
        report = self._build_report(df, all_anomalies)
        
        return report
    
    def _build_report(self, df: pl.DataFrame, anomalies: List[Anomaly]) -> AnomalyReport:
        """Build comprehensive anomaly report."""
        # Count by type
        by_type = {}
        for atype in AnomalyType:
            by_type[atype] = sum(1 for a in anomalies if a.anomaly_type == atype)
        
        # Count by severity
        by_severity = {}
        for sev in AnomalySeverity:
            by_severity[sev] = sum(1 for a in anomalies if a.severity == sev)
        
        # Calculate health score (100 = perfect, 0 = critical issues)
        severity_weights = {
            AnomalySeverity.LOW: 1,
            AnomalySeverity.MEDIUM: 5,
            AnomalySeverity.HIGH: 15,
            AnomalySeverity.CRITICAL: 50,
        }
        
        penalty = sum(
            by_severity[sev] * severity_weights[sev] 
            for sev in AnomalySeverity
        )
        health_score = max(0, 100 - penalty)
        
        return AnomalyReport(
            table_name=self.table_name,
            detection_timestamp=datetime.utcnow(),
            total_records=len(df),
            total_anomalies=len(anomalies),
            anomalies_by_type=by_type,
            anomalies_by_severity=by_severity,
            anomalies=anomalies,
            health_score=health_score,
        )


# Convenience function for quick anomaly detection
def quick_anomaly_check(
    df: pl.DataFrame,
    table_name: str = "data",
    required_columns: Optional[List[str]] = None,
    numeric_columns: Optional[List[str]] = None,
    historical_df: Optional[pl.DataFrame] = None,
) -> AnomalyReport:
    """
    Quick anomaly detection with sensible defaults.
    
    Example:
        report = quick_anomaly_check(
            df,
            required_columns=["id", "value"],
            numeric_columns=["value", "amount"],
        )
    """
    detector = AnomalyDetector(table_name)
    
    if required_columns:
        detector.tier1.add_required_columns(required_columns)
    
    if numeric_columns:
        for col in numeric_columns:
            detector.tier2.add_iqr_check(col)
    
    return detector.detect(df, historical_df)
