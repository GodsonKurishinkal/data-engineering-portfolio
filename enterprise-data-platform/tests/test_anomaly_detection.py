"""The three-tier detector.

Tier 1 is a gate: a deterministic rule violation is a validation anomaly and
must always fire. Tier 2 is statistical and must NOT fire on well-behaved
data — a detector that cries wolf gets muted, which is the failure mode the
case study describes. Both properties are pinned here.
"""

from __future__ import annotations

from datetime import date, timedelta

import polars as pl
import pytest

from data_quality.anomaly_detection.anomaly_detector import (
    AnomalySeverity,
    AnomalyType,
    TierOneValidator,
    TierTwoOutlierDetector,
)


class TestTierOneGate:
    def test_null_in_a_required_column_is_flagged(self):
        df = pl.DataFrame({"sku_id": ["A", None, "C"], "qty": [1, 2, 3]})

        found = TierOneValidator("inventory").add_required_columns(["sku_id"]).detect(df)

        assert len(found) == 1
        assert found[0].anomaly_type is AnomalyType.VALIDATION
        assert found[0].column == "sku_id"

    def test_a_complete_required_column_produces_nothing(self):
        df = pl.DataFrame({"sku_id": ["A", "B"], "qty": [1, 2]})

        found = TierOneValidator("inventory").add_required_columns(["sku_id"]).detect(df)

        assert found == []

    def test_negative_quantity_is_flagged(self):
        df = pl.DataFrame({"on_hand_qty": [5, -3, 10]})

        found = TierOneValidator("inventory").add_positive_check(["on_hand_qty"]).detect(df)

        assert len(found) == 1
        assert found[0].affected_records == 1

    def test_future_dated_rows_are_flagged(self):
        tomorrow = date.today() + timedelta(days=1)
        df = pl.DataFrame({"extract_date": [date.today(), tomorrow]})

        found = TierOneValidator("inventory").add_non_future_date(["extract_date"]).detect(df)

        assert len(found) == 1

    def test_a_custom_business_rule_can_be_registered_and_fires(self):
        df = pl.DataFrame({"ship_date": [2, 1], "order_date": [1, 2]})

        found = (
            TierOneValidator("orders")
            .add_business_rule(
                "ship_after_order",
                lambda d: d["ship_date"] >= d["order_date"],
                "ship_date must not precede order_date",
            )
            .detect(df)
        )

        assert len(found) == 1
        assert found[0].affected_records == 1

    def test_rules_compose_and_each_reports_separately(self):
        df = pl.DataFrame({"sku_id": [None], "on_hand_qty": [-1]})

        found = (
            TierOneValidator("inventory")
            .add_required_columns(["sku_id"])
            .add_positive_check(["on_hand_qty"])
            .detect(df)
        )

        assert len(found) == 2

    def test_one_failing_rule_does_not_prevent_the_others_running(self):
        """A rule that throws is logged, not allowed to swallow the batch."""
        df = pl.DataFrame({"on_hand_qty": [-5]})

        found = (
            TierOneValidator("inventory")
            .add_business_rule("explodes", lambda d: d["column_that_is_not_here"] > 0)
            .add_positive_check(["on_hand_qty"])
            .detect(df)
        )

        assert len(found) == 1
        assert found[0].column == "on_hand_qty"


class TestTierTwoStatistical:
    def test_iqr_finds_the_planted_outlier(self, outlier_series):
        found = TierTwoOutlierDetector("t").add_iqr_check("qty").detect(outlier_series)

        assert len(found) == 1
        assert found[0].anomaly_type is AnomalyType.OUTLIER
        assert found[0].affected_records == 1

    def test_iqr_stays_quiet_on_well_behaved_data(self):
        df = pl.DataFrame({"qty": [10.0, 11.0, 12.0, 11.5, 10.5, 11.2, 10.8]})

        assert TierTwoOutlierDetector("t").add_iqr_check("qty").detect(df) == []

    def test_zscore_finds_the_planted_outlier(self, outlier_series):
        found = TierTwoOutlierDetector("t").add_zscore_check("qty", threshold=3.0).detect(outlier_series)

        assert len(found) == 1
        assert found[0].deviation_score > 3.0

    def test_zero_variance_cannot_produce_a_divide_by_zero(self):
        """Every value identical: std is 0. Must return nothing, not NaN or a crash."""
        df = pl.DataFrame({"qty": [7.0] * 10})

        assert TierTwoOutlierDetector("t").add_zscore_check("qty").detect(df) == []

    def test_mad_is_not_fooled_by_the_outlier_it_is_hunting(self):
        """Mean and std are dragged by extremes; median and MAD are not.

        Needs genuine spread — see the degenerate case below.
        """
        base = [9.0, 10.0, 11.0, 10.5, 9.5, 10.2, 9.8, 10.1, 9.9, 10.3]
        df = pl.DataFrame({"qty": base * 2 + [5000.0]})

        found = TierTwoOutlierDetector("t").add_mad_check("qty", threshold=3.5).detect(df)

        assert len(found) == 1

    def test_mad_of_zero_cannot_produce_a_divide_by_zero(self):
        """If most values equal the median, MAD is 0.

        The modified z-score would divide by it. The detector must return
        nothing rather than NaN — the same guard the z-score path has for
        zero variance.
        """
        df = pl.DataFrame({"qty": [10.0] * 20 + [5000.0]})

        assert TierTwoOutlierDetector("t").add_mad_check("qty").detect(df) == []

    def test_severity_is_carried_through_to_the_anomaly(self, outlier_series):
        found = (
            TierTwoOutlierDetector("t")
            .add_iqr_check("qty", severity=AnomalySeverity.CRITICAL)
            .detect(outlier_series)
        )

        assert found[0].severity is AnomalySeverity.CRITICAL

    def test_iqr_reports_the_bounds_it_judged_against(self):
        df = pl.DataFrame({"qty": [8.0, 9.0, 10.0, 11.0, 12.0, 500.0]})

        found = TierTwoOutlierDetector("t").add_iqr_check("qty").detect(df)

        low, high = found[0].expected_range
        assert low < 8.0
        assert 12.0 < high < 500.0

    def test_a_wider_multiplier_suppresses_a_borderline_outlier(self):
        df = pl.DataFrame({"qty": [10.0, 11.0, 12.0, 13.0, 14.0, 30.0]})

        tight = TierTwoOutlierDetector("t").add_iqr_check("qty", multiplier=1.5).detect(df)
        loose = TierTwoOutlierDetector("t").add_iqr_check("qty", multiplier=6.0).detect(df)

        assert len(tight) == 1
        assert loose == []
