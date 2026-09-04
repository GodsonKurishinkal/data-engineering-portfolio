"""SchemaEnforcer — the Bronze→Silver boundary.

The platform's first principle is that a bad batch stops at the boundary
rather than being logged and forgotten. These tests pin that behaviour:
strict mode raises, permissive mode records, and neither silently passes.
"""

from __future__ import annotations

import polars as pl
import pytest

from etl_framework.transformers.schema_enforcer import (
    ColumnSpec,
    SchemaEnforcer,
    SchemaRegistry,
    SchemaSpec,
    SchemaViolationError,
)


@pytest.fixture
def spec() -> SchemaSpec:
    return SchemaSpec(
        table_name="inventory",
        columns=[
            ColumnSpec("sku_id", pl.Utf8, nullable=False),
            ColumnSpec("on_hand_qty", pl.Int64, nullable=False),
            ColumnSpec("warehouse", pl.Utf8, nullable=True, default="UNKNOWN"),
            ColumnSpec("note", pl.Utf8, nullable=True),
        ],
        primary_key=["sku_id"],
    )


class TestRequiredColumns:
    def test_strict_mode_raises_on_missing_required_column(self, spec):
        enforcer = SchemaEnforcer(spec, strict=True)
        df = pl.DataFrame({"sku_id": ["A"]})  # on_hand_qty absent

        with pytest.raises(SchemaViolationError, match="on_hand_qty"):
            enforcer.enforce(df)

    def test_the_gate_blocks_by_default(self, spec):
        """The default must block, not warn.

        The platform's first claim is that a bad batch cannot pass the
        boundary. If the default were permissive, every call site that
        forgot an argument would quietly let one through — which is the
        failure this whole layer exists to prevent. Skipping the gate has
        to be explicit.
        """
        enforcer = SchemaEnforcer(spec)  # no strict= argument

        assert enforcer.strict is True
        with pytest.raises(SchemaViolationError, match="on_hand_qty"):
            enforcer.enforce(pl.DataFrame({"sku_id": ["A"]}))

    def test_registry_enforcers_also_block_by_default(self, spec):
        registry = SchemaRegistry()
        registry.register(spec)

        assert registry.get_enforcer("inventory").strict is True

    def test_permissive_mode_records_violation_instead_of_raising(self, spec):
        enforcer = SchemaEnforcer(spec, strict=False)
        df = pl.DataFrame({"sku_id": ["A"]})

        enforcer.enforce(df)

        violations = enforcer.get_violations()
        assert len(violations) == 1
        assert violations[0]["type"] == "missing_column"

    def test_a_missing_column_is_never_silently_ignored(self, spec):
        """Permissive is not the same as quiet."""
        enforcer = SchemaEnforcer(spec, strict=False)
        enforcer.enforce(pl.DataFrame({"sku_id": ["A"]}))
        assert enforcer.get_violations(), "a violation must be recorded"

    def test_required_columns_derive_from_nullability(self, spec):
        assert set(spec.get_required_columns()) == {"sku_id", "on_hand_qty"}


class TestDefaultsAndShape:
    def test_missing_optional_column_gets_its_default(self, spec):
        enforcer = SchemaEnforcer(spec)
        out = enforcer.enforce(pl.DataFrame({"sku_id": ["A"], "on_hand_qty": [5]}))

        assert out["warehouse"].to_list() == ["UNKNOWN"]

    def test_missing_nullable_column_without_default_becomes_null(self, spec):
        enforcer = SchemaEnforcer(spec)
        out = enforcer.enforce(pl.DataFrame({"sku_id": ["A"], "on_hand_qty": [5]}))

        assert out["note"].to_list() == [None]

    def test_output_column_order_follows_the_schema_not_the_input(self, spec):
        enforcer = SchemaEnforcer(spec)
        scrambled = pl.DataFrame(
            {"note": ["x"], "on_hand_qty": [5], "warehouse": ["DC"], "sku_id": ["A"]}
        )

        out = enforcer.enforce(scrambled)

        assert out.columns == ["sku_id", "on_hand_qty", "warehouse", "note"]

    def test_columns_outside_the_schema_are_dropped(self, spec):
        enforcer = SchemaEnforcer(spec)
        out = enforcer.enforce(
            pl.DataFrame({"sku_id": ["A"], "on_hand_qty": [5], "rogue": ["drop me"]})
        )

        assert "rogue" not in out.columns


class TestTypeCoercion:
    def test_string_digits_are_cast_to_the_declared_integer_type(self, spec):
        enforcer = SchemaEnforcer(spec)
        out = enforcer.enforce(pl.DataFrame({"sku_id": ["A"], "on_hand_qty": ["42"]}))

        assert out["on_hand_qty"].dtype == pl.Int64
        assert out["on_hand_qty"].to_list() == [42]

    def test_already_correct_types_survive_unchanged(self, spec, ):
        enforcer = SchemaEnforcer(spec)
        out = enforcer.enforce(pl.DataFrame({"sku_id": ["A"], "on_hand_qty": [7]}))

        assert out["on_hand_qty"].to_list() == [7]


class TestRegistry:
    def test_registered_schema_round_trips(self, spec):
        registry = SchemaRegistry()
        registry.register(spec)

        assert registry.get("inventory") is spec

    def test_unknown_table_returns_none_rather_than_raising(self):
        assert SchemaRegistry().get("does_not_exist") is None

    def test_registry_builds_an_enforcer_bound_to_the_schema(self, spec):
        registry = SchemaRegistry()
        registry.register(spec)

        enforcer = registry.get_enforcer("inventory", strict=True)

        assert isinstance(enforcer, SchemaEnforcer)
        assert enforcer.strict is True
        assert enforcer.schema.table_name == "inventory"


class TestValidate:
    def test_validate_is_true_for_a_conforming_frame(self, spec):
        enforcer = SchemaEnforcer(spec)
        assert enforcer.validate(pl.DataFrame({"sku_id": ["A"], "on_hand_qty": [1]}))

    def test_validate_is_false_when_a_required_column_is_absent(self, spec):
        enforcer = SchemaEnforcer(spec)
        assert not enforcer.validate(pl.DataFrame({"sku_id": ["A"]}))
