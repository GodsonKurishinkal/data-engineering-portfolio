"""Shared fixtures.

The suite covers the pure, deterministic parts of the framework: schema
enforcement, cleaning, and the statistical detectors. Anything that needs a
live source system (database, portal, browser) is out of scope here — those
are integration concerns and are not faked into looking tested.
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


@pytest.fixture
def inventory_df() -> pl.DataFrame:
    """A small, well-formed inventory frame."""
    return pl.DataFrame(
        {
            "sku_id": ["SKU-001", "SKU-002", "SKU-003"],
            "location_id": ["DC-01", "DC-01", "DC-02"],
            "on_hand_qty": [10, 25, 0],
            "extract_date": [date(2026, 1, 1)] * 3,
        }
    )


@pytest.fixture
def outlier_series() -> pl.DataFrame:
    """Twenty tight values plus one obvious outlier."""
    return pl.DataFrame({"qty": [10.0] * 20 + [500.0]})
