"""Reproducible exploratory-data summary for the PBC cohort."""

from __future__ import annotations

from typing import Any

import pandas as pd

from .data import EXPECTED_COLUMNS


def summarize_dataset(frame: pd.DataFrame) -> dict[str, Any]:
    """Return schema, missingness, duplicates, and target distributions."""

    missing = frame.isna().sum()
    stage_counts = frame["Stage"].value_counts(dropna=False).sort_index()
    return {
        "rows": int(len(frame)),
        "columns": int(len(frame.columns)),
        "schema_valid": list(frame.columns) == EXPECTED_COLUMNS,
        "duplicate_rows": int(frame.duplicated().sum()),
        "missing_by_column": {
            str(column): int(count) for column, count in missing.items() if count
        },
        "stage_counts": {
            ("missing" if pd.isna(stage) else str(int(stage))): int(count)
            for stage, count in stage_counts.items()
        },
    }
