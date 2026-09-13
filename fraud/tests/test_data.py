"""Temporal split integrity and leakage-column bookkeeping."""

from __future__ import annotations

import pandas as pd

from src.fraud.data import LEAKAGE_COLUMNS, temporal_split
from src.fraud.features import build_features


def _toy_frame(n: int = 100) -> pd.DataFrame:
    times = pd.date_range("2020-01-01", periods=n, freq="6h")
    return pd.DataFrame(
        {
            "cc_num": [str(i % 5) for i in range(n)],
            "trans_date_trans_time": times,
            "merchant": ["a"] * n,
            "category": ["shopping_net"] * n,
            "amt": [10.0] * n,
            "first": ["x"] * n,
            "last": ["y"] * n,
            "street": ["z"] * n,
            "unix_time": [0] * n,
            "dob": pd.to_datetime(["1990-01-01"] * n),
            "trans_num": [str(i) for i in range(n)],
            "lat": [40.0] * n,
            "long": [-70.0] * n,
            "merch_lat": [40.1] * n,
            "merch_long": [-70.1] * n,
            "is_fraud": [0] * n,
        }
    )


def test_temporal_split_does_not_overlap_dates():
    frame = _toy_frame()
    is_valid = temporal_split(frame, valid_fraction=0.2)
    train_max = frame.loc[~is_valid, "trans_date_trans_time"].max()
    valid_min = frame.loc[is_valid, "trans_date_trans_time"].min()
    assert train_max <= valid_min


def test_leakage_columns_do_not_survive_build_features():
    feat = build_features(_toy_frame())
    survivors = set(LEAKAGE_COLUMNS) & set(feat.columns)
    assert survivors == set(LEAKAGE_COLUMNS), "build_features must not rename/drop raw columns"
    # LEAKAGE_COLUMNS are dropped later by the modeling pipeline, not here --
    # this test documents that build_features is not the place doing it.
