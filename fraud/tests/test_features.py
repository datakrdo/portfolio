"""The causal invariant: no aggregate in `build_features` may see the current row or the future."""

from __future__ import annotations

import pandas as pd

from src.fraud.features import build_features


def _toy_frame() -> pd.DataFrame:
    times = pd.to_datetime(
        ["2020-01-01 10:00:00", "2020-01-01 10:30:00", "2020-01-01 11:00:00", "2020-01-02 09:00:00"]
    )
    return pd.DataFrame(
        {
            "cc_num": ["1", "1", "1", "1"],
            "trans_date_trans_time": times,
            "merchant": ["a", "a", "b", "b"],
            "category": ["shopping_net", "shopping_net", "grocery_pos", "grocery_pos"],
            "amt": [10.0, 20.0, 30.0, 40.0],
            "dob": pd.to_datetime(["1990-01-01"] * 4),
            "lat": [40.0] * 4,
            "long": [-70.0] * 4,
            "merch_lat": [40.1] * 4,
            "merch_long": [-70.1] * 4,
            "is_fraud": [0, 0, 0, 1],
        }
    )


def test_first_transaction_has_no_history():
    feat = build_features(_toy_frame())
    first = feat.iloc[0]
    assert first["tx_count_card_1h"] == 0
    assert first["amt_sum_card_1h"] == 0.0
    assert bool(first["is_new_merchant_for_card"]) is True
    assert bool(first["is_new_category_for_card"]) is True


def test_rolling_windows_exclude_current_row():
    baseline = build_features(_toy_frame())

    altered = _toy_frame()
    altered.loc[3, "amt"] = 999_999.0  # only the future row changes

    changed = build_features(altered)

    unaffected = baseline.iloc[:3]
    still = changed.iloc[:3]
    for col in ["tx_count_card_1h", "amt_sum_card_1h", "amt_zscore_vs_card_history"]:
        assert (unaffected[col].to_numpy() == still[col].to_numpy()).all(), col


def test_second_transaction_within_window_sees_only_the_first():
    feat = build_features(_toy_frame())
    second = feat.iloc[1]
    assert second["tx_count_card_1h"] == 1
    assert second["amt_sum_card_1h"] == 10.0
