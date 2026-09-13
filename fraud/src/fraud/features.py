"""Causal feature engineering: every aggregate looks only at a card's past.

`build_features` is the single place that turns a raw Sparkov frame into model
input, so the notebook, training, and batch scoring all compute features the
same way. The rolling windows are the load-bearing part of this module: they
must never see the current row or anything after it, or validation metrics
stop meaning anything. See `test_features.py::test_rolling_windows_exclude_current_row`.
"""

from __future__ import annotations

from typing import Final

import numpy as np
import pandas as pd

# Sparkov's `merch_lat`/`merch_long` are drawn uniformly around the
# cardholder's home address rather than from real merchant locations, so
# distance to the merchant carries no fraud signal in this dataset (verified
# in the notebook: ~76 km for both classes). Built anyway, because that null
# result is itself the point, and dropped from `NUMERIC_FEATURES` below.
EARTH_RADIUS_KM: Final[float] = 6371.0

# 22-23h shows a 5x lift in fraud rate over the 0.58% baseline (measured on
# fraudTrain); the cut is that empirical boundary, not an arbitrary "night".
NIGHT_HOURS: Final[set[int]] = {22, 23, 0, 1, 2, 3}

ROLLING_WINDOWS: Final[tuple[str, ...]] = ("1h", "24h", "7D")


def _haversine_km(lat1: pd.Series, lon1: pd.Series, lat2: pd.Series, lon2: pd.Series) -> pd.Series:
    lat1, lon1, lat2, lon2 = map(np.radians, (lat1, lon1, lat2, lon2))
    a = (
        np.sin((lat2 - lat1) / 2) ** 2
        + np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1) / 2) ** 2
    )
    return pd.Series(2 * EARTH_RADIUS_KM * np.arcsin(np.sqrt(a)), index=lat1.index)


def _card_velocity_features(frame: pd.DataFrame) -> pd.DataFrame:
    """Rolling transaction count/amount per card, strictly before the current row.

    Relies on `frame` already being sorted by `(cc_num, trans_date_trans_time)`
    (guaranteed by `data.load_transactions`): `groupby().rolling(on=..., closed="left")`
    returns its values in that same row order, so they can be assigned back
    positionally with `.to_numpy()`. `closed="left"` excludes the current
    transaction from its own window -- the invariant this whole module exists
    to protect.
    """

    out = {}
    grouped = frame.groupby("cc_num")
    seconds_since_prev = grouped["trans_date_trans_time"].diff().dt.total_seconds()
    # A card's first transaction has no predecessor. NaN (not np.inf) so it
    # stays finite for StandardScaler; 30 days is far past any real window
    # and clearly separable from an active card's actual gaps.
    out["seconds_since_prev_tx"] = seconds_since_prev.fillna(30 * 24 * 3600).to_numpy()

    for window in ROLLING_WINDOWS:
        rolled = grouped.rolling(window, on="trans_date_trans_time", closed="left")["amt"]
        out[f"tx_count_card_{window}"] = rolled.count().fillna(0).to_numpy()
        out[f"amt_sum_card_{window}"] = rolled.sum().fillna(0.0).to_numpy()

    return pd.DataFrame(out, index=frame.index)


def _card_history_features(frame: pd.DataFrame) -> pd.DataFrame:
    """Cumulative, strictly-past summaries of a card's amount and merchant history.

    `.shift(1)` on an `expanding()` aggregate is what keeps these causal: the
    expanding window naturally includes the current row, and shifting drops
    it back out. The first transaction on every card therefore has no history
    (`amt_zscore` is 0, `is_new_merchant`/`is_new_category` are True by
    construction), which is the correct cold-start behaviour, not an edge case
    to special-case away.
    """

    grouped = frame.groupby("cc_num")["amt"]
    running_mean = grouped.apply(lambda s: s.expanding().mean().shift(1)).droplevel(0)
    running_std = grouped.apply(lambda s: s.expanding().std().shift(1)).droplevel(0)
    running_median = grouped.apply(lambda s: s.expanding().median().shift(1)).droplevel(0)

    zscore = (frame["amt"] - running_mean) / running_std.replace(0, np.nan)
    ratio_to_median = frame["amt"] / running_median.replace(0, np.nan)

    seen_merchant = frame.groupby("cc_num")["merchant"].apply(lambda s: s.duplicated())
    seen_category = frame.groupby("cc_num")["category"].apply(lambda s: s.duplicated())

    return pd.DataFrame(
        {
            "amt_zscore_vs_card_history": zscore.fillna(0.0).to_numpy(),
            "amt_ratio_to_card_median": ratio_to_median.fillna(1.0).to_numpy(),
            "is_new_merchant_for_card": (~seen_merchant.to_numpy(dtype=bool)),
            "is_new_category_for_card": (~seen_category.to_numpy(dtype=bool)),
        },
        index=frame.index,
    )


def build_features(frame: pd.DataFrame) -> pd.DataFrame:
    """Derive the full feature set from a raw, causally-sorted Sparkov frame.

    Pure function of `frame`: no fitted state, so the same call produces
    identical columns whether it runs in the notebook, `pipeline.py`, or
    `cli.py score`. Categorical/high-cardinality columns (`category`,
    `merchant`, `job`, `state`) and the columns consumed here (`amt`,
    `trans_date_trans_time`, `dob`, `cc_num`, `lat`/`long`, `merch_lat`/`merch_long`)
    are left in the returned frame; `data.LEAKAGE_COLUMNS` are dropped later,
    by the modeling pipeline, so this function stays reusable for EDA too.
    """

    hour = frame["trans_date_trans_time"].dt.hour
    age_years = (frame["trans_date_trans_time"] - frame["dob"]).dt.days / 365.25

    engineered = pd.DataFrame(
        {
            "hour_sin": np.sin(2 * np.pi * hour / 24),
            "hour_cos": np.cos(2 * np.pi * hour / 24),
            "day_of_week": frame["trans_date_trans_time"].dt.dayofweek,
            "is_weekend": frame["trans_date_trans_time"].dt.dayofweek.isin([5, 6]),
            "is_night": hour.isin(NIGHT_HOURS),
            "age_years": age_years,
            "log_amt": np.log1p(frame["amt"]),
            "distance_km": _haversine_km(
                frame["lat"], frame["long"], frame["merch_lat"], frame["merch_long"]
            ),
        },
        index=frame.index,
    )

    return pd.concat(
        [frame, engineered, _card_velocity_features(frame), _card_history_features(frame)],
        axis=1,
    )
