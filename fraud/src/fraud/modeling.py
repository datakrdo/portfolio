"""Estimator factories: a dummy floor, a linear baseline, and two GPU boosters."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from sklearn.compose import ColumnTransformer
from sklearn.dummy import DummyClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from .features import ROLLING_WINDOWS

# Columns handed to the model. `distance_km` is included deliberately: the
# notebook measures that it carries no signal and the point is to show that
# measurement, not to hide the feature. High-cardinality `merchant`/`job` are
# left out of the one-hot set (693/494 levels) -- `category` and `state` are
# the categoricals cheap enough to one-hot.
NUMERIC_FEATURES: list[str] = [
    "amt",
    "log_amt",
    "city_pop",
    "hour_sin",
    "hour_cos",
    "day_of_week",
    "age_years",
    "distance_km",
    "amt_zscore_vs_card_history",
    "amt_ratio_to_card_median",
    "seconds_since_prev_tx",
    *[f"tx_count_card_{w}" for w in ROLLING_WINDOWS],
    *[f"amt_sum_card_{w}" for w in ROLLING_WINDOWS],
]
BOOLEAN_FEATURES: list[str] = [
    "is_weekend",
    "is_night",
    "is_new_merchant_for_card",
    "is_new_category_for_card",
]
CATEGORICAL_FEATURES: list[str] = ["category", "state"]
FEATURE_COLUMNS: list[str] = NUMERIC_FEATURES + BOOLEAN_FEATURES + CATEGORICAL_FEATURES


def build_preprocessor() -> ColumnTransformer:
    """One-hot the low-cardinality categoricals; scale numerics for the linear model.

    Tree models (LightGBM/XGBoost) don't need scaling, but a shared
    preprocessor keeps one pipeline definition per model instead of two, and
    `StandardScaler` on already-reasonable numeric ranges does not hurt them.
    """

    return ColumnTransformer(
        [
            ("numeric", StandardScaler(), NUMERIC_FEATURES),
            ("boolean", "passthrough", BOOLEAN_FEATURES),
            ("categorical", OneHotEncoder(handle_unknown="ignore"), CATEGORICAL_FEATURES),
        ]
    )


@dataclass(frozen=True)
class ModelSpec:
    """How to build one named model: its class and its default hyperparameters."""

    estimator_cls: type
    defaults: Callable[[float], dict[str, Any]]


def _scale_pos_weight(prevalence: float) -> float:
    return (1 - prevalence) / prevalence


MODEL_SPECS: dict[str, ModelSpec] = {
    "dummy": ModelSpec(DummyClassifier, lambda prevalence: dict(strategy="constant", constant=0)),
    "logreg": ModelSpec(
        LogisticRegression,
        lambda prevalence: dict(class_weight="balanced", max_iter=1000),
    ),
}

try:
    from lightgbm import LGBMClassifier
except ImportError:
    pass
else:
    MODEL_SPECS["lightgbm"] = ModelSpec(
        LGBMClassifier,
        lambda prevalence: dict(
            device="gpu",
            n_estimators=300,
            learning_rate=0.05,
            num_leaves=63,
            scale_pos_weight=_scale_pos_weight(prevalence),
            random_state=41,
            verbosity=-1,
        ),
    )

try:
    from xgboost import XGBClassifier
except ImportError:
    pass
else:
    MODEL_SPECS["xgboost"] = ModelSpec(
        XGBClassifier,
        lambda prevalence: dict(
            device="cuda",
            tree_method="hist",
            n_estimators=300,
            max_depth=6,
            learning_rate=0.05,
            scale_pos_weight=_scale_pos_weight(prevalence),
            random_state=41,
            eval_metric="aucpr",
        ),
    )


def build_pipeline(name: str, *, prevalence: float) -> Pipeline:
    """Build an unfitted preprocessing-plus-estimator pipeline for a registered model.

    `prevalence` (fraud rate of the training fold) drives `scale_pos_weight`,
    the loss-reweighting alternative to resampling -- see the notebook section
    on why this project doesn't use SMOTE.
    """

    if name not in MODEL_SPECS:
        raise KeyError(
            f"Unknown model {name!r}; choose from {sorted(MODEL_SPECS)} "
            "(install the [gpu] extra for lightgbm/xgboost)."
        )
    spec = MODEL_SPECS[name]
    estimator = spec.estimator_cls(**spec.defaults(prevalence))
    return Pipeline([("preprocess", build_preprocessor()), ("model", estimator)])
