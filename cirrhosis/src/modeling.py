"""Estimator factories, fitting helpers, and a cumulative ordinal classifier."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, ClassifierMixin, clone
from sklearn.ensemble import HistGradientBoostingClassifier, RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline

from .preprocessing import build_native_preprocessor, build_preprocessor

# Clinically motivated monotonic constraints for HistGradientBoosting when
# paired with `build_native_preprocessor` (columns kept in their original
# frame order). Higher bilirubin/copper/prothrombin and lower albumin/
# platelets should never *decrease* predicted advanced-stage risk, regardless
# of what an unconstrained tree finds in 329 training rows. Edema is left out
# even though its outcome rate is monotone (N < S < Y): sklearn's native
# categorical splits (`categorical_features="from_dtype"`) reject monotonic
# constraints on categorical columns, and Edema is cast to `category` dtype
# for this estimator -- see `NativeCategoricalCaster`. Sex, Ascites,
# Hepatomegaly, and Spiders are left unconstrained (0).
MONOTONIC_DIRECTIONS: dict[str, int] = {
    "Bilirubin": 1,
    "Albumin": -1,
    "Copper": 1,
    "Platelets": -1,
    "Prothrombin": 1,
}


class CumulativeOrdinalClassifier(BaseEstimator, ClassifierMixin):
    """Cumulative-link style classifier using one model for each ordered cutpoint."""

    def __init__(self, base_estimator: Any | None = None):
        self.base_estimator = base_estimator

    def fit(self, X: np.ndarray, y: pd.Series | np.ndarray) -> CumulativeOrdinalClassifier:
        values = np.asarray(y, dtype=int)
        self.classes_ = np.sort(np.unique(values))
        if len(self.classes_) < 2:
            raise ValueError("Ordinal classification requires at least two classes.")
        estimator = self.base_estimator or LogisticRegression(max_iter=2000, solver="lbfgs")
        self.models_ = []
        for cutpoint in self.classes_[:-1]:
            model = clone(estimator)
            model.fit(X, (values > cutpoint).astype(int))
            self.models_.append(model)
        return self

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        if not hasattr(self, "models_"):
            raise ValueError("The ordinal classifier must be fitted first.")
        cumulative = np.column_stack([model.predict_proba(X)[:, 1] for model in self.models_])
        cumulative = np.minimum.accumulate(cumulative, axis=1)
        probabilities = np.column_stack(
            [
                1.0 - cumulative[:, 0],
                *[
                    cumulative[:, index - 1] - cumulative[:, index]
                    for index in range(1, cumulative.shape[1])
                ],
                cumulative[:, -1],
            ]
        )
        probabilities = np.clip(probabilities, 0.0, 1.0)
        return probabilities / probabilities.sum(axis=1, keepdims=True)

    def predict(self, X: np.ndarray) -> np.ndarray:
        return self.classes_[np.argmax(self.predict_proba(X), axis=1)]


def monotonic_constraints_for(columns: Sequence[str]) -> list[int]:
    """Map `MONOTONIC_DIRECTIONS` onto a column order for `monotonic_cst`."""

    return [MONOTONIC_DIRECTIONS.get(column, 0) for column in columns]


def build_model_pipeline(
    frame: pd.DataFrame,
    estimator: Any,
    *,
    scale_numeric: bool = True,
    native_categorical: bool = False,
    splines: bool = False,
) -> Pipeline:
    """Create a fresh preprocessing-plus-estimator pipeline.

    ``native_categorical=True`` skips imputation and one-hot encoding
    entirely, casting text columns to `category` dtype instead. Only
    estimators that natively support missing values and categorical dtypes
    (currently `hist_gradient_boosting`) should be paired with it.

    ``splines=True`` adds a `SplineTransformer` after scaling for the imputed
    (non-native) preprocessor only, matching the configuration that gave the
    lowest-variance logistic AUROC in cross-validation.
    """

    if native_categorical:
        preprocessor = build_native_preprocessor(frame)
        if (
            isinstance(estimator, HistGradientBoostingClassifier)
            and estimator.monotonic_cst is None
        ):
            estimator = clone(estimator).set_params(
                monotonic_cst=monotonic_constraints_for(list(frame.columns))
            )
    else:
        preprocessor = build_preprocessor(frame, scale_numeric=scale_numeric, splines=splines)
    return Pipeline([("preprocess", preprocessor), ("model", estimator)])


@dataclass(frozen=True)
class ModelSpec:
    """One row of the model comparison table: how to build and fit a named model."""

    estimator_cls: type
    defaults: Callable[[int], dict[str, Any]]
    scale_numeric: bool = True
    native_categorical: bool = False


# Single source of truth for every estimator's default hyperparameters and
# preprocessing contract. `logistic_C=0.1` (ridge) was empirically better than
# the previous unregularized `C=1.0` at this sample size (5x5 repeated CV
# AUROC: 0.708 vs 0.690); HistGB's capacity is deliberately restricted for the
# same reason (see the plan). `train_named_model` tunes `logistic`,
# `random_forest`, `hist_gradient_boosting`, and `lightgbm` per outer CV fold
# via Optuna rather than fixing these by inspecting the scores being reported.
MODEL_SPECS: dict[str, ModelSpec] = {
    "logistic": ModelSpec(
        LogisticRegression,
        lambda rs: dict(max_iter=2000, class_weight="balanced", solver="lbfgs", C=0.1),
    ),
    "random_forest": ModelSpec(
        RandomForestClassifier,
        lambda rs: dict(
            n_estimators=300,
            min_samples_leaf=10,
            max_features="sqrt",
            class_weight="balanced",
            random_state=rs,
            n_jobs=-1,
        ),
        scale_numeric=False,
    ),
    "hist_gradient_boosting": ModelSpec(
        HistGradientBoostingClassifier,
        lambda rs: dict(
            categorical_features="from_dtype",
            max_depth=2,
            max_iter=60,
            learning_rate=0.05,
            l2_regularization=5.0,
            min_samples_leaf=25,
            class_weight="balanced",
            random_state=rs,
        ),
        scale_numeric=False,
        native_categorical=True,
    ),
    "multiclass_logistic": ModelSpec(
        LogisticRegression, lambda rs: dict(max_iter=2000, solver="lbfgs")
    ),
    "ordinal_logistic": ModelSpec(CumulativeOrdinalClassifier, lambda rs: {}),
}
try:
    from lightgbm import LGBMClassifier
except ImportError:
    pass
else:
    MODEL_SPECS["lightgbm"] = ModelSpec(
        LGBMClassifier,
        lambda rs: dict(
            n_estimators=200,
            learning_rate=0.04,
            num_leaves=15,
            subsample=0.8,
            colsample_bytree=0.8,
            class_weight="balanced",
            random_state=rs,
            verbosity=-1,
        ),
        scale_numeric=False,
    )

TUNABLE_MODELS = {"logistic", "random_forest", "hist_gradient_boosting", "lightgbm"}


def train_named_model(
    name: str,
    X_train: pd.DataFrame,
    y_train: pd.Series,
    n_trials: int | None = None,
    *,
    random_state: int = 41,
) -> Pipeline:
    """Fit one of the four tunable comparators, optionally Optuna-tuned.

    Shared by `train_models` and `src.validation`'s nested-CV inner loop, so
    both go through the same tuning path instead of one reimplementing it --
    hyperparameter search must sit inside the outer CV fold it is evaluated
    on, not be fixed once from the full data.
    """

    if name not in TUNABLE_MODELS:
        raise KeyError(f"Unknown primary model {name!r}.")
    if name == "lightgbm" and name not in MODEL_SPECS:
        raise ImportError("LightGBM is optional; install pbc-modeling[lgbm].")
    spec = MODEL_SPECS[name]
    params = spec.defaults(random_state)
    splines = False
    if n_trials:
        from .hpo import run_hyperparameter_study

        study = run_hyperparameter_study(
            X_train, y_train, model_name=name, n_trials=n_trials, random_state=random_state
        )
        best = dict(study.best_params)
        splines = best.pop("splines", False)
        params = {**params, **best}
    estimator = spec.estimator_cls(**params)
    pipe = build_model_pipeline(
        X_train,
        estimator,
        scale_numeric=spec.scale_numeric,
        native_categorical=spec.native_categorical,
        splines=splines,
    )
    pipe.fit(X_train, y_train)
    return pipe


def train_models(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    *,
    models: Sequence[str] = ("logistic", "random_forest"),
    random_state: int = 41,
    hpo_trials: int | None = None,
) -> dict[str, Pipeline]:
    """Fit the requested primary models with a common preprocessing contract."""

    return {
        name: train_named_model(name, X_train, y_train, hpo_trials, random_state=random_state)
        for name in models
    }


def build_named_pipeline(name: str, X_train: pd.DataFrame, *, random_state: int = 41) -> Pipeline:
    """Return an unfitted preprocessing-plus-estimator pipeline for `name`."""

    if name == "lightgbm" and name not in MODEL_SPECS:
        raise ImportError("LightGBM is optional; install pbc-modeling[lgbm].")
    if name not in MODEL_SPECS:
        raise KeyError(f"Unknown model {name!r}; choose from {sorted(MODEL_SPECS)}.")
    spec = MODEL_SPECS[name]
    estimator = spec.estimator_cls(**spec.defaults(random_state))
    return build_model_pipeline(
        X_train,
        estimator,
        scale_numeric=spec.scale_numeric,
        native_categorical=spec.native_categorical,
    )


def fit_model(
    name: str,
    X_train: pd.DataFrame,
    y_train: pd.Series,
    *,
    random_state: int = 41,
) -> Pipeline:
    """Fit one named model, with preprocessing learned from X_train only."""

    pipeline = build_named_pipeline(name, X_train, random_state=random_state)
    pipeline.fit(X_train, y_train)
    return pipeline
