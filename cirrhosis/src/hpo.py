"""Optional Optuna hyperparameter optimization for primary comparators."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from sklearn.model_selection import StratifiedKFold, cross_val_score

from .modeling import build_model_pipeline


@dataclass(frozen=True)
class StudyResult:
    """Small stable wrapper around an Optuna study."""

    study: Any

    @property
    def best_params(self) -> dict[str, Any]:
        return dict(self.study.best_params)

    @property
    def best_value(self) -> float:
        return float(self.study.best_value)


def run_hyperparameter_study(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    *,
    model_name: str = "random_forest",
    n_trials: int = 20,
    random_state: int = 41,
    cv: int = 3,
) -> StudyResult:
    """Optimize RF or LightGBM AUROC with a median-pruner study."""

    if n_trials < 1:
        raise ValueError("n_trials must be at least one.")
    if model_name not in {"logistic", "random_forest", "hist_gradient_boosting", "lightgbm"}:
        raise ValueError(
            "Optuna tuning supports logistic, random_forest, hist_gradient_boosting, and lightgbm."
        )
    try:
        import optuna
    except ImportError as exc:
        raise ImportError("Optuna is optional; install pbc-modeling[optuna].") from exc

    def objective(trial: Any) -> float:
        native = False
        splines = False
        if model_name == "logistic":
            from sklearn.linear_model import LogisticRegression

            estimator = LogisticRegression(
                max_iter=2000,
                class_weight="balanced",
                solver="lbfgs",
                C=trial.suggest_float("C", 1e-3, 1e2, log=True),
            )
            scale = True
            splines = trial.suggest_categorical("splines", [False, True])
        elif model_name == "random_forest":
            from sklearn.ensemble import RandomForestClassifier

            estimator = RandomForestClassifier(
                n_estimators=trial.suggest_int("n_estimators", 100, 500),
                max_depth=trial.suggest_int("max_depth", 2, 15),
                min_samples_leaf=trial.suggest_int("min_samples_leaf", 1, 8),
                max_features=trial.suggest_float("max_features", 0.4, 1.0),
                class_weight="balanced",
                random_state=random_state,
                n_jobs=-1,
            )
            scale = False
        elif model_name == "hist_gradient_boosting":
            from sklearn.ensemble import HistGradientBoostingClassifier

            estimator = HistGradientBoostingClassifier(
                categorical_features="from_dtype",
                max_iter=trial.suggest_int("max_iter", 100, 500),
                max_depth=trial.suggest_int("max_depth", 2, 8),
                learning_rate=trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
                l2_regularization=trial.suggest_float("l2_regularization", 0.0, 5.0),
                min_samples_leaf=trial.suggest_int("min_samples_leaf", 5, 40),
                class_weight="balanced",
                random_state=random_state,
            )
            scale = False
            native = True
        else:
            try:
                from lightgbm import LGBMClassifier
            except ImportError as exc:
                raise ImportError("LightGBM is optional; install pbc-modeling[lgbm].") from exc
            estimator = LGBMClassifier(
                n_estimators=trial.suggest_int("n_estimators", 50, 400),
                learning_rate=trial.suggest_float("learning_rate", 0.01, 0.2, log=True),
                num_leaves=trial.suggest_int("num_leaves", 7, 63),
                max_depth=trial.suggest_int("max_depth", 2, 10),
                min_child_samples=trial.suggest_int("min_child_samples", 5, 40),
                class_weight="balanced",
                random_state=random_state,
                verbosity=-1,
            )
            scale = False
        pipeline = build_model_pipeline(
            X_train, estimator, scale_numeric=scale, native_categorical=native, splines=splines
        )
        scores = cross_val_score(
            pipeline,
            X_train,
            y_train,
            cv=StratifiedKFold(cv, shuffle=True, random_state=random_state),
            scoring="roc_auc",
            n_jobs=1,
        )
        value = float(np.mean(scores))
        trial.report(value, step=0)
        if trial.should_prune():
            raise optuna.TrialPruned()
        return value

    sampler = optuna.samplers.TPESampler(seed=random_state)
    study = optuna.create_study(
        direction="maximize",
        sampler=sampler,
        pruner=optuna.pruners.MedianPruner(n_startup_trials=min(5, n_trials)),
    )
    study.optimize(objective, n_trials=n_trials)
    return StudyResult(study)
