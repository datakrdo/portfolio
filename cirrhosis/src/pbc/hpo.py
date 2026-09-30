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
    """Optimize cross-validated log-loss with a median-pruner study.

    Log-loss (a proper scoring rule) rather than AUROC: AUROC ignores
    calibration and happily picks a near-constant, over-shrunk model, whose
    probabilities are useless for a risk estimate.
    """

    if n_trials < 1:
        raise ValueError("n_trials must be at least one.")
    if model_name not in {"logistic", "random_forest", "hist_gradient_boosting"}:
        raise ValueError(
            "Optuna tuning supports logistic, random_forest, and hist_gradient_boosting."
        )
    try:
        import optuna
    except ImportError as exc:
        raise ImportError("Optuna is optional; install pbc-modeling[optuna].") from exc
    optuna.logging.set_verbosity(
        optuna.logging.WARNING
    )  # one INFO line per trial floods notebooks and CI logs

    def objective(trial: Any) -> float:
        native = False
        splines = False
        if model_name == "logistic":
            from sklearn.linear_model import LogisticRegression

            estimator = LogisticRegression(
                max_iter=2000,
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
                random_state=random_state,
                n_jobs=-1,
            )
            scale = False
        else:
            from sklearn.ensemble import HistGradientBoostingClassifier

            estimator = HistGradientBoostingClassifier(
                categorical_features="from_dtype",
                max_iter=trial.suggest_int("max_iter", 100, 500),
                max_depth=trial.suggest_int("max_depth", 2, 8),
                learning_rate=trial.suggest_float("learning_rate", 0.01, 0.3, log=True),
                l2_regularization=trial.suggest_float("l2_regularization", 0.0, 5.0),
                min_samples_leaf=trial.suggest_int("min_samples_leaf", 5, 40),
                random_state=random_state,
            )
            scale = False
            native = True
        pipeline = build_model_pipeline(
            X_train, estimator, scale_numeric=scale, native_categorical=native, splines=splines
        )
        scores = cross_val_score(
            pipeline,
            X_train,
            y_train,
            cv=StratifiedKFold(cv, shuffle=True, random_state=random_state),
            scoring="neg_log_loss",
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
