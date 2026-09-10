"""Small-sample validation: repeated nested cross-validation and optimism
correction, replacing a single 80/20 split as the headline estimate.

With n=412 and an outer test fold of ~83 rows, a single split gives a 95% CI
on AUROC roughly 0.26 wide (see `metrics.json` from the original split) --
too wide to compare models or defend a threshold. Two complementary
small-sample estimators are provided instead:

- `nested_cv_evaluate`: repeated stratified k-fold outer loop, with threshold
  selection and (optionally) hyperparameter search confined to each outer
  training fold. Reports the distribution of metrics across folds rather
  than a single point estimate.
- `bootstrap_optimism_correction`: Harrell's 0.632-style optimism correction,
  which fits on the full 412 rows (no data held out) and subtracts the
  average apparent-vs-original gap measured over bootstrap resamples. This
  is the method Harrell recommends specifically for small clinical cohorts
  where reserving a test set is wasteful.

Both operate on the primary binary endpoint only.
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd
from sklearn.base import clone
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.model_selection import RepeatedStratifiedKFold
from sklearn.utils import resample

from .evaluation import binary_metrics, select_operating_threshold
from .modeling import build_named_pipeline, train_named_model


def _tuned_pipeline(
    model_name: str,
    X_train: pd.DataFrame,
    y_train: np.ndarray,
    *,
    n_trials: int,
    random_state: int,
) -> Any:
    """Unfitted pipeline with hyperparameters chosen by an inner-loop Optuna study.

    Fits once via `train_named_model` to run the search, then returns an
    unfitted `clone()` (same hyperparameters, no leaked fit state) so callers
    -- `select_operating_threshold`'s internal CV in particular -- do their
    own fitting on data the search has already seen only through CV, never
    through a held-out fold used for reporting.
    """

    try:
        tuned = train_named_model(model_name, X_train, y_train, n_trials, random_state=random_state)
    except ImportError:
        # Optuna (or an optional model dependency) is unavailable; fall back
        # to `MODEL_SPECS`' fixed defaults rather than fail the fold.
        return build_named_pipeline(model_name, X_train, random_state=random_state)
    return clone(tuned)


def nested_cv_evaluate(
    X: pd.DataFrame,
    y: pd.Series,
    model_name: str,
    *,
    outer_splits: int = 5,
    outer_repeats: int = 5,
    inner_splits: int = 3,
    threshold_scoring: str = "balanced_accuracy",
    random_state: int = 41,
    tune_hyperparameters: bool = True,
    n_trials: int = 10,
) -> dict[str, Any]:
    """Repeated nested CV: outer loop estimates performance, inner loop tunes
    hyperparameters (`tune_hyperparameters=True`, the default) and the
    decision threshold. No test-fold observation informs its own
    hyperparameters, threshold, or preprocessing fit.

    `tune_hyperparameters` matters for the headline numbers: fixing
    hyperparameters once by inspecting cross-validated scores and then
    reporting those same scores is the exact optimism this module exists to
    avoid (see Phase 9 of the plan). With it on, each outer fold reruns its
    own Optuna search (`n_trials`, scored via `inner_splits`-fold CV) before
    threshold selection, so the reported AUROC already reflects the cost of
    tuning rather than assuming it away.

    Returns per-fold metric lists plus mean/std/95% percentile summaries --
    the fold-to-fold spread is the point, not just the mean.
    """

    if outer_splits < 2:
        raise ValueError("outer_splits must be at least 2.")
    y_array = np.asarray(y)
    outer_cv = RepeatedStratifiedKFold(
        n_splits=outer_splits, n_repeats=outer_repeats, random_state=random_state
    )
    fold_metrics: dict[str, list[float]] = {
        key: [] for key in ("auroc", "auprc", "sensitivity", "specificity", "brier_score")
    }
    thresholds: list[float] = []
    for train_idx, test_idx in outer_cv.split(X, y_array):
        X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
        y_train, y_test = y_array[train_idx], y_array[test_idx]
        pipeline = (
            _tuned_pipeline(
                model_name, X_train, y_train, n_trials=n_trials, random_state=random_state
            )
            if tune_hyperparameters
            else build_named_pipeline(model_name, X_train, random_state=random_state)
        )
        threshold, tuned = select_operating_threshold(
            pipeline,
            X_train,
            y_train,
            scoring=threshold_scoring,
            cv=min(inner_splits, int(np.min(np.bincount(y_train)))),
            random_state=random_state,
        )
        probabilities = tuned.predict_proba(X_test)[:, 1]
        metrics = binary_metrics(y_test, probabilities, threshold=threshold)
        for key in fold_metrics:
            fold_metrics[key].append(metrics[key])
        thresholds.append(threshold)
    summary = {
        key: {
            "mean": float(np.mean(values)),
            "std": float(np.std(values)),
            "ci_95": [
                float(np.percentile(values, 2.5)),
                float(np.percentile(values, 97.5)),
            ],
        }
        for key, values in fold_metrics.items()
    }
    return {
        "model": model_name,
        "n_outer_folds": outer_splits * outer_repeats,
        "fold_metrics": fold_metrics,
        "summary": summary,
        "median_threshold": float(np.median(thresholds)),
    }


def bootstrap_optimism_correction(
    X: pd.DataFrame,
    y: pd.Series,
    model_name: str,
    *,
    n_boot: int = 200,
    random_state: int = 41,
    tune_hyperparameters: bool = False,
    n_trials: int = 10,
) -> dict[str, Any]:
    """Harrell's optimism correction for AUROC/AUPRC, fit on the full cohort.

    1. Fit on the full (apparent) data, score the full data -> `apparent`.
    2. For each of `n_boot` bootstrap resamples D*: fit on D*, score on D*
       (`boot_apparent`) and score the same D*-fitted model on the original
       full data (`boot_test_original`). `optimism_b = boot_apparent -
       boot_test_original`.
    3. `corrected = apparent - mean(optimism_b)`.

    This uses every row for both fitting and evaluation, appropriate for a
    cohort too small to sacrifice a held-out fold as the headline number.

    `tune_hyperparameters` defaults to `False`: rerunning an Optuna search
    inside each of `n_boot` resamples is `n_boot * n_trials` fits and isn't
    worth the runtime here -- `nested_cv_evaluate(tune_hyperparameters=True)`
    is the estimator that already prices in tuning cost honestly. Set it
    `True` only if you specifically need optimism-corrected numbers under a
    per-resample search.
    """

    if n_boot < 1:
        raise ValueError("n_boot must be at least one.")
    rng = np.random.default_rng(random_state)
    y_array = np.asarray(y)
    scorers = {"auroc": roc_auc_score, "auprc": average_precision_score}

    def _fresh_pipeline(X_fit: pd.DataFrame, y_fit: np.ndarray) -> Any:
        if tune_hyperparameters:
            return _tuned_pipeline(
                model_name, X_fit, y_fit, n_trials=n_trials, random_state=random_state
            )
        return build_named_pipeline(model_name, X_fit, random_state=random_state)

    apparent_pipeline = _fresh_pipeline(X, y_array)
    apparent_pipeline.fit(X, y_array)
    apparent_proba = apparent_pipeline.predict_proba(X)[:, 1]
    apparent = {name: float(scorer(y_array, apparent_proba)) for name, scorer in scorers.items()}

    optimism: dict[str, list[float]] = {name: [] for name in scorers}
    n = len(X)
    for _b in range(n_boot):
        boot_idx = resample(
            np.arange(n),
            replace=True,
            n_samples=n,
            stratify=y_array,
            random_state=rng.integers(0, 2**32 - 1),
        )
        if len(np.unique(y_array[boot_idx])) < 2:
            continue
        X_boot, y_boot = X.iloc[boot_idx], y_array[boot_idx]
        boot_pipeline = _fresh_pipeline(X_boot, y_boot)
        boot_pipeline.fit(X_boot, y_boot)
        boot_apparent_proba = boot_pipeline.predict_proba(X_boot)[:, 1]
        boot_original_proba = boot_pipeline.predict_proba(X)[:, 1]
        for name, scorer in scorers.items():
            boot_apparent = scorer(y_boot, boot_apparent_proba)
            boot_original = scorer(y_array, boot_original_proba)
            optimism[name].append(boot_apparent - boot_original)

    mean_optimism = {name: float(np.mean(values)) for name, values in optimism.items()}
    corrected = {name: apparent[name] - mean_optimism[name] for name in scorers}
    return {
        "model": model_name,
        "n_boot": n_boot,
        "apparent": apparent,
        "optimism": mean_optimism,
        "corrected": corrected,
    }
