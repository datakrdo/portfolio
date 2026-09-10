"""Leakage-aware endpoint metrics, calibration, and plotting helpers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, TypedDict

import numpy as np
from sklearn.calibration import calibration_curve
from sklearn.metrics import (
    average_precision_score,
    balanced_accuracy_score,
    brier_score_loss,
    cohen_kappa_score,
    confusion_matrix,
    f1_score,
    roc_auc_score,
)


class MetricsResult(TypedDict, total=False):
    """Serializable primary-endpoint metrics."""

    auroc: float
    auprc: float
    sensitivity: float
    specificity: float
    threshold: float
    positive_rate: float
    brier_score: float
    calibration_slope: float
    bootstrap_95_ci: dict[str, list[float]]


class ShapResult(TypedDict, total=False):
    """Plot-ready SHAP result container."""

    summary_plot_data: Any
    expected_values: Any
    feature_importance_df: Any
    X: Any


def select_operating_threshold(
    estimator: Any,
    X_train: Any,
    y_train: np.ndarray,
    *,
    scoring: str = "balanced_accuracy",
    cv: int = 5,
    random_state: int = 41,
) -> tuple[float, Any]:
    """Pick a decision threshold with `TunedThresholdClassifierCV`, never test labels.

    Replaces the manual validation-split threshold search: the estimator is
    refit across `cv` folds of the training data only, and the threshold that
    maximizes `scoring` on the held-out fold is kept. Returns the threshold
    and the CV-refit estimator (already `fit` on the full training data).
    """

    from sklearn.model_selection import StratifiedKFold, TunedThresholdClassifierCV

    tuned = TunedThresholdClassifierCV(
        estimator,
        scoring=scoring,
        cv=StratifiedKFold(cv, shuffle=True, random_state=random_state),
        random_state=random_state,
    )
    tuned.fit(X_train, y_train)
    return float(tuned.best_threshold_), tuned


def binary_metrics(
    y_true: np.ndarray, probabilities: np.ndarray, *, threshold: float = 0.5
) -> MetricsResult:
    """Calculate primary binary endpoint metrics at a preselected threshold."""

    y_true = np.asarray(y_true)
    probabilities = np.asarray(probabilities, dtype=float)
    predicted = (probabilities >= threshold).astype(int)
    tn, fp, fn, tp = confusion_matrix(y_true, predicted, labels=[0, 1]).ravel()
    return {
        "auroc": float(roc_auc_score(y_true, probabilities)),
        "auprc": float(average_precision_score(y_true, probabilities)),
        "sensitivity": float(tp / (tp + fn)) if tp + fn else 0.0,
        "specificity": float(tn / (tn + fp)) if tn + fp else 0.0,
        "threshold": float(threshold),
        "positive_rate": float(predicted.mean()),
        "brier_score": float(brier_score_loss(y_true, probabilities)),
    }


def multiclass_metrics(y_true: np.ndarray, predicted: np.ndarray) -> dict[str, float]:
    """Calculate secondary exact-stage and ordinal metrics."""

    return {
        "macro_f1": float(f1_score(y_true, predicted, average="macro")),
        "balanced_accuracy": float(balanced_accuracy_score(y_true, predicted)),
        "quadratic_weighted_kappa": float(
            cohen_kappa_score(y_true, predicted, weights="quadratic")
        ),
    }


def bootstrap_metric_intervals(
    y_true: np.ndarray,
    probabilities: np.ndarray,
    *,
    threshold: float,
    n_bootstrap: int = 500,
    random_state: int = 41,
) -> dict[str, list[float]]:
    """Estimate percentile intervals on a held-out set for uncertainty reporting."""

    if n_bootstrap < 1:
        return {}
    rng = np.random.default_rng(random_state)
    values: dict[str, list[float]] = {
        key: [] for key in ("auroc", "auprc", "sensitivity", "specificity")
    }
    for _ in range(n_bootstrap):
        indices = rng.integers(0, len(y_true), len(y_true))
        sample_y, sample_p = y_true[indices], probabilities[indices]
        if len(np.unique(sample_y)) < 2:
            continue
        metrics = binary_metrics(sample_y, sample_p, threshold=threshold)
        for key in values:
            values[key].append(metrics[key])
    return {
        key: [float(np.percentile(scores, 2.5)), float(np.percentile(scores, 97.5))]
        for key, scores in values.items()
        if scores
    }


def calibration_metrics(
    y_true: np.ndarray, probabilities: np.ndarray, *, n_bins: int = 10
) -> dict[str, Any]:
    """Return Brier score and points for a reliability diagram."""

    fraction, mean_predicted = calibration_curve(
        y_true, np.clip(probabilities, 0, 1), n_bins=n_bins, strategy="quantile"
    )
    slope, intercept = calibration_slope_intercept(y_true, probabilities)
    return {
        "brier_score": float(brier_score_loss(y_true, probabilities)),
        "fraction_of_positives": fraction.tolist(),
        "mean_predicted_value": mean_predicted.tolist(),
        "calibration_slope": slope,
        "calibration_intercept": intercept,
    }


def calibration_slope_intercept(
    y_true: np.ndarray, probabilities: np.ndarray, *, eps: float = 1e-6
) -> tuple[float, float]:
    """Calibration-in-the-large slope/intercept (Cox, 1958).

    Fits `logit(y) ~ intercept + slope * logit(p)` by unregularized logistic
    regression. Slope 1 and intercept 0 indicate perfect calibration; slope
    below 1 means predicted probabilities are too extreme (overconfident).
    """

    from sklearn.linear_model import LogisticRegression

    clipped = np.clip(np.asarray(probabilities, dtype=float), eps, 1 - eps)
    logit_p = np.log(clipped / (1 - clipped)).reshape(-1, 1)
    model = LogisticRegression(C=np.inf, max_iter=1000)  # unregularized (sklearn >=1.8)
    model.fit(logit_p, y_true)
    return float(model.coef_[0, 0]), float(model.intercept_[0])


def calibration_before_after(
    pipeline: Any,
    X_train: Any,
    y_train: np.ndarray,
    X_test: Any,
    y_test: np.ndarray,
    *,
    method: str = "sigmoid",
    cv: int = 5,
) -> dict[str, Any]:
    """Fit `pipeline` on train, wrap it with `calibrate_model`, and report the
    Cox calibration slope/intercept on `X_test` before and after.

    Kept as a single train/test comparison rather than refit inside every
    `nested_cv_evaluate` fold: 25 outer folds x a 5-fold `CalibratedClassifierCV`
    on top of hyperparameter tuning was not worth the runtime for what is
    fundamentally a diagnostic, not a headline discrimination metric.
    """

    from sklearn.base import clone
    from sklearn.calibration import CalibratedClassifierCV

    raw = clone(pipeline).fit(X_train, y_train)
    raw_proba = raw.predict_proba(X_test)[:, 1]
    raw_slope, raw_intercept = calibration_slope_intercept(y_test, raw_proba)

    calibrated = CalibratedClassifierCV(clone(pipeline), method=method, cv=cv)
    calibrated.fit(X_train, y_train)
    calibrated_proba = calibrated.predict_proba(X_test)[:, 1]
    cal_slope, cal_intercept = calibration_slope_intercept(y_test, calibrated_proba)

    return {
        "raw": {"slope": raw_slope, "intercept": raw_intercept},
        "calibrated": {"slope": cal_slope, "intercept": cal_intercept},
        "method": method,
    }


def net_benefit(
    y_true: np.ndarray, probabilities: np.ndarray, thresholds: np.ndarray
) -> dict[str, list[float]]:
    """Vickers decision-curve net benefit vs. the treat-all/treat-none defaults.

    At threshold probability `pt`, net benefit of the model is
    `TP/n - FP/n * pt/(1-pt)`; "treat all" uses the same formula with every
    patient predicted positive; "treat none" is always 0. A model only earns
    its complexity where its curve sits above both.
    """

    y_true = np.asarray(y_true)
    probabilities = np.asarray(probabilities, dtype=float)
    thresholds = np.asarray(thresholds, dtype=float)
    n = len(y_true)
    prevalence = float(y_true.mean())
    model_nb, treat_all_nb = [], []
    for pt in thresholds:
        if pt <= 0 or pt >= 1:
            model_nb.append(float("nan"))
            treat_all_nb.append(float("nan"))
            continue
        predicted = (probabilities >= pt).astype(int)
        tp = int(((predicted == 1) & (y_true == 1)).sum())
        fp = int(((predicted == 1) & (y_true == 0)).sum())
        odds = pt / (1 - pt)
        model_nb.append(tp / n - fp / n * odds)
        treat_all_nb.append(prevalence - (1 - prevalence) * odds)
    return {
        "threshold": thresholds.tolist(),
        "model": model_nb,
        "treat_all": treat_all_nb,
        "treat_none": [0.0] * len(thresholds),
    }


def decision_curve_analysis(
    y_true: np.ndarray,
    probabilities: np.ndarray,
    *,
    threshold_low: float = 0.05,
    threshold_high: float = 0.5,
    n_thresholds: int = 46,
) -> dict[str, list[float]]:
    """`net_benefit` over the clinically plausible threshold range."""

    thresholds = np.linspace(threshold_low, threshold_high, n_thresholds)
    return net_benefit(y_true, probabilities, thresholds)


def riley_minimum_sample_size(
    n_predictors: int,
    prevalence: float,
    *,
    expected_cs_r2: float = 0.15,
    shrinkage: float = 0.9,
    margin_of_error: float = 0.05,
) -> dict[str, float]:
    """Riley et al. (2020, *Stat Med*) minimum sample size for a binary model.

    Combines their three criteria and returns the max (their recommendation):
    (1) global shrinkage >= `shrinkage` via van Houwelingen's heuristic,
    (2) <=0.05 absolute difference between apparent and adjusted Cox-Snell
    R2 (a small-sample-size penalty term), and (3) a margin of error on the
    estimated outcome risk. `expected_cs_r2` should come from a comparable
    published model; 0.15 is a conservative placeholder when none exists.
    """

    if not 0 < prevalence < 1:
        raise ValueError("prevalence must be in (0, 1).")
    if not 0 < shrinkage < 1:
        raise ValueError("shrinkage must be in (0, 1).")
    p = n_predictors
    max_r2_cs = 1 - prevalence**prevalence * (1 - prevalence) ** (1 - prevalence)
    r2_cs_adj = expected_cs_r2 / max_r2_cs if max_r2_cs > 0 else expected_cs_r2
    # Criterion 1: shrinkage factor.
    n1 = p / ((shrinkage - 1) * np.log(1 - r2_cs_adj / shrinkage))
    # Criterion 2: small-sample-size penalty on Cox-Snell R2 (<=0.05 absolute).
    n2 = p / ((shrinkage - 1) * np.log(1 - r2_cs_adj))
    # Criterion 3: precision on the average outcome risk (margin of error).
    n3 = (1.96 / margin_of_error) ** 2 * prevalence * (1 - prevalence)
    n_required = max(n1, n2, n3)
    events_required = n_required * prevalence
    return {
        "n_required": float(np.ceil(n_required)),
        "events_required": float(np.ceil(events_required)),
        "epv_required": float(events_required / p) if p else float("nan"),
        "criterion_shrinkage": float(np.ceil(n1)),
        "criterion_r2_penalty": float(np.ceil(n2)),
        "criterion_precision": float(np.ceil(n3)),
    }


def prediction_stability(
    pipeline: Any,
    X: Any,
    y: np.ndarray,
    *,
    n_boot: int = 200,
    random_state: int = 41,
) -> dict[str, Any]:
    """Riley & Collins (2023) prediction instability: bootstrap-refit vs.
    original-fit predicted probabilities for every patient.

    Refits `pipeline` on `n_boot` bootstrap resamples of (X, y), scores the
    *original* rows each time, and compares each bootstrap-model prediction
    to the original model's prediction for the same patient. Wide spread
    means "which patients look high-risk" depends on which 412 rows happened
    to be sampled -- exactly the small-sample fragility EPV already predicts.
    """

    from sklearn.base import clone
    from sklearn.utils import resample

    y_array = np.asarray(y)
    original = clone(pipeline)
    original.fit(X, y_array)
    original_proba = original.predict_proba(X)[:, 1]

    n = len(X)
    boot_predictions = np.full((n_boot, n), np.nan)
    rng = np.random.default_rng(random_state)
    for b in range(n_boot):
        boot_idx = resample(
            np.arange(n),
            replace=True,
            n_samples=n,
            stratify=y_array,
            random_state=rng.integers(0, 2**32 - 1),
        )
        if len(np.unique(y_array[boot_idx])) < 2:
            continue
        boot_model = clone(pipeline)
        boot_model.fit(X.iloc[boot_idx], y_array[boot_idx])
        boot_predictions[b] = boot_model.predict_proba(X)[:, 1]

    valid = ~np.isnan(boot_predictions).all(axis=0)
    spread = np.nanstd(boot_predictions, axis=0)
    mape = float(
        np.nanmean(np.abs(boot_predictions - original_proba) / np.maximum(original_proba, 1e-6))
    )
    return {
        "original_probabilities": original_proba.tolist(),
        "bootstrap_mean": np.nanmean(boot_predictions, axis=0).tolist(),
        "bootstrap_std": spread.tolist(),
        "instability_mape": mape,
        "n_boot_valid": int(valid.sum()),
    }


def plot_roc_curve(
    y_true: np.ndarray, probabilities: np.ndarray, *, ax: Any = None, label: str = "model"
) -> Any:
    """Plot an AUROC curve and return its matplotlib axes."""

    import matplotlib.pyplot as plt
    from sklearn.metrics import RocCurveDisplay

    if ax is None:
        _, ax = plt.subplots(figsize=(6, 5))
    RocCurveDisplay.from_predictions(y_true, probabilities, name=label, ax=ax)
    ax.set_title("ROC curve")
    return ax


def plot_pr_curve(
    y_true: np.ndarray, probabilities: np.ndarray, *, ax: Any = None, label: str = "model"
) -> Any:
    """Plot a precision-recall curve and return its matplotlib axes."""

    import matplotlib.pyplot as plt
    from sklearn.metrics import PrecisionRecallDisplay

    if ax is None:
        _, ax = plt.subplots(figsize=(6, 5))
    PrecisionRecallDisplay.from_predictions(y_true, probabilities, name=label, ax=ax)
    ax.set_title("Precision-recall curve")
    return ax


def plot_calibration_curve(
    y_true: np.ndarray,
    probabilities: np.ndarray,
    *,
    ax: Any = None,
    label: str = "model",
    n_bins: int = 10,
) -> Any:
    """Plot predicted versus observed risk."""

    import matplotlib.pyplot as plt

    if ax is None:
        _, ax = plt.subplots(figsize=(6, 5))
    values = calibration_metrics(y_true, probabilities, n_bins=n_bins)
    ax.plot(values["mean_predicted_value"], values["fraction_of_positives"], "o-", label=label)
    ax.plot([0, 1], [0, 1], "--", color="grey", label="perfect calibration")
    ax.set(xlabel="Mean predicted probability", ylabel="Observed fraction", title="Calibration")
    ax.legend()
    return ax


def write_metrics(metrics: dict[str, Any], output_path: str | Path) -> None:
    """Write JSON metrics, creating only the requested output directory."""

    destination = Path(output_path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(metrics, indent=2, sort_keys=True) + "\n")
