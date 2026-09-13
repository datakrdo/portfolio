"""Metrics for a rare-event classifier: everything read relative to prevalence."""

from __future__ import annotations

from typing import TypedDict

import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score, confusion_matrix, roc_auc_score


class MetricsResult(TypedDict):
    """One model's scorecard at a fixed decision threshold."""

    prevalence: float
    pr_auc: float
    pr_auc_lift: float
    roc_auc: float
    precision: float
    recall: float
    tn: int
    fp: int
    fn: int
    tp: int


def binary_metrics(
    y_true: pd.Series | np.ndarray, proba: np.ndarray, *, threshold: float
) -> MetricsResult:
    """Score predicted probabilities against labels at one threshold.

    `pr_auc_lift` divides PR-AUC by the prevalence baseline: a random ranker
    scores PR-AUC == prevalence, so a raw PR-AUC of 0.25 at 0.4% prevalence is
    a ~60x lift, not the unimpressive number it looks like next to 1.0.
    Accuracy is deliberately not returned -- the constant-negative classifier
    scores >99% on this dataset, so it would only mislead.
    """

    y_true = np.asarray(y_true)
    prevalence = float(y_true.mean())
    pr_auc = float(average_precision_score(y_true, proba))
    tn, fp, fn, tp = confusion_matrix(y_true, proba >= threshold).ravel()
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    return MetricsResult(
        prevalence=prevalence,
        pr_auc=pr_auc,
        pr_auc_lift=pr_auc / prevalence if prevalence else float("nan"),
        roc_auc=float(roc_auc_score(y_true, proba)),
        precision=precision,
        recall=recall,
        tn=int(tn),
        fp=int(fp),
        fn=int(fn),
        tp=int(tp),
    )


def precision_at_k(y_true: pd.Series | np.ndarray, proba: np.ndarray, k: int) -> float:
    """Precision among the top-`k` highest-scored transactions.

    The metric a fixed-capacity review queue actually cares about: if the
    team can only look at `k` alerts a day, this is the number that matters,
    not PR-AUC integrated over every threshold.
    """

    y_true = np.asarray(y_true)
    if k <= 0:
        raise ValueError("k must be positive.")
    top_k = np.argsort(proba)[::-1][:k]
    return float(y_true[top_k].mean())
