"""PR-AUC baselines: a random ranker should score close to prevalence, not 0.5."""

from __future__ import annotations

import numpy as np

from src.fraud.evaluation import binary_metrics, precision_at_k

RNG = np.random.default_rng(41)


def test_random_classifier_pr_auc_is_near_prevalence():
    n = 20_000
    y = (RNG.random(n) < 0.01).astype(float)
    proba = RNG.random(n)  # uninformative
    metrics = binary_metrics(y, proba, threshold=0.5)
    assert abs(metrics["pr_auc"] - metrics["prevalence"]) < 0.01
    assert 0.7 < metrics["pr_auc_lift"] < 1.5


def test_precision_at_k_perfect_ranker():
    n = 1000
    y = np.zeros(n)
    y[:10] = 1
    proba = np.where(y == 1, 1.0, 0.0)
    assert precision_at_k(y, proba, k=10) == 1.0
