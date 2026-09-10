import numpy as np
import pytest

from src.evaluation import (
    binary_metrics,
    bootstrap_metric_intervals,
    calibration_metrics,
    net_benefit,
    riley_minimum_sample_size,
)


def test_binary_metrics_and_calibration():
    y = np.array([0, 0, 1, 1])
    p = np.array([0.1, 0.3, 0.7, 0.9])
    metrics = binary_metrics(y, p)
    assert 0 <= metrics["auroc"] <= 1
    assert 0 <= metrics["brier_score"] <= 1
    assert set(calibration_metrics(y, p)["fraction_of_positives"]) <= {0.0, 1.0}


def test_bootstrap_metric_intervals_is_reproducible():
    y = np.array([0, 0, 1, 1, 0, 1])
    p = np.array([0.1, 0.2, 0.4, 0.8, 0.3, 0.9])
    first = bootstrap_metric_intervals(y, p, threshold=0.5, n_bootstrap=20)
    second = bootstrap_metric_intervals(y, p, threshold=0.5, n_bootstrap=20)
    assert first == second


def test_net_benefit_treat_all_is_zero_at_prevalence_threshold():
    y = np.array([0, 0, 0, 1, 1, 1, 1, 0, 1, 0])
    prevalence = float(y.mean())
    p = np.linspace(0.1, 0.9, len(y))
    result = net_benefit(y, p, np.array([prevalence]))
    assert result["treat_all"][0] == pytest.approx(0.0, abs=1e-9)


def test_riley_minimum_sample_size_matches_published_formula():
    """Independently reimplements Riley et al. (2020, Stat Med) eqs 5-7."""
    n_predictors, prevalence, expected_cs_r2, shrinkage, margin = 12, 0.3, 0.15, 0.9, 0.05
    max_r2_cs = 1 - prevalence**prevalence * (1 - prevalence) ** (1 - prevalence)
    r2_cs_adj = expected_cs_r2 / max_r2_cs
    n1 = n_predictors / ((shrinkage - 1) * np.log(1 - r2_cs_adj / shrinkage))
    n2 = n_predictors / ((shrinkage - 1) * np.log(1 - r2_cs_adj))
    n3 = (1.96 / margin) ** 2 * prevalence * (1 - prevalence)
    expected_n = np.ceil(max(n1, n2, n3))

    result = riley_minimum_sample_size(
        n_predictors,
        prevalence,
        expected_cs_r2=expected_cs_r2,
        shrinkage=shrinkage,
        margin_of_error=margin,
    )
    assert result["n_required"] == pytest.approx(expected_n)
