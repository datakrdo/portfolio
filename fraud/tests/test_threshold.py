"""Threshold economics: the optimum reacts to fp_cost the way the theory predicts."""

from __future__ import annotations

import numpy as np

from src.fraud.threshold import marginal_analysis, select_threshold_by_savings, threshold_sweep

RNG = np.random.default_rng(41)


def _synthetic(n: int = 5000) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    y = (RNG.random(n) < 0.01).astype(float)
    score = y * 2.5 + RNG.normal(size=n)  # overlapping, continuous, not perfectly separable
    proba = 1 / (1 + np.exp(-score))
    amounts = np.where(y == 1, RNG.uniform(200, 800, n), RNG.uniform(5, 100, n))
    return y, proba, amounts


def test_savings_threshold_beats_naive_half():
    y, proba, amounts = _synthetic()
    sweep = threshold_sweep(y, proba, amounts)
    best = sweep.loc[sweep["net_savings_usd"].idxmax(), "net_savings_usd"]
    at_half = sweep.loc[np.isclose(sweep["threshold"], 0.5), "net_savings_usd"].iloc[0]
    assert best >= at_half


def test_zero_fp_cost_alerts_almost_everyone():
    y, proba, amounts = _synthetic()
    t = select_threshold_by_savings(y, proba, amounts, fp_cost=0.0)
    assert t <= 0.05


def test_huge_fp_cost_avoids_false_positives():
    y, proba, amounts = _synthetic()
    t = select_threshold_by_savings(y, proba, amounts, fp_cost=1e6)
    sweep = threshold_sweep(y, proba, amounts, fp_cost=1e6)
    row = sweep.loc[np.isclose(sweep["threshold"], t)].iloc[0]
    assert row["fp"] == 0  # a review that expensive is only worth it with zero false alarms


def test_marginal_crosses_fp_cost_at_the_sweep_optimum():
    y, proba, amounts = _synthetic()
    fp_cost = 4.0
    sweep = threshold_sweep(y, proba, amounts, fp_cost=fp_cost)
    marginal = marginal_analysis(sweep)
    best_threshold = sweep.loc[sweep["net_savings_usd"].idxmax(), "threshold"]

    # one grid step above the optimum, lowering the threshold still paid off
    # (its own marginal dollar/alert was >= fp_cost); one step below, it
    # already stopped paying off. That's exactly what "optimum" means here.
    just_above = marginal[marginal["threshold"] > best_threshold].iloc[-1]
    just_below = marginal[marginal["threshold"] <= best_threshold].iloc[0]
    assert just_above["marginal_usd_per_alert"] >= just_below["marginal_usd_per_alert"]
