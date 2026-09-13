"""Turning a probability into a decision: the net-savings threshold and its economics.

Every function here works in real dollars (`amt`), never `log_amt` -- the
whole point of this module is that the operating threshold is a business
decision, not a modeling one, and a business decision has to be denominated
in money someone actually recognizes.
"""

from __future__ import annotations

from typing import Final

import numpy as np
import pandas as pd

# Cost of one analyst reviewing one flagged transaction, in USD. Deliberately
# a constant, not a fitted parameter: it is an assumption from outside the
# data, and `sensitivity_to_fp_cost` exists precisely to show how much the
# recommended threshold depends on it.
FP_COST: Final[float] = 4.0


def threshold_sweep(
    y_true: pd.Series | np.ndarray,
    proba: np.ndarray,
    amounts: pd.Series | np.ndarray,
    *,
    fp_cost: float = FP_COST,
    grid: np.ndarray | None = None,
    days: float = 1.0,
) -> pd.DataFrame:
    """Net savings, alert volume, and confusion counts across a grid of thresholds.

    Vectorized: transactions are sorted once by score, and every count at
    threshold `t` is a cumulative sum over that sorted order, so the whole
    grid costs one sort plus a handful of cumsums rather than a loop that
    reruns a confusion matrix per threshold.
    """

    y_true = np.asarray(y_true, dtype=float)
    amounts = np.asarray(amounts, dtype=float)
    if grid is None:
        grid = np.linspace(0.0, 1.0, 101)

    order = np.argsort(-proba)
    y_sorted = y_true[order]
    amt_sorted = amounts[order]
    proba_sorted = proba[order]

    cum_tp_amt = np.concatenate([[0.0], np.cumsum(y_sorted * amt_sorted)])
    cum_tp = np.concatenate([[0.0], np.cumsum(y_sorted)])
    total_fraud_amt = amt_sorted[y_sorted == 1].sum()
    total_fraud_n = y_sorted.sum()

    # Number of alerts at threshold t = count of scores >= t. `-proba_sorted`
    # is ascending (proba_sorted is descending), so the count of elements
    # <= -t in it -- found with searchsorted(side="right") -- is exactly the
    # count of original scores >= t, ties included.
    n_alerts = np.searchsorted(-proba_sorted, -grid, side="right")

    tp = cum_tp[n_alerts]
    fraud_caught_usd = cum_tp_amt[n_alerts]
    fp = n_alerts - tp
    fn = total_fraud_n - tp
    fraud_missed_usd = total_fraud_amt - fraud_caught_usd
    review_cost_usd = fp * fp_cost
    net_savings_usd = fraud_caught_usd - review_cost_usd

    with np.errstate(divide="ignore", invalid="ignore"):
        precision = np.where(n_alerts > 0, tp / n_alerts, 0.0)
        recall = np.where(total_fraud_n > 0, tp / total_fraud_n, 0.0)

    return pd.DataFrame(
        {
            "threshold": grid,
            "alerts": n_alerts,
            "alerts_per_day": n_alerts / days,
            "tp": tp,
            "fp": fp,
            "fn": fn,
            "precision": precision,
            "recall": recall,
            "fraud_caught_usd": fraud_caught_usd,
            "fraud_missed_usd": fraud_missed_usd,
            "review_cost_usd": review_cost_usd,
            "net_savings_usd": net_savings_usd,
        }
    )


def select_threshold_by_savings(
    y_true: pd.Series | np.ndarray,
    proba: np.ndarray,
    amounts: pd.Series | np.ndarray,
    *,
    fp_cost: float = FP_COST,
    grid: np.ndarray | None = None,
) -> float:
    """Threshold maximizing net savings, meant to be picked on validation and frozen.

    Selecting this on the holdout would be the same leak as tuning any other
    hyperparameter there; `pipeline.py` calls this on validation predictions
    only and stores the result alongside the fitted model.
    """

    sweep = threshold_sweep(y_true, proba, amounts, fp_cost=fp_cost, grid=grid)
    return float(sweep.loc[sweep["net_savings_usd"].idxmax(), "threshold"])


def marginal_analysis(sweep: pd.DataFrame) -> pd.DataFrame:
    """Dollar-per-alert cost of moving to the next lower threshold in `sweep`.

    `sweep` must be sorted by descending threshold (the default `grid` from
    `threshold_sweep` already is). Each row answers: lowering the threshold
    to this level catches how many more dollars of fraud, at the cost of how
    many more alerts, and is that marginal dollar-per-alert still worth
    reviewing? The net-savings optimum is exactly where `marginal_usd_per_alert`
    crosses the review cost -- above it, lowering the threshold still helps;
    below it, it starts destroying value.
    """

    ordered = sweep.sort_values("threshold", ascending=False).reset_index(drop=True)
    d_fraud_caught = ordered["fraud_caught_usd"].diff().fillna(0.0)
    d_alerts = ordered["alerts"].diff().fillna(0.0)
    with np.errstate(divide="ignore", invalid="ignore"):
        marginal = np.where(d_alerts > 0, d_fraud_caught / d_alerts, 0.0)
    return ordered.assign(
        marginal_fraud_caught_usd=d_fraud_caught,
        marginal_alerts=d_alerts,
        marginal_usd_per_alert=marginal,
    )


def sensitivity_to_fp_cost(
    y_true: pd.Series | np.ndarray,
    proba: np.ndarray,
    amounts: pd.Series | np.ndarray,
    *,
    fp_costs: tuple[float, ...] = (1.0, 4.0, 10.0, 50.0),
) -> pd.DataFrame:
    """How the optimal threshold and its savings move as the assumed review cost changes."""

    rows = []
    for cost in fp_costs:
        sweep = threshold_sweep(y_true, proba, amounts, fp_cost=cost)
        best = sweep.loc[sweep["net_savings_usd"].idxmax()]
        rows.append(
            {
                "fp_cost": cost,
                "optimal_threshold": best["threshold"],
                "net_savings_usd": best["net_savings_usd"],
                "alerts_per_day": best["alerts_per_day"],
            }
        )
    return pd.DataFrame(rows)
