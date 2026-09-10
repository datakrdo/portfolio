"""Unified machine-readable and visual reporting for model evaluations."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import numpy as np

from .evaluation import (
    decision_curve_analysis,
    plot_calibration_curve,
    plot_pr_curve,
    plot_roc_curve,
    write_metrics,
)


def plot_decision_curve(
    y_true: Any, probabilities: Any, *, ax: Any = None, label: str = "model"
) -> Any:
    """Plot net benefit vs. treat-all/treat-none over the plausible threshold range."""

    import matplotlib.pyplot as plt

    if ax is None:
        _, ax = plt.subplots(figsize=(7, 5))
    dca = decision_curve_analysis(y_true, probabilities)
    ax.plot(dca["threshold"], dca["model"], label=label)
    ax.plot(dca["threshold"], dca["treat_all"], "--", color="grey", label="treat all")
    ax.plot(dca["threshold"], dca["treat_none"], ":", color="black", label="treat none")
    ax.set(
        xlabel="Threshold probability",
        ylabel="Net benefit",
        title="Decision curve analysis",
    )
    ax.legend()
    return ax


def plot_prediction_stability(stability: dict[str, Any], *, ax: Any = None) -> Any:
    """Scatter original vs. bootstrap-mean predicted probability per patient.

    Points on the diagonal are stable; scatter off it shows how much
    "who looks high-risk" would change under a different 412-row sample.
    """

    import matplotlib.pyplot as plt
    import numpy as np

    if ax is None:
        _, ax = plt.subplots(figsize=(6, 6))
    original = np.asarray(stability["original_probabilities"])
    boot_mean = np.asarray(stability["bootstrap_mean"])
    ax.scatter(original, boot_mean, alpha=0.4, s=15)
    ax.plot([0, 1], [0, 1], "--", color="grey")
    ax.set(
        xlabel="Original-fit probability",
        ylabel="Bootstrap-mean probability",
        title=f"Prediction stability (MAPE={stability['instability_mape']:.2f})",
    )
    return ax


def build_table_one(frame: Any, *, group_col: str = "trial_cohort") -> Any:
    """Baseline characteristics table stratified by `group_col`.

    Follows the usual clinical-publication "Table 1" convention: numeric
    variables as median [IQR] per group, categorical variables as n (%) per
    group. Defaults to stratifying by `trial_cohort` (randomised vs registry)
    since that split explains this cohort's structural missingness -- see
    `src.data.add_cohort_indicator`.
    """

    import pandas as pd

    from .data import CATEGORICAL_COLUMNS, NUMERIC_COLUMNS, add_cohort_indicator

    enriched = add_cohort_indicator(frame) if group_col not in frame.columns else frame
    groups = list(enriched[group_col].dropna().unique())
    rows: list[dict[str, Any]] = []

    numeric_vars = [c for c in NUMERIC_COLUMNS if c not in {"ID", "Stage"}]
    for column in numeric_vars:
        row: dict[str, Any] = {"variable": column, "type": "numeric"}
        for group in groups:
            values = enriched.loc[enriched[group_col] == group, column].dropna()
            if values.empty:
                row[str(group)] = "—"
                continue
            q1, median, q3 = values.quantile([0.25, 0.5, 0.75])
            row[str(group)] = f"{median:.1f} [{q1:.1f}, {q3:.1f}]"
        rows.append(row)

    categorical_vars = [c for c in CATEGORICAL_COLUMNS if c not in {"Status", "Drug"}]
    for column in categorical_vars:
        for level in sorted(enriched[column].dropna().unique()):
            row = {"variable": f"{column} = {level}", "type": "categorical"}
            for group in groups:
                subset = enriched.loc[enriched[group_col] == group, column]
                n = int((subset == level).sum())
                denom = int(subset.notna().sum())
                pct = 100 * n / denom if denom else 0.0
                row[str(group)] = f"{n} ({pct:.0f}%)"
            rows.append(row)

    counts = {"variable": "n", "type": "count"}
    for group in groups:
        counts[str(group)] = int((enriched[group_col] == group).sum())
    return pd.DataFrame([counts, *rows])


def generate_report(
    results: dict[str, Any],
    output_dir: str | Path = "outputs",
    *,
    y_test: np.ndarray | None = None,
    probabilities: dict[str, np.ndarray] | None = None,
    shap_results: dict[str, Any] | None = None,
    baseline_frame: Any | None = None,
    stability_results: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Write metrics, comparison tables, and optional ROC/PR/calibration plots.

    `baseline_frame`, if given the labeled (pre-leakage-drop) dataset, also
    writes Table 1 stratified by trial cohort and by Stage. `stability_results`,
    if given `prediction_stability`'s per-model output, writes
    `prediction_stability.png` alongside the decision curve.
    """

    destination = Path(output_dir)
    destination.mkdir(parents=True, exist_ok=True)
    write_metrics(results, destination / "metrics.json")
    if baseline_frame is not None:
        build_table_one(baseline_frame, group_col="trial_cohort").to_csv(
            destination / "table_one_by_cohort.csv", index=False
        )
        build_table_one(baseline_frame, group_col="Stage").to_csv(
            destination / "table_one_by_stage.csv", index=False
        )
    models = results.get("models", results)
    rows = []
    for name, metrics in models.items():
        if isinstance(metrics, dict) and "auroc" in metrics:
            rows.append({"model": name, **metrics})
    if rows:
        (destination / "model_comparison.json").write_text(json.dumps(rows, indent=2) + "\n")
        ci_rows = []
        for row in rows:
            for metric, interval in row.get("bootstrap_95_ci", {}).items():
                ci_rows.append(
                    {
                        "model": row["model"],
                        "metric": metric,
                        "lower": interval[0],
                        "upper": interval[1],
                    }
                )
        if ci_rows:
            import csv

            with (destination / "bootstrap_intervals.csv").open("w", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=["model", "metric", "lower", "upper"])
                writer.writeheader()
                writer.writerows(ci_rows)
    if y_test is not None and probabilities:
        import matplotlib.pyplot as plt

        for plot_name, plotter in (
            ("roc_curve.png", plot_roc_curve),
            ("pr_curve.png", plot_pr_curve),
            ("calibration.png", plot_calibration_curve),
            ("decision_curve.png", plot_decision_curve),
        ):
            fig, ax = plt.subplots(figsize=(7, 5))
            for name, values in probabilities.items():
                plotter(y_test, np.asarray(values), ax=ax, label=name)
            fig.tight_layout()
            fig.savefig(destination / plot_name, dpi=160)
            plt.close(fig)
    if stability_results:
        import matplotlib.pyplot as plt

        fig, axes = plt.subplots(1, len(stability_results), figsize=(6 * len(stability_results), 6))
        for ax, (name, stability) in zip(
            np.atleast_1d(axes), stability_results.items(), strict=True
        ):
            plot_prediction_stability(stability, ax=ax)
            ax.set_title(f"{name}: {ax.get_title()}")
        fig.tight_layout()
        fig.savefig(destination / "prediction_stability.png", dpi=160)
        plt.close(fig)
    if shap_results:
        from .explainability import plot_shap_summary

        for name, shap_result in shap_results.items():
            plot_shap_summary(shap_result, shap_result.get("X", np.empty((0, 0))), show=False)
            import matplotlib.pyplot as plt

            plt.savefig(destination / f"shap_{name}.png", dpi=160, bbox_inches="tight")
            plt.close()
    return results
