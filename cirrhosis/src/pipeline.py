"""Command-line pipeline orchestration for the PBC stage endpoints."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

import numpy as np

from .data import events_per_variable, load_pbc_data, train_test_split_pipeline
from .evaluation import (
    binary_metrics,
    bootstrap_metric_intervals,
    calibration_before_after,
    calibration_metrics,
    decision_curve_analysis,
    multiclass_metrics,
    prediction_stability,
    riley_minimum_sample_size,
    select_operating_threshold,
)
from .modeling import build_named_pipeline, fit_model, train_named_model
from .reporting import generate_report
from .validation import bootstrap_optimism_correction, nested_cv_evaluate


def run_pipeline(
    data_path: str | Path = "data/raw/pbc.csv",
    output_dir: str | Path = "outputs",
    *,
    models: Sequence[str] = ("logistic", "random_forest", "hist_gradient_boosting"),
    random_state: int = 41,
    bootstrap: int = 100,
    hpo_trials: int | None = None,
    shap: bool = False,
    nested_cv: bool = False,
    outer_splits: int = 5,
    outer_repeats: int = 5,
    n_boot_optimism: int = 200,
    nested_cv_trials: int = 10,
) -> dict:
    """Run primary binary models and secondary stage models without test leakage.

    The single held-out test split (`split.X_test`) is kept as one external
    validation, but it is no longer the headline number: with ~83 test rows
    its AUROC CI is roughly 0.26 wide, too wide to compare models on its own.
    When `nested_cv=True`, `nested_cv_evaluate` and
    `bootstrap_optimism_correction` (`src/validation.py`) are additionally run
    on the full labeled cohort and reported alongside it.
    """

    frame = load_pbc_data(data_path)
    split = train_test_split_pipeline(frame, random_state=random_state)
    results: dict[str, Any] = {
        "dataset": {
            "source_rows": int(len(frame)),
            "labeled_rows": int(len(split.y_binary_train) + len(split.y_binary_test)),
            "train_rows": int(len(split.y_binary_train)),
            "test_rows": int(len(split.y_binary_test)),
            "events_per_variable_train": events_per_variable(split.X_train, split.y_binary_train),
            "riley_min_n": riley_minimum_sample_size(
                split.X_train.shape[1], float(split.y_binary_train.mean())
            ),
        },
        "primary_endpoint": "Stage 3-4 vs Stage 1-2",
        "models": {},
    }
    final_models: dict[str, Any] = {}
    stability_outputs: dict[str, Any] = {}
    for name in models:
        if hpo_trials and name in {"random_forest", "hist_gradient_boosting", "lightgbm"}:
            from sklearn.base import clone

            tunable = clone(
                train_named_model(
                    name, split.X_train, split.y_binary_train, hpo_trials, random_state=random_state
                )
            )
        else:
            tunable = build_named_pipeline(name, split.X_train, random_state=random_state)
        threshold, tuned = select_operating_threshold(
            tunable, split.X_train, split.y_binary_train.to_numpy(), random_state=random_state
        )
        final_models[name] = tuned
        test_probability = tuned.predict_proba(split.X_test)[:, 1]
        metrics: dict[str, Any] = binary_metrics(
            split.y_binary_test.to_numpy(), test_probability, threshold=threshold
        )
        if bootstrap:
            metrics["bootstrap_95_ci"] = bootstrap_metric_intervals(
                split.y_binary_test.to_numpy(),
                test_probability,
                threshold=threshold,
                n_bootstrap=bootstrap,
                random_state=random_state,
            )
        metrics["calibration"] = calibration_metrics(
            split.y_binary_test.to_numpy(), test_probability
        )
        metrics["decision_curve"] = decision_curve_analysis(
            split.y_binary_test.to_numpy(), test_probability
        )
        metrics["calibration_before_after"] = calibration_before_after(
            tunable,
            split.X_train,
            split.y_binary_train.to_numpy(),
            split.X_test,
            split.y_binary_test.to_numpy(),
        )
        if nested_cv:
            metrics["nested_cv"] = nested_cv_evaluate(
                split.X_train,
                split.y_binary_train,
                name,
                outer_splits=outer_splits,
                outer_repeats=outer_repeats,
                random_state=random_state,
                n_trials=nested_cv_trials,
            )["summary"]
            metrics["optimism_correction"] = bootstrap_optimism_correction(
                split.X_train,
                split.y_binary_train,
                name,
                n_boot=n_boot_optimism,
                random_state=random_state,
            )
            stability_outputs[name] = prediction_stability(
                tunable, split.X_train, split.y_binary_train.to_numpy(), n_boot=n_boot_optimism
            )
            metrics["prediction_stability"] = {
                "instability_mape": stability_outputs[name]["instability_mape"]
            }
        results["models"][name] = metrics

    multiclass = fit_model(
        "multiclass_logistic",
        split.X_train,
        split.y_stage_train,
        random_state=random_state,
    )
    results["secondary_multiclass"] = multiclass_metrics(
        split.y_stage_test.to_numpy(), np.asarray(multiclass.predict(split.X_test))
    )
    ordinal = fit_model(
        "ordinal_logistic",
        split.X_train,
        split.y_stage_train,
        random_state=random_state,
    )
    results["secondary_ordinal"] = multiclass_metrics(
        split.y_stage_test.to_numpy(), np.asarray(ordinal.predict(split.X_test))
    )
    probabilities = {
        name: np.asarray(model.predict_proba(split.X_test))[:, 1]
        for name, model in final_models.items()
    }
    shap_outputs: dict[str, Any] = {}
    if shap:
        from .explainability import compute_shap_explanations

        for name in models:
            shap_outputs[name] = compute_shap_explanations(
                final_models[name].estimator_, split.X_train, split.X_test
            )
        results["shap"] = {
            name: value["feature_importance_df"].to_dict(orient="records")
            for name, value in shap_outputs.items()
        }
    generate_report(
        results,
        output_dir,
        y_test=split.y_binary_test.to_numpy(),
        probabilities=probabilities,
        shap_results=shap_outputs if shap else None,
        baseline_frame=frame,
        stability_results=stability_outputs or None,
    )
    return results
