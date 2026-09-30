"""Command-line pipeline orchestration for the PBC stage endpoints."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

import numpy as np
from sklearn.base import clone

from .data import (
    FEATURE_SETS,
    events_per_variable,
    load_pbc_data,
    select_feature_set,
    train_test_split_pipeline,
)
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
    write_metrics,
)
from .modeling import TUNABLE_MODELS, build_named_pipeline, fit_model, train_named_model
from .reporting import build_table_one, generate_report
from .scores import SCORES
from .validation import OPTIMISM_MODELS, bootstrap_optimism_correction, nested_cv_evaluate


def _score_models(feature_set: str, X_columns: Sequence[str]) -> list[str]:
    """Clinical score baselines computable from the columns of `feature_set`."""

    return [name for name, (_, required) in SCORES.items() if set(required) <= set(X_columns)]


def _run_feature_set(
    frame: Any,
    feature_set: str,
    output_dir: Path,
    *,
    models: Sequence[str],
    random_state: int,
    bootstrap: int,
    hpo_trials: int | None,
    shap: bool,
    nested_cv: bool,
    outer_splits: int,
    outer_repeats: int,
    n_boot_optimism: int,
    nested_cv_trials: int,
) -> dict[str, Any]:
    split = train_test_split_pipeline(frame, feature_set=feature_set, random_state=random_state)
    X_all, y_all, _ = select_feature_set(frame, feature_set)
    n_features = split.X_train.shape[1]
    block: dict[str, Any] = {
        "dataset": {
            "n_features": n_features,
            "labeled_rows": int(len(y_all)),
            "train_rows": int(len(split.y_binary_train)),
            "test_rows": int(len(split.y_binary_test)),
            "prevalence": float(y_all.mean()),
            "events_per_variable_train": events_per_variable(split.X_train, split.y_binary_train),
            "riley_min_n": riley_minimum_sample_size(
                n_features, float(split.y_binary_train.mean())
            ),
        },
        "models": {},
    }
    names = [*models, *_score_models(feature_set, split.X_train.columns)]
    final_models: dict[str, Any] = {}
    stability_outputs: dict[str, Any] = {}
    y_test = split.y_binary_test.to_numpy()
    for name in names:
        if hpo_trials and name in TUNABLE_MODELS:
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
        metrics: dict[str, Any] = binary_metrics(y_test, test_probability, threshold=threshold)
        if bootstrap:
            metrics["bootstrap_95_ci"] = bootstrap_metric_intervals(
                y_test,
                test_probability,
                threshold=threshold,
                n_bootstrap=bootstrap,
                random_state=random_state,
            )
        metrics["calibration"] = calibration_metrics(y_test, test_probability)
        metrics["decision_curve"] = decision_curve_analysis(y_test, test_probability)
        metrics["calibration_before_after"] = calibration_before_after(
            tunable,
            split.X_train,
            split.y_binary_train.to_numpy(),
            split.X_test,
            y_test,
        )
        if nested_cv:
            nested = nested_cv_evaluate(
                X_all,
                y_all,
                name,
                outer_splits=outer_splits,
                outer_repeats=outer_repeats,
                random_state=random_state,
                n_trials=nested_cv_trials,
            )
            metrics["nested_cv"] = {
                **nested["summary"],
                "calibration": nested["calibration"],
                "median_threshold": nested["median_threshold"],
            }
            if name in OPTIMISM_MODELS:
                metrics["optimism_correction"] = bootstrap_optimism_correction(
                    X_all, y_all, name, n_boot=n_boot_optimism, random_state=random_state
                )
            if name in models:
                stability_outputs[name] = prediction_stability(
                    tunable, X_all, y_all.to_numpy(), n_boot=n_boot_optimism
                )
                metrics["prediction_stability"] = {
                    "instability_mape": stability_outputs[name]["instability_mape"]
                }
        block["models"][name] = metrics

    if feature_set == "core":
        for key, model_name in (
            ("secondary_multiclass", "multiclass_logistic"),
            ("secondary_ordinal", "ordinal_logistic"),
        ):
            secondary = fit_model(
                model_name, split.X_train, split.y_stage_train, random_state=random_state
            )
            block[key] = multiclass_metrics(
                split.y_stage_test.to_numpy(), np.asarray(secondary.predict(split.X_test))
            )
    shap_outputs: dict[str, Any] = {}
    if shap:
        from .explainability import compute_shap_explanations

        for name in models:
            shap_outputs[name] = compute_shap_explanations(
                final_models[name].estimator_, split.X_train, split.X_test
            )
        block["shap"] = {
            name: value["feature_importance_df"].to_dict(orient="records")
            for name, value in shap_outputs.items()
        }
    generate_report(
        block,
        output_dir,
        y_test=y_test,
        probabilities={
            name: np.asarray(model.predict_proba(split.X_test))[:, 1]
            for name, model in final_models.items()
        },
        shap_results=shap_outputs if shap else None,
        stability_results=stability_outputs or None,
    )
    return block


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
    """Run the primary binary endpoint for each feature set, without test leakage.

    For each of `core` (all 412 labeled patients) and `full` (the 312 trial
    patients with the extended panel) this fits the requested ML models plus the
    Mayo (and, when SGOT is available, APRI) clinical score baselines, all through
    the same fold-fitted pipeline and rule-out threshold selection.

    The single held-out split is kept as one external-validation demonstration,
    but it is not the headline number: with 62-83 test rows its AUROC CI is
    roughly 0.26 wide. When `nested_cv=True`, `nested_cv_evaluate` (and, for
    low-capacity models, `bootstrap_optimism_correction`) run on *every* labeled
    row of the feature set and are the estimates to read.
    """

    frame = load_pbc_data(data_path)
    output_dir = Path(output_dir)
    results: dict[str, Any] = {
        "primary_endpoint": "Stage 3-4 vs Stage 1-2",
        "source_rows": int(len(frame)),
        "feature_sets": {},
    }
    for feature_set in FEATURE_SETS:
        results["feature_sets"][feature_set] = _run_feature_set(
            frame,
            feature_set,
            output_dir / feature_set,
            models=models,
            random_state=random_state,
            bootstrap=bootstrap,
            hpo_trials=hpo_trials,
            shap=shap,
            nested_cv=nested_cv,
            outer_splits=outer_splits,
            outer_repeats=outer_repeats,
            n_boot_optimism=n_boot_optimism,
            nested_cv_trials=nested_cv_trials,
        )
    output_dir.mkdir(parents=True, exist_ok=True)
    write_metrics(results, output_dir / "metrics.json")
    for group_col, filename in (
        ("trial_cohort", "table_one_by_cohort.csv"),
        ("Stage", "table_one_by_stage.csv"),
    ):
        build_table_one(frame, group_col=group_col).to_csv(output_dir / filename, index=False)
    return results
