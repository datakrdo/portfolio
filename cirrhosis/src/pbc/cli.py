#!/usr/bin/env python3
"""Command-line entry points: `pbc pipeline | eda | train | predict`."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from .data import load_pbc_data
from .eda import summarize_dataset
from .pipeline import run_pipeline
from .predict import DEFAULT_MODEL_DIR, predict_patient, train_final


def _pipeline(args: argparse.Namespace) -> None:
    results = run_pipeline(
        args.data,
        args.output_dir,
        models=args.models,
        random_state=args.seed,
        bootstrap=args.bootstrap,
        hpo_trials=args.hpo_trials,
        shap=args.shap,
        nested_cv=args.nested_cv,
        outer_splits=args.outer_splits,
        outer_repeats=args.outer_repeats,
        n_boot_optimism=args.n_boot_optimism,
        nested_cv_trials=args.nested_cv_trials,
    )
    print(f"Wrote {Path(args.output_dir) / 'metrics.json'}")
    for feature_set, block in results["feature_sets"].items():
        print(f"{feature_set}: {', '.join(block['models'])}")


def _eda(args: argparse.Namespace) -> None:
    summary = summarize_dataset(load_pbc_data(args.data))
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(summary, indent=2) + "\n")
    print(f"Wrote {output_path}")


def _train(args: argparse.Namespace) -> None:
    metadata = train_final(
        args.data,
        args.model_dir,
        metrics_path=args.metrics,
        n_trials=args.trials,
        random_state=args.seed,
    )
    for feature_set, info in metadata["feature_sets"].items():
        print(f"{feature_set}: n={info['n_train']} threshold={info['threshold']:.3f}")
    print(f"Saved models to {args.model_dir}")


def _predict(args: argparse.Namespace) -> None:
    patient = json.loads(Path(args.input).read_text())
    print(json.dumps(predict_patient(patient, args.model_dir), indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(prog="pbc", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    pipeline = sub.add_parser("pipeline", help="Run the modeling pipeline and write outputs/.")
    pipeline.add_argument("--data", default="data/raw/pbc.csv")
    pipeline.add_argument("--output-dir", default="outputs")
    pipeline.add_argument(
        "--models",
        nargs="+",
        default=["logistic", "random_forest", "hist_gradient_boosting"],
        choices=["logistic", "random_forest", "hist_gradient_boosting"],
        help="ML comparators to fit; clinical score baselines always run.",
    )
    pipeline.add_argument("--bootstrap", type=int, default=100)
    pipeline.add_argument(
        "--hpo-trials", type=int, default=0, help="Optuna trials (0 disables HPO)."
    )
    pipeline.add_argument("--shap", action="store_true", help="Compute SHAP explanations.")
    pipeline.add_argument(
        "--nested-cv",
        action="store_true",
        help="Also run repeated nested CV and bootstrap optimism correction.",
    )
    pipeline.add_argument("--outer-splits", type=int, default=5)
    pipeline.add_argument("--outer-repeats", type=int, default=5)
    pipeline.add_argument("--n-boot-optimism", type=int, default=200)
    pipeline.add_argument("--nested-cv-trials", type=int, default=10)
    pipeline.add_argument("--seed", type=int, default=41)
    pipeline.set_defaults(func=_pipeline)

    eda = sub.add_parser("eda", help="Write a machine-readable EDA summary.")
    eda.add_argument("--data", default="data/raw/pbc.csv")
    eda.add_argument("--output", default="outputs/eda_summary.json")
    eda.set_defaults(func=_eda)

    train = sub.add_parser("train", help="Fit the deployed models and save them to models/.")
    train.add_argument("--data", default="data/raw/pbc.csv")
    train.add_argument("--model-dir", default=str(DEFAULT_MODEL_DIR))
    train.add_argument("--metrics", default="outputs/metrics.json")
    train.add_argument("--trials", type=int, default=20)
    train.add_argument("--seed", type=int, default=41)
    train.set_defaults(func=_train)

    predict = sub.add_parser("predict", help="Score one patient from a JSON file.")
    predict.add_argument("--input", required=True, help="JSON with dataset columns + age_years.")
    predict.add_argument("--model-dir", default=str(DEFAULT_MODEL_DIR))
    predict.set_defaults(func=_predict)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
