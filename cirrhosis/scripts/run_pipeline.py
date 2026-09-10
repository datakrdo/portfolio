#!/usr/bin/env python3
"""Run the production PBC pipeline from the cirrhosis project directory."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.pipeline import run_pipeline  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", default="data/raw/pbc.csv")
    parser.add_argument("--output-dir", default="outputs")
    parser.add_argument(
        "--models",
        nargs="+",
        default=["logistic", "random_forest", "hist_gradient_boosting"],
        choices=["logistic", "random_forest", "hist_gradient_boosting", "lightgbm"],
        help="Primary comparators to fit.",
    )
    parser.add_argument("--bootstrap", type=int, default=100)
    parser.add_argument(
        "--hpo-trials",
        type=int,
        default=0,
        help="Optuna trials for RF/HistGB/LightGBM (0 disables HPO).",
    )
    parser.add_argument("--shap", action="store_true", help="Compute optional SHAP explanations.")
    parser.add_argument(
        "--nested-cv",
        action="store_true",
        help="Also run repeated nested CV and bootstrap optimism "
        "correction (src/validation.py) as the headline "
        "small-sample estimate, alongside the single split.",
    )
    parser.add_argument("--outer-splits", type=int, default=5)
    parser.add_argument("--outer-repeats", type=int, default=5)
    parser.add_argument("--n-boot-optimism", type=int, default=200)
    parser.add_argument(
        "--nested-cv-trials",
        type=int,
        default=10,
        help="Optuna trials per outer fold inside nested_cv_evaluate's inner "
        "hyperparameter search (only used with --nested-cv).",
    )
    parser.add_argument("--seed", type=int, default=41)
    args = parser.parse_args()
    results = run_pipeline(
        args.data,
        args.output_dir,
        models=args.models,
        random_state=args.seed,
        bootstrap=args.bootstrap,
        hpo_trials=args.hpo_trials or None,
        shap=args.shap,
        nested_cv=args.nested_cv,
        outer_splits=args.outer_splits,
        outer_repeats=args.outer_repeats,
        n_boot_optimism=args.n_boot_optimism,
        nested_cv_trials=args.nested_cv_trials,
    )
    print(f"Wrote {Path(args.output_dir) / 'metrics.json'}")
    print(f"Primary models: {', '.join(results['models'])}")


if __name__ == "__main__":
    main()
