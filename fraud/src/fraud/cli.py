#!/usr/bin/env python3
"""Command-line entry points: `fraud train`, `fraud score`, `fraud evaluate`."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from .data import load_transactions
from .evaluation import binary_metrics
from .pipeline import FraudModel, run_score, run_train


def _train(args: argparse.Namespace) -> None:
    model, metrics = run_train(
        args.data, args.test_data, model_name=args.model, fp_cost=args.fp_cost
    )
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    model_path = out_dir / f"{args.model}.joblib"
    model.save(model_path)
    (out_dir / f"{args.model}-metrics.json").write_text(json.dumps(metrics, indent=2))
    print(f"Saved {model_path}")
    print(json.dumps(metrics, indent=2))


def _score(args: argparse.Namespace) -> None:
    model = FraudModel.load(args.model)
    frame = load_transactions(args.transactions)
    scored = run_score(model, frame)
    scored.to_csv(args.out, index=False)
    print(f"Scored {len(scored)} transactions -> {args.out} ({scored['flagged'].sum()} flagged)")


def _evaluate(args: argparse.Namespace) -> None:
    model = FraudModel.load(args.model)
    frame = load_transactions(args.data)
    from .features import build_features

    feat = build_features(frame)
    proba = model.predict_proba(frame)
    metrics = binary_metrics(feat["is_fraud"], proba, threshold=model.threshold)
    print(json.dumps(metrics, indent=2))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    train_p = sub.add_parser("train", help="Fit a model and freeze its threshold on validation.")
    train_p.add_argument("--data", default="data/raw/fraudTrain.csv")
    train_p.add_argument("--test-data", default="data/raw/fraudTest.csv")
    train_p.add_argument(
        "--model", default="xgboost", choices=["dummy", "logreg", "lightgbm", "xgboost"]
    )
    train_p.add_argument("--fp-cost", type=float, default=4.0)
    train_p.add_argument("--out", default="models")
    train_p.set_defaults(func=_train)

    score_p = sub.add_parser("score", help="Batch-score a CSV of raw Sparkov-schema transactions.")
    score_p.add_argument("transactions")
    score_p.add_argument("--model", required=True)
    score_p.add_argument("-o", "--out", default="scores.csv")
    score_p.set_defaults(func=_score)

    eval_p = sub.add_parser("evaluate", help="Report metrics for a saved model on a labeled CSV.")
    eval_p.add_argument("--model", required=True)
    eval_p.add_argument("--data", default="data/raw/fraudTest.csv")
    eval_p.set_defaults(func=_evaluate)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
