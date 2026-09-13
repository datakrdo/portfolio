"""Single orchestrator: raw CSVs in, a scored/calibrated/thresholded artifact out."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from sklearn.isotonic import IsotonicRegression
from sklearn.pipeline import Pipeline

from .data import make_split
from .evaluation import binary_metrics
from .features import build_features
from .modeling import FEATURE_COLUMNS, build_pipeline
from .threshold import FP_COST, select_threshold_by_savings

RANDOM_STATE = 41


@dataclass
class FraudModel:
    """A fitted pipeline plus the calibrator and threshold it was shipped with.

    A model's probabilities are only meaningful together with the calibrator
    that maps them to real fraud rates, and a threshold is only meaningful
    together with the model it was tuned against -- so the three travel as
    one artifact instead of three files that can drift apart.
    """

    pipeline: Pipeline
    calibrator: IsotonicRegression
    threshold: float
    fp_cost: float

    def predict_proba(self, frame: pd.DataFrame) -> np.ndarray:
        raw = self.pipeline.predict_proba(build_features(frame)[FEATURE_COLUMNS])[:, 1]
        return self.calibrator.transform(raw)

    def save(self, path: str | Path) -> None:
        joblib.dump(self, path)

    @staticmethod
    def load(path: str | Path) -> FraudModel:
        return joblib.load(path)


def run_train(
    train_path: str | Path,
    test_path: str | Path,
    *,
    model_name: str,
    valid_fraction: float = 0.2,
    fp_cost: float = FP_COST,
) -> tuple[FraudModel, dict[str, Any]]:
    """Fit `model_name` on the temporal train split, calibrate and threshold on validation.

    Returns the artifact and a metrics dict for both validation and the
    untouched holdout, so a single call produces everything `cli.py train`
    needs to report and save.
    """

    split = make_split(train_path, test_path, valid_fraction=valid_fraction)
    train_feat = build_features(split.train)
    valid_feat = build_features(split.valid)
    test_feat = build_features(split.test)

    prevalence = float(train_feat["is_fraud"].mean())
    pipeline = build_pipeline(model_name, prevalence=prevalence)
    pipeline.fit(train_feat[FEATURE_COLUMNS], train_feat["is_fraud"])

    valid_raw = pipeline.predict_proba(valid_feat[FEATURE_COLUMNS])[:, 1]
    calibrator = IsotonicRegression(out_of_bounds="clip").fit(valid_raw, valid_feat["is_fraud"])
    valid_proba = calibrator.transform(valid_raw)

    threshold = select_threshold_by_savings(
        valid_feat["is_fraud"], valid_proba, valid_feat["amt"], fp_cost=fp_cost
    )
    model = FraudModel(
        pipeline=pipeline, calibrator=calibrator, threshold=threshold, fp_cost=fp_cost
    )

    test_proba = model.predict_proba(split.test)
    metrics = {
        "model": model_name,
        "prevalence_train": prevalence,
        "threshold": threshold,
        "fp_cost": fp_cost,
        "valid": binary_metrics(valid_feat["is_fraud"], valid_proba, threshold=threshold),
        "test": binary_metrics(test_feat["is_fraud"], test_proba, threshold=threshold),
    }
    return model, metrics


def run_score(model: FraudModel, frame: pd.DataFrame) -> pd.DataFrame:
    """Score arbitrary transactions and flag those above the frozen threshold."""

    proba = model.predict_proba(frame)
    return frame.assign(fraud_probability=proba, flagged=proba >= model.threshold)
