"""Train the deployed models, persist them, and score a single new patient.

The deployed estimator is the pre-registered `logistic` (chosen for parsimony at
this sample size, calibration and explainability, not because it ranked first),
refit on every labeled row of each feature set, with a rule-out threshold picked
by cross-validation. `predict_patient` is the single entry point shared by the
CLI and the Streamlit demo.
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd

from .data import FEATURE_SETS, load_pbc_data, select_feature_set
from .evaluation import TARGET_SENSITIVITY, select_operating_threshold
from .scores import DAYS_PER_YEAR, SCORES
from .validation import tuned_pipeline

DEPLOYED_MODEL = "logistic"
DEFAULT_MODEL_DIR = Path("models")


def train_final(
    data_path: str | Path = "data/raw/pbc.csv",
    model_dir: str | Path = DEFAULT_MODEL_DIR,
    *,
    metrics_path: str | Path = "outputs/metrics.json",
    n_trials: int = 20,
    random_state: int = 41,
) -> dict[str, Any]:
    """Fit and save `pbc_{core,full}.joblib` plus `metadata.json`."""

    frame = load_pbc_data(data_path)
    out_dir = Path(model_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    reported = {}
    if Path(metrics_path).exists():
        reported = json.loads(Path(metrics_path).read_text()).get("feature_sets", {})
    metadata: dict[str, Any] = {
        "model": DEPLOYED_MODEL,
        "target_sensitivity": TARGET_SENSITIVITY,
        "feature_sets": {},
    }
    for feature_set, columns in FEATURE_SETS.items():
        X, y, _ = select_feature_set(frame, feature_set)
        pipeline = tuned_pipeline(
            DEPLOYED_MODEL, X, y.to_numpy(), n_trials=n_trials, random_state=random_state
        )
        threshold, tuned = select_operating_threshold(
            pipeline, X, y.to_numpy(), random_state=random_state
        )
        joblib.dump(
            {"feature_set": feature_set, "features": list(columns), "model": tuned},
            out_dir / f"pbc_{feature_set}.joblib",
        )
        nested = reported.get(feature_set, {}).get("models", {}).get(DEPLOYED_MODEL, {})
        metadata["feature_sets"][feature_set] = {
            "features": list(columns),
            "n_train": int(len(X)),
            "prevalence": float(y.mean()),
            "threshold": threshold,
            "nested_cv": nested.get("nested_cv"),
        }
    (out_dir / "metadata.json").write_text(json.dumps(metadata, indent=2) + "\n")
    return metadata


@lru_cache(maxsize=4)
def _load(model_dir: str, feature_set: str) -> dict[str, Any]:
    path = Path(model_dir) / f"pbc_{feature_set}.joblib"
    if not path.exists():
        raise FileNotFoundError(f"{path} not found; run `pbc train` first.")
    return joblib.load(path)


def _present(value: Any) -> bool:
    return value is not None and not (isinstance(value, float) and np.isnan(value))


def predict_patient(
    patient: dict[str, Any], model_dir: str | Path = DEFAULT_MODEL_DIR
) -> dict[str, Any]:
    """Score one patient given as a dict of dataset column names.

    `age_years` replaces the dataset's `Age` (which is in days). The `full` model
    is used when every one of its predictors is provided, otherwise `core`; if
    any `core` predictor is missing a `ValueError` lists them.
    """

    row = {key: value for key, value in patient.items() if key != "age_years"}
    if _present(patient.get("age_years")):
        row["Age"] = float(patient["age_years"]) * DAYS_PER_YEAR
    used = "full" if all(_present(row.get(c)) for c in FEATURE_SETS["full"]) else "core"
    missing = [c for c in FEATURE_SETS["core"] if not _present(row.get(c))]
    if missing:
        missing = ["age_years" if c == "Age" else c for c in missing]
        raise ValueError(f"Missing required predictors: {missing}.")
    bundle = _load(str(model_dir), used)
    frame = pd.DataFrame([{c: row.get(c, np.nan) for c in bundle["features"]}])
    model = bundle["model"]
    probability = float(model.predict_proba(frame)[0, 1])
    advanced_not_excluded = bool(model.predict(frame)[0])
    result: dict[str, Any] = {
        "model_used": used,
        "probability_advanced": probability,
        "threshold": float(model.best_threshold_),
        "decision": "advanced_not_excluded" if advanced_not_excluded else "advanced_ruled_out",
    }
    scores_frame = pd.DataFrame([row])
    for name, (fn, required) in SCORES.items():
        if all(_present(row.get(c)) for c in required):
            result[name] = float(fn(scores_frame).iloc[0])
    return result
