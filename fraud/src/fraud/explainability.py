"""SHAP values for the final model, gated behind the optional [shap] extra."""

from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.pipeline import Pipeline

from .modeling import FEATURE_COLUMNS


def shap_values(
    pipeline: Pipeline, frame: pd.DataFrame, *, sample_size: int = 5000
) -> tuple[np.ndarray, pd.DataFrame]:
    """SHAP values for the fitted estimator, computed on the transformed feature matrix.

    Sampled to `sample_size` rows: SHAP's TreeExplainer is linear in rows but
    this project's holdout is 555k transactions, and a portfolio explanation
    needs representative attributions, not every row scored twice.
    """

    import shap

    sample = frame.sample(min(sample_size, len(frame)), random_state=41)
    X = sample[FEATURE_COLUMNS]
    transformed = pipeline.named_steps["preprocess"].transform(X)
    feature_names = pipeline.named_steps["preprocess"].get_feature_names_out()
    explainer = shap.TreeExplainer(pipeline.named_steps["model"])
    values = explainer.shap_values(transformed)
    return values, pd.DataFrame(transformed, columns=feature_names, index=sample.index)
