"""Published clinical scores used as comparators for the ML models.

Both scores are fixed formulas (nothing is fitted on this cohort here); the
modeling layer only recalibrates their output to a stage probability with a
one-feature logistic regression, per fold.

Caveat: the Mayo PBC risk score was derived on this same Mayo cohort, and for
*survival*, not histological stage. As a stage predictor it is a favourable but
not external comparator. APRI comes from hepatitis C and is only an off-label
proxy for PBC fibrosis.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin

DAYS_PER_YEAR = 365.25
AST_UPPER_LIMIT_NORMAL = 40.0  # U/L; APRI's conventional ULN for SGOT/AST.
# Dickson et al. 1989, Hepatology 10:1-7: bilirubin in mg/dL, albumin in g/dL,
# prothrombin time in seconds, edema 0 / 0.5 (no diuretics) / 1 (despite diuretics).
EDEMA_LEVELS = {"N": 0.0, "S": 0.5, "Y": 1.0}
MAYO_REQUIRED = ("Age", "Bilirubin", "Albumin", "Prothrombin", "Edema")
APRI_REQUIRED = ("SGOT", "Platelets")


def mayo_risk_score(frame: pd.DataFrame) -> pd.Series:
    """Mayo PBC risk score; `Age` is in days in this dataset."""

    edema = frame["Edema"].astype(str).map(EDEMA_LEVELS).astype(float)
    return (
        0.0394 * frame["Age"] / DAYS_PER_YEAR
        + 0.8707 * np.log(frame["Bilirubin"])
        - 2.533 * np.log(frame["Albumin"])
        + 2.380 * np.log(frame["Prothrombin"])
        + 0.859 * edema
    ).rename("mayo_risk_score")


def apri(frame: pd.DataFrame) -> pd.Series:
    """AST-to-platelet ratio index: (AST / ULN) / platelets (10^9/L) * 100."""

    return ((frame["SGOT"] / AST_UPPER_LIMIT_NORMAL) / frame["Platelets"] * 100).rename("apri")


SCORES = {"mayo": (mayo_risk_score, MAYO_REQUIRED), "apri": (apri, APRI_REQUIRED)}


class ScoreTransformer(BaseEstimator, TransformerMixin):
    """Turn a patient frame into the single-column value of a named clinical score."""

    def __init__(self, score: str = "mayo"):
        self.score = score

    def fit(self, X: pd.DataFrame, y: object = None) -> ScoreTransformer:
        if self.score not in SCORES:
            raise KeyError(f"Unknown score {self.score!r}; choose from {sorted(SCORES)}.")
        missing = set(SCORES[self.score][1]) - set(X.columns)
        if missing:
            raise ValueError(f"{self.score} needs columns {sorted(missing)}.")
        return self

    def transform(self, X: pd.DataFrame) -> np.ndarray:
        return SCORES[self.score][0](X).to_numpy(dtype=float).reshape(-1, 1)

    def get_feature_names_out(self, input_features: object = None) -> list[str]:
        return [self.score]
