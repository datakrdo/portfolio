"""Fold-safe preprocessing for mixed clinical tabular data."""

from __future__ import annotations

from collections.abc import Iterable

import numpy as np
import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer, OneHotEncoder, OrdinalEncoder, StandardScaler

MISSING_CATEGORY = "__MISSING__"

# Laboratory values with right-skew 1.45-3.41 (max/median 5-20x); log1p brings
# skew down to |0.4-1.5|. Verified against `data/raw/pbc.csv` -- see the EDA
# notebook. Age, Albumin, and Platelets are already close to symmetric and are
# left untransformed.
SKEWED_NUMERIC_COLUMNS: tuple[str, ...] = (
    "Bilirubin",
    "Cholesterol",
    "Alk_Phos",
    "SGOT",
    "Tryglicerides",
    "Copper",
    "Prothrombin",
)

# Edema is recorded as N (none) / S (subclinical) / Y (overt), and the outcome
# rate is monotone across those three levels (see EDA notebook), so it is
# encoded as an ordinal 0/1/2 rather than one-hot, preserving that order for
# every downstream model instead of only for the ones that learn interactions.
EDEMA_COLUMN = "Edema"
EDEMA_ORDER: tuple[str, ...] = ("N", "S", "Y")


def feature_columns(frame: pd.DataFrame) -> tuple[list[str], list[str]]:
    """Identify numeric and categorical columns from an input frame."""

    categorical = frame.select_dtypes(include=["object", "category", "string"]).columns
    categorical_columns = list(categorical)
    numeric_columns = [column for column in frame.columns if column not in categorical]
    return numeric_columns, categorical_columns


def _numeric_pipeline(
    *,
    log_transform: bool,
    scale_numeric: bool,
    add_indicator: bool,
    splines: bool,
) -> Pipeline:
    steps: list[tuple[str, object]] = [
        ("impute", SimpleImputer(strategy="median", add_indicator=add_indicator))
    ]
    if log_transform:
        steps.append(
            (
                "log1p",
                FunctionTransformer(
                    np.log1p, inverse_func=np.expm1, feature_names_out="one-to-one"
                ),
            )
        )
    if scale_numeric:
        steps.append(("scale", StandardScaler()))
    if splines:
        from sklearn.preprocessing import SplineTransformer

        steps.append(("splines", SplineTransformer(n_knots=3, degree=2, extrapolation="constant")))
    return Pipeline(steps)


def build_preprocessor(
    frame: pd.DataFrame,
    *,
    scale_numeric: bool = True,
    add_indicator: bool = False,
    log_transform: bool = True,
    ordinal_edema: bool = True,
    splines: bool = False,
    numeric_columns: Iterable[str] | None = None,
    categorical_columns: Iterable[str] | None = None,
) -> ColumnTransformer:
    """Build an unfitted transformer; all imputers fit only on training folds.

    ``add_indicator`` defaults to `False`: missing-indicator columns were
    verified to carry no outcome signal on this cohort (P(y=1 | missing) is
    within 4 points of P(y=1 | observed) for every predictor) while consuming
    degrees of freedom at events-per-variable ~5.6, and a logistic model's top
    SHAP feature was a missing-indicator rather than a laboratory value --
    i.e. it was fitting noise. `trial_cohort` remains the one declared,
    outcome-relevant missingness signal (see `src.data.add_cohort_indicator`).
    """

    inferred_numeric, inferred_categorical = feature_columns(frame)
    numbers = list(numeric_columns) if numeric_columns is not None else inferred_numeric
    categories = (
        list(categorical_columns) if categorical_columns is not None else inferred_categorical
    )
    skewed = [c for c in numbers if c in SKEWED_NUMERIC_COLUMNS]
    plain = [c for c in numbers if c not in SKEWED_NUMERIC_COLUMNS]

    transformers: list[tuple[str, object, list[str]]] = []
    if skewed:
        transformers.append(
            (
                "numeric_skewed",
                _numeric_pipeline(
                    log_transform=log_transform,
                    scale_numeric=scale_numeric,
                    add_indicator=add_indicator,
                    splines=splines,
                ),
                skewed,
            )
        )
    if plain:
        transformers.append(
            (
                "numeric_plain",
                _numeric_pipeline(
                    log_transform=False,
                    scale_numeric=scale_numeric,
                    add_indicator=add_indicator,
                    splines=splines,
                ),
                plain,
            )
        )

    use_ordinal_edema = ordinal_edema and EDEMA_COLUMN in categories
    edema_categories = [c for c in categories if c == EDEMA_COLUMN] if use_ordinal_edema else []
    other_categories = [c for c in categories if c not in edema_categories]

    if edema_categories:
        transformers.append(
            (
                "edema_ordinal",
                Pipeline(
                    [
                        ("impute", SimpleImputer(strategy="most_frequent")),
                        (
                            "ordinal",
                            OrdinalEncoder(categories=[list(EDEMA_ORDER)]),
                        ),
                    ]
                ),
                edema_categories,
            )
        )
    if other_categories:
        transformers.append(
            (
                "categorical",
                Pipeline(
                    [
                        ("impute", SimpleImputer(strategy="constant", fill_value=MISSING_CATEGORY)),
                        ("one_hot", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
                    ]
                ),
                other_categories,
            )
        )

    return ColumnTransformer(
        transformers,
        remainder="drop",
        verbose_feature_names_out=False,
    )


class NativeCategoricalCaster(BaseEstimator, TransformerMixin):
    """Cast object/string columns to `category` dtype; impute nothing.

    Pairs with `sklearn.ensemble.HistGradientBoostingClassifier(
    categorical_features="from_dtype")`, which natively splits on missing
    numeric values and on an explicit missing category. This lets the 25% of
    the cohort with structurally missing labs (the unrandomised registry
    subcohort, see `src.data.add_cohort_indicator`) inform the model as a
    signal instead of being papered over by median/KNN imputation.
    """

    def __init__(self, categorical_columns: Iterable[str] | None = None):
        self.categorical_columns = categorical_columns

    def fit(self, X: pd.DataFrame, y: object = None) -> NativeCategoricalCaster:
        _, inferred_categorical = feature_columns(X)
        self.categorical_columns_ = (
            list(self.categorical_columns)
            if self.categorical_columns is not None
            else inferred_categorical
        )
        self.feature_names_out_ = list(X.columns)
        return self

    def transform(self, X: pd.DataFrame) -> pd.DataFrame:
        casted = X.copy()
        for column in self.categorical_columns_:
            casted[column] = casted[column].astype("category")
        return casted

    def get_feature_names_out(self, input_features: object = None) -> list[str]:
        return list(self.feature_names_out_)


def build_native_preprocessor(
    frame: pd.DataFrame,
    *,
    categorical_columns: Iterable[str] | None = None,
) -> NativeCategoricalCaster:
    """Build the no-impute preprocessor used by the primary HistGB comparator."""

    return NativeCategoricalCaster(categorical_columns=categorical_columns)
