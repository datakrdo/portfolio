"""Data loading, validation, target construction, and leakage-safe splitting."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Final

import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split

EXPECTED_COLUMNS = [
    "ID",
    "N_Days",
    "Status",
    "Drug",
    "Age",
    "Sex",
    "Ascites",
    "Hepatomegaly",
    "Spiders",
    "Edema",
    "Bilirubin",
    "Cholesterol",
    "Albumin",
    "Copper",
    "Alk_Phos",
    "SGOT",
    "Tryglicerides",
    "Platelets",
    "Prothrombin",
    "Stage",
]
LEAKAGE_COLUMNS = ["Stage", "ID", "N_Days", "Status", "Drug"]
NUMERIC_COLUMNS: Final[tuple[str, ...]] = (
    "ID",
    "N_Days",
    "Age",
    "Bilirubin",
    "Cholesterol",
    "Albumin",
    "Copper",
    "Alk_Phos",
    "SGOT",
    "Tryglicerides",
    "Platelets",
    "Prothrombin",
    "Stage",
)
CATEGORICAL_COLUMNS: Final[tuple[str, ...]] = (
    "Status",
    "Drug",
    "Sex",
    "Ascites",
    "Hepatomegaly",
    "Spiders",
    "Edema",
)

# The trial enrolled 312 patients into the randomised D-penicillamine study; the
# remaining patients (ID 313-418) are an unrandomised registry cohort that Mayo
# followed for basic clinical parameters only. That is why `Drug` and the
# laboratory/clinical block (Ascites, Hepatomegaly, Spiders, Cholesterol,
# Alk_Phos, SGOT, Copper, Tryglicerides) are missing in lockstep for exactly
# those 106 rows -- the missingness is structural, not random. See
# `add_cohort_indicator` and the EDA notebook for the verification.
TRIAL_COHORT_MAX_ID: Final[int] = 312
COHORT_COLUMN = "trial_cohort"


@dataclass(frozen=True)
class DatasetSplit:
    """A labeled train/test split and its two endpoint targets."""

    X_train: pd.DataFrame
    X_test: pd.DataFrame
    y_binary_train: pd.Series
    y_binary_test: pd.Series
    y_stage_train: pd.Series
    y_stage_test: pd.Series


def validate_pbc_dataset(
    frame: pd.DataFrame,
    *,
    expected_rows: int | None = 418,
    require_stage: bool = False,
) -> bool:
    """Validate the PBC schema and basic value constraints.

    ``require_stage`` is useful for downstream training, while the raw cohort
    intentionally contains six records with an unknown stage.
    """

    if not isinstance(frame, pd.DataFrame):
        raise TypeError(f"Expected a pandas DataFrame, received {type(frame).__name__}.")
    if list(frame.columns) != EXPECTED_COLUMNS:
        missing = sorted(set(EXPECTED_COLUMNS) - set(frame.columns))
        extra = sorted(set(frame.columns) - set(EXPECTED_COLUMNS))
        raise ValueError(
            f"Unexpected PBC columns (missing={missing}, extra={extra}). "
            f"Expected order: {EXPECTED_COLUMNS}."
        )
    if expected_rows is not None and len(frame) != expected_rows:
        raise ValueError(f"Expected {expected_rows} source rows, received {len(frame)}.")
    for column in NUMERIC_COLUMNS:
        converted = pd.to_numeric(frame[column], errors="coerce")
        invalid = frame[column].notna() & converted.isna()
        if invalid.any():
            raise TypeError(f"Column {column!r} contains non-numeric values.")
    for column in CATEGORICAL_COLUMNS:
        dtype = frame[column].dtype
        is_categorical = isinstance(dtype, pd.CategoricalDtype)
        if not (
            pd.api.types.is_object_dtype(frame[column])
            or pd.api.types.is_string_dtype(frame[column])
            or is_categorical
        ):
            raise TypeError(f"Column {column!r} must contain categorical/text values.")
        non_missing = frame[column].dropna()
        if not non_missing.map(lambda value: isinstance(value, str)).all():
            raise TypeError(f"Column {column!r} contains non-text values.")
    if not frame["Stage"].dropna().isin([1, 2, 3, 4]).all():
        raise ValueError("Stage values must be integers in the range 1-4 or missing.")
    if require_stage and frame["Stage"].isna().any():
        raise ValueError("Stage contains missing labels; call make_targets first.")
    return True


def load_pbc_data(path: str | Path = "data/raw/pbc.csv") -> pd.DataFrame:
    """Load and validate the historical PBC CSV without fitting or mutation."""

    csv_path = Path(path)
    if not csv_path.exists():
        raise FileNotFoundError(f"Dataset not found at {csv_path}. Restore data/raw/pbc.csv first.")
    frame = pd.read_csv(csv_path, na_values=["NA", ""], keep_default_na=True)
    validate_pbc_dataset(frame, expected_rows=418)
    return frame


def make_targets(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series, pd.Series]:
    """Remove unlabeled rows and create binary and ordered Stage targets."""

    if "Stage" not in frame:
        raise ValueError("Stage is required to construct targets.")
    labeled = frame.loc[frame["Stage"].notna()].copy()
    stages = pd.to_numeric(labeled["Stage"], errors="raise").astype(int)
    if not stages.isin([1, 2, 3, 4]).all():
        raise ValueError("Stage labels must be integers from 1 through 4.")
    binary = (stages >= 3).astype(int).rename("advanced")
    return labeled, binary, stages.rename("stage")


def add_cohort_indicator(frame: pd.DataFrame) -> pd.DataFrame:
    """Derive `trial_cohort` (randomised vs registry) from `ID`.

    The randomised D-penicillamine trial enrolled patients ID 1-312; ID 313-418
    is an unrandomised registry cohort with no `Drug` assignment and no
    laboratory/clinical follow-up block. Modeling this split explicitly, instead
    of silently imputing across it, keeps the two subcohorts distinguishable to
    the model and to the reader.
    """

    if "ID" not in frame.columns:
        raise ValueError("ID is required to derive the trial cohort indicator.")
    enriched = frame.copy()
    enriched[COHORT_COLUMN] = pd.Categorical(
        np.where(enriched["ID"] <= TRIAL_COHORT_MAX_ID, "randomised", "registry"),
        categories=["randomised", "registry"],
    )
    return enriched


def predictor_frame(labeled: pd.DataFrame) -> pd.DataFrame:
    """Return predictors with identifiers, follow-up, and outcome metadata removed.

    Adds the derived `trial_cohort` indicator and then drops `LEAKAGE_COLUMNS`
    (which includes `Drug`, since a null `Drug` perfectly identifies the
    registry cohort and the trial found no drug effect on outcome).
    """

    missing = set(LEAKAGE_COLUMNS) - set(labeled.columns)
    if missing:
        raise ValueError(f"Missing leakage columns: {sorted(missing)}.")
    enriched = add_cohort_indicator(labeled)
    return enriched.drop(columns=LEAKAGE_COLUMNS)


def events_per_variable(X: pd.DataFrame, y: pd.Series) -> float:
    """Events-per-variable ratio (Peduzzi et al.) for the minority class of `y`.

    A commonly cited rule of thumb calls for EPV >= 10 before a binary model's
    coefficients/splits are considered stable. Reported alongside every fit so
    the small-sample limitation is explicit rather than implied.
    """

    n_events = int(min(y.value_counts()))
    n_predictors = int(X.shape[1])
    if n_predictors == 0:
        raise ValueError("X must have at least one predictor column.")
    return n_events / n_predictors


def train_test_split_pipeline(
    frame: pd.DataFrame,
    *,
    test_size: float = 0.2,
    random_state: int = 41,
) -> DatasetSplit:
    """Split labeled patients before any fitted preprocessing is applied.

    Stratifies on the binary endpoint crossed with `trial_cohort`, so both the
    randomised and registry subcohorts are represented proportionally in train
    and test rather than left to chance.
    """

    labeled, y_binary, y_stage = make_targets(frame)
    X = predictor_frame(labeled)
    indices = X.index
    strata = y_binary.astype(str) + "_" + X[COHORT_COLUMN].astype(str)
    train_idx, test_idx = train_test_split(
        indices,
        test_size=test_size,
        random_state=random_state,
        stratify=strata,
    )
    return DatasetSplit(
        X_train=X.loc[train_idx],
        X_test=X.loc[test_idx],
        y_binary_train=y_binary.loc[train_idx],
        y_binary_test=y_binary.loc[test_idx],
        y_stage_train=y_stage.loc[train_idx],
        y_stage_test=y_stage.loc[test_idx],
    )
