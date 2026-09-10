import pandas as pd
import pytest

from src.data import (
    EXPECTED_COLUMNS,
    load_pbc_data,
    make_targets,
    validate_pbc_dataset,
)


def test_load_and_target():
    frame = load_pbc_data("data/raw/pbc.csv")
    assert list(frame.columns) == EXPECTED_COLUMNS
    _, target, _ = make_targets(frame)
    assert len(target) == 412
    assert set(target.unique()) == {0, 1}


def test_validation_rejects_schema():
    with pytest.raises(ValueError, match="Unexpected"):
        validate_pbc_dataset(pd.DataFrame({"Stage": [1]}), expected_rows=None)


def test_validation_rejects_non_numeric():
    frame = load_pbc_data()
    frame["Albumin"] = frame["Albumin"].astype(object)
    frame.loc[0, "Albumin"] = "bad"
    with pytest.raises(TypeError, match="Albumin"):
        validate_pbc_dataset(frame)


def test_validation_rejects_numeric_masquerading_as_categorical():
    """Regression test: a numeric column must not satisfy the categorical dtype check.

    pandas 3.0 made ``is_object_dtype``/``is_categorical_dtype`` unreliable for the
    default string dtype; this guards against a validator that silently accepts
    numeric data where text is required.
    """

    frame = load_pbc_data()
    frame["Sex"] = 0  # numeric column standing in for a categorical predictor
    with pytest.raises(TypeError, match="Sex"):
        validate_pbc_dataset(frame)


def test_validation_accepts_categorical_dtype():
    frame = load_pbc_data()
    frame["Sex"] = frame["Sex"].astype("category")
    assert validate_pbc_dataset(frame) is True
