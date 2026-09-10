import numpy as np
import pandas as pd

from src.preprocessing import (
    MISSING_CATEGORY,
    build_native_preprocessor,
    build_preprocessor,
)


def _frame():
    return pd.DataFrame(
        {
            "age": [40.0, np.nan, 60.0],
            "albumin": [3.1, 3.5, np.nan],
            "sex": ["F", None, "F"],
        }
    )


def test_median_preprocessor_is_fold_fitted_and_finite():
    frame = _frame()
    transformer = build_preprocessor(frame)
    transformed = transformer.fit_transform(frame.iloc[[0, 1]]).astype(float)
    assert transformed.shape[0] == 2
    assert np.isfinite(transformed).all()
    assert (
        MISSING_CATEGORY
        in transformer.named_transformers_["categorical"].named_steps["impute"].fill_value
    )


def test_native_preprocessor_casts_without_imputing():
    frame = _frame()
    transformer = build_native_preprocessor(frame)
    transformed = transformer.fit_transform(frame)
    assert isinstance(transformed["sex"].dtype, pd.CategoricalDtype)
    # Missing values are preserved, not imputed -- HistGradientBoosting
    # natively splits on them.
    assert transformed["age"].isna().sum() == frame["age"].isna().sum()
    assert transformed["sex"].isna().sum() == frame["sex"].isna().sum()


def _skew_frame():
    return pd.DataFrame(
        {
            "Bilirubin": [0.5, 1.0, 2.0, 25.0],
            "Albumin": [3.0, 3.4, 3.8, 4.1],
            "Edema": ["N", "S", "Y", "N"],
        }
    )


def test_log1p_applied_only_to_skewed_columns():
    frame = _skew_frame()
    transformer = build_preprocessor(frame, scale_numeric=False)
    transformed = transformer.fit_transform(frame)
    names = list(transformer.get_feature_names_out())
    bilirubin_col = transformed[:, names.index("Bilirubin")]
    albumin_col = transformed[:, names.index("Albumin")]
    assert np.allclose(bilirubin_col, np.log1p(frame["Bilirubin"]))
    assert np.allclose(albumin_col, frame["Albumin"])


def test_edema_encoded_ordinal_in_clinical_order():
    frame = _skew_frame()
    transformer = build_preprocessor(frame, scale_numeric=False)
    transformed = transformer.fit_transform(frame)
    names = list(transformer.get_feature_names_out())
    edema_col = transformed[:, names.index("Edema")]
    assert list(edema_col) == [0.0, 1.0, 2.0, 0.0]


def test_no_missing_indicators_by_default():
    frame = _skew_frame()
    transformer = build_preprocessor(frame)
    names = list(transformer.fit(frame).get_feature_names_out())
    assert not any("missingindicator" in name.lower() for name in names)
