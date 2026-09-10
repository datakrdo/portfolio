"""Optional SHAP explanations for fitted PBC models."""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from .evaluation import ShapResult

_TREE_ESTIMATOR_SUFFIXES = ("forestclassifier", "lgbmclassifier", "boostingclassifier")


def _encode_for_shap(transformed: Any) -> np.ndarray:
    """Numeric matrix for SHAP's tree-traversal C extension.

    `NativeCategoricalCaster` (used ahead of HistGradientBoostingClassifier)
    returns a DataFrame with pandas `category` dtype columns and NaN left in
    place; SHAP's TreeExplainer needs a plain float matrix. Category codes
    line up with what `HistGradientBoostingClassifier(categorical_features=
    "from_dtype")` saw internally, since both read off the same
    `CategoricalDtype.categories` ordering; NaN is preserved (code -1 becomes
    NaN again) so the explainer sees the same "missing" signal the model did.
    """

    if isinstance(transformed, pd.DataFrame):
        encoded = transformed.copy()
        for column in encoded.select_dtypes("category").columns:
            codes = encoded[column].cat.codes.astype(float)
            codes[encoded[column].isna()] = np.nan
            encoded[column] = codes
        return encoded.astype(float).to_numpy()
    return np.asarray(transformed, dtype=float)


def compute_shap_explanations(
    model: Any,
    X_train: pd.DataFrame,
    X_test: pd.DataFrame,
    *,
    max_background: int = 100,
    max_samples: int = 200,
) -> ShapResult:
    """Compute SHAP values using TreeExplainer or a KernelExplainer fallback."""

    try:
        import shap
    except ImportError as exc:
        raise ImportError("SHAP is optional; install pbc-modeling[shap].") from exc
    preprocessor = model.named_steps.get("preprocess", model)
    estimator = model.named_steps.get("model", model)
    train_raw = preprocessor.transform(X_train) if hasattr(preprocessor, "transform") else X_train
    test_raw = preprocessor.transform(X_test) if hasattr(preprocessor, "transform") else X_test
    names = (
        list(preprocessor.get_feature_names_out())
        if hasattr(preprocessor, "get_feature_names_out")
        else [str(i) for i in range(np.asarray(test_raw).shape[1])]
    )
    is_tree = estimator.__class__.__name__.lower().endswith(_TREE_ESTIMATOR_SUFFIXES)
    train_transformed = (
        _encode_for_shap(train_raw) if is_tree else np.asarray(train_raw, dtype=float)
    )
    test_transformed = _encode_for_shap(test_raw) if is_tree else np.asarray(test_raw, dtype=float)
    background = train_transformed[:max_background]
    values_input = test_transformed[:max_samples]
    if is_tree:
        explainer = shap.TreeExplainer(
            estimator, data=background, feature_perturbation="interventional"
        )
        # LightGBM's raw-score rounding is known to fail SHAP's additivity
        # check by tiny margins (SHAP #2828); values themselves are unaffected.
        check_additivity = estimator.__class__.__name__.lower() != "lgbmclassifier"
        values = explainer(values_input, check_additivity=check_additivity)
        shap_values = values.values if hasattr(values, "values") else values
        expected = (
            values.base_values if hasattr(values, "base_values") else explainer.expected_value
        )
    else:
        explainer = shap.KernelExplainer(estimator.predict_proba, background)
        shap_values = explainer.shap_values(values_input, silent=True)
        expected = explainer.expected_value
    if isinstance(shap_values, list):
        shap_values = shap_values[1] if len(shap_values) > 1 else shap_values[0]
    shap_values = np.asarray(shap_values)
    if shap_values.ndim == 3:
        # Binary classifiers that return a (n_samples, n_features, n_classes)
        # explanation; keep the positive class.
        shap_values = shap_values[:, :, -1]
    importance = np.abs(shap_values).mean(axis=0)
    importance_df = pd.DataFrame({"feature": names, "importance": importance}).sort_values(
        "importance", ascending=False, ignore_index=True
    )
    return {
        "summary_plot_data": shap_values,
        "expected_values": expected,
        "feature_importance_df": importance_df,
        "X": pd.DataFrame(values_input, columns=names),
    }


def plot_shap_summary(
    shap_result: ShapResult, X: pd.DataFrame, *, max_display: int = 20, show: bool = False
) -> Any:
    """Render a SHAP beeswarm summary plot."""

    import shap

    return shap.summary_plot(
        shap_result["summary_plot_data"], X, max_display=max_display, show=show
    )
