import pytest

pytest.importorskip("shap")

from src.data import load_pbc_data, train_test_split_pipeline  # noqa: E402
from src.explainability import compute_shap_explanations  # noqa: E402
from src.modeling import fit_model  # noqa: E402


@pytest.fixture(scope="module")
def split():
    frame = load_pbc_data()
    return train_test_split_pipeline(frame)


def test_shap_explanations_for_hist_gradient_boosting(split):
    """TreeExplainer over the native (unimputed, categorical-dtype) pipeline."""

    model = fit_model("hist_gradient_boosting", split.X_train, split.y_binary_train)
    result = compute_shap_explanations(
        model, split.X_train, split.X_test, max_background=40, max_samples=20
    )
    importance = result["feature_importance_df"]
    assert set(importance["feature"]) == set(split.X_train.columns)
    assert (importance["importance"] >= 0).all()
    assert result["summary_plot_data"].shape == (20, split.X_train.shape[1])


def test_shap_explanations_for_random_forest(split):
    """TreeExplainer over the one-hot-encoded pipeline."""

    model = fit_model("random_forest", split.X_train, split.y_binary_train)
    result = compute_shap_explanations(
        model, split.X_train, split.X_test, max_background=40, max_samples=20
    )
    assert (result["feature_importance_df"]["importance"] >= 0).all()
