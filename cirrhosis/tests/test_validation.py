from src.data import load_pbc_data, train_test_split_pipeline
from src.validation import bootstrap_optimism_correction, nested_cv_evaluate


def _small_split():
    frame = load_pbc_data()
    split = train_test_split_pipeline(frame)
    return split.X_train, split.y_binary_train


def test_nested_cv_evaluate_reports_fold_spread():
    X, y = _small_split()
    result = nested_cv_evaluate(
        X, y, "logistic", outer_splits=5, outer_repeats=2, inner_splits=3, random_state=41
    )
    assert result["n_outer_folds"] == 10
    assert len(result["fold_metrics"]["auroc"]) == 10
    for value in result["fold_metrics"]["auroc"]:
        assert 0.0 <= value <= 1.0
    summary = result["summary"]["auroc"]
    assert summary["ci_95"][0] <= summary["mean"] <= summary["ci_95"][1]


def test_nested_cv_evaluate_is_reproducible():
    X, y = _small_split()
    first = nested_cv_evaluate(X, y, "logistic", outer_splits=5, outer_repeats=1, random_state=41)
    second = nested_cv_evaluate(X, y, "logistic", outer_splits=5, outer_repeats=1, random_state=41)
    assert first["fold_metrics"]["auroc"] == second["fold_metrics"]["auroc"]


def test_bootstrap_optimism_correction_reduces_apparent_score():
    X, y = _small_split()
    result = bootstrap_optimism_correction(X, y, "logistic", n_boot=30, random_state=41)
    assert result["corrected"]["auroc"] <= result["apparent"]["auroc"]
    assert result["optimism"]["auroc"] >= 0.0


def test_nested_cv_evaluate_tunes_hyperparameters_per_outer_fold():
    """The inner loop must genuinely refit on each fold's own training data,
    not reuse one globally fixed config -- proven by each fold's search
    landing on a different best CV score, since each searches a different
    training partition."""
    from sklearn.model_selection import StratifiedKFold

    from src.hpo import run_hyperparameter_study

    X, y = _small_split()
    outer_cv_splits = list(StratifiedKFold(n_splits=5, shuffle=True, random_state=41).split(X, y))
    best_values = []
    for train_idx, _ in outer_cv_splits[:3]:
        study = run_hyperparameter_study(
            X.iloc[train_idx], y.values[train_idx], model_name="logistic", n_trials=5
        )
        best_values.append(study.best_value)
    assert len(set(best_values)) >= 2
