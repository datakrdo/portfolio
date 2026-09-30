import numpy as np
import pytest

from pbc.data import FEATURE_SETS, load_pbc_data, select_feature_set
from pbc.evaluation import _rule_out_score, binary_metrics, select_operating_threshold
from pbc.modeling import build_named_pipeline
from pbc.predict import predict_patient, train_final

CORE_PATIENT = {
    "age_years": 55,
    "Sex": "F",
    "Edema": "N",
    "Bilirubin": 1.4,
    "Albumin": 3.5,
    "Platelets": 250,
    "Prothrombin": 10.6,
}
FULL_PATIENT = {
    **CORE_PATIENT,
    "Ascites": "N",
    "Hepatomegaly": "Y",
    "Spiders": "N",
    "Cholesterol": 300,
    "Copper": 73,
    "Alk_Phos": 1500,
    "SGOT": 110,
    "Tryglicerides": 120,
}


@pytest.fixture(scope="module")
def model_dir(tmp_path_factory):
    path = tmp_path_factory.mktemp("models")
    train_final(model_dir=path, metrics_path=path / "none.json", n_trials=2)
    return path


def test_predict_uses_full_model_only_with_the_whole_panel(model_dir):
    core = predict_patient(CORE_PATIENT, model_dir)
    full = predict_patient(FULL_PATIENT, model_dir)
    assert (core["model_used"], full["model_used"]) == ("core", "full")
    assert "apri" not in core and "apri" in full
    assert 0.0 <= core["probability_advanced"] <= 1.0
    assert core["decision"] in {"advanced_not_excluded", "advanced_ruled_out"}


def test_predict_names_missing_core_predictors(model_dir):
    with pytest.raises(ValueError, match="Bilirubin"):
        predict_patient({k: v for k, v in CORE_PATIENT.items() if k != "Bilirubin"}, model_dir)


def test_rule_out_threshold_meets_target_sensitivity_in_sample():
    X, y, _ = select_feature_set(load_pbc_data(), "core")
    pipeline = build_named_pipeline("logistic", X)
    threshold, tuned = select_operating_threshold(pipeline, X, y.to_numpy())
    metrics = binary_metrics(y.to_numpy(), tuned.predict_proba(X)[:, 1], threshold=threshold)
    assert metrics["sensitivity"] >= 0.85  # CV-chosen threshold, small tolerance in-sample


def test_rule_out_scorer_prefers_meeting_the_target():
    def scorer(y_true, predicted):
        return _rule_out_score(y_true, predicted, target_sensitivity=0.9)

    y = np.array([1] * 10 + [0] * 10)
    meets = np.array([1] * 9 + [0] + [1] * 5 + [0] * 5)  # sens 0.9, spec 0.5
    misses = np.array([1] * 5 + [0] * 5 + [0] * 10)  # sens 0.5, spec 1.0
    assert scorer(y, meets) == pytest.approx(0.5)
    assert scorer(y, misses) < 0


def test_trial_cohort_and_drug_are_never_predictors():
    for columns in FEATURE_SETS.values():
        assert not {"trial_cohort", "Drug", "ID", "N_Days", "Status", "Stage"} & set(columns)
