import numpy as np

from src.reporting import generate_report


def test_generate_report_writes_decision_curve_and_stability_plots(tmp_path):
    y_test = np.array([0, 0, 1, 1, 0, 1, 1, 0])
    probabilities = {"logistic": np.array([0.1, 0.2, 0.6, 0.8, 0.3, 0.7, 0.9, 0.4])}
    stability = {
        "logistic": {
            "original_probabilities": probabilities["logistic"].tolist(),
            "bootstrap_mean": (probabilities["logistic"] + 0.02).tolist(),
            "bootstrap_std": [0.05] * 8,
            "instability_mape": 0.1,
            "n_boot_valid": 20,
        }
    }
    generate_report(
        {"models": {}},
        tmp_path,
        y_test=y_test,
        probabilities=probabilities,
        stability_results=stability,
    )
    assert (tmp_path / "decision_curve.png").exists()
    assert (tmp_path / "prediction_stability.png").exists()
