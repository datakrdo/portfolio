import math

import pandas as pd
import pytest

from pbc.scores import apri, mayo_risk_score


def test_mayo_risk_score_matches_hand_calculation():
    frame = pd.DataFrame(
        {
            "Age": [50 * 365.25],
            "Bilirubin": [2.0],
            "Albumin": [3.5],
            "Prothrombin": [11.0],
            "Edema": ["S"],
        }
    )
    expected = (
        0.0394 * 50
        + 0.8707 * math.log(2.0)
        - 2.533 * math.log(3.5)
        + 2.380 * math.log(11.0)
        + 0.859 * 0.5
    )
    assert mayo_risk_score(frame).iloc[0] == pytest.approx(expected)


def test_apri_matches_hand_calculation():
    frame = pd.DataFrame({"SGOT": [80.0], "Platelets": [200.0]})
    assert apri(frame).iloc[0] == pytest.approx(1.0)  # (80/40)/200*100
