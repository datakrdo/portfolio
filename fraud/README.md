[English](README.md) · [Español](README_es.md)

# Fraud detection — credit card transactions

Fraud detection on the synthetic Sparkov credit card transaction dataset: ~1.85 million
transactions from 983 cards between January 2019 and December 2020, with a fraud
prevalence of 0.58% in the training period and 0.39% in the holdout.

Two deliverables:

- **[`fraud-portfolio.ipynb`](fraud-portfolio.ipynb)** — self-contained notebook
  (does not import `src/`), executed end to end. The full walkthrough: problem, EDA,
  causal feature engineering, the model ladder, the imbalance-handling decision,
  calibration, and the threshold-and-money section.
- **`src/fraud/`** — installable production package with `uv`: loading and temporal
  split, feature engineering, GPU models, evaluation, threshold, orchestrator, and a
  training/batch-scoring CLI.

## Tools 🛠️

- Python, pandas, NumPy
- scikit-learn (`Pipeline`, `ColumnTransformer`, `IsotonicRegression`, `DummyClassifier`)
- XGBoost and LightGBM on GPU (`device="cuda"` / `"gpu"`)
- imbalanced-learn (`SMOTENC`, `RandomUnderSampler`, inside `imblearn.Pipeline`)
- SHAP
- matplotlib, seaborn
- uv, ruff, pytest, GitHub Actions

## Skills 🧠

- Causal feature engineering (rolling windows and aggregates with no temporal leakage)
- Classification under extreme imbalance: PR-AUC, probability calibration, cost-benefit
- Temporal validation for event time series (date-based split, production holdout)
- Decision-threshold optimization by expected dollar value
- Model explainability (SHAP) and communicating results to a non-technical audience
- Packaging an ML project as an installable package with CLI, tests, and CI

## Design decisions 🧭

- **PR-AUC against prevalence, never accuracy.** A constant classifier that never flags
  fraud scores 99.4% accuracy on this dataset. `src/fraud/evaluation.py::binary_metrics`
  reports PR-AUC and its lift over the real prevalence (0.58% train / 0.39% holdout), not
  against the 0.5 of a random ranking.
- **Temporal split, not random.** The model scores future transactions with data from the
  past; a random split would mix a card's future transactions into that same card's
  training set. `src/fraud/data.py::temporal_split` cuts by date, and `fraudTest.csv` is
  treated as the dataset's natural holdout, untouched until final evaluation.
- **Causal velocity features.** The 22–23h hour shows a 5x lift over the base prevalence,
  and the median seconds since a card's previous transaction is 4,908 in fraud versus
  16,623 in legitimate transactions — the burst is real signal. That whole family of
  features (`src/fraud/features.py`) is computed with
  `groupby("cc_num").rolling(window, on=..., closed="left")`, which excludes the current
  row: an aggregate that leaks into itself inflates validation PR-AUC without the model
  having learned anything generalizable.
- **`distance_km` is built and discarded.** `merch_lat`/`merch_long` in this dataset are
  generated uniformly around the cardholder's home, so cardholder-to-merchant distance
  doesn't discriminate (≈76 km in both classes). It's kept documented as a negative
  result, not omitted.
- **Loss weighting, not SMOTE.** `scale_pos_weight` (boosted models) / `class_weight="balanced"`
  (LogReg) instead of resampling. Reasons: (1) 9,651 absolute fraud cases isn't a
  label-scarcity problem; (2) SMOTE interpolates in a space with high-cardinality
  categoricals (`merchant`, 693 levels) where Euclidean distance is meaningless; (3)
  weighting doesn't touch the data, so it can't leak into validation; (4) resampling
  distorts probabilities, and this project needs calibrated probabilities for the dollar
  savings calculation. The notebook measures, not just asserts: it compares validation
  PR-AUC across no-treatment / weighting / SMOTENC / undersampling before locking in the
  decision.
- **Threshold by net dollar savings, frozen on validation.** `src/fraud/threshold.py`
  maximizes `Σ amt[TP] − FP_COST·|FP| − Σ amt[FN]` over validation, never over the
  holdout — choosing it there would be the same leakage as tuning any hyperparameter
  against the final evaluation set.

## Quick start 🚀

`data/` is in `.gitignore`: download the Sparkov dataset from
[Kaggle](https://www.kaggle.com/datasets/kartik2112/fraud-detection) and place
`fraudTrain.csv`/`fraudTest.csv` in `data/raw/` before running the following.

```bash
uv sync --all-extras
uv run ruff check src/ tests/ && uv run ruff format --check src/ tests/
uv run pytest -q

uv run fraud train --data data/raw/fraudTrain.csv --model xgboost --out models/
uv run fraud evaluate --model models/xgboost.joblib --data data/raw/fraudTest.csv
uv run fraud score transactions.csv --model models/xgboost.joblib -o scores.csv
```

`uv run jupyter nbconvert --execute --inplace fraud-portfolio.ipynb` reproduces the
notebook from scratch.

## Module design 📦

- `src/fraud/data.py` — loads, validates schema, casts `cc_num` to string, sorts
  causally, and cuts `fraudTrain.csv` into train/validation by date (`TemporalSplit`).
- `src/fraud/features.py` — `build_features`, pure and stateless: temporal features,
  amount, causal per-card velocity, merchant/category novelty, and `distance_km`.
- `src/fraud/modeling.py` — `MODEL_SPECS` registry (`dummy`, `logreg`, `lightgbm`,
  `xgboost`), the last two using GPU (`device="cuda"`/`"gpu"`) with graceful degradation
  if the `[gpu]` extras aren't installed.
- `src/fraud/evaluation.py` — PR-AUC against prevalence, precision@k, confusion matrix.
- `src/fraud/threshold.py` — vectorized threshold sweep, net savings, marginal analysis,
  and `FP_COST` sensitivity.
- `src/fraud/pipeline.py` — single orchestrator: split → features → fit → isotonic
  calibration on validation → frozen threshold. `FraudModel` packages the three
  together, because a threshold without its model isn't a reproducible decision.
- `src/fraud/cli.py` — `train` / `score` / `evaluate` subcommands.
- `src/fraud/explainability.py` — SHAP on the final model, behind the `[shap]` extra.

## Data and limitations ⚠️

Synthetic dataset (Sparkov), no external validation. No device, session, or payment
channel data. `FP_COST` (manual review cost, USD 4) is an estimate external to the
dataset — the notebook's sensitivity section shows how much the recommendation depends
on it. 983 cards is a small population. The model has no way to handle a card with no
history (cold start): velocity features are zero by construction there, not by model
error.
