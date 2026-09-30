🇬🇧 [English](README.md) · 🇪🇸 [Español](README_es.md)

# PBC disease-stage modeling 🩺

Can routine bloodwork **rule out advanced primary biliary cirrhosis (PBC)** and spare some patients a
liver biopsy? This project models the 418-row Mayo Clinic PBC cohort (1974–1984) as *non-invasive
staging*: the primary endpoint is **Stage 3–4 versus Stage 1–2**, every model is compared with the
published clinical scores (Mayo risk score, APRI), and the operating threshold is a *rule-out* threshold
(sensitivity ≥ 90%). Exact-stage and ordinal analyses are exploratory secondary endpoints.

**Short answer: not with this data.** The models are no better than the 1989 Mayo score, and at 90%
sensitivity they would spare a biopsy to only ~10–15% of patients, about half of whom would still have
advanced disease. The value of the project is the honest, leakage-safe way that answer is reached, plus a
deployable pipeline, CLI and demo.

The self-contained, narrative deliverable is
**[`pbc-portfolio.ipynb`](pbc-portfolio.ipynb)** — it imports no local `pbc` package and can be read or
re-run on its own. The maintained, tested source lives in `src/pbc/`, orchestrated by the `pbc` CLI. The deployed
model is documented in **[`MODEL_CARD.md`](MODEL_CARD.md)**.

## Results 📊

Repeated nested cross-validation (5 × 5 folds, Optuna tuning and rule-out threshold selected inside every
fold) on every labeled row. Target sensitivity 90%; "biopsies avoided" is the share of patients falling below
the threshold.

**`core` set — routine variables, all 412 labeled patients**

| Model | AUROC [95% interval] | Sensitivity | Specificity | NPV | Biopsies avoided | Calib. slope |
| --- | --- | --- | --- | --- | --- | --- |
| logistic (deployed) | 0.67 [0.57, 0.77] | 0.93 | 0.23 | 0.55 | 12% | 0.89 |
| random forest | 0.67 [0.56, 0.77] | 0.93 | 0.20 | 0.56 | 11% | 1.27 |
| HistGB | 0.67 [0.56, 0.77] | 0.94 | 0.17 | 0.54 | 9% | 0.71 |
| Mayo risk score | 0.69 [0.60, 0.80] | 0.92 | 0.22 | 0.50 | 12% | 0.95 |

**`full` set — plus the extended lab panel, 312 trial patients**

| Model | AUROC [95% interval] | Sensitivity | Specificity | NPV | Biopsies avoided | Calib. slope |
| --- | --- | --- | --- | --- | --- | --- |
| logistic (deployed) | 0.73 [0.63, 0.81] | 0.91 | 0.23 | 0.52 | 13% | 0.80 |
| random forest | 0.74 [0.61, 0.82] | 0.91 | 0.26 | 0.48 | 14% | 0.98 |
| HistGB | 0.71 [0.60, 0.81] | 0.90 | 0.20 | 0.41 | 13% | 0.74 |
| Mayo risk score | 0.71 [0.60, 0.80] | 0.91 | 0.20 | 0.45 | 12% | 0.97 |
| APRI | 0.67 [0.55, 0.82] | 0.96 | 0.07 | 0.24 | 5% | 0.69 |

- No ML model beats the Mayo score, and all intervals overlap: the differences are within noise.
- Prevalence of Stage 3–4 is 73%, so even at 90% sensitivity the NPV is only ~0.5: about half of the patients
  "ruled out" are actually advanced. This is not a biopsy-sparing test.
- `core` meets Riley et al.'s minimum sample size (306 needed, 412 available); `full` does not (359 needed, 312 available).
- The Mayo score was derived on this same cohort (for survival), so it is a favourable comparator, not an external one.

## Why it's built this way 🧭

1. **Missingness is structural, not random.** The 106 rows missing `Drug` (patient IDs 313–418) are exactly the
   rows missing the whole lab/clinical block (`Ascites`, `Hepatomegaly`, `Spiders`, `Alk_Phos`, `SGOT`, `Copper`,
   `Cholesterol`, `Tryglicerides`): the Mayo trial's **non-randomised registry patients**. Imputing across the two
   populations would mix them silently, and `Drug` (or a `trial_cohort` label) would let a model learn "which cohort
   is this row from" — something a new patient does not have. Instead the structure is explicit: a **`core`** feature
   set (7 routine variables, all 412 patients) and a **`full`** set (15 variables, fit on the 312 trial patients only).
   `trial_cohort` survives only as the Table 1 stratifier. See `pbc.data.FEATURE_SETS` and `outputs/table_one_by_cohort.csv`.
2. **A single 80/20 split is too fragile to be the headline number.** With 63–83 test rows the 95% CI on AUROC is
   about 0.25–0.3 wide. The split is still run as one external-validation demonstration, but `pbc.validation` adds
   **repeated nested cross-validation** on every row and **Harrell's optimism correction**, which is only computed
   where it is valid (logistic and the clinical scores): tree ensembles memorise bootstrap duplicates, so their "corrected" AUROC
   lands above the honest nested estimate.
3. **Probabilities must mean what they say.** No `class_weight` (reweighting shifts predictions to a 50% prevalence and
   wrecks calibration), and hyperparameters are tuned on log-loss, a proper scoring rule, instead of AUROC. Class imbalance is
   handled by the rule-out threshold.
4. **The comparator is what a clinician already has.** Mayo and APRI enter through the same fold-fitted pipeline (fixed
   formula + one-feature recalibration), so a model has to beat them under identical validation.

Every transformer is fold-fitted inside a `Pipeline` and only `.transform`s held-out rows, so nothing about test data leaks
back into training.

## Quick start 🚀

Environment is managed with [`uv`](https://docs.astral.sh/uv/):

```bash
uv sync --all-extras          # creates .venv, installs core + shap + optuna + streamlit + dev
uv run pytest -q              # 28 tests
uv run ruff check src/ tests/ app/
uv run ruff format --check src/ tests/ app/
```

Run the pipeline (both feature sets, ML models plus the clinical score baselines):

```bash
uv run pbc pipeline --nested-cv --shap
```

This reads `data/raw/pbc.csv` and writes `outputs/metrics.json`, `outputs/table_one_by_cohort.csv`,
`outputs/table_one_by_stage.csv`, and per feature set (`outputs/core/`, `outputs/full/`) ROC/PR/calibration/decision-curve
plots and (with `--shap`) SHAP summaries. Useful flags: `--models logistic` (fast run), `--hpo-trials 20` (Optuna tuning),
`--bootstrap 0` (no single-split bootstrap CIs). `uv run pbc eda` writes `outputs/eda_summary.json`.

Train and use the deployed model (saved under `models/`):

```bash
uv run pbc train                                   # fits logistic on all rows of each set + rule-out threshold
uv run pbc predict --input patient.json            # age_years + dataset columns; uses `full` if the whole panel is given
uv run streamlit run app/streamlit_app.py          # bilingual EN/ES demo
```

## Design 🏗️

- `src/pbc/data.py` validates the CSV, builds the targets, defines `CORE_FEATURES`/`FULL_FEATURES` and
  `select_feature_set` (the `full` set keeps only trial patients), and reports events-per-variable (Peduzzi et al.).
- `src/pbc/preprocessing.py` provides fold-fitted transformers: median/`__MISSING__` impute + scale
  (`build_preprocessor`), KNN imputation, and a no-imputation caster for `HistGradientBoostingClassifier`'s native
  NaN/categorical support.
- `src/pbc/scores.py` implements the Mayo risk score and APRI (with unit-tested formulas).
- `src/pbc/modeling.py` defines `logistic` (deployed), `random_forest`, `hist_gradient_boosting`, `mayo`, `apri` and the
  secondary multiclass/ordinal estimators. `build_named_pipeline` is the single source of truth for "unfitted pipeline for model X".
- `src/pbc/evaluation.py` provides AUROC/AUPRC/sensitivity/specificity/PPV/NPV/Brier, Cox calibration slope/intercept,
  bootstrap intervals, decision-curve analysis, Riley sample size, prediction stability, and `select_operating_threshold`
  with the rule-out scorer (max specificity s.t. sensitivity ≥ 90%, via `TunedThresholdClassifierCV`).
- `src/pbc/validation.py` — repeated nested CV (with pooled out-of-fold calibration) and optimism correction.
- `src/pbc/predict.py` — `train_final` and `predict_patient`, shared by the CLI and the Streamlit app in `app/`.
- `src/pbc/explainability.py`, `hpo.py`, `reporting.py` — SHAP, Optuna, and artifact generation (incl. Table 1).

## Notebooks 📓

- `01_eda.ipynb` — cohort schema, structural missingness, the two feature sets.
- `02_modeling.ipynb` — single-split results for both feature sets, clinical score baselines, rule-out thresholds.
- `03_model_development.ipynb` — nested CV, optimism correction, Optuna, SHAP and the full comparison.
- `pbc-portfolio.ipynb` (repo root) — self-contained narrative deliverable, no `pbc` package needed.

Each notebook has a Spanish twin (`*_es.ipynb`).

## Skills 🧠

- Cohort-aware data handling and feature-set design for structural missingness
- Leakage-safe preprocessing pipelines
- Binary, multiclass, and ordinal classification
- Nested cross-validation and bootstrap optimism correction (and knowing when it is invalid)
- Calibration, decision-curve analysis and rule-out threshold selection
- Benchmarking against published clinical scores (Mayo, APRI)
- Statistical sample-size assessment (Riley et al.)
- Model explainability (SHAP), model card, CLI and Streamlit demo
- Reproducible environments and CI (uv, ruff, pytest, GitHub Actions)

## Tools 🛠️

- Python, pandas, NumPy, scikit-learn (pipelines, `HistGradientBoostingClassifier`, `TunedThresholdClassifierCV`)
- Optuna, SHAP, Streamlit, joblib
- Matplotlib, seaborn
- uv, ruff, pytest

## Data provenance and limitations 📎

`data/raw/pbc.csv` is the original 418-row, 20-column PBC dataset (412 labeled `Stage` values); no external cohort is added.
It is a small, historical (1974–1984), single-center, class-imbalanced cohort with structural missingness by design. There is
no external or temporal validation, and stage is a histological label with known inter-rater disagreement. Results are exploratory
and **must not inform clinical decisions**; see [`MODEL_CARD.md`](MODEL_CARD.md).
