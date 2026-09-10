# PBC disease-stage modeling 🩺

Production-oriented modeling of the 418-row Mayo Clinic primary biliary cirrhosis (PBC)
cohort (1974–1984). The primary endpoint is **Stage 3–4 versus Stage 1–2**; exact-stage and
cumulative ordinal analyses remain exploratory secondary endpoints.

The self-contained, narrative deliverable is
**[`pbc-portfolio-2026.ipynb`](pbc-portfolio-2026.ipynb)** — it imports no local `src/`
modules and can be read or re-run on its own. The maintained, tested source lives under
`src/`, orchestrated by `scripts/run_pipeline.py`.

## Why it's built this way 🧭

Two properties of this cohort drive most of the design:

1. **Missingness is structural, not random.** The 106 rows missing `Drug` (patient IDs
   313–418) are exactly the rows missing the entire lab/clinical block (`Ascites`,
   `Hepatomegaly`, `Spiders`, `Alk_Phos`, `SGOT`, `Copper`, `Cholesterol`, `Tryglicerides`).
   These are the Mayo trial's **non-randomised registry patients**, not a random subsample.
   Imputing a shared median across both groups mixes two clinically distinct subcohorts
   without saying so, and `Drug` — null exactly for the registry group — would let a model
   learn "which cohort is this row from" instead of anything clinical. `Drug` is treated as a
   leakage column; an explicit `trial_cohort` (`randomised`/`registry`) indicator replaces it
   as a declared predictor. See `src/data.add_cohort_indicator`, and `table_one_by_cohort.csv`
   in `outputs/` for the empirical confirmation.
2. **A single 80/20 split is too fragile to be the headline number.** With ~83 test rows,
   the 95% CI on AUROC is roughly 0.26 wide — too wide to compare models or defend a
   threshold. The single split is still run (as one external-validation demonstration), but
   `src/validation.py` adds **repeated nested cross-validation** and **Harrell's bootstrap
   optimism correction** as the small-sample-appropriate estimates that are actually reported.

Every transformer (imputer, scaler, encoder) is fold-fitted inside a `Pipeline` and only ever
`.transform`s the held-out rows, so nothing about test data leaks back into training.

## Quick start 🚀

Environment is managed with [`uv`](https://docs.astral.sh/uv/):

```bash
uv sync --all-extras          # creates .venv, installs core + shap + optuna + lgbm + dev
uv run pytest -q              # 15 tests
uv run ruff check src/ scripts/ tests/
uv run ruff format --check src/ scripts/ tests/
```

Run the pipeline:

```bash
uv run python scripts/run_pipeline.py \
  --models logistic random_forest hist_gradient_boosting lightgbm \
  --nested-cv --shap
```

This reads `data/raw/pbc.csv` and writes `outputs/metrics.json`, `outputs/table_one_by_cohort.csv`,
`outputs/table_one_by_stage.csv`, ROC/PR/calibration plots, and (with `--shap`) SHAP summary
plots. Useful flags:

- `--models logistic` — fast smoke run.
- `--hpo-trials 20` — Optuna tuning for RF/HistGB/LightGBM.
- `--nested-cv` — also run `nested_cv_evaluate` and `bootstrap_optimism_correction`
  (`src/validation.py`) per model, alongside the single-split metrics.
- `--bootstrap 0` — disable single-split bootstrap CIs.

```bash
uv run python scripts/run_eda.py
```

writes `outputs/eda_summary.json` without mutating the source data.

## Design 🏗️

- `src/data.py` validates the restored CSV, removes unlabeled `Stage` rows, derives
  `trial_cohort` from `ID` (`add_cohort_indicator`), and excludes `Stage`, `ID`, `N_Days`,
  `Status`, and `Drug` from predictors (`LEAKAGE_COLUMNS`). `train_test_split_pipeline`
  stratifies on `y_binary × trial_cohort` so both subcohorts are represented in both splits.
  `events_per_variable` reports the EPV ratio for the fitted feature count (Peduzzi et al.).
- `src/preprocessing.py` provides three fold-fitted transformers: `build_preprocessor`
  (median/`__MISSING__`-category impute + scale, the interpretable-model default),
  `build_knn_preprocessor` (KNN imputation), and `build_native_preprocessor` (casts to
  `category` dtype, imputes nothing — feeds `HistGradientBoostingClassifier`'s native
  NaN/categorical support).
- `src/modeling.py` provides `logistic` (interpretable baseline), `random_forest`,
  `hist_gradient_boosting` (**primary comparator** — treats structural missingness as
  signal instead of smoothing over it), and optional `lightgbm`; plus the exact-multiclass
  and cumulative-ordinal secondary estimators. `build_named_pipeline` is the single source
  of truth for "unfitted pipeline for model X", shared by `fit_model`, `src/validation.py`,
  and `src/pipeline.py`.
- `src/evaluation.py` provides AUROC/AUPRC/sensitivity/specificity/Brier metrics,
  `calibration_slope`/`calibration_intercept` (Cox calibration-in-the-large), bootstrap
  intervals, and `select_operating_threshold` (`TunedThresholdClassifierCV`-based, CV-only
  threshold selection — no manual validation split).
- `src/validation.py` — repeated nested CV (`nested_cv_evaluate`) and bootstrap optimism
  correction (`bootstrap_optimism_correction`), the small-sample estimators described above.
- `src/explainability.py`, `src/hpo.py`, and `src/reporting.py` provide SHAP (with a
  `HistGradientBoostingClassifier`-specific category-code encoding fix for SHAP's
  TreeExplainer), Optuna HPO, and unified artifact generation, including the clinical
  "Table 1" (`build_table_one`) stratified by cohort and by stage.

No leakage: every transformer is fold-fitted inside a `Pipeline`, the operating threshold is
selected only via `TunedThresholdClassifierCV` on training folds, and `Drug` is absent from
`get_feature_names_out()` for every model (asserted in CI).

## Notebooks 📓

- `01_eda.ipynb` — cohort schema, structural missingness, cohort/clinical context.
- `02_modeling.ipynb` — baseline production pipeline (logistic, RF, HistGB), single-split
  results.
- `03_model_development.ipynb` — nested CV, bootstrap optimism correction, Optuna HPO, SHAP,
  and the full model comparison.
- `pbc-portfolio-2026.ipynb` (repo root) — self-contained narrative deliverable; the one
  meant to be read or re-run on its own, without `src/`.

Every executable notebook cell is immediately followed by a short discussion of what the
output shows, what it means clinically, and what was decided as a result.

## Skills 🧠

- Data wrangling and cohort-aware feature engineering
- Exploratory data analysis on a small clinical cohort
- Leakage-safe preprocessing pipeline design
- Binary, multiclass, and ordinal classification
- Nested cross-validation and bootstrap optimism correction
- Hyperparameter tuning (Optuna, nested search)
- Model explainability (SHAP)
- Calibration and decision-curve analysis
- Statistical sample-size assessment (Riley et al.)
- Reproducible environments and CI (uv, ruff, pytest, GitHub Actions)

## Tools 🛠️

- Python, pandas, NumPy
- scikit-learn (pipelines, `HistGradientBoostingClassifier`, `TunedThresholdClassifierCV`)
- LightGBM
- Optuna
- SHAP
- Matplotlib
- uv, ruff, pytest

## Data provenance and limitations 📎

`data/raw/pbc.csv` is the original 418-row, 20-column PBC dataset (412 labeled `Stage`
values); no external cohort is added. This is a small, historical (1974–1984), single-center,
class-imbalanced cohort with structural missingness by design (see above). Events-per-variable
on the training fold is below the conventional EPV ≥ 10 rule of thumb — reported explicitly in
`outputs/metrics.json`, not hidden. There is no external or temporal validation; results are
exploratory and are not intended to inform clinical decisions.
