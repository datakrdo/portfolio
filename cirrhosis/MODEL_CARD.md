🇬🇧 [English](MODEL_CARD.md) · 🇪🇸 [Español](MODEL_CARD_es.md)

# Model card — PBC advanced-stage rule-out (`pbc-modeling`)

## Intended use

Educational/portfolio demonstration of a *non-invasive staging* model: estimate the probability that a patient with
primary biliary cirrhosis (PBC) has histological **Stage 3–4** (versus Stage 1–2) from routine clinical and laboratory
variables, and flag patients below a rule-out threshold (sensitivity ≥ 90%).

**Out of scope — do not use for:** clinical decisions, deciding whether to perform or skip a liver biopsy, diagnosis,
prognosis or treatment. The model does not replace biopsy or clinician assessment.

## Model

- **Estimator:** `logistic` (ridge-regularised logistic regression; `C` and an optional spline expansion tuned with Optuna on
  log-loss). Chosen *before* looking at results for parsimony at this sample size, calibration and explainability — not because
  it ranked first. No `class_weight`, so probabilities keep the cohort prevalence (73% advanced).
- **Preprocessing:** median imputation, `log1p` on right-skewed labs, standardisation, ordinal `Edema` (N < S < Y), one-hot for the
  remaining categoricals. Fitted inside the pipeline; nothing is learned from held-out rows.
- **Operating threshold:** chosen by cross-validation on the training rows (`TunedThresholdClassifierCV`) to maximise specificity
  subject to sensitivity ≥ 90%. Deployed thresholds: **0.579 (`core`)**, **0.542 (`full`)**.
- **Two models, selected automatically by `pbc.predict.predict_patient`:**
  - `core` — age, sex, edema, bilirubin, albumin, platelets, prothrombin. Fit on all 412 labeled patients.
  - `full` — `core` plus ascites, hepatomegaly, spiders, cholesterol, copper, alkaline phosphatase, SGOT, triglycerides. Fit on the
    312 randomised-trial patients only (the only ones with that panel). Used only if every one of those variables is provided.
- **Inputs never used:** `Drug`, `trial_cohort`, patient ID, follow-up time and vital status (`Drug`/`trial_cohort` proxy how the
  cohort was recruited and do not exist for a new patient; follow-up and status encode the outcome).

## Training data

Mayo Clinic PBC trial cohort, 1974–1984 (418 patients; 412 with a labeled Stage; 312 randomised to D-penicillamine/placebo and 100
labeled registry patients without the extended lab panel). Single centre, historical, prevalence of Stage 3–4 = 73%.

## Performance (repeated nested CV, 5 × 5, all rows; see `README.md` for the full table)

| Set | AUROC [95% interval] | Specificity at ~90% sensitivity | NPV | Share below threshold | Calibration slope / intercept |
| --- | --- | --- | --- | --- | --- |
| `core` (n=412) | 0.67 [0.57, 0.77] | 0.23 | 0.55 | 12% | 0.89 / 0.09 |
| `full` (n=312) | 0.73 [0.63, 0.81] | 0.23 | 0.52 | 13% | 0.80 / 0.16 |

Published comparators on the same folds: Mayo risk score AUROC 0.69 (`core`) and 0.71 (`full`); APRI 0.67 (`full`). The model is
**not** better than the Mayo score, and the intervals overlap fully.

## Limitations and risks

- **The rule-out does not work.** At 73% prevalence and ~90% sensitivity the negative predictive value is only ~0.5: about half of
  the patients below the threshold have advanced disease. A "ruled out" output must never be used to skip a biopsy.
- No external or temporal validation; one 1974–1984 single-centre cohort. Spectrum, assays and treatment era differ from today's practice.
- Small sample: `core` meets Riley et al.'s minimum sample size (306), `full` does not (359 needed, 312 available; events-per-variable ≈ 5).
- The Mayo comparator was derived on this cohort (for survival), and APRI is an off-label proxy; the comparison is favourable to the scores.
- Stage is a histological label with known inter-rater disagreement; the model inherits that label noise.
- Sex is a predictor; the cohort is ~90% female, so estimates for male patients are especially uncertain.

## How to use

```bash
uv run pbc train                          # (re)fit and save models/pbc_{core,full}.joblib + metadata.json
uv run pbc predict --input patient.json   # {"age_years": 55, "Sex": "F", "Edema": "N", "Bilirubin": 1.4, ...}
uv run streamlit run app/streamlit_app.py
```

Metrics are reproducible: `uv run pbc pipeline --nested-cv --shap` (seed 41) regenerates `outputs/metrics.json`.
