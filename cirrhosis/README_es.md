🇬🇧 [English](README.md) · 🇪🇸 [Español](README_es.md)

# Predicción de estadio en cirrosis biliar primaria (PBC) 🩺

Modelado orientado a producción sobre la cohorte de 418 filas de Mayo Clinic de cirrosis
biliar primaria (PBC) (1974–1984). El endpoint primario es **Estadio 3–4 versus Estadio
1–2**; los análisis de estadio exacto y ordinal acumulativo siguen siendo endpoints
secundarios exploratorios.

El entregable narrativo y autocontenido es
**[`pbc-portfolio.ipynb`](pbc-portfolio.ipynb)** — no importa módulos locales de `src/`
y puede leerse o re-ejecutarse por sí solo. El código fuente mantenido y testeado vive en
`src/`, orquestado por `scripts/run_pipeline.py`.

## Por qué está construido así 🧭

Dos propiedades de esta cohorte guían la mayor parte del diseño:

1. **La ausencia de datos es estructural, no aleatoria.** Las 106 filas sin `Drug`
   (IDs de paciente 313–418) son exactamente las filas sin todo el bloque de
   laboratorio/clínico (`Ascites`, `Hepatomegaly`, `Spiders`, `Alk_Phos`, `SGOT`, `Copper`,
   `Cholesterol`, `Tryglicerides`). Son los **pacientes de registro no aleatorizados** del
   ensayo Mayo, no un subsample al azar. Imputar una mediana compartida entre ambos grupos
   mezcla dos subcohortes clínicamente distintas sin decirlo, y `Drug` — nulo exactamente
   para el grupo de registro — dejaría que un modelo aprenda "de qué cohorte viene esta
   fila" en lugar de algo clínico. `Drug` se trata como una columna de fuga (leakage); un
   indicador explícito `trial_cohort` (`randomised`/`registry`) la reemplaza como predictor
   declarado. Ver `src/data.add_cohort_indicator`, y `table_one_by_cohort.csv` en `outputs/`
   para la confirmación empírica.
2. **Un único split 80/20 es demasiado frágil como número principal.** Con ~83 filas de
   test, el IC 95% del AUROC tiene un ancho de aproximadamente 0.26 — demasiado ancho para
   comparar modelos o defender un umbral. El split único igual se ejecuta (como una
   demostración de validación externa), pero `src/validation.py` agrega **validación
   cruzada anidada repetida** y **corrección de optimismo bootstrap de Harrell** como las
   estimaciones apropiadas para muestra pequeña que realmente se reportan.

Cada transformador (imputador, escalador, encoder) se ajusta por fold dentro de un
`Pipeline` y solo hace `.transform` sobre las filas held-out, así que nada de los datos de
test se filtra de vuelta al entrenamiento.

## Inicio rápido 🚀

El entorno se gestiona con [`uv`](https://docs.astral.sh/uv/):

```bash
uv sync --all-extras          # crea .venv, instala core + shap + optuna + lgbm + dev
uv run pytest -q              # 21 tests
uv run ruff check src/ scripts/ tests/
uv run ruff format --check src/ scripts/ tests/
```

Ejecutar el pipeline:

```bash
uv run python scripts/run_pipeline.py \
  --models logistic random_forest hist_gradient_boosting lightgbm \
  --nested-cv --shap
```

Esto lee `data/raw/pbc.csv` y escribe `outputs/metrics.json`, `outputs/table_one_by_cohort.csv`,
`outputs/table_one_by_stage.csv`, gráficos ROC/PR/calibración, y (con `--shap`) gráficos
resumen de SHAP. Flags útiles:

- `--models logistic` — corrida rápida de verificación.
- `--hpo-trials 20` — tuning con Optuna para RF/HistGB/LightGBM.
- `--nested-cv` — también ejecuta `nested_cv_evaluate` y `bootstrap_optimism_correction`
  (`src/validation.py`) por modelo, junto a las métricas del split único.
- `--bootstrap 0` — deshabilita los ICs bootstrap del split único.

```bash
uv run python scripts/run_eda.py
```

escribe `outputs/eda_summary.json` sin mutar los datos fuente.

## Diseño 🏗️

- `src/data.py` valida el CSV restaurado, elimina filas sin `Stage` etiquetado, deriva
  `trial_cohort` a partir de `ID` (`add_cohort_indicator`), y excluye `Stage`, `ID`,
  `N_Days`, `Status`, y `Drug` de los predictores (`LEAKAGE_COLUMNS`).
  `train_test_split_pipeline` estratifica sobre `y_binary × trial_cohort` para que ambas
  subcohortes estén representadas en ambos splits. `events_per_variable` reporta la razón
  EPV para la cantidad de features ajustadas (Peduzzi et al.).
- `src/preprocessing.py` provee tres transformadores ajustados por fold: `build_preprocessor`
  (imputación con mediana/categoría `__MISSING__` + escalado, el default para modelos
  interpretables), `build_knn_preprocessor` (imputación KNN), y `build_native_preprocessor`
  (castea a dtype `category`, no imputa nada — alimenta el soporte nativo de NaN/categóricas
  de `HistGradientBoostingClassifier`).
- `src/modeling.py` provee `logistic` (baseline interpretable), `random_forest`,
  `hist_gradient_boosting` (**comparador primario** — trata la ausencia estructural de
  datos como señal en lugar de suavizarla), y `lightgbm` opcional; más los estimadores
  secundarios de multiclase exacta y ordinal acumulativo. `build_named_pipeline` es la
  única fuente de verdad para "pipeline sin ajustar para el modelo X", compartida por
  `fit_model`, `src/validation.py`, y `src/pipeline.py`.
- `src/evaluation.py` provee métricas de AUROC/AUPRC/sensibilidad/especificidad/Brier,
  `calibration_slope`/`calibration_intercept` (calibración de Cox a gran escala),
  intervalos bootstrap, y `select_operating_threshold` (basado en
  `TunedThresholdClassifierCV`, selección de umbral solo por CV — sin split de validación
  manual).
- `src/validation.py` — CV anidada repetida (`nested_cv_evaluate`) y corrección de
  optimismo bootstrap (`bootstrap_optimism_correction`), los estimadores para muestra
  pequeña descritos arriba.
- `src/explainability.py`, `src/hpo.py`, y `src/reporting.py` proveen SHAP (con un fix de
  codificación de categorías específico para `HistGradientBoostingClassifier` que necesita
  el `TreeExplainer` de SHAP), HPO con Optuna, y generación unificada de artefactos,
  incluyendo la "Tabla 1" clínica (`build_table_one`) estratificada por cohorte y por
  estadio.

Sin fuga de datos: cada transformador se ajusta por fold dentro de un `Pipeline`, el
umbral operativo se selecciona solo vía `TunedThresholdClassifierCV` sobre folds de
entrenamiento, y `Drug` está ausente de `get_feature_names_out()` para todos los modelos
(verificado en CI).

## Notebooks 📓

- `01_eda.ipynb` — esquema de la cohorte, ausencia estructural de datos, contexto
  clínico/de cohorte.
- `02_modeling.ipynb` — pipeline de producción baseline (logistic, RF, HistGB), resultados
  de split único.
- `03_model_development.ipynb` — CV anidada, corrección de optimismo bootstrap, HPO con
  Optuna, SHAP, y la comparación completa de modelos.
- `pbc-portfolio.ipynb` (raíz del repo) — entregable narrativo autocontenido; el pensado
  para leerse o re-ejecutarse por sí solo, sin `src/`.

## Skills 🧠

- Manejo de datos e ingeniería de features consciente de la cohorte
- Análisis exploratorio de datos en una cohorte clínica pequeña
- Diseño de pipeline de preprocesamiento sin fuga de datos
- Clasificación binaria, multiclase, y ordinal
- Validación cruzada anidada y corrección de optimismo bootstrap
- Ajuste de hiperparámetros (Optuna, búsqueda anidada)
- Explicabilidad de modelos (SHAP)
- Calibración y análisis de curva de decisión
- Evaluación estadística de tamaño de muestra (Riley et al.)
- Entornos reproducibles y CI (uv, ruff, pytest, GitHub Actions)

## Herramientas 🛠️

- Python, pandas, NumPy
- scikit-learn (pipelines, `HistGradientBoostingClassifier`, `TunedThresholdClassifierCV`)
- LightGBM
- Optuna
- SHAP
- Matplotlib
- uv, ruff, pytest

## Procedencia de los datos y limitaciones 📎

`data/raw/pbc.csv` es el dataset PBC original de 418 filas y 20 columnas (412 valores de
`Stage` etiquetados); no se agrega ninguna cohorte externa. Es una cohorte pequeña,
histórica (1974–1984), de un solo centro, con clases desbalanceadas y ausencia estructural
de datos por diseño (ver arriba). El events-per-variable en el fold de entrenamiento está
por debajo de la regla convencional de EPV ≥ 10 — reportado explícitamente en
`outputs/metrics.json`, no oculto. No hay validación externa ni temporal; los resultados
son exploratorios y no están pensados para informar decisiones clínicas.
