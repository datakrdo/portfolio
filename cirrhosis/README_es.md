🇬🇧 [English](README.md) · 🇪🇸 [Español](README_es.md)

# Modelado del estadio de la cirrosis biliar primaria (PBC) 🩺

¿Puede un laboratorio de rutina **descartar la cirrosis biliar primaria (PBC) avanzada** y ahorrar una biopsia hepática
a algunos pacientes? Este proyecto modela la cohorte de 418 filas de PBC de Mayo Clinic (1974–1984) como *estadificación
no invasiva*: el desenlace primario es **Estadio 3–4 versus Estadio 1–2**, cada modelo se compara con los scores clínicos
publicados (score de riesgo de Mayo, APRI), y el umbral operativo es un umbral de *descarte* (sensibilidad ≥ 90%). Los
análisis de estadio exacto y ordinal son desenlaces secundarios exploratorios.

**Respuesta corta: no con estos datos.** Los modelos no mejoran el score de Mayo de 1989 y, con 90% de sensibilidad,
ahorrarían la biopsia a solo ~10–15% de los pacientes, de los cuales cerca de la mitad igual tendría enfermedad avanzada. El
valor del proyecto es la forma honesta y libre de fugas de información en que se llega a esa respuesta, más un pipeline,
un CLI y una demo desplegables.

El entregable narrativo y autocontenido es
**[`pbc-portfolio_es.ipynb`](pbc-portfolio_es.ipynb)** — no importa el paquete local `pbc` y puede leerse o re-ejecutarse
por sí solo. El código fuente mantenido y testeado vive en `src/pbc/`, orquestado por el CLI `pbc`. El modelo desplegado
está documentado en **[`MODEL_CARD_es.md`](MODEL_CARD_es.md)**.

## Resultados 📊

Validación cruzada anidada repetida (5 × 5 folds, ajuste con Optuna y selección del umbral de descarte dentro de cada fold)
sobre todas las filas etiquetadas. Sensibilidad objetivo 90%; "biopsias evitadas" es la proporción de pacientes que cae por
debajo del umbral.

**Conjunto `core` — variables de rutina, los 412 pacientes etiquetados**

| Modelo | AUROC [IC 95%] | Sensibilidad | Especificidad | VPN | Biopsias evitadas | Pend. calib. |
| --- | --- | --- | --- | --- | --- | --- |
| logistic (desplegado) | 0,67 [0,57, 0,77] | 0,93 | 0,23 | 0,55 | 12% | 0,89 |
| random forest | 0,67 [0,56, 0,77] | 0,93 | 0,20 | 0,56 | 11% | 1,27 |
| HistGB | 0,67 [0,56, 0,77] | 0,94 | 0,17 | 0,54 | 9% | 0,71 |
| Score de Mayo | 0,69 [0,60, 0,80] | 0,92 | 0,22 | 0,50 | 12% | 0,95 |

**Conjunto `full` — más el panel de laboratorio extendido, 312 pacientes del ensayo**

| Modelo | AUROC [IC 95%] | Sensibilidad | Especificidad | VPN | Biopsias evitadas | Pend. calib. |
| --- | --- | --- | --- | --- | --- | --- |
| logistic (desplegado) | 0,73 [0,63, 0,81] | 0,91 | 0,23 | 0,52 | 13% | 0,80 |
| random forest | 0,74 [0,61, 0,82] | 0,91 | 0,26 | 0,48 | 14% | 0,98 |
| HistGB | 0,71 [0,60, 0,81] | 0,90 | 0,20 | 0,41 | 13% | 0,74 |
| Score de Mayo | 0,71 [0,60, 0,80] | 0,91 | 0,20 | 0,45 | 12% | 0,97 |
| APRI | 0,67 [0,55, 0,82] | 0,96 | 0,07 | 0,24 | 5% | 0,69 |

- Ningún modelo de ML supera al score de Mayo, y todos los intervalos se superponen: las diferencias están dentro del ruido.
- La prevalencia de Estadio 3–4 es 73%, así que incluso con 90% de sensibilidad el VPN es de solo ~0,5: cerca de la mitad de los
  pacientes "descartados" en realidad tiene enfermedad avanzada. No es una prueba que ahorre biopsias.
- `core` cumple el tamaño muestral mínimo de Riley et al. (306 necesarios, 412 disponibles); `full` no (359 necesarios, 312 disponibles).
- El score de Mayo se derivó en esta misma cohorte (para supervivencia), así que es un comparador favorable, no externo.

## Por qué está construido así 🧭

1. **La ausencia de datos es estructural, no aleatoria.** Las 106 filas sin `Drug` (IDs de paciente 313–418) son exactamente
   las filas sin todo el bloque de laboratorio/clínico (`Ascites`, `Hepatomegaly`, `Spiders`, `Alk_Phos`, `SGOT`, `Copper`,
   `Cholesterol`, `Tryglicerides`): los **pacientes de registro no aleatorizados** del ensayo Mayo. Imputar entre las dos
   poblaciones las mezclaría en silencio, y `Drug` (o una etiqueta `trial_cohort`) dejaría que el modelo aprenda "de qué cohorte
   viene esta fila" — algo que un paciente nuevo no tiene. En cambio la estructura es explícita: un conjunto de variables
   **`core`** (7 variables de rutina, los 412 pacientes) y uno **`full`** (15 variables, ajustado solo con los 312 pacientes
   del ensayo). `trial_cohort` sobrevive solo como estratificador de la Tabla 1. Ver `pbc.data.FEATURE_SETS` y
   `outputs/table_one_by_cohort.csv`.
2. **Un único split 80/20 es demasiado frágil como número principal.** Con 63–83 filas de test, el IC 95% del AUROC tiene un ancho
   de aproximadamente 0,25–0,3. El split igual se ejecuta como una demostración de validación externa, pero `pbc.validation`
   agrega **validación cruzada anidada repetida** sobre todas las filas y la **corrección de optimismo de Harrell**, que solo se
   calcula donde es válida (logística y scores clínicos): los ensambles de árboles memorizan los duplicados del bootstrap, así que su
   AUROC "corregido" queda por encima de la estimación anidada honesta.
3. **Las probabilidades deben significar lo que dicen.** Sin `class_weight` (reponderar lleva las predicciones a una prevalencia del
   50% y arruina la calibración), y los hiperparámetros se ajustan con log-loss, una regla de puntuación propia, en vez de AUROC. El
   desbalance de clases se maneja con el umbral de descarte.
4. **El comparador es lo que un médico ya tiene.** Mayo y APRI entran por el mismo pipeline ajustado por fold (fórmula fija +
   recalibración de una variable), así que un modelo tiene que superarlos bajo idéntica validación.

Cada transformador se ajusta por fold dentro de un `Pipeline` y solo hace `.transform` sobre las filas retenidas, así que nada de los
datos de test se filtra de vuelta al entrenamiento.

## Inicio rápido 🚀

El entorno se gestiona con [`uv`](https://docs.astral.sh/uv/):

```bash
uv sync --all-extras          # crea .venv, instala core + shap + optuna + streamlit + dev
uv run pytest -q              # 28 tests
uv run ruff check src/ tests/ app/
uv run ruff format --check src/ tests/ app/
```

Ejecutar el pipeline (ambos conjuntos de variables, modelos de ML más los scores clínicos de referencia):

```bash
uv run pbc pipeline --nested-cv --shap
```

Esto lee `data/raw/pbc.csv` y escribe `outputs/metrics.json`, `outputs/table_one_by_cohort.csv`, `outputs/table_one_by_stage.csv` y, por
conjunto de variables (`outputs/core/`, `outputs/full/`), gráficos ROC/PR/calibración/curva de decisión y (con `--shap`) resúmenes
SHAP. Flags útiles: `--models logistic` (corrida rápida), `--hpo-trials 20` (tuning con Optuna), `--bootstrap 0` (sin ICs bootstrap del
split único). `uv run pbc eda` escribe `outputs/eda_summary.json`.

Entrenar y usar el modelo desplegado (guardado en `models/`):

```bash
uv run pbc train                                   # ajusta logistic con todas las filas de cada conjunto + umbral de descarte
uv run pbc predict --input paciente.json           # age_years + columnas del dataset; usa `full` si se da todo el panel
uv run streamlit run app/streamlit_app.py          # demo bilingüe EN/ES
```

## Diseño 🏗️

- `src/pbc/data.py` valida el CSV, construye los desenlaces, define `CORE_FEATURES`/`FULL_FEATURES` y `select_feature_set` (el
  conjunto `full` conserva solo pacientes del ensayo), y reporta eventos por variable (Peduzzi et al.).
- `src/pbc/preprocessing.py` provee transformadores ajustados por fold: imputación por mediana/`__MISSING__` + escalado
  (`build_preprocessor`), imputación KNN, y un caster sin imputación para el soporte nativo de NaN/categóricas de
  `HistGradientBoostingClassifier`.
- `src/pbc/scores.py` implementa el score de riesgo de Mayo y el APRI (con fórmulas testeadas unitariamente).
- `src/pbc/modeling.py` define `logistic` (desplegado), `random_forest`, `hist_gradient_boosting`, `mayo`, `apri` y los estimadores
  secundarios multiclase/ordinal. `build_named_pipeline` es la única fuente de verdad de "pipeline sin ajustar para el modelo X".
- `src/pbc/evaluation.py` provee AUROC/AUPRC/sensibilidad/especificidad/VPP/VPN/Brier, pendiente/intercepto de calibración de Cox,
  intervalos bootstrap, análisis de curva de decisión, tamaño muestral de Riley, estabilidad de predicciones y
  `select_operating_threshold` con el scorer de descarte (especificidad máxima con sensibilidad ≥ 90%, vía `TunedThresholdClassifierCV`).
- `src/pbc/validation.py` — CV anidada repetida (con calibración agrupada fuera de fold) y corrección de optimismo.
- `src/pbc/predict.py` — `train_final` y `predict_patient`, compartidos por el CLI y la app de Streamlit en `app/`.
- `src/pbc/explainability.py`, `hpo.py`, `reporting.py` — SHAP, Optuna y generación de artefactos (incluida la Tabla 1).

## Notebooks 📓

- `01_eda_es.ipynb` — esquema de la cohorte, ausencia estructural de datos, los dos conjuntos de variables.
- `02_modeling_es.ipynb` — resultados de split único para ambos conjuntos, scores clínicos de referencia, umbrales de descarte.
- `03_model_development_es.ipynb` — CV anidada, corrección de optimismo, Optuna, SHAP y la comparación completa.
- `pbc-portfolio_es.ipynb` (raíz del repo) — entregable narrativo autocontenido, no necesita el paquete `pbc`.

Cada notebook tiene un gemelo en inglés (`*.ipynb`).

## Skills 🧠

- Manejo de datos consciente de la cohorte y diseño de conjuntos de variables ante ausencia estructural
- Pipelines de preprocesamiento sin fuga de datos
- Clasificación binaria, multiclase y ordinal
- Validación cruzada anidada y corrección de optimismo bootstrap (y saber cuándo no es válida)
- Calibración, análisis de curva de decisión y selección de umbral de descarte
- Comparación contra scores clínicos publicados (Mayo, APRI)
- Evaluación estadística de tamaño de muestra (Riley et al.)
- Explicabilidad de modelos (SHAP), model card, CLI y demo en Streamlit
- Entornos reproducibles y CI (uv, ruff, pytest, GitHub Actions)

## Herramientas 🛠️

- Python, pandas, NumPy, scikit-learn (pipelines, `HistGradientBoostingClassifier`, `TunedThresholdClassifierCV`)
- Optuna, SHAP, Streamlit, joblib
- Matplotlib, seaborn
- uv, ruff, pytest

## Procedencia de los datos y limitaciones 📎

`data/raw/pbc.csv` es el dataset PBC original de 418 filas y 20 columnas (412 valores de `Stage` etiquetados); no se agrega ninguna
cohorte externa. Es una cohorte pequeña, histórica (1974–1984), de un solo centro, con clases desbalanceadas y ausencia estructural de
datos por diseño. No hay validación externa ni temporal, y el estadio es una etiqueta histológica con desacuerdo interobservador
conocido. Los resultados son exploratorios y **no deben informar decisiones clínicas**; ver [`MODEL_CARD_es.md`](MODEL_CARD_es.md).
