# Fraud detection — credit card transactions

Detección de fraude sobre el dataset sintético Sparkov de transacciones con tarjeta de
crédito: ~1,85 millones de transacciones de 983 tarjetas entre enero de 2019 y diciembre
de 2020, con prevalencia de fraude de 0,58 % en el período de entrenamiento y 0,39 % en el
holdout.

Dos entregables:

- **[`fraud-portfolio.ipynb`](fraud-portfolio.ipynb)** — notebook autocontenida
  (no importa `src/`), narrativa en español, ejecutada de punta a punta. Es el recorrido
  completo: problema, EDA, feature engineering causal, escalera de modelos, la decisión de
  cómo tratar el desbalance, calibración, y la sección de umbral y dinero.
- **`src/fraud/`** — paquete productivo instalable con `uv`: carga y split temporal,
  feature engineering, modelos GPU, evaluación, umbral, orquestador y CLI de entrenamiento
  y scoring batch.

## Herramientas 🛠️

- Python, pandas, NumPy
- scikit-learn (`Pipeline`, `ColumnTransformer`, `IsotonicRegression`, `DummyClassifier`)
- XGBoost y LightGBM sobre GPU (`device="cuda"` / `"gpu"`)
- imbalanced-learn (`SMOTENC`, `RandomUnderSampler`, dentro de `imblearn.Pipeline`)
- SHAP
- matplotlib, seaborn
- uv, ruff, pytest, GitHub Actions

## Skills 🧠

- Ingeniería de features causal (rolling windows y agregados sin fuga temporal)
- Clasificación con desbalance extremo: PR-AUC, calibración de probabilidades, costo-beneficio
- Validación temporal para series de eventos (split por fecha, holdout de producción)
- Optimización de umbral de decisión por valor esperado en dólares
- Explicabilidad de modelos (SHAP) y comunicación de resultados a una audiencia no técnica
- Empaquetado de un proyecto de ML como paquete instalable con CLI, tests y CI

## Decisiones de diseño 🧭

- **PR-AUC contra prevalencia, nunca accuracy.** Un clasificador constante que nunca marca
  fraude saca 99,4 % de accuracy en este dataset. `src/fraud/evaluation.py::binary_metrics`
  reporta PR-AUC y su lift sobre la prevalencia real (0,58 % train / 0,39 % holdout), no
  contra el 0,5 de un ranking aleatorio.
- **Split temporal, no aleatorio.** El modelo puntúa transacciones futuras con datos del
  pasado; partir al azar mezclaría transacciones futuras de una tarjeta en el conjunto de
  entrenamiento de esa misma tarjeta. `src/fraud/data.py::temporal_split` corta por fecha, y
  `fraudTest.csv` se trata como el holdout natural del dataset, intocado hasta la evaluación
  final.
- **Features de velocidad causales.** La hora 22–23 h muestra un lift de 5× sobre la
  prevalencia base, y la mediana de segundos desde la transacción previa de la tarjeta es
  4.908 en fraude contra 16.623 en legítimas — la ráfaga es señal real. Toda esa familia de
  features (`src/fraud/features.py`) se calcula con
  `groupby("cc_num").rolling(window, on=..., closed="left")`, que excluye la fila actual: un
  agregado que se filtra a sí mismo infla el PR-AUC de validación sin que el modelo haya
  aprendido nada generalizable.
- **`distance_km` se construye y se descarta.** `merch_lat`/`merch_long` en este dataset se
  generan de forma uniforme alrededor del domicilio del titular, así que la distancia
  titular↔comercio no discrimina (≈76 km en ambas clases). Se deja documentada como
  resultado negativo, no se omite.
- **Ponderación de la pérdida, no SMOTE.** `scale_pos_weight` (boosteados) /
  `class_weight="balanced"` (LogReg) en vez de resamplear. Motivos: (1) 9.651 fraudes
  absolutos no es un problema de escasez de etiquetas; (2) SMOTE interpola en un espacio con
  categóricas de alta cardinalidad (`merchant`, 693 niveles) donde la distancia euclídea no
  significa nada; (3) ponderar no toca los datos, por lo que no puede filtrar validación;
  (4) resamplear distorsiona las probabilidades, y este proyecto necesita probabilidades
  calibradas para el cálculo de ahorro en dólares. La notebook mide, no sólo afirma: compara
  PR-AUC de validación entre no-tratar / ponderar / SMOTENC / undersampling antes de fijar la
  decisión.
- **Umbral por ahorro neto en dólares, congelado en validación.** `src/fraud/threshold.py`
  maximiza `Σ amt[TP] − FP_COST·|FP| − Σ amt[FN]` sobre la validación, nunca sobre el
  holdout — elegirlo ahí sería la misma fuga que tunear cualquier hiperparámetro contra el
  conjunto de evaluación final.

## Quick start 🚀

`data/` está en `.gitignore`: descargar el dataset Sparkov desde
[Kaggle](https://www.kaggle.com/datasets/kartik2112/fraud-detection) y colocar
`fraudTrain.csv`/`fraudTest.csv` en `data/raw/` antes de correr lo siguiente.

```bash
uv sync --all-extras
uv run ruff check src/ tests/ && uv run ruff format --check src/ tests/
uv run pytest -q

uv run fraud train --data data/raw/fraudTrain.csv --model xgboost --out models/
uv run fraud evaluate --model models/xgboost.joblib --data data/raw/fraudTest.csv
uv run fraud score transacciones.csv --model models/xgboost.joblib -o scores.csv
```

`uv run jupyter nbconvert --execute --inplace fraud-portfolio.ipynb` reproduce la
notebook desde cero.

## Diseño por módulo 📦

- `src/fraud/data.py` — carga, valida esquema, castea `cc_num` a string, ordena
  causalmente, y corta `fraudTrain.csv` en train/validación por fecha (`TemporalSplit`).
- `src/fraud/features.py` — `build_features`, pura y sin estado: features temporales,
  monto, velocidad causal por tarjeta, novedad de comercio/categoría, y `distance_km`.
- `src/fraud/modeling.py` — registro `MODEL_SPECS` (`dummy`, `logreg`, `lightgbm`,
  `xgboost`), con los dos últimos usando GPU (`device="cuda"`/`"gpu"`) y degradación
  elegante si los extras `[gpu]` no están instalados.
- `src/fraud/evaluation.py` — PR-AUC contra prevalencia, precision@k, matriz de confusión.
- `src/fraud/threshold.py` — barrido de umbral vectorizado, ahorro neto, análisis marginal
  y sensibilidad a `FP_COST`.
- `src/fraud/pipeline.py` — orquestador único: split → features → fit → calibración
  isotónica sobre validación → umbral congelado. `FraudModel` empaqueta las tres cosas
  juntas, porque un umbral sin su modelo no es una decisión reproducible.
- `src/fraud/cli.py` — subcomandos `train` / `score` / `evaluate`.
- `src/fraud/explainability.py` — SHAP sobre el modelo final, detrás del extra `[shap]`.

## Datos y limitaciones ⚠️

Dataset sintético (Sparkov), sin validación externa. Sin datos de dispositivo, sesión ni
canal de pago. `FP_COST` (costo de revisión manual, USD 4) es una estimación externa al
dataset — la sección de sensibilidad de la notebook muestra cuánto depende de ella la
recomendación. 983 tarjetas es una población chica. El modelo no tiene forma de manejar una
tarjeta sin historial (arranque en frío): ahí las features de velocidad valen cero por
construcción, no por un error del modelo.
