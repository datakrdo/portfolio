🇬🇧 [English](MODEL_CARD.md) · 🇪🇸 [Español](MODEL_CARD_es.md)

# Model card — Descarte de estadio avanzado en PBC (`pbc-modeling`)

## Uso previsto

Demostración educativa/de portfolio de un modelo de *estadificación no invasiva*: estimar la probabilidad de que un paciente con
cirrosis biliar primaria (PBC) tenga **Estadio 3–4** histológico (frente a Estadio 1–2) a partir de variables clínicas y de laboratorio
de rutina, y marcar a los pacientes por debajo de un umbral de descarte (sensibilidad ≥ 90%).

**Fuera de alcance — no usar para:** decisiones clínicas, decidir si realizar u omitir una biopsia hepática, diagnóstico, pronóstico o
tratamiento. El modelo no reemplaza la biopsia ni la evaluación clínica.

## Modelo

- **Estimador:** `logistic` (regresión logística con regularización ridge; `C` y una expansión opcional con splines ajustadas con Optuna
  sobre log-loss). Elegido *antes* de ver los resultados por parsimonia a este tamaño muestral, calibración y explicabilidad — no porque
  haya quedado primero. Sin `class_weight`, así que las probabilidades conservan la prevalencia de la cohorte (73% avanzados).
- **Preprocesamiento:** imputación por mediana, `log1p` en laboratorios asimétricos, estandarización, `Edema` ordinal (N < S < Y), one-hot
  para las demás categóricas. Se ajusta dentro del pipeline; nada se aprende de filas retenidas.
- **Umbral operativo:** elegido por validación cruzada sobre las filas de entrenamiento (`TunedThresholdClassifierCV`) para maximizar la
  especificidad sujeta a sensibilidad ≥ 90%. Umbrales desplegados: **0,579 (`core`)**, **0,542 (`full`)**.
- **Dos modelos, seleccionados automáticamente por `pbc.predict.predict_patient`:**
  - `core` — edad, sexo, edema, bilirrubina, albúmina, plaquetas, protrombina. Ajustado con los 412 pacientes etiquetados.
  - `full` — `core` más ascitis, hepatomegalia, arañas vasculares, colesterol, cobre, fosfatasa alcalina, SGOT, triglicéridos. Ajustado solo
    con los 312 pacientes del ensayo aleatorizado (los únicos con ese panel). Se usa solo si se proporcionan todas esas variables.
- **Entradas que nunca se usan:** `Drug`, `trial_cohort`, ID del paciente, tiempo de seguimiento y estado vital (`Drug`/`trial_cohort` son proxies
  de cómo se reclutó la cohorte y no existen para un paciente nuevo; el seguimiento y el estado codifican el desenlace).

## Datos de entrenamiento

Cohorte del ensayo de PBC de Mayo Clinic, 1974–1984 (418 pacientes; 412 con Stage etiquetado; 312 aleatorizados a D-penicilamina/placebo y 100
pacientes de registro etiquetados sin el panel de laboratorio extendido). Un solo centro, histórica, prevalencia de Estadio 3–4 = 73%.

## Desempeño (CV anidada repetida, 5 × 5, todas las filas; ver `README_es.md` para la tabla completa)

| Conjunto | AUROC [IC 95%] | Especificidad con ~90% de sensibilidad | VPN | Proporción bajo el umbral | Pendiente / intercepto de calibración |
| --- | --- | --- | --- | --- | --- |
| `core` (n=412) | 0,67 [0,57, 0,77] | 0,23 | 0,55 | 12% | 0,89 / 0,09 |
| `full` (n=312) | 0,73 [0,63, 0,81] | 0,23 | 0,52 | 13% | 0,80 / 0,16 |

Comparadores publicados sobre los mismos folds: AUROC del score de riesgo de Mayo 0,69 (`core`) y 0,71 (`full`); APRI 0,67 (`full`). El modelo
**no** es mejor que el score de Mayo, y los intervalos se superponen por completo.

## Limitaciones y riesgos

- **El descarte no funciona.** Con 73% de prevalencia y ~90% de sensibilidad, el valor predictivo negativo es de solo ~0,5: cerca de la mitad de
  los pacientes por debajo del umbral tiene enfermedad avanzada. Una salida "descartado" nunca debe usarse para omitir una biopsia.
- Sin validación externa ni temporal; una cohorte de un solo centro de 1974–1984. El espectro, los ensayos y la era de tratamiento difieren de la práctica actual.
- Muestra pequeña: `core` cumple el tamaño muestral mínimo de Riley et al. (306), `full` no (359 necesarios, 312 disponibles; eventos por variable ≈ 5).
- El comparador de Mayo se derivó en esta cohorte (para supervivencia) y el APRI es un proxy fuera de indicación; la comparación favorece a los scores.
- El estadio es una etiqueta histológica con desacuerdo interobservador conocido; el modelo hereda ese ruido de etiqueta.
- El sexo es un predictor; la cohorte es ~90% mujeres, así que las estimaciones para pacientes varones son especialmente inciertas.

## Cómo usarlo

```bash
uv run pbc train                          # (re)ajusta y guarda models/pbc_{core,full}.joblib + metadata.json
uv run pbc predict --input paciente.json  # {"age_years": 55, "Sex": "F", "Edema": "N", "Bilirubin": 1.4, ...}
uv run streamlit run app/streamlit_app.py
```

Las métricas son reproducibles: `uv run pbc pipeline --nested-cv --shap` (semilla 41) regenera `outputs/metrics.json`.
