"""Streamlit demo: rule-out estimate of advanced PBC stage (III-IV) from routine labs.

Run from the project root: `uv run streamlit run app/streamlit_app.py`.
"""

from __future__ import annotations

from pathlib import Path

import streamlit as st

from pbc.predict import predict_patient

MODEL_DIR = Path(__file__).resolve().parents[1] / "models"

TEXT = {
    "en": {
        "title": "PBC advanced-stage rule-out (demo)",
        "disclaimer": (
            "Educational portfolio demo trained on the 1974-84 Mayo Clinic cohort (single "
            "centre, no external validation). **Not for clinical use.**"
        ),
        "basic": "Routine data",
        "age": "Age (years)",
        "sex": "Sex",
        "edema": "Edema",
        "edema_opts": {"N": "None", "S": "Controlled / no diuretics", "Y": "Despite diuretics"},
        "bilirubin": "Bilirubin (mg/dL)",
        "albumin": "Albumin (g/dL)",
        "platelets": "Platelets (10^9/L)",
        "prothrombin": "Prothrombin time (s)",
        "extended": "I also have the extended panel",
        "ascites": "Ascites",
        "hepatomegaly": "Hepatomegaly",
        "spiders": "Spider angiomas",
        "yn": {"N": "No", "Y": "Yes"},
        "cholesterol": "Cholesterol (mg/dL)",
        "copper": "Urine copper (ug/day)",
        "alk_phos": "Alkaline phosphatase (U/L)",
        "sgot": "SGOT / AST (U/L)",
        "trig": "Triglycerides (mg/dL)",
        "go": "Estimate",
        "model": "Model used",
        "model_names": {"core": "core (routine data)", "full": "full (extended panel)"},
        "prob": "Estimated probability of stage 3-4",
        "ruled_out": "Below the rule-out threshold (90% sensitivity). Caution: in validation roughly "
        "half of the patients below it still had advanced disease (NPV about 0.5), so this does NOT "
        "support skipping a biopsy.",
        "not_excluded": "Advanced stage NOT ruled out: this tool does not support skipping "
        "further work-up.",
        "compare": "Reference clinical scores",
    },
    "es": {
        "title": "Descarte de estadio avanzado en PBC (demo)",
        "disclaimer": (
            "Demo educativa de portfolio entrenada con la cohorte Mayo Clinic 1974-84 (un solo "
            "centro, sin validación externa). **No apta para uso clínico.**"
        ),
        "basic": "Datos de rutina",
        "age": "Edad (años)",
        "sex": "Sexo",
        "edema": "Edema",
        "edema_opts": {
            "N": "Ausente",
            "S": "Controlado / sin diuréticos",
            "Y": "A pesar de diuréticos",
        },
        "bilirubin": "Bilirrubina (mg/dL)",
        "albumin": "Albúmina (g/dL)",
        "platelets": "Plaquetas (10^9/L)",
        "prothrombin": "Tiempo de protrombina (s)",
        "extended": "También tengo el panel extendido",
        "ascites": "Ascitis",
        "hepatomegaly": "Hepatomegalia",
        "spiders": "Arañas vasculares",
        "yn": {"N": "No", "Y": "Sí"},
        "cholesterol": "Colesterol (mg/dL)",
        "copper": "Cobre urinario (ug/día)",
        "alk_phos": "Fosfatasa alcalina (U/L)",
        "sgot": "SGOT / AST (U/L)",
        "trig": "Triglicéridos (mg/dL)",
        "go": "Estimar",
        "model": "Modelo usado",
        "model_names": {"core": "core (datos de rutina)", "full": "full (panel extendido)"},
        "prob": "Probabilidad estimada de estadio 3-4",
        "ruled_out": "Por debajo del umbral de descarte (90% de sensibilidad). Cuidado: en la validación "
        "aproximadamente la mitad de los pacientes por debajo igual tenía enfermedad avanzada (VPN cercano a "
        "0,5), así que esto NO respalda omitir una biopsia.",
        "not_excluded": "Estadio avanzado NO descartado: esta herramienta no respalda omitir "
        "más estudios.",
        "compare": "Scores clínicos de referencia",
    },
}

language = st.sidebar.radio("Language / Idioma", ["en", "es"], format_func=str.upper)
t = TEXT[language]
st.title(t["title"])
st.warning(t["disclaimer"])

st.subheader(t["basic"])
left, right = st.columns(2)
patient = {
    "age_years": left.number_input(t["age"], 18, 100, 55),
    "Sex": right.selectbox(t["sex"], ["F", "M"]),
    "Edema": left.selectbox(t["edema"], list(t["edema_opts"]), format_func=t["edema_opts"].get),
    "Bilirubin": right.number_input(t["bilirubin"], 0.1, 30.0, 1.4, step=0.1),
    "Albumin": left.number_input(t["albumin"], 1.0, 5.5, 3.5, step=0.1),
    "Platelets": right.number_input(t["platelets"], 20, 800, 250),
    "Prothrombin": left.number_input(t["prothrombin"], 8.0, 20.0, 10.6, step=0.1),
}
if st.checkbox(t["extended"]):
    a, b = st.columns(2)
    yn = list(t["yn"])
    patient |= {
        "Ascites": a.selectbox(t["ascites"], yn, format_func=t["yn"].get),
        "Hepatomegaly": b.selectbox(t["hepatomegaly"], yn, format_func=t["yn"].get),
        "Spiders": a.selectbox(t["spiders"], yn, format_func=t["yn"].get),
        "Cholesterol": b.number_input(t["cholesterol"], 50, 2000, 300),
        "Copper": a.number_input(t["copper"], 1, 700, 73),
        "Alk_Phos": b.number_input(t["alk_phos"], 100, 15000, 1500),
        "SGOT": a.number_input(t["sgot"], 10.0, 500.0, 110.0),
        "Tryglicerides": b.number_input(t["trig"], 20, 700, 120),
    }

if st.button(t["go"], type="primary"):
    result = predict_patient(patient, MODEL_DIR)
    st.metric(t["prob"], f"{result['probability_advanced']:.0%}")
    st.caption(f"{t['model']}: {t['model_names'][result['model_used']]}")
    if result["decision"] == "advanced_ruled_out":
        st.warning(t["ruled_out"])
    else:
        st.error(t["not_excluded"])
    st.subheader(t["compare"])
    st.write({name: round(result[name], 2) for name in ("mayo", "apri") if name in result})
