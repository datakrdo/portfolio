// Colors from the validated reference palette (blue sequential ramp, red alert).
export const alert = (dark) => (dark ? "#e66767" : "#e34948");
export const rateRamp = (dark) => (dark ? ["#184f95", "#9ec5f4"] : ["#9ec5f4", "#104281"]);
export const bands = (dark) =>
  dark
    ? {"Éxito": "#222a36", "Seguridad": "#2b3a52", "Alerta": "#35507a"}
    : {"Éxito": "#e8f0fa", "Seguridad": "#cde2fb", "Alerta": "#9ec5f4"};
export const fmt = (n, d = 0) =>
  n == null ? "—" : Number(n).toLocaleString("es-AR", {maximumFractionDigits: d});
export const toDate = (s) => new Date(`${s}T00:00:00`);
