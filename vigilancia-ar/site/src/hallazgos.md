---
title: Hallazgos
sql:
  timing: ./data/find_season_timing.parquet
  spread: ./data/find_spread.parquet
  hotspots: ./data/find_hotspots.parquet
  warning: ./data/find_early_warning.parquet
  provinces: ./data/find_provinces.parquet
  nowcast: ./data/find_nowcast.parquet
---

# Seis preguntas que los datos permiten responder

<p class="lede">Cada hallazgo parte de una pregunta concreta de vigilancia, está calculado en SQL sobre las mismas tablas que alimentan el resto del sitio, y termina en qué conviene hacer con la respuesta. Algunos resultados son negativos, y por eso útiles.</p>

```js
import {alert, fmt} from "./components/theme.js";
const num = (rows) => rows.map((d) => Object.fromEntries(Object.entries(d).map(([k, v]) => [k, typeof v === "bigint" ? Number(v) : v])));
```

<div class="finding" id="exceso-sobre-el-corredor">

## 1. ¿Qué provincias sufrieron más, una vez descontado su tamaño?

<p class="so-what">Ordenar por casos totales premia a las provincias grandes. Ordenadas por exceso sobre el techo del corredor cada 100.000 habitantes, el ranking cambia.</p>

</div>

```js
const timing = num([...await sql`select * from timing order by season_start_year desc, total_excess_per_100k desc`]);
const seasons = [...new Set(timing.map((d) => d.season_label))];
const season = view(Inputs.select(seasons, {label: "Temporada (SE 31 a SE 30)"}));
```

```js
const t = timing.filter((d) => d.season_label === season);
display(Plot.plot({
  marginLeft: 130, height: 40 * t.length + 40,
  x: {label: "Casos en exceso sobre el percentil 75, cada 100.000 hab.", grid: true},
  y: {label: null, domain: t.map((d) => d.province_name)},
  marks: [
    Plot.barX(t, {y: "province_name", x: "total_excess_per_100k", fill: "var(--accent)",
      title: (d) => `${d.province_name}\nExceso: ${fmt(d.total_excess_cases_over_corridor)} casos (${fmt(d.total_excess_per_100k)} cada 100.000)\nLlegó al 50% de los casos en la SE ${d.epi_week_reaching_50pct} y al 90% en la SE ${d.epi_week_reaching_90pct}`, tip: true}),
    Plot.ruleX([0])
  ]
}));
```

<div class="finding" id="gradiente-norte-sur">

## 2. ¿La temporada avanza de norte a sur?

<p class="so-what">No de forma confiable: el ajuste por latitud explica poco. No hay evidencia para una regla de alerta basada solo en la distancia al norte.</p>

</div>

```js
const spread = num([...await sql`select * from spread order by season_start_year`]);
display(Plot.plot({
  height: 260, marginBottom: 50,
  x: {label: "Temporada", tickRotate: -20},
  y: {label: "R² del ajuste semanas vs. latitud", domain: [0, 1], grid: true},
  marks: [
    Plot.barY(spread, {x: "season_label", y: "fit_r2", fill: "var(--accent)",
      title: (d) => `${d.season_label}\nR² = ${d.fit_r2} con ${d.provinces_reaching_outbreak} provincias en brote\n${d.weeks_per_degree_latitude} semanas por grado de latitud`, tip: true}),
    Plot.ruleY([0.15], {stroke: alert(dark), strokeDasharray: "4,3"}),
    Plot.text([{x: spread[0]?.season_label, y: 0.15}], {x: "x", y: "y", text: ["umbral 0,15"], dy: -8, textAnchor: "start", fill: alert(dark)}),
    Plot.ruleY([0])
  ]
}));
```

<p class="fineprint">Las temporadas con pocas provincias en brote (aún en curso) pueden dar R² altos por casualidad; el tooltip muestra cuántas son.</p>

<div class="finding" id="alerta-temprana">

## 3. ¿Una señal de crecimiento sirve como alerta de brote?

<p class="so-what">Sola, no: detecta pocos brotes y da muchas falsas alarmas. Pero cuando acierta, avisa con semanas de anticipación; sirve como señal para revisar, no como alarma automática.</p>

</div>

```js
const w = num([...await sql`select * from warning`])[0] ?? {};
```

<div class="kpis">
  <div class="kpi"><div class="n">${fmt(w.recall_pct, 1)}%</div><div class="l">de los brotes (${fmt(w.outbreaks_with_advance_signal)} de ${fmt(w.total_outbreaks)}) tuvo una señal previa</div></div>
  <div class="kpi"><div class="n">${fmt(w.precision_pct, 1)}%</div><div class="l">de las señales (${fmt(w.signals_confirmed_by_outbreak)} de ${fmt(w.total_growth_signals)}) fue seguida por un brote</div></div>
  <div class="kpi"><div class="n">${fmt(w.median_lead_time_weeks, 1)} sem.</div><div class="l">de anticipación mediana cuando la señal acierta</div></div>
  <div class="kpi"><div class="n">${fmt(w.p10_lead_time_weeks_conservative, 1)} sem.</div><div class="l">de anticipación en el caso conservador (percentil 10)</div></div>
</div>

<div class="finding" id="concentracion-y-persistencia">

## 4. ¿Los focos se concentran y son los mismos cada año?

<p class="so-what">Los casos se concentran en pocos departamentos, pero los focos cambian de una temporada a la siguiente: no alcanza con reforzar siempre los mismos lugares.</p>

</div>

```js
const hot = num([...await sql`select * from hotspots order by season_start_year`]);
display(Plot.plot({
  height: 300, marginBottom: 50,
  x: {label: "Temporada", tickRotate: -20, domain: hot.map((d) => d.season_label)},
  y: {label: "Índice (0 a 1)", domain: [0, 1], grid: true},
  color: {legend: true, domain: ["Gini de casos", "Gini ponderado por población", "Superposición con la temporada previa (Jaccard)"], range: ["#2a78d6", "#eb6834", "#1baf7a"]},
  marks: [
    Plot.line(hot, {x: "season_label", y: "gini_coefficient", stroke: () => "Gini de casos", strokeWidth: 2}),
    Plot.line(hot, {x: "season_label", y: "population_weighted_gini_coefficient", stroke: () => "Gini ponderado por población", strokeWidth: 2}),
    Plot.line(hot.filter((d) => d.jaccard_similarity_to_prior_season != null), {x: "season_label", y: "jaccard_similarity_to_prior_season", stroke: () => "Superposición con la temporada previa (Jaccard)", strokeWidth: 2}),
    Plot.dot(hot, {x: "season_label", y: "gini_coefficient", fill: "#2a78d6"}),
    Plot.dot(hot, {x: "season_label", y: "population_weighted_gini_coefficient", fill: "#eb6834"}),
    Plot.dot(hot.filter((d) => d.jaccard_similarity_to_prior_season != null), {x: "season_label", y: "jaccard_similarity_to_prior_season", fill: "#1baf7a"}),
    Plot.ruleY([0])
  ]
}));
```

<p class="fineprint">El Gini ponderado es menor que el de casos: parte de la concentración aparente es solo dónde vive la gente, no mayor riesgo.</p>

<div class="finding" id="exceso-significativo">

## 5. ¿Qué provincias tienen una tasa realmente más alta, y no solo ruido?

<p class="so-what">Una provincia pequeña puede tener una tasa alta por azar. La razón de tasas con su intervalo de confianza al 95% separa un exceso real (el intervalo no cruza 1) de una fluctuación.</p>

</div>

```js
const prov = num([...await sql`select * from provinces order by rate_ratio_vs_rest_of_country desc`]);
display(Plot.plot({
  marginLeft: 150, height: 26 * prov.length + 50,
  x: {type: "log", label: "Razón de tasas vs. el resto del país (escala logarítmica)", grid: true, ticks: [0.25, 0.5, 1, 2, 4, 8, 16, 32], tickFormat: (d) => `×${d}`},
  y: {label: null, domain: prov.map((d) => d.province_name)},
  color: {domain: [true, false], range: [alert(dark), "#8d8c86"]},
  marks: [
    Plot.ruleX([1], {stroke: "var(--ink)"}),
    Plot.ruleY(prov, {y: "province_name", x1: "ci95_low", x2: "ci95_high", stroke: "is_significantly_elevated", strokeWidth: 2}),
    Plot.dot(prov, {y: "province_name", x: "rate_ratio_vs_rest_of_country", fill: "is_significantly_elevated", r: 4,
      title: (d) => `${d.province_name}\nRazón de tasas ${fmt(d.rate_ratio_vs_rest_of_country, 2)} (IC 95%: ${fmt(d.ci95_low, 2)}–${fmt(d.ci95_high, 2)})\n${fmt(d.province_rate_per_100k, 1)} casos cada 100.000`, tip: true})
  ]
}));
```

<p class="fineprint">Rojo: exceso estadísticamente significativo. Gris: indistinguible del resto del país.</p>

<div class="finding" id="retraso-de-notificacion">

## 6. ¿Cuánto hay que corregir los números de la última semana?

<p class="so-what">Casi nada en general: pocos períodos se revisan después de publicarse. Pero cuando se revisan con demora, 1 de cada 10 llega a duplicarse; el ajuste solo importa para esa minoría.</p>

</div>

```js
const nc = num([...await sql`select * from nowcast order by avg_reporting_lag_days`]);
display(Plot.plot({
  height: 280,
  x: {label: "Demora entre el fin de la semana y la primera publicación", domain: nc.map((d) => d.reporting_lag_bucket)},
  y: {label: "Factor de corrección (conteo final / primer conteo)", grid: true, domain: [1, d3.max(nc, (d) => d.p90_correction_factor) * 1.1]},
  marks: [
    Plot.barY(nc, {x: "reporting_lag_bucket", y1: 1, y2: "p90_correction_factor", fill: "var(--accent)", fillOpacity: 0.35,
      title: (d) => `${d.reporting_lag_bucket}: ${fmt(d.n_periods)} períodos revisados\nMediana ×${d.median_correction_factor} · percentil 90 ×${d.p90_correction_factor}`, tip: true}),
    Plot.barY(nc, {x: "reporting_lag_bucket", y1: 1, y2: "median_correction_factor", fill: "var(--accent)"}),
    Plot.ruleY([1])
  ]
}));
```

<p class="fineprint">Barra sólida: mediana. Barra clara: percentil 90.</p>
