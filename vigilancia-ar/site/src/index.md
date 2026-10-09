---
title: Situación actual
sql:
  departments: ./data/departments.parquet
  corridor: ./data/corridor.parquet
---

# Dengue y Zika en Argentina: dónde está el riesgo, no solo dónde hay más casos

<p class="lede">Casos notificados al SNVS 2.0 por departamento de residencia, con la tasa cada 100.000 habitantes. Una provincia grande siempre suma más casos; la tasa y el <a href="./corredor">corredor endémico</a> dicen si la situación es realmente anormal.</p>

```js
import {alert, rateRamp, fmt} from "./components/theme.js";
const events = [...await sql`select distinct event_code, event_name from departments order by 2`];
const years = [...await sql`select distinct year from departments order by 1 desc`].map((d) => d.year);
```

<div class="controls">

```js
const event = view(Inputs.select(events, {label: "Evento", format: (d) => d.event_name, value: events.find((d) => d.event_code === "DENGUE") ?? events[0]}));
const year = view(Inputs.select(years, {label: "Año", format: String}));
```

</div>

```js
const depts = [...await sql`
  select department_name, province_name, centroid_lat, centroid_lon, case_count, rate_per_100k, last_epi_week
  from departments where event_code = ${event.event_code} and year = ${year} and case_count > 0`];
const provinces = await FileAttachment("data/ar_provinces.geojson").json();
const prov = [...await sql`
  select province_name,
         cast(sum(case_count) as double) as cases,
         sum(case_count) / nullif(max(population), 0) * 100000 as rate
  from corridor c where event_code = ${event.event_code} and year = ${year} and population is not null
  group by province_name order by rate desc`];
const inBrote = [...await sql`
  select province_name from corridor c
  where event_code = ${event.event_code} and year = ${year} and corridor_zone = 'Brote'
    and epi_week = (select max(epi_week) from corridor c2 where c2.event_code = c.event_code and c2.year = c.year)
  order by 1`];
const lastWeek = depts.length ? depts[0].last_epi_week : null;
const total = d3.sum(depts, (d) => d.case_count);
```

<div class="kpis">
  <div class="kpi"><div class="n">${fmt(total)}</div><div class="l">casos notificados en ${year}${lastWeek ? ` (hasta la SE ${lastWeek})` : ""}</div></div>
  <div class="kpi"><div class="n">${fmt(depts.length)}</div><div class="l">departamentos con al menos un caso</div></div>
  <div class="kpi"><div class="n">${prov[0] ? prov[0].province_name : "—"}</div><div class="l">mayor tasa: ${prov[0] ? fmt(prov[0].rate, 1) : "—"} casos cada 100.000 hab.</div></div>
  <div class="kpi alert"><div class="n">${fmt(inBrote.length)}</div><div class="l">provincias en zona de brote la última semana${inBrote.length ? `: ${inBrote.map((d) => d.province_name).join(", ")}` : ""}</div></div>
</div>

## Mapa por departamento

El tamaño del círculo es la cantidad de casos; el color, la tasa cada 100.000 habitantes. Los círculos grises no tienen población de referencia (las comunas de CABA solo se publican como ciudad completa).

```js
display(Plot.plot({
  width: Math.min(width, 640),
  height: 840,
  projection: {type: "mercator", domain: provinces, inset: 12},
  r: {range: [1.5, 24]},
  color: {type: "sqrt", range: rateRamp(dark), unknown: "#8d8c86", legend: true, label: "Casos cada 100.000 hab."},
  marks: [
    Plot.geo(provinces, {fill: "var(--land)", stroke: "var(--line)"}),
    Plot.dot(depts, {
      x: "centroid_lon", y: "centroid_lat", r: "case_count", fill: "rate_per_100k",
      fillOpacity: 0.8, stroke: "var(--surface)", strokeWidth: 0.6, sort: {channel: "-r"},
      channels: {Departamento: "department_name", Provincia: "province_name"},
      tip: {format: {r: (d) => `${fmt(d)} casos`, fill: (d) => d == null ? "sin población" : `${fmt(d, 1)} cada 100.000`, x: false, y: false}}
    })
  ]
}));
```

## Tasa por provincia

```js
display(Plot.plot({
  marginLeft: 150,
  height: 24 * prov.length + 40,
  x: {label: "Casos cada 100.000 habitantes", grid: true},
  y: {label: null, domain: prov.map((d) => d.province_name)},
  marks: [
    Plot.barX(prov, {y: "province_name", x: "rate", fill: "var(--accent)", tip: {format: {x: (d) => fmt(d, 1)}}}),
    Plot.ruleX([0])
  ]
}));
```

<p class="fineprint">Las notificaciones se corrigen con el tiempo: las semanas más recientes suelen estar subestimadas (ver <a href="./hallazgos#retraso-de-notificacion">retraso de notificación</a>). Datos por residencia del caso, no por lugar de contagio.</p>
