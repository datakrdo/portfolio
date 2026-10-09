---
title: Corredor endémico
sql:
  corridor: ./data/corridor.parquet
---

# Corredor endémico: ¿esta semana es normal para esta provincia?

<p class="lede">El corredor compara cada semana con las mismas semanas de los 5 años anteriores. Si los casos superan el percentil 75 de esos años, la semana entra en zona de <b>brote</b>. Es la herramienta que usa el Boletín Epidemiológico Nacional.</p>

```js
import {alert, bands, fmt, toDate} from "./components/theme.js";
const events = [...await sql`select distinct event_code, event_name from corridor order by 2`];
const provinces = [...await sql`select distinct province_name from corridor order by 1`].map((d) => d.province_name);
const years = [...await sql`select distinct year from corridor order by 1 desc`].map((d) => d.year);
```

<div class="controls">

```js
const event = view(Inputs.select(events, {label: "Evento", format: (d) => d.event_name, value: events.find((d) => d.event_code === "DENGUE") ?? events[0]}));
const province = view(Inputs.select(provinces, {label: "Provincia", value: provinces.includes("Córdoba") ? "Córdoba" : provinces[0]}));
const year = view(Inputs.select(years, {label: "Año", format: String}));
```

</div>

```js
const rows = [...await sql`
  select epi_week, week_start_date, case_count, p25_case_count as p25, p50_case_count as p50,
         p75_case_count as p75, corridor_zone as zone, n_years_in_corridor as n_years
  from corridor
  where event_code = ${event.event_code} and province_name = ${province} and year = ${year}
  order by epi_week`];
const withBase = rows.filter((d) => d.p75 != null);
const top = d3.max(rows, (d) => Math.max(d.case_count, d.p75 ?? 0)) * 1.1 || 1;
const c = bands(dark);
const brote = rows.filter((d) => d.zone === "Brote");
```

```js
display(rows.length === 0 ? html`<p>Sin datos para esta combinación.</p>` : Plot.plot({
  width,
  height: 420,
  x: {label: "Semana epidemiológica", ticks: 13},
  y: {label: "Casos por semana", grid: true, domain: [0, top]},
  marks: [
    Plot.areaY(withBase, {x: "epi_week", y1: 0, y2: "p25", fill: c["Éxito"]}),
    Plot.areaY(withBase, {x: "epi_week", y1: "p25", y2: "p50", fill: c["Seguridad"]}),
    Plot.areaY(withBase, {x: "epi_week", y1: "p50", y2: "p75", fill: c["Alerta"]}),
    Plot.areaY(withBase, {x: "epi_week", y1: "p75", y2: top, fill: alert(dark), fillOpacity: 0.14}),
    Plot.lineY(withBase, {x: "epi_week", y: "p50", stroke: "var(--ink-2)", strokeDasharray: "3,3", strokeWidth: 1}),
    Plot.lineY(rows, {x: "epi_week", y: "case_count", stroke: "var(--ink)", strokeWidth: 2}),
    Plot.dot(brote, {x: "epi_week", y: "case_count", fill: alert(dark), stroke: "var(--surface)", r: 4.5}),
    Plot.ruleY([0]),
    Plot.tip(rows, Plot.pointerX({x: "epi_week", y: "case_count", title: (d) =>
      `Semana ${d.epi_week} (${d.week_start_date})\n${fmt(d.case_count)} casos\n${d.zone ? `Zona: ${d.zone}` : "Sin años previos para comparar"}${d.p75 != null ? `\nMediana ${fmt(d.p50)} · p75 ${fmt(d.p75)}` : ""}`}))
  ]
}));
```

<p class="fineprint"><b>Cómo leerlo:</b> la línea sólida son los casos de ${year}. Las franjas azules son lo habitual para esa semana (hasta el percentil 25: <i>éxito</i>; hasta la mediana: <i>seguridad</i>; hasta el percentil 75: <i>alerta</i>). Por encima, la zona roja es <i>brote</i>, y los puntos rojos marcan las semanas en que los casos la atravesaron. Los primeros años del dataset (2018–2022) usan menos de 5 años de historia; sin ninguno, no hay corredor.</p>

<details><summary>Ver como tabla</summary>

```js
display(Inputs.table(rows, {columns: ["epi_week", "week_start_date", "case_count", "zone", "p25", "p50", "p75", "n_years"], header: {epi_week: "SE", week_start_date: "Inicio", case_count: "Casos", zone: "Zona", p25: "p25", p50: "p50", p75: "p75", n_years: "Años de base"}, format: {p25: (d) => fmt(d, 1), p50: (d) => fmt(d, 1), p75: (d) => fmt(d, 1)}}));
```

</details>
