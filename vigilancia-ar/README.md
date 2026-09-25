🇬🇧 [English](README.md) · 🇪🇸 [Español](README_es.md)

# vigilancia-ar 🦟

Analytics engineering on Dengue and Zika epidemiological surveillance in Argentina
(SNVS 2.0, Ministry of Health), built with **dbt-databricks** on Databricks Free
Edition (Unity Catalog, serverless SQL warehouse). No dashboard — the focus is the
SQL layer.

Source: [datos.salud.gob.ar/dataset/vigilancia-de-dengue-y-zika](https://datos.salud.gob.ar/dataset/vigilancia-de-dengue-y-zika)
(CC-BY 4.0). Full data profile, its two schema "eras" and the quality issues found:
[`docs/data_profile.md`](docs/data_profile.md).

## Architecture (medallion) 🏗️

```
seeds/                  georef_departments, age_group_buckets (geo/age-group crosswalks)
sources (Volume CSVs) ──read_files()──► staging/
  stg_dengue_zika__legacy       (2018-2022, XLS/CSV, old schema)
  stg_dengue_zika__current      (2023+, new schema)
  stg_renaper_population        (INDEC population projections)
        │
        ▼
intermediate/
  int_dengue_zika_all_revisions  every publication of every period (mart_revisions' grain)
  int_dengue_zika_unified        one canonical row per period (latest revision)
  int_renaper_population
        │
        ▼
snapshots/
  snap_dengue_zika_weekly        weekly SCD2 over int_dengue_zika_unified
        │
        ▼
marts/
  dim_geography, dim_age_group, dim_event, dim_epi_week
  fct_weekly_cases        event x department x age group x epi week x year (densified)
  mart_incidence_rates    rate per 100k, cumulative-within-year, year-over-year comparison
  mart_endemic_corridor   endemic corridor (25th/50th/75th percentiles of the 5 prior years)
  mart_outbreaks          outbreak episodes (gaps & islands over the corridor)
  mart_revisions          how much an already-published figure changes on later revisions
```

Every mart (`dim_*`, `fct_*`, `mart_*`) has `contract: enforced: true`.

## Running it 🚀

```bash
uv sync
cp profiles.yml.example ~/.dbt/profiles.yml   # fill in host/http_path, log in via `databricks auth login`
dbt deps
dbt build
```

Lint (Databricks dialect, dbt templater):

```bash
sqlfluff lint models/ analyses/
```

CI (`.github/workflows/vigilancia-ar-ci.yml`): on PR it runs `sqlfluff lint` + `dbt
parse`; on push to `main` it runs a full `dbt build` against Databricks (token auth via
repo secrets).

## Findings (`analyses/`) 🔍

- [`season_timing_and_severity.sql`](analyses/season_timing_and_severity.sql) —
  per province and Dengue season (epi week 31 → week 30), the week reaching 50%/90% of
  that season's cases and total excess over the endemic corridor's p75 ceiling, ranked
  by excess **per 100k population** so big provinces don't top the list just for being
  big. In 2023-24, Tucumán (4,464 excess cases/100k) and La Rioja (2,725/100k) rank
  above Córdoba (2,572/100k), even though Córdoba's raw excess (100,536 cases) was the
  largest in the dataset.
- [`geographic_spread_of_season.sql`](analyses/geographic_spread_of_season.sql) —
  tests whether each season spreads geographically from a north-to-south gradient
  (haversine distance, `regr_slope`/`regr_r2` of outbreak-onset week vs. latitude).
  Finding: it doesn't hold up (R² < 0.15 in every full season) — a useful negative result.
- [`outbreak_early_warning_lead_time.sql`](analyses/outbreak_early_warning_lead_time.sql) —
  scores a rolling case-growth threshold as an early-warning signal for outbreak onset.
  Both recall and precision land around 10%, but the outbreaks it does catch get a
  median 6-week lead time — worth a review flag, not a stand-alone alert.
- [`hotspot_concentration_and_persistence.sql`](analyses/hotspot_concentration_and_persistence.sql) —
  Pareto/Gini concentration of cases across departments, a population-weighted Gini
  (Lorenz curve by rate, not raw cases) to separate risk concentration from where people
  live, plus season-to-season Jaccard overlap of the top decile by rate. Dengue is
  extremely concentrated by raw count (Gini 0.85-0.98) but *not* persistent (Jaccard
  0.0-0.14) — the hot spots move. The population-weighted Gini runs lower every season
  (e.g. 2023-24: 0.85 raw vs. 0.57 weighted), showing part of the raw concentration is
  just population, not risk.
- [`provinces_with_significant_excess_rate.sql`](analyses/provinces_with_significant_excess_rate.sql) —
  rate ratio vs. the rest of the country with a 95% log-Poisson CI, to separate a real
  excess from a small-population fluke. Uses province-level population so CABA (whose
  population RENAPER only publishes citywide) isn't silently dropped. Only 4 of 21
  provinces clear significance for 2025.
- [`reporting_delay_nowcast_factor.sql`](analyses/reporting_delay_nowcast_factor.sql) —
  correction factor (latest known count / first published count) by reporting-lag bucket,
  for the small subset of periods that do get revised after publication.

## Status ✅

`dbt build` green (models + 76 tests + snapshot), `sqlfluff lint` clean, CI green.
