🇬🇧 [English](README.md) · 🇪🇸 [Español](README_es.md)

# vigilancia-ar 🦟

Analytics engineering on Dengue and Zika epidemiological surveillance in Argentina
(SNVS 2.0, Ministry of Health), built with **dbt-databricks** on Databricks Free
Edition (Unity Catalog, serverless SQL warehouse). Built for public-health teams: the
SQL layer feeds a public site, a Power BI model and weekly self-updating data.

Source: [datos.salud.gob.ar/dataset/vigilancia-de-dengue-y-zika](https://datos.salud.gob.ar/dataset/vigilancia-de-dengue-y-zika)
(CC-BY 4.0). Full data profile, its two schema "eras" and the quality issues found:
[`docs/data_profile.md`](docs/data_profile.md).

## See it 👀

- **[Public site](https://datakrdo.github.io/portfolio/)** (Observable Framework, Spanish): cases and rate by department, interactive endemic corridor per province, outbreaks, and the six findings below in plain language.
- **[dbt docs](https://datakrdo.github.io/portfolio/dbt/)**: full lineage, column docs and tests.
- **Power BI** composite model (DirectQuery detail + imported aggregation) with row- and object-level security: [`docs/powerbi.md`](docs/powerbi.md), project in [`powerbi/`](powerbi/).

Both sites rebuild by themselves when the source publishes new data (see *Automatic updates*).

## Read this before using the numbers ⚠️

- Cases are assigned to the department of **residence**, not where the infection happened.
- The most recent weeks are **incomplete** (late notification); see the nowcast finding.
- CABA's population is only published citywide, so its comunas have no rate.
- The public site shows only department/province aggregates, never age breakdowns (small counts could identify people).

## Architecture (medallion) 🏗️

```
seeds/                  georef_departments, age_group_buckets (crosswalks), rls_province_access (demo users for RLS)
sources (Volume CSVs) ──read_files()──► staging/
  stg_dengue_zika__legacy       (2018-2022, XLS/CSV, old schema)
  stg_dengue_zika__current      (2023+, new schema)
  stg_renaper_population        (RENAPER identified population by department)
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
  dim_geography, dim_province, dim_age_group, dim_event, dim_epi_week, dim_user_access
  fct_weekly_cases        event x department x age group x epi week x year (densified)
  mart_incidence_rates    rate per 100k, cumulative-within-year, year-over-year comparison
  mart_endemic_corridor   endemic corridor (25th/50th/75th percentiles of the 5 prior years)
  mart_outbreaks          outbreak episodes (gaps & islands over the corridor)
  mart_revisions          how much an already-published figure changes on later revisions
```

Every mart (`dim_*`, `fct_*`, `mart_*`) has `contract: enforced: true`.

## Running it 🚀

```bash
uv sync --group ingest
cp profiles.yml.example ~/.dbt/profiles.yml   # fill in host/http_path, log in via `databricks auth login`
uv run python scripts/fetch_data.py --upload /Volumes/workspace/bronze/raw_data   # download + sync to the Volume
dbt deps
dbt build        # models, seeds, snapshot, tests and unit tests
```

The Volume path is the `raw_volume_path` var in `dbt_project.yml`.

Lint (Databricks dialect, dbt templater):

```bash
sqlfluff lint models/ analyses/
```

## Automatic updates 🔄

The source publishes irregularly (from a week to several months apart), so CI
(`.github/workflows/vigilancia-ar-ci.yml`) checks **every Monday**: `scripts/fetch_data.py`
downloads the files, uploads only the new or changed ones to the Volume, and only then runs
`dbt build` (which also records revisions in the snapshot), regenerates the dbt docs and
redeploys the site to GitHub Pages. Nothing new → nothing rebuilt. On pull requests it runs
`sqlfluff lint` and `dbt parse` with a dummy profile (no secrets). Needs the repo secrets
`DATABRICKS_HOST`, `DATABRICKS_HTTP_PATH`, `DATABRICKS_TOKEN`.

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

## Data and licenses

Code: MIT ([`LICENSE`](LICENSE)). Data: Ministry of Health (SNVS 2.0) and RENAPER, CC-BY 4.0; province
outlines from Natural Earth (public domain).
