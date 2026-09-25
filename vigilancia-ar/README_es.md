🇬🇧 [English](README.md) · 🇪🇸 [Español](README_es.md)

# vigilancia-ar

Analytics engineering sobre vigilancia epidemiológica de Dengue y Zika en Argentina
(SNVS 2.0, Ministerio de Salud), con **dbt-databricks** sobre Databricks Free Edition
(Unity Catalog, SQL warehouse serverless). Sin dashboard: el foco es la capa SQL.

Fuente: [datos.salud.gob.ar/dataset/vigilancia-de-dengue-y-zika](https://datos.salud.gob.ar/dataset/vigilancia-de-dengue-y-zika)
(CC-BY 4.0). Perfil completo de los datos, sus dos "eras" de esquema y los problemas
de calidad encontrados: [`docs/data_profile.md`](docs/data_profile.md).

## Arquitectura (medallion)

```
seeds/                  georef_departments, age_group_buckets (crosswalks geo/etario)
sources (Volume CSVs) ──read_files()──► staging/
  stg_dengue_zika__legacy       (2018-2022, XLS/CSV, esquema viejo)
  stg_dengue_zika__current      (2023+, esquema nuevo)
  stg_renaper_population        (proyecciones de población INDEC)
        │
        ▼
intermediate/
  int_dengue_zika_all_revisions  toda publicación de cada período (grano de mart_revisions)
  int_dengue_zika_unified        una fila canónica por período (última revisión)
  int_renaper_population
        │
        ▼
snapshots/
  snap_dengue_zika_weekly        SCD2 semanal sobre int_dengue_zika_unified
        │
        ▼
marts/
  dim_geography, dim_age_group, dim_event, dim_epi_week
  fct_weekly_cases        evento x depto x grupo etario x semana epi x año (densificado)
  mart_incidence_rates    tasas por 100k, acumulado anual, comparación interanual
  mart_endemic_corridor   corredor endémico (percentiles 25/50/75 de los 5 años previos)
  mart_outbreaks          episodios de brote (gaps & islands sobre el corredor)
  mart_revisions          cuánto cambia un dato ya publicado en revisiones sucesivas
```

Todos los marts (`dim_*`, `fct_*`, `mart_*`) tienen `contract: enforced: true`.

## Cómo correrlo

```bash
uv sync
cp profiles.yml.example ~/.dbt/profiles.yml   # completar host/http_path, login con `databricks auth login`
dbt deps
dbt build
```

Lint (dialecto Databricks, plantilla dbt):

```bash
sqlfluff lint models/ analyses/
```

CI (`.github/workflows/vigilancia-ar-ci.yml`): en PR corre `sqlfluff lint` + `dbt parse`;
en push a `main` corre un `dbt build` completo contra Databricks (auth por token, vía
secrets del repo).

## Hallazgos (`analyses/`)

- [`season_timing_and_severity.sql`](analyses/season_timing_and_severity.sql) —
  por provincia y temporada de Dengue (SE 31 → SE 30), la semana en la que se alcanza
  el 50%/90% de los casos de esa temporada y el exceso total sobre el techo p75 del
  corredor endémico, rankeado por exceso **cada 100 mil habitantes** para que una
  provincia grande no encabece la lista solo por tamaño. En 2023-24, Tucumán (4.464
  casos de exceso/100k) y La Rioja (2.725/100k) quedan por encima de Córdoba
  (2.572/100k), aunque el exceso crudo de Córdoba (100.536 casos) fue el más grande
  del dataset.
- [`geographic_spread_of_season.sql`](analyses/geographic_spread_of_season.sql) —
  evalúa si cada temporada se propaga geográficamente de norte a sur (distancia
  haversine, `regr_slope`/`regr_r2` de la semana de entrada en brote vs. latitud).
  Hallazgo: no se sostiene (R² < 0,15 en todas las temporadas completas) — un
  resultado negativo útil.
- [`outbreak_early_warning_lead_time.sql`](analyses/outbreak_early_warning_lead_time.sql) —
  evalúa un umbral de crecimiento de casos como señal de alerta temprana de brote.
  Recall y precisión rondan el 10%, pero los brotes que sí detecta dan una anticipación
  mediana de 6 semanas — vale como señal a revisar, no como alerta autónoma.
- [`hotspot_concentration_and_persistence.sql`](analyses/hotspot_concentration_and_persistence.sql) —
  concentración de casos por departamento (Pareto/Gini), un Gini ponderado por
  población (curva de Lorenz por tasa, no por casos crudos) para separar concentración
  de riesgo de dónde vive la gente, y superposición Jaccard del decil superior por tasa
  entre temporadas consecutivas. El Dengue está muy concentrado por casos crudos
  (Gini 0,85-0,98) pero *no* es persistente (Jaccard 0,0-0,14) — los focos se mueven. El
  Gini ponderado por población es más bajo todas las temporadas (ej. 2023-24: 0,85 crudo
  vs. 0,57 ponderado), lo que muestra que parte de la concentración cruda es solo
  población, no riesgo.
- [`provinces_with_significant_excess_rate.sql`](analyses/provinces_with_significant_excess_rate.sql) —
  rate ratio contra el resto del país con IC 95% log-Poisson, para separar un exceso
  real de un artefacto de población chica. Usa población a nivel provincia para que
  CABA (cuya población RENAPER solo publica a nivel ciudad) no quede afuera del
  ranking. Solo 4 de 21 provincias son significativas en 2025.
- [`reporting_delay_nowcast_factor.sql`](analyses/reporting_delay_nowcast_factor.sql) —
  factor de corrección (último valor conocido / primer valor publicado) por bucket de
  demora de notificación, para el pequeño subconjunto de períodos que sí se revisan
  después de publicados.

## Estado

`dbt build` verde (modelos + 76 tests + snapshot), `sqlfluff lint` sin errores, CI en
verde.
