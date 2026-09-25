{{
  config(
    materialized='view'
  )
}}

-- Same population, aggregated one level up: province x publish_date, for marts that
-- only need a province-level denominator (`mart_endemic_corridor`, `mart_outbreaks`).
-- Every real department id already starts with its 2-digit province code (INDEC
-- convention, verified: no department's id disagrees with its Georef province), and
-- `int_renaper_population`'s synthetic "Unknown department" ids follow the same
-- convention (`{province}999`), so the province is always just the first 2 characters
-- of the department id -- no join needed. This is exactly how CABA's population ends
-- up here: RENAPER publishes CABA as one citywide row that never matches an
-- individual Georef comuna (see `int_renaper_population`), so it lands on `02999` and
-- would otherwise silently disappear from a province total that only counted real,
-- matched departments. The fully-unknown `99999` bucket has no real province and is
-- excluded.

select
    left(department_indec_id, 2) as province_indec_id,
    publish_date,
    sum(population) as population
from {{ ref('int_renaper_population') }}
where department_indec_id <> '99999'
group by left(department_indec_id, 2), publish_date
