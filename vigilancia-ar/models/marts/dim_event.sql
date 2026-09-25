{{
  config(
    materialized='table'
  )
}}

-- Small, clean value set straight from the source (no free-text inconsistency to
-- resolve here, unlike geography or age group) — an explicit `case` is enough, no seed.

select distinct
    event as event_name,
    case
        when event = 'Dengue' then 'DENGUE'
        when event = 'Dengue durante la gestación' then 'DENGUE_GESTATIONAL'
        when event = 'Enfermedad por Virus del Zika' then 'ZIKA'
    end as event_code
from {{ ref('int_dengue_zika_unified') }}
