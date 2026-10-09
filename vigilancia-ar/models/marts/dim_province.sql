{{
  config(
    materialized='table'
  )
}}

-- One row per province (including the synthetic "Unknown" one), derived from
-- `dim_geography`. Shared slicer / RLS anchor in BI: province-grain marts
-- (`mart_endemic_corridor`, `mart_outbreaks`) join here directly, and
-- department-grain ones reach it through `dim_geography`.

select distinct
    province_indec_id,
    province_name
from {{ ref('dim_geography') }}
