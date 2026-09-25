{{
  config(
    materialized='table'
  )
}}

-- Canonical department dimension, sourced from the Georef API (datos.gob.ar) seed
-- (`georef_departments`). Adds two kinds of synthetic member so every source row has
-- somewhere to join, even when residence isn't fully identified (see
-- docs/data_profile.md, "Rate denominator and canonical geography"):
--   - one "Unknown department" per real province (province known, department not)
--   - one fully "Unknown" member (neither province nor department known)
-- The `province_indec_id || '999'` / `'99999'` ids are deliberately chosen to match
-- what the current-era source already uses for unidentified residence
-- (`id_depto_indec_residencia = '999'`, `id_prov_indec_residencia = '99'`), so
-- `int_dengue_zika_unified`'s name-based geography match falls back to the same ids
-- for unmatched province/department names, no special-casing needed there.

with real_departments as (

    select
        department_indec_id,
        department_name,
        province_indec_id,
        province_name,
        cast(centroid_lat as double) as centroid_lat,
        cast(centroid_lon as double) as centroid_lon,
        false as is_unknown
    from {{ ref('georef_departments') }}

),

unknown_per_province as (

    select distinct
        province_indec_id || '999' as department_indec_id,
        'Unknown' as department_name,
        province_indec_id,
        province_name,
        cast(null as double) as centroid_lat,
        cast(null as double) as centroid_lon,
        true as is_unknown
    from {{ ref('georef_departments') }}

),

fully_unknown as (

    select
        '99999' as department_indec_id,
        'Unknown' as department_name,
        '99' as province_indec_id,
        'Unknown' as province_name,
        cast(null as double) as centroid_lat,
        cast(null as double) as centroid_lon,
        true as is_unknown

)

select * from real_departments
union all
select * from unknown_per_province
union all
select * from fully_unknown
