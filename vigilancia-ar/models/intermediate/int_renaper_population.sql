{{
  config(
    materialized='view'
  )
}}

-- Resolves `department_indec_id` for each RENAPER population cut, same way
-- `int_dengue_zika_unified` does: name match against Georef, not the source's own
-- department code (unreliable across sources, per docs/data_profile.md).
--
-- CABA is published as a single citywide row here ("Ciudad Autónoma de Buenos
-- Aires"), not broken down into its 15 comunas the way Georef (and the dengue/zika
-- current-era source) is. That citywide row doesn't name-match any individual
-- Georef department, so it falls through to the per-province "Unknown department"
-- synthetic id (`province_indec_id || '999'`, same fallback `int_dengue_zika_unified`
-- uses) — meaning CABA's population is attached to that synthetic member, not to any
-- of its real comuna ids. `mart_incidence_rates` therefore can't compute a
-- comuna-level rate for CABA (no population to divide by there); this is a real gap
-- in the source, not a bug, and is left as a null rate rather than guessed at.

with population as (

    select * from {{ ref('stg_renaper_population') }}

),

georef as (

    select distinct
        department_indec_id,
        {{ normalize_geo_name('department_name') }} as department_name_norm,
        province_indec_id,
        {{ normalize_geo_name('province_name') }} as province_name_norm
    from {{ ref('georef_departments') }}

),

georef_provinces as (

    select distinct province_indec_id, province_name_norm
    from georef

),

matched as (

    select
        p.publish_date,
        p.population,
        dept_match.department_indec_id as matched_department_id,
        prov_match.province_indec_id as matched_province_id
    from population p
    left join georef_provinces prov_match
        on (
            {{ normalize_geo_name('p.province_name') }} = prov_match.province_name_norm
            or prov_match.province_name_norm like concat({{ normalize_geo_name('p.province_name') }}, '%')
            or ({{ normalize_geo_name('p.province_name') }} = 'CABA' and prov_match.province_name_norm like 'CIUDAD AUTONOMA%')
        )
    left join georef dept_match
        on
            dept_match.province_indec_id = prov_match.province_indec_id
            and dept_match.department_name_norm = {{ normalize_geo_name('p.department_name') }}

)

select
    publish_date,
    case
        when matched_department_id is not null then matched_department_id
        when matched_province_id is not null then concat(matched_province_id, '999')
        else '99999'
    end as department_indec_id,
    sum(population) as population
from matched
group by 1, 2
