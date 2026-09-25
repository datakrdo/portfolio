{{
  config(
    materialized='table'
  )
}}

-- "Corredor endémico" (endemic corridor): the standard tool Argentina's Boletín
-- Epidemiológico Nacional uses to flag outbreak weeks, per plan.md. Grain: event x
-- province x epi week x target year. Province, not department, because 5 years of
-- weekly counts at department grain would be too sparse for a stable percentile in
-- most departments (most weeks would be 0 cases).
--
-- For each (province, event, epi week), the 25th/50th/75th percentiles are computed
-- over that week's case counts in the **5 years before** the target year (a trailing
-- window, not a fixed calendar range, so the corridor itself shifts forward each
-- year) — years with fewer than 5 prior years available (2018-2022) still get a
-- corridor, just built from whatever years exist before them; `n_years_in_corridor`
-- makes that visible rather than hiding it.
--
-- Zone classification follows the conventional 4-band split: at/below p25 = "Éxito"
-- (better than usual), p25-p50 = "Seguridad" (normal/safety), p50-p75 = "Alerta"
-- (above normal, watch), above p75 = "Brote" (outbreak zone) — the actual signal
-- `mart_outbreaks` acts on.

with weekly_by_province as (

    select
        g.province_indec_id,
        g.province_name,
        f.event_code,
        f.event_name,
        f.year,
        f.epi_week,
        sum(f.case_count) as case_count
    from {{ ref('fct_weekly_cases') }} f
    inner join {{ ref('dim_geography') }} g on g.department_indec_id = f.department_indec_id
    where not g.is_unknown
    group by 1, 2, 3, 4, 5, 6

),

target_years as (

    select distinct year as target_year from weekly_by_province

),

corridor_history as (

    select
        w.province_indec_id,
        w.province_name,
        w.event_code,
        w.event_name,
        w.epi_week,
        t.target_year,
        w.case_count
    from weekly_by_province w
    inner join target_years t
        on w.year between t.target_year - 5 and t.target_year - 1

),

corridor_thresholds as (

    select
        province_indec_id,
        province_name,
        event_code,
        event_name,
        epi_week,
        target_year,
        count(*) as n_years_in_corridor,
        percentile_cont(0.25) within group (order by case_count) as p25_case_count,
        percentile_cont(0.5) within group (order by case_count) as p50_case_count,
        percentile_cont(0.75) within group (order by case_count) as p75_case_count
    from corridor_history
    group by 1, 2, 3, 4, 5, 6

)

select
    w.province_indec_id,
    w.province_name,
    w.event_code,
    w.event_name,
    w.year,
    w.epi_week,
    w.case_count,
    t.n_years_in_corridor,
    t.p25_case_count,
    t.p50_case_count,
    t.p75_case_count,
    case
        when t.n_years_in_corridor is null or t.n_years_in_corridor = 0 then null
        when w.case_count <= t.p25_case_count then 'Éxito'
        when w.case_count <= t.p50_case_count then 'Seguridad'
        when w.case_count <= t.p75_case_count then 'Alerta'
        else 'Brote'
    end as corridor_zone
from weekly_by_province w
left join corridor_thresholds t
    on
        t.province_indec_id = w.province_indec_id
        and t.event_code = w.event_code
        and t.epi_week = w.epi_week
        and t.target_year = w.year
