{{
  config(
    materialized='table'
  )
}}

-- Outbreak episodes: consecutive runs of weeks in the "Brote" (outbreak) zone from
-- `mart_endemic_corridor`, per province x event — the classic gaps-and-islands
-- pattern. Weeks with no corridor (`corridor_zone is null`, no prior-year history
-- yet) are treated as non-outbreak weeks here: they can't be classified as an
-- outbreak without a baseline to compare against, so they're excluded from runs
-- rather than silently joined into whichever neighboring run they happen to touch.
--
-- The island id is the classic `row_number() - row_number()` trick: a stable id per
-- province+event that only changes when the outbreak flag flips, which turns
-- "find consecutive weeks" into a plain `group by`.

with corridor as (

    select
        c.province_indec_id,
        c.province_name,
        c.event_code,
        c.event_name,
        c.year,
        c.epi_week,
        c.case_count,
        c.corridor_zone,
        w.week_start_date,
        w.week_end_date,
        case when c.corridor_zone = 'Brote' then 1 else 0 end as is_outbreak_week
    from {{ ref('mart_endemic_corridor') }} c
    inner join {{ ref('dim_epi_week') }} w on w.year = c.year and w.epi_week = c.epi_week

),

islands as (

    select
        *,
        row_number()
            over (
                partition by province_indec_id, event_code
                order by week_start_date
            )
        -
        row_number() over (
            partition by province_indec_id, event_code, is_outbreak_week
            order by week_start_date
        ) as island_group

    from corridor

)

select
    province_indec_id,
    province_name,
    event_code,
    event_name,
    island_group as outbreak_id,
    min(week_start_date) as outbreak_start_date,
    max(week_end_date) as outbreak_end_date,
    count(*) as duration_weeks,
    sum(case_count) as total_cases,
    max(case_count) as peak_case_count,
    max_by(week_start_date, case_count) as peak_week_start_date
from islands
where is_outbreak_week = 1
group by 1, 2, 3, 4, 5
order by province_name, event_name, outbreak_start_date
