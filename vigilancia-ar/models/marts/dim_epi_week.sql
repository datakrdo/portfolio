{{
  config(
    materialized='table'
  )
}}

-- Epidemiological week calendar (PAHO/CDC convention, per plan.md): epi week 1 of a year
-- is the Sunday-Saturday week that contains the year's first Saturday. Years then run in
-- consecutive 7-day blocks until the next year's week 1 starts — giving 52 or 53 weeks
-- depending on where Dec 31 falls. Generated with `sequence()`/`explode()` rather than a
-- seed, since it's fully derived from the calendar, not source data — except that a
-- handful of source rows (2020, 2025) report an epi week 53 the calendar rule doesn't
-- produce for that year; the week count is widened to cover whatever the source reports
-- too, so `fct_weekly_cases` never silently drops those rows for lack of a calendar week.

with years as (

    select explode(sequence(
        (select min(year) from {{ ref('int_dengue_zika_unified') }}),
        (select max(year) from {{ ref('int_dengue_zika_unified') }})
    )) as year

),

year_bounds as (

    select
        year,
        date_add(next_day(make_date(year - 1, 12, 31), 'SAT'), -6) as week_1_start,
        date_add(next_day(make_date(year, 12, 31), 'SAT'), -6) as next_year_week_1_start
    from years

),

source_max_week as (

    select year, max(epi_week) as max_epi_week
    from {{ ref('int_dengue_zika_unified') }}
    group by 1

),

weeks_needed as (

    select
        b.year,
        b.week_1_start,
        greatest(
            cast(datediff(b.next_year_week_1_start, b.week_1_start) / 7 as int),
            coalesce(s.max_epi_week, 0)
        ) - 1 as max_week_offset
    from year_bounds b
    left join source_max_week s on b.year = s.year

),

weeks as (

    select
        year,
        week_1_start,
        explode(sequence(0, max_week_offset)) as week_offset
    from weeks_needed

)

select
    year,
    cast(week_offset + 1 as int) as epi_week,
    date_add(week_1_start, cast(week_offset * 7 as int)) as week_start_date,
    date_add(week_1_start, cast(week_offset * 7 as int) + 6) as week_end_date
from weeks
order by year, epi_week
