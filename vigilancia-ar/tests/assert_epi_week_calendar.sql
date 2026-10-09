-- Every epi week starts on a Sunday (PAHO/CDC), and consecutive weeks of a year are
-- exactly 7 days apart. Returns the weeks that break either rule.
select
    year,
    epi_week,
    week_start_date
from (
    select
        year,
        epi_week,
        week_start_date,
        datediff(
            week_start_date,
            lag(week_start_date) over (partition by year order by epi_week)
        ) as days_since_prev_week
    from {{ ref('dim_epi_week') }}
)
where dayofweek(week_start_date) <> 1 or coalesce(days_since_prev_week, 7) <> 7
