-- Question: for a week that was just published, how much should its case
-- count be corrected upward to estimate what it'll eventually settle at, as a
-- function of how long ago it was first published? A bulletin written from
-- the freshest data needs a nowcast factor, not a face-value read.
-- Answer: for every period, the reporting lag (days from that epi week's end
-- to its first publication) bucketed, with the median and p90 correction
-- factor (latest known count / first published count) observed in each
-- bucket.
-- Finding: dataset-wide, only 1,369 periods out of ~145k ever changed after
-- first publication (see mart_revisions) -- most of those (1,090) took 90+
-- days to get revised, with a median correction of just 1.02x but a p90 of
-- 2.0x, i.e. the typical revised week barely moves, but 1 in 10 doubles. A
-- nowcast adjustment only matters for this small, long-tail subset -- not a
-- blanket rule to apply to every freshly published week.

with first_publication as (

    select department_indec_id, event, age_group_name, year, epi_week, published_date, case_count as first_published_case_count
    from {{ ref('mart_revisions') }}
    where revision_number = 1

),

latest_publication as (

    select department_indec_id, event, age_group_name, year, epi_week, case_count as latest_case_count
    from {{ ref('mart_revisions') }}
    where is_latest_revision

),

combined as (

    select
        f.first_published_case_count,
        l.latest_case_count,
        datediff(f.published_date, w.week_end_date) as reporting_lag_days
    from first_publication f
    inner join latest_publication l
        on
            f.department_indec_id = l.department_indec_id
            and f.event = l.event
            and f.age_group_name = l.age_group_name
            and f.year = l.year
            and f.epi_week = l.epi_week
    inner join {{ ref('dim_epi_week') }} w on w.year = f.year and w.epi_week = f.epi_week
    where f.first_published_case_count is not null and f.first_published_case_count <> l.latest_case_count

),

bucketed as (

    select
        *,
        case
            when reporting_lag_days <= 7 then '0-7 days'
            when reporting_lag_days <= 30 then '8-30 days'
            when reporting_lag_days <= 90 then '31-90 days'
            else '90+ days'
        end as reporting_lag_bucket,
        try_divide(latest_case_count, nullif(first_published_case_count, 0)) as nowcast_correction_factor
    from combined

)

select
    reporting_lag_bucket,
    count(*) as n_periods,
    round(avg(reporting_lag_days), 0) as avg_reporting_lag_days,
    round(percentile_cont(0.5) within group (order by nowcast_correction_factor), 2) as median_correction_factor,
    round(percentile_cont(0.9) within group (order by nowcast_correction_factor), 2) as p90_correction_factor
from bucketed
group by reporting_lag_bucket
order by avg_reporting_lag_days
