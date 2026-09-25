-- Question: does a rolling case-growth signal (3-week trailing log-linear
-- growth rate) actually precede "Brote" onset with enough lead time and few
-- enough false alarms to be worth wiring into an alert? A threshold nobody
-- can act on in time, or one that cries wolf, isn't worth deploying.
-- Answer: a single scorecard -- recall (% of outbreaks a signal preceded),
-- precision (% of signals confirmed by a real outbreak within 8 weeks), and
-- the lead-time distribution for the outbreaks a signal did catch.
-- Finding: both recall and precision land around 10% (37/355 outbreaks had a
-- preceding signal, 67/636 signals were followed by one) -- a single
-- province-level growth threshold is a weak, noisy predictor on its own. When
-- it does catch a real outbreak, though, the lead time is meaningful: a
-- median of 6 weeks (p10 of 2 weeks in the worst cases) is enough runway to
-- act on, which is why this is worth a "review" flag rather than either an
-- automated alert or a discarded idea.

with weekly as (

    select
        c.province_indec_id,
        c.event_code,
        c.case_count,
        w.week_start_date,
        row_number() over (partition by c.province_indec_id, c.event_code order by w.week_start_date) as week_seq
    from {{ ref('mart_endemic_corridor') }} c
    inner join {{ ref('dim_epi_week') }} w on w.year = c.year and w.epi_week = c.epi_week
    where c.event_code = 'DENGUE'

),

growth as (

    select
        *,
        regr_slope(ln(case_count + 1), week_seq) over (
            partition by province_indec_id, event_code
            order by week_seq
            rows between 2 preceding and current row
        ) as trailing_growth_slope
    from weekly

),

-- growth_slope >= 0.25/week ~ doubling every ~2.8 weeks; case_count >= 5 keeps
-- a jump from 1 to 3 cases from registering as "explosive" growth.
signals as (

    select province_indec_id, event_code, week_start_date
    from growth
    where trailing_growth_slope >= 0.25 and case_count >= 5

),

outbreaks as (

    select province_indec_id, event_code, outbreak_id, outbreak_start_date
    from {{ ref('mart_outbreaks') }}
    where event_code = 'DENGUE'

),

-- every (signal, outbreak) pair where the signal fired in the 8 weeks before
-- that outbreak's onset in the same province
signal_hits as (

    select s.province_indec_id, s.event_code, s.week_start_date, o.outbreak_id, o.outbreak_start_date
    from signals s
    inner join outbreaks o
        on
            s.province_indec_id = o.province_indec_id
            and s.event_code = o.event_code
            and s.week_start_date < o.outbreak_start_date
            and s.week_start_date >= date_sub(o.outbreak_start_date, 56)

),

-- earliest signal per outbreak = best-case lead time an always-on monitor
-- would have delivered for that episode
best_signal_per_outbreak as (

    select
        outbreak_id,
        max(datediff(outbreak_start_date, week_start_date)) / 7.0 as lead_time_weeks
    from signal_hits
    group by outbreak_id

)

select
    (select count(*) from outbreaks) as total_outbreaks,
    (select count(*) from best_signal_per_outbreak) as outbreaks_with_advance_signal,
    round(100.0 * (select count(*) from best_signal_per_outbreak) / (select count(*) from outbreaks), 1) as recall_pct,
    (select count(*) from signals) as total_growth_signals,
    (select count(distinct province_indec_id, event_code, week_start_date) from signal_hits) as signals_confirmed_by_outbreak,
    round(
        100.0 * (select count(distinct province_indec_id, event_code, week_start_date) from signal_hits)
        / (select count(*) from signals),
        1
    ) as precision_pct,
    (select round(percentile_cont(0.5) within group (order by lead_time_weeks), 1) from best_signal_per_outbreak)
        as median_lead_time_weeks,
    (select round(percentile_cont(0.1) within group (order by lead_time_weeks), 1) from best_signal_per_outbreak)
        as p10_lead_time_weeks_conservative
