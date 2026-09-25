-- Question: for each Dengue season (epi week 31 through week 30 of the next
-- calendar year, per the national epidemiological bulletin convention) and
-- province, how early does the season build up -- the epi week where it hits
-- 50%/90% of its own final case count -- and how far over the endemic
-- corridor's alert ceiling (p75) did it run? Vector control has to be
-- scheduled *before* the 50% week, not after it.
-- Answer: per province x season, the epi weeks reaching 50%/90% cumulative
-- share and the total case-weeks spent above p75, top 5 seasons ranked by
-- excess **per 100k population** -- raw excess-over-corridor case counts
-- favor big provinces (Buenos Aires, Córdoba) just for being big, so the
-- ranking is by rate; the absolute case count stays as a column for sizing
-- the response.
-- Finding: ranked by rate, the 2023-24 season is still the clear outlier,
-- but the composition changes -- Tucumán (4,464 excess cases/100k) and La
-- Rioja (2,725/100k) now lead, ahead of higher-case, higher-population
-- Córdoba (2,572/100k), which had the largest raw excess (100,536 cases) but
-- ranks 3rd once its population is accounted for. Smaller provinces running
-- proportionally hotter (Formosa, Catamarca, both ~2,400-2,500/100k) get
-- seen instead of hidden behind Córdoba/Buenos Aires-scale case volumes.

with corridor_seasoned as (

    select
        province_name,
        event_code,
        event_name,
        case when epi_week >= 31 then year else year - 1 end as season_start_year,
        epi_week,
        case_count,
        population,
        p75_case_count
    from {{ ref('mart_endemic_corridor') }}
    where event_code = 'DENGUE'

),

season_totals as (

    select
        province_name,
        season_start_year,
        sum(case_count) as season_total_cases
    from corridor_seasoned
    group by province_name, season_start_year

),

cumulative as (

    select
        cs.*,
        st.season_total_cases,
        sum(cs.case_count) over (
            partition by cs.province_name, cs.season_start_year
            order by cs.epi_week
            rows between unbounded preceding and current row
        ) as cumulative_cases,
        greatest(cs.case_count - cs.p75_case_count, 0) as excess_over_p75,
        case
            when cs.population > 0
                then greatest(cs.case_count - cs.p75_case_count, 0) / cs.population * 100000
        end as excess_over_p75_per_100k
    from corridor_seasoned cs
    inner join season_totals st
        on cs.province_name = st.province_name and cs.season_start_year = st.season_start_year

),

with_share as (

    select
        *,
        try_divide(cumulative_cases, nullif(season_total_cases, 0)) as cumulative_share
    from cumulative

),

season_summary as (

    select
        province_name,
        season_start_year,
        max(season_total_cases) as season_total_cases,
        min(case when cumulative_share >= 0.5 then epi_week end) as epi_week_reaching_50pct,
        min(case when cumulative_share >= 0.9 then epi_week end) as epi_week_reaching_90pct,
        sum(excess_over_p75) as total_excess_cases_over_corridor,
        sum(excess_over_p75_per_100k) as total_excess_per_100k
    from with_share
    group by province_name, season_start_year

)

select
    province_name,
    season_start_year,
    concat(season_start_year, '-', season_start_year + 1) as season_label,
    season_total_cases,
    epi_week_reaching_50pct,
    epi_week_reaching_90pct,
    total_excess_cases_over_corridor,
    round(total_excess_per_100k, 1) as total_excess_per_100k
from season_summary
where season_total_cases > 0
qualify row_number() over (partition by season_start_year order by total_excess_per_100k desc) <= 5
order by season_start_year desc, total_excess_per_100k desc
