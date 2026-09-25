-- Question: how concentrated is Dengue across departments -- what share of
-- the 554 departments accounts for 80% of a season's cases, how unequal is
-- the distribution (Gini), and are the same departments the hotspots season
-- after season, or does the map reshuffle? Concentrated + persistent hotspots
-- justify standing local resources; concentrated + shifting ones don't.
-- Answer: per complete season, the % of departments needed to reach 80% of
-- cases, the Gini coefficient (raw case counts --
-- this dimensions response *volume*: beds, reagents scale with case volume
-- regardless of where people live), a population-weighted concentration
-- index (departments ordered by rate instead of raw cases, so it measures
-- *risk* concentration instead of population concentration), and the
-- Jaccard overlap of that season's top decile of departments -- by rate,
-- with a >= 20 case floor so a department of 4,000 people with 3 cases can't
-- rank above a real hotspot -- with the prior season's.
-- Departments without a department-level population (CABA's comunas --
-- RENAPER only publishes CABA citywide, see `mart_incidence_rates`) can't
-- get a rate and are excluded from the rate-based columns only; the raw
-- Pareto/Gini columns are unaffected.
-- Finding: Dengue in Argentina is extremely concentrated by raw case count
-- (Gini 0.85-0.98 every complete season) and *not* persistent -- top-decile
-- hotspot overlap between consecutive seasons is low even by rate (Jaccard
-- 0.0-0.14, worst in the biggest season: 2023-24 vs 2022-23 is only 0.14).
-- The population-weighted index is lower than the raw Gini every season --
-- part of the raw concentration is just where people live, not risk -- and
-- the gap is largest exactly in the biggest outbreak season (2023-24: 0.85
-- raw vs. 0.57 weighted), meaning most of that season's raw concentration
-- was population, not risk; smaller seasons stay closer together (e.g.
-- 2021-22: 0.99 raw vs. 0.94 weighted). Both findings justify a rapid,
-- flexible response capacity aimed wherever that season's risk concentrates,
-- rather than standing infrastructure permanently parked in last season's
-- hotspots.

with dept_season_totals as (

    select
        f.department_indec_id,
        case when f.epi_week >= 31 then f.year else f.year - 1 end as season_start_year,
        sum(f.case_count) as total_cases
    from {{ ref('fct_weekly_cases') }} f
    inner join {{ ref('dim_geography') }} g on g.department_indec_id = f.department_indec_id
    where f.event_code = 'DENGUE' and not g.is_unknown
    group by f.department_indec_id, case when f.epi_week >= 31 then f.year else f.year - 1 end
    having case when f.epi_week >= 31 then f.year else f.year - 1 end <= year(current_date()) - 1

),

-- department population per season: `mart_incidence_rates` already carries
-- one population value per department x year (matched to the closest
-- RENAPER cut); a season can span two calendar years, so `max()` picks the
-- more recent of the (at most two) year's cuts.
dept_population as (

    select
        department_indec_id,
        case when epi_week >= 31 then year else year - 1 end as season_start_year,
        max(population) as population
    from {{ ref('mart_incidence_rates') }}
    where event_code = 'DENGUE'
    group by 1, 2

),

with_population as (

    select d.*, p.population
    from dept_season_totals d
    left join dept_population p
        on p.department_indec_id = d.department_indec_id and p.season_start_year = d.season_start_year

),

ranked as (

    select
        *,
        row_number() over (partition by season_start_year order by total_cases desc) as rank_desc,
        row_number() over (partition by season_start_year order by total_cases asc) as rank_asc,
        count(*) over (partition by season_start_year) as n_depts,
        sum(total_cases) over (partition by season_start_year) as season_total_cases,
        ntile(10) over (partition by season_start_year order by total_cases desc) as decile_desc,
        case when population > 0 then total_cases / population * 100000 end as rate_per_100k
    from with_population

),

pareto as (

    select
        *,
        sum(total_cases) over (
            partition by season_start_year order by rank_desc
            rows between unbounded preceding and current row
        ) as cumulative_cases_desc
    from ranked

),

pareto_summary as (

    select
        season_start_year,
        max(n_depts) as n_depts,
        max(season_total_cases) as season_total_cases,
        min(case when try_divide(cumulative_cases_desc, season_total_cases) >= 0.8 then rank_desc end)
            as depts_needed_for_80pct_cases
    from pareto
    group by season_start_year

),

-- Gini over sorted case counts: (2*sum(rank_asc*x) / (n*sum(x))) - (n+1)/n
gini_summary as (

    select
        season_start_year,
        round(
            (2.0 * sum(rank_asc * total_cases) / (max(n_depts) * sum(total_cases)))
            - (max(n_depts) + 1.0) / max(n_depts),
            3
        ) as gini_coefficient
    from ranked
    group by season_start_year

),

-- population-weighted concentration index (Lorenz curve, departments ordered
-- by rate instead of by raw cases): trapezoid-rule Gini,
-- 1 - sum((X_i - X_i-1) * (Y_i + Y_i-1)), X = cumulative population share,
-- Y = cumulative case share. Departments with no rate (no population) are
-- excluded -- they can't be placed on the population axis.
lorenz_input as (

    select
        *,
        row_number() over (partition by season_start_year order by rate_per_100k) as rate_rank_asc
    from ranked
    where population > 0

),

lorenz_shares as (

    select
        *,
        sum(population) over (
            partition by season_start_year order by rate_rank_asc
            rows between unbounded preceding and current row
        ) / sum(population) over (partition by season_start_year) as cum_population_share,
        sum(total_cases) over (
            partition by season_start_year order by rate_rank_asc
            rows between unbounded preceding and current row
        ) / sum(total_cases) over (partition by season_start_year) as cum_case_share
    from lorenz_input

),

lorenz_with_lag as (

    select
        *,
        lag(cum_population_share, 1, 0) over (partition by season_start_year order by rate_rank_asc)
            as prior_population_share,
        lag(cum_case_share, 1, 0) over (partition by season_start_year order by rate_rank_asc)
            as prior_case_share
    from lorenz_shares

),

weighted_gini_summary as (

    select
        season_start_year,
        round(
            1 - sum(
                (cum_population_share - prior_population_share) * (cum_case_share + prior_case_share)
            ),
            2
        ) as population_weighted_gini_coefficient
    from lorenz_with_lag
    group by season_start_year

),

-- top decile by rate, not raw cases, so a handful of cases in a tiny
-- department can't outrank a real hotspot; a >= 20 case floor keeps a
-- near-zero-case, near-zero-population department out of the decile purely
-- on a divide-by-tiny-population fluke.
top_decile_rate_sets as (

    select season_start_year, collect_set(department_indec_id) as top_decile_depts
    from (
        select
            *,
            ntile(10) over (partition by season_start_year order by rate_per_100k desc) as rate_decile_desc
        from ranked
        where population > 0 and total_cases >= 20
    )
    where rate_decile_desc = 1
    group by season_start_year

),

persistence as (

    select
        season_start_year,
        top_decile_depts,
        lag(top_decile_depts) over (order by season_start_year) as prior_top_decile_depts
    from top_decile_rate_sets

),

jaccard as (

    select
        season_start_year,
        round(
            try_divide(
                size(array_intersect(top_decile_depts, prior_top_decile_depts)),
                size(array_union(top_decile_depts, prior_top_decile_depts))
            ),
            2
        ) as jaccard_similarity_to_prior_season
    from persistence

)

select
    ps.season_start_year,
    concat(ps.season_start_year, '-', ps.season_start_year + 1) as season_label,
    ps.n_depts,
    ps.season_total_cases,
    ps.depts_needed_for_80pct_cases,
    round(100.0 * ps.depts_needed_for_80pct_cases / ps.n_depts, 1) as pct_of_depts_for_80pct_cases,
    gs.gini_coefficient,
    wg.population_weighted_gini_coefficient,
    j.jaccard_similarity_to_prior_season
from pareto_summary ps
inner join gini_summary gs on ps.season_start_year = gs.season_start_year
left join weighted_gini_summary wg on ps.season_start_year = wg.season_start_year
left join jaccard j on ps.season_start_year = j.season_start_year
where ps.season_total_cases > 0
order by ps.season_start_year desc
