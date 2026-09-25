-- Question: which provinces have a Dengue incidence rate that's statistically
-- significantly higher than the rest of the country -- not just numerically
-- higher, which can happen by chance in a small population? Ranking by raw
-- rate alone (as in a naive top-N) rewards small-population provinces with
-- noisy rates; a rate ratio with a confidence interval doesn't.
-- Answer: for the latest complete season, each province's case rate vs. the
-- rest of the country as a rate ratio (RR) with a 95% CI (log-Poisson
-- approximation), flagged significant when the CI excludes 1.
-- Uses `mart_endemic_corridor`'s province-level population (not
-- `mart_incidence_rates`' department-level one), because CABA only has a
-- population figure at the province level -- RENAPER publishes it as one
-- citywide row, so a department-level rollup silently drops CABA entirely
-- (see `int_province_population`). This is the only rate-based analysis
-- that includes CABA as a result.
-- Finding: for 2025, 4 of 21 provinces clear significance -- Santa Fe
-- (RR 17.2, CI 16.6-17.7), Córdoba (RR 3.2), Tucumán (RR 2.8), and La Pampa
-- (RR 1.6, the tightest call, CI 1.38-1.81). CABA now enters the ranking
-- instead of silently disappearing, but its rate is unremarkable (4.6/100k,
-- RR 0.12, CI 0.10-0.14) -- it's a low-Dengue-burden district this season,
-- now correctly shown as such instead of missing. Every other province's CI
-- includes 1: e.g. Formosa's point rate (34.4/100k) looks meaningfully above
-- national, but its case count is too small this season for that to be
-- distinguishable from noise (CI 0.84-1.10).

with latest_season as (

    -- same "most recent complete year" guard as elsewhere: fct_weekly_cases
    -- densifies the current, still-in-progress year to zero, so a naive
    -- max(year) would silently pick the wrong one.
    select max(year) as year
    from {{ ref('mart_endemic_corridor') }}
    where event_code = 'DENGUE' and year < year(current_date())

),

by_province as (

    select
        c.province_name,
        sum(c.case_count) as province_cases,
        max(c.population) as province_population
    from {{ ref('mart_endemic_corridor') }} c
    inner join latest_season ly on c.year = ly.year
    where c.event_code = 'DENGUE'
    group by c.province_name
    having max(c.population) is not null

),

national as (

    select sum(province_cases) as national_cases, sum(province_population) as national_population
    from by_province

),

-- rate ratio of province vs. everyone else (excluding itself, so a large
-- province can't be compared against a "rest of country" that's mostly itself)
rate_ratio as (

    select
        p.province_name,
        p.province_cases,
        p.province_population,
        round(1e5 * p.province_cases / nullif(p.province_population, 0), 1) as province_rate_per_100k,
        n.national_cases - p.province_cases as rest_of_country_cases,
        n.national_population - p.province_population as rest_of_country_population,
        (p.province_cases / p.province_population)
        / ((n.national_cases - p.province_cases) / nullif(n.national_population - p.province_population, 0))
            as rate_ratio
    from by_province p
    cross join national n

),

-- 95% CI on ln(RR): ln(RR) +/- 1.96 * sqrt(1/a + 1/b), a/b = case counts
-- (standard log-Poisson rate ratio approximation)
with_ci as (

    select
        *,
        1.96 * sqrt(1.0 / nullif(province_cases, 0) + 1.0 / nullif(rest_of_country_cases, 0)) as log_rr_margin,
        exp(
            ln(rate_ratio)
            - 1.96 * sqrt(1.0 / nullif(province_cases, 0) + 1.0 / nullif(rest_of_country_cases, 0))
        ) as rate_ratio_ci_low,
        exp(
            ln(rate_ratio)
            + 1.96 * sqrt(1.0 / nullif(province_cases, 0) + 1.0 / nullif(rest_of_country_cases, 0))
        ) as rate_ratio_ci_high
    from rate_ratio
    where province_cases > 0

)

select
    province_name,
    province_rate_per_100k,
    round(rate_ratio, 2) as rate_ratio_vs_rest_of_country,
    round(rate_ratio_ci_low, 2) as ci95_low,
    round(rate_ratio_ci_high, 2) as ci95_high,
    rate_ratio_ci_low > 1 as is_significantly_elevated
from with_ci
order by rate_ratio_vs_rest_of_country desc
