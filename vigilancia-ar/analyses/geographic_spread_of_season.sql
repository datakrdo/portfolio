-- Question: does each Dengue season spread geographically from a northern
-- focus southward, and how fast (weeks per degree of latitude)? If the spread
-- is measurable and consistent, southern provinces get an evidence-based
-- lead time before their own season starts.
-- Answer: per season, each province's first "Brote" epi week vs. its distance
-- (haversine, km) from that season's earliest-affected province, plus the
-- fitted weeks-per-degree-latitude slope and its R^2.
-- Finding: the naive "spreads north to south" hypothesis doesn't hold up --
-- fit is weak (R^2 < 0.15) in every full season (2022-23, 2023-24), so a
-- latitude-based early-warning rule for southern provinces isn't supported by
-- this data; the only high-R^2 season (2025-26, R^2=0.93) has just 4
-- provinces so far and shouldn't be read as a trend. Useful negative result:
-- outbreak timing is driven by something other than straight-line latitude
-- (temperature, vector presence, urban density), not distance from the north.

with province_centroid as (

    -- Antártida Argentina (dept 94028) is an uninhabited claim whose centroid
    -- (~lat -82.8) drags Tierra del Fuego's average far past any real town;
    -- excluded so the province centroid reflects where people actually are.
    select
        province_name,
        avg(centroid_lat) as province_lat,
        avg(centroid_lon) as province_lon
    from {{ ref('dim_geography') }}
    where not is_unknown and department_name <> 'Antártida Argentina'
    group by province_name

),

first_outbreak_week as (

    select
        province_name,
        case when epi_week >= 31 then year else year - 1 end as season_start_year,
        min(epi_week) as first_brote_epi_week
    from {{ ref('mart_endemic_corridor') }}
    where event_code = 'DENGUE' and corridor_zone = 'Brote'
    group by province_name, case when epi_week >= 31 then year else year - 1 end

),

-- One origin province per season, tie-broken deterministically by name so a
-- multi-way tie for earliest week can't fan the join out below.
season_origin as (

    select season_start_year, province_name as origin_province_name, first_brote_epi_week as origin_epi_week
    from first_outbreak_week
    qualify row_number() over (
        partition by season_start_year
        order by first_brote_epi_week, province_name
    ) = 1

),

spread as (

    select
        f.province_name,
        f.season_start_year,
        f.first_brote_epi_week,
        f.first_brote_epi_week - o.origin_epi_week as weeks_after_season_origin,
        pc.province_lat,
        -- haversine distance (km) from this province's centroid to the centroid
        -- of the earliest-affected province that same season
        6371 * acos(least(1.0, greatest(
            -1.0,
            sin(radians(pc.province_lat)) * sin(radians(origin_pc.province_lat))
            + cos(radians(pc.province_lat)) * cos(radians(origin_pc.province_lat))
            * cos(radians(pc.province_lon - origin_pc.province_lon))
        ))) as km_from_season_origin
    from first_outbreak_week f
    inner join season_origin o on f.season_start_year = o.season_start_year
    inner join province_centroid pc on f.province_name = pc.province_name
    inner join province_centroid origin_pc on o.origin_province_name = origin_pc.province_name

)

select
    season_start_year,
    concat(season_start_year, '-', season_start_year + 1) as season_label,
    count(*) as provinces_reaching_outbreak,
    round(regr_slope(weeks_after_season_origin, province_lat), 2) as weeks_per_degree_latitude,
    round(regr_r2(weeks_after_season_origin, province_lat), 2) as fit_r2,
    round(regr_slope(weeks_after_season_origin, km_from_season_origin) * 100, 2) as weeks_per_100km
from spread
group by season_start_year
having count(*) >= 3
order by season_start_year desc
