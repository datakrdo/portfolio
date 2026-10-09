from _db import MARTS, emit

emit(f"""
select
    c.province_indec_id,
    c.province_name,
    c.event_code,
    c.event_name,
    cast(c.year as int) as year,
    cast(c.epi_week as int) as epi_week,
    cast(w.week_start_date as string) as week_start_date,
    cast(c.case_count as double) as case_count,
    cast(c.population as double) as population,
    c.incidence_rate_per_100k,
    cast(c.n_years_in_corridor as int) as n_years_in_corridor,
    c.p25_case_count,
    c.p50_case_count,
    c.p75_case_count,
    c.corridor_zone
from {MARTS}.mart_endemic_corridor c
join {MARTS}.dim_epi_week w on w.year = c.year and w.epi_week = c.epi_week
""")
