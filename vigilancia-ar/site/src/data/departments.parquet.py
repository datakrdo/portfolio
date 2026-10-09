from _db import MARTS, emit

emit(f"""
select
    g.department_indec_id,
    g.department_name,
    g.province_indec_id,
    g.province_name,
    g.centroid_lat,
    g.centroid_lon,
    r.event_code,
    r.event_name,
    cast(r.year as int) as year,
    cast(max(r.epi_week) as int) as last_epi_week,
    cast(sum(r.case_count) as double) as case_count,
    cast(max(r.population) as double) as population,
    try_divide(sum(r.case_count), max(r.population)) * 100000 as rate_per_100k
from {MARTS}.mart_incidence_rates r
join {MARTS}.dim_geography g on g.department_indec_id = r.department_indec_id
where not g.is_unknown
group by all
""")
