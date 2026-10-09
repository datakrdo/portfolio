from _db import MARTS, emit

emit(f"""
select
    province_indec_id,
    province_name,
    event_code,
    event_name,
    cast(outbreak_start_date as string) as outbreak_start_date,
    cast(outbreak_end_date as string) as outbreak_end_date,
    cast(duration_weeks as int) as duration_weeks,
    cast(total_cases as double) as total_cases,
    cast(peak_case_count as double) as peak_case_count,
    cast(peak_week_start_date as string) as peak_week_start_date,
    attack_rate_per_100k,
    peak_incidence_rate_per_100k
from {MARTS}.mart_outbreaks
""")
