{{
  config(
    materialized='view'
  )
}}

-- Canonical grain: event x department x age group x epi week x year — one row per
-- period, no revision history. Several years were published more than once (2021 in
-- legacy, 2023-2026 in current — see docs/data_profile.md); every revision lives in
-- `int_dengue_zika_all_revisions`, but only the latest (`published_date desc`) is kept
-- here to avoid double-counting. `mart_revisions` is what uses the other revisions.

with all_revisions as (

    select * from {{ ref('int_dengue_zika_all_revisions') }}

),

canonical_file_per_year as (

    select year, _source_file
    from (
        select
            year,
            _source_file,
            row_number() over (
                partition by year order by published_date desc, _source_file desc
            ) as rn
        from all_revisions
        group by year, _source_file, published_date
    )
    where rn = 1

)

select r.*
from all_revisions r
inner join canonical_file_per_year c
    on r.year = c.year and r._source_file = c._source_file
