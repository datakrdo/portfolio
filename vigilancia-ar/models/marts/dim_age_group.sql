{{
  config(
    materialized='table'
  )
}}

-- Canonical age-group dimension, sourced from the `age_group_buckets` seed rather than
-- the source `age_group_id`/`age_group_desc`, which aren't stable across files (see
-- docs/data_profile.md and `int_dengue_zika_unified`). Two bucket names carry
-- "(rango de fuente)" — they preserve a genuine inconsistency in the source's own age
-- boundaries (15-24 and 45-65 overlapping with the more common 15-19/20-24 and 45-64/65+
-- splits) rather than silently merging them into a neighboring bucket.

select distinct
    age_group_name,
    age_group_sort_order
from {{ ref('age_group_buckets') }}
order by age_group_sort_order
