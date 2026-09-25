{{
  config(
    materialized='table'
  )
}}

-- Grain: event x department x age group x epi week x year x publication. Answers
-- "how much did a published count change after the fact" (late notification, per
-- plan.md) — most periods have exactly one publication and nothing to compare, but
-- the ones that were republished (2021, 2024, 2025 — see docs/data_profile.md) show
-- the real gap between a first-published and a final count.
--
-- Sourced from `int_dengue_zika_all_revisions`, which already carries every known
-- historical publication (one row per distinct raw file). `snap_dengue_zika_weekly`
-- (a `strategy=check` snapshot on the canonical unified view) extends this same
-- picture forward from today: as this dataset gets republished going forward, each
-- `dbt snapshot` run adds a new `published_date` here that no raw-file revision
-- could have captured on its own.

with all_revisions as (

    select
        department_indec_id,
        event,
        age_group_name,
        year,
        epi_week,
        published_date,
        case_count
    from {{ ref('int_dengue_zika_all_revisions') }}

),

from_snapshot as (

    select
        department_indec_id,
        event,
        age_group_name,
        year,
        epi_week,
        dbt_valid_from as published_date,
        case_count
    from {{ ref('snap_dengue_zika_weekly') }}

),

unioned as (

    select * from all_revisions
    union distinct
    select * from from_snapshot

),

with_revision_number as (

    select
        *,
        row_number() over (
            partition by department_indec_id, event, age_group_name, year, epi_week
            order by published_date
        ) as revision_number,
        count(*) over (
            partition by department_indec_id, event, age_group_name, year, epi_week
        ) as total_revisions,
        lag(case_count) over (
            partition by department_indec_id, event, age_group_name, year, epi_week
            order by published_date
        ) as prior_case_count
    from unioned

)

select
    department_indec_id,
    event,
    age_group_name,
    year,
    epi_week,
    published_date,
    revision_number,
    total_revisions,
    case_count,
    prior_case_count,
    case_count - prior_case_count as case_count_change_from_prior_revision,
    revision_number = total_revisions as is_latest_revision
from with_revision_number
