{% snapshot snap_dengue_zika_weekly %}

{{
    config(
        target_schema='snapshots',
        unique_key=['department_indec_id', 'event', 'age_group_name', 'year', 'epi_week'],
        strategy='check',
        check_cols=['case_count'],
    )
}}

-- Captures published case counts as they stand at each `dbt snapshot` run. This
-- dataset keeps getting revised after publication (see docs/data_profile.md,
-- 2024 was republished 3 times with different counts) — running this on a schedule
-- is how that revision history gets recorded going forward, without needing to keep
-- every raw file forever. `int_dengue_zika_all_revisions` already carries the known
-- historical revisions (from distinct raw files); this snapshot is what extends that
-- picture from today onward. `mart_revisions` reads from both.

    select
        department_indec_id,
        event,
        age_group_name,
        year,
        epi_week,
        case_count
    from {{ ref('int_dengue_zika_unified') }}

{% endsnapshot %}
