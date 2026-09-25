{{
  config(
    materialized='view'
  )
}}

-- Every published revision of the dengue/zika dataset, fully resolved (geography,
-- age group), at grain: event x department x age group x epi week x year x
-- published_date. `int_dengue_zika_unified` picks the single canonical (latest)
-- revision per year from this model; `mart_revisions` uses every revision, since
-- that's the whole point of it — see plan.md invariant 7.
--
-- Both eras are matched by normalized department + province name against the Georef
-- seed, not by composite code. The current era's `id_depto_indec_residencia` looks like
-- an INDEC code but ISN'T Georef's own department code (e.g. source "015" for CABA
-- "Comuna 15" vs Georef's real "02105" for the same comuna, confirmed by inspection) —
-- composite-code matching silently produced wrong department assignments here, so name
-- matching is used uniformly for both eras instead. Unmatched department names fall
-- back to "Unknown department in this province"; unmatched province names fall back to
-- fully "Unknown" (see docs/data_profile.md).
--
-- `age_group_id` isn't a stable key across source files either (same id maps to
-- different age buckets in different years), so the canonical `age_group_name` is
-- resolved here too, via the `age_group_buckets` seed keyed on normalized free-text
-- description rather than the id.
--
-- The current era's filenames end in a `YYYY-MM-DD` publish-date suffix, used as
-- `published_date` directly; legacy has no such uniform suffix, so its known
-- duplicates are dated explicitly by filename/hash instead:
--   - 2021: two files, the newer one (known hash) dated after the older.
--   - 2018: also two files, discovered while building this model (not previously
--     documented) — `vigilancia-de-dengue-y-zika-201812.*` is the original
--     old-style filename (this dataset's earliest naming convention), and
--     `informacion-publica-dengue-zika-nacional-hasta-20181231.csv` matches the
--     naming convention of the later `hasta-<date>` series used for 2019-2021 —
--     i.e. a historical backfill published together with those, not a same-day
--     original. Dated accordingly: original file first, `hasta-*` backfill later.
--
-- Grouped and summed at the end on the *resolved* key (department_indec_id,
-- age_group_name), not the raw one: distinct raw `age_group_desc` strings that
-- resolve to the same canonical bucket (encoding/wording variants, see
-- docs/data_profile.md) would otherwise surface as separate rows for the same
-- period/publication — `mart_revisions` needs exactly one row per period per
-- publication to compare revisions meaningfully, or its revision-over-revision diff
-- ends up comparing unrelated raw rows instead of the same real-world count.
--
-- One raw row (2018 legacy file, Los Andes/Salta, Dengue, age 10-14, epi week 11) has a
-- genuinely null `cantidad_casos` in the source CSV — coalesced to 0 here rather than
-- dropped, consistent with how the rest of the pipeline treats unreported counts.

with legacy as (

    select
        'legacy' as schema_era,
        department_id,
        department_name,
        province_id,
        province_name,
        year,
        epi_week,
        event,
        age_group_id,
        age_group_desc,
        case_count,
        _source_file,
        _loaded_at
    from {{ ref('stg_dengue_zika__legacy') }}

),

current_era as (

    select
        'current' as schema_era,
        department_id,
        department_name,
        province_id,
        province_name,
        year,
        epi_week,
        event,
        age_group_id,
        age_group_desc,
        case_count,
        _source_file,
        _loaded_at
    from {{ ref('stg_dengue_zika__current') }}

),

unioned as (

    select * from legacy
    union all
    select * from current_era

),

with_published_date as (

    select
        *,
        case
            when schema_era = 'current'
                then to_date(regexp_extract(_source_file, '(\\d{4}-\\d{2}-\\d{2})\\.csv$', 1))
            when _source_file like '%1648473440%' then date('2022-03-28')
            when _source_file like '%vigilancia-de-dengue-y-zika-201812%' then date('2019-01-01')
            when _source_file like '%hasta-20181231%' then date('2022-01-01')
            else date(concat(cast(year as string), '-12-31'))
        end as published_date
    from unioned

),

georef as (

    select distinct
        department_indec_id,
        {{ normalize_geo_name('department_name') }} as department_name_norm,
        province_indec_id,
        {{ normalize_geo_name('province_name') }} as province_name_norm
    from {{ ref('georef_departments') }}

),

georef_provinces as (

    select distinct province_indec_id, province_name_norm
    from georef

),

age_buckets as (

    select desc_norm, age_group_name, age_group_sort_order
    from {{ ref('age_group_buckets') }}

),

geo_matched as (

    select
        u.*,
        dept_match.department_indec_id as matched_department_id,
        prov_match.province_indec_id as matched_province_id,
        bucket.age_group_name,
        bucket.age_group_sort_order
    from with_published_date u
    left join georef_provinces prov_match
        on (
            {{ normalize_geo_name('u.province_name') }} = prov_match.province_name_norm
            or prov_match.province_name_norm like concat({{ normalize_geo_name('u.province_name') }}, '%')
            or ({{ normalize_geo_name('u.province_name') }} = 'CABA' and prov_match.province_name_norm like 'CIUDAD AUTONOMA%')
        )
    left join georef dept_match
        on
            dept_match.province_indec_id = prov_match.province_indec_id
            and dept_match.department_name_norm = {{ normalize_geo_name('u.department_name') }}
    left join age_buckets bucket
        on bucket.desc_norm = {{ normalize_age_desc('u.age_group_desc') }}

),

resolved as (

    select
        schema_era,
        case
            when matched_department_id is not null then matched_department_id
            when matched_province_id is not null then concat(matched_province_id, '999')
            else '99999'
        end as department_indec_id,
        year,
        epi_week,
        event,
        coalesce(age_group_name, 'Sin especificar') as age_group_name,
        coalesce(age_group_sort_order, 999) as age_group_sort_order,
        case_count,
        published_date,
        _source_file,
        _loaded_at
    from geo_matched

)

select
    schema_era,
    department_indec_id,
    year,
    epi_week,
    event,
    age_group_name,
    age_group_sort_order,
    coalesce(sum(case_count), 0) as case_count,
    published_date,
    _source_file,
    max(_loaded_at) as _loaded_at
from resolved
group by 1, 2, 3, 4, 5, 6, 7, 9, 10
