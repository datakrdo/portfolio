{{
  config(
    materialized='view'
  )
}}

-- Current schema era (2023 onward): 6 files, all `;`-delimited, but 2 different
-- encodings. Every 2024/2025 revision after the first is ISO-8859-1 (accented
-- characters come through as replacement chars in UTF-8); only the original 2023
-- file is real UTF-8 (with a BOM). See docs/data_profile.md for the full profile.
--
-- Revisions of the same period (2024 was republished 3 times) are NOT deduped or
-- collapsed here — staging stays 1:1 with source. `_source_file` is kept so a later
-- snapshot/intermediate layer can pick the latest revision or track how published
-- numbers changed over time (see plan.md, invariant 7).
--
-- Both branches pass an explicit `schema` to `read_files()` instead of relying on type
-- inference: department/province codes are zero-padded INDEC codes in the source
-- (e.g. "009"), and inferring them as int (then casting to string afterwards) silently
-- drops that padding, which would break the canonical geography join later.

with utf8_2023 as (

    select
        department_id,
        department_name,
        province_id,
        province_name,
        cast(year_raw as int) as year,
        evento as event,
        id_grupo_etario as age_group_id,
        grupo_etario as age_group_desc,
        cast(sepi_min as int) as epi_week,
        cast(cantidad as int) as case_count,
        _metadata.file_path as _source_file
    from
        read_files(
            '/Volumes/workspace/bronze/raw_data/dengue_zika/current/informacion-publica-dengue-zika-nacional-se-1-a-52-de-2023-2024-06-10.csv',
            format => 'csv',
            header => true,
            delimiter => ';',
            encoding => 'UTF-8',
            schema => 'department_id STRING, department_name STRING, province_id STRING, province_name STRING, year_raw STRING, evento STRING, id_grupo_etario STRING, grupo_etario STRING, sepi_min STRING, cantidad STRING'
        )
    where sepi_min is not null

),

latin1_variants as (

    select
        department_id,
        department_name,
        province_id,
        province_name,
        cast(year_raw as int) as year,
        evento as event,
        id_grupo_etario as age_group_id,
        grupo_etario as age_group_desc,
        cast(sepi_min as int) as epi_week,
        cast(cantidad as int) as case_count,
        _metadata.file_path as _source_file
    from
        read_files(
            '/Volumes/workspace/bronze/raw_data/dengue_zika/current/*.csv',
            format => 'csv',
            header => true,
            delimiter => ';',
            encoding => 'ISO-8859-1',
            schema => 'department_id STRING, department_name STRING, province_id STRING, province_name STRING, year_raw STRING, evento STRING, id_grupo_etario STRING, grupo_etario STRING, sepi_min STRING, cantidad STRING'
        )
    where sepi_min is not null
    -- the 2023 file is real UTF-8; reading it as ISO-8859-1 here would mangle
    -- accented names, so it's excluded and handled in its own branch above
    and _metadata.file_path not like '%se-1-a-52-de-2023-2024-06-10.csv'

),

unioned as (

    select * from utf8_2023
    union all
    select * from latin1_variants

)

select
    department_id,
    trim(department_name) as department_name,
    province_id,
    trim(province_name) as province_name,
    year,
    epi_week,
    trim(event) as event,
    age_group_id,
    trim(age_group_desc) as age_group_desc,
    case_count,
    _source_file,
    current_timestamp() as _loaded_at
from unioned
