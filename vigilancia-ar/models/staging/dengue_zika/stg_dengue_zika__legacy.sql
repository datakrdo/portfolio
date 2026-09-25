{{
  config(
    materialized='view'
  )
}}

-- Legacy schema era (2018-2022): 7 files with 3 distinct delimiter/encoding/year-column-
-- name combinations, plus a genuine data-entry bug in the 2020 file. See
-- docs/data_profile.md for the full source profile.
--
-- A single `read_files()` glob can't parse everything (delimiter and encoding differ
-- across files), so the two formats are read in separate branches and unioned below.
--
-- Both branches pass an explicit `schema` to `read_files()` instead of relying on type
-- inference: department/province codes are zero-padded INDEC codes in the source
-- (e.g. "009"), and inferring them as int (then casting to string afterwards) silently
-- drops that padding, which would break the canonical geography join later. An
-- explicit schema also sidesteps the year column's name varying across files
-- (`ano`/`anio`/`año`) since `read_files()` matches an explicit schema positionally,
-- not by header name.

with semicolon_latin1 as (

    select
        department_id,
        department_name,
        province_id,
        province_name,
        cast(year_raw as int) as year,
        cast(semanas_epidemiologicas as int) as epi_week,
        evento_nombre as event,
        grupo_edad_id as age_group_id,
        grupo_edad_desc as age_group_desc,
        cast(cast(cantidad_casos as double) as int) as case_count,
        _metadata.file_path as _source_file
    from read_files(
        '/Volumes/workspace/bronze/raw_data/dengue_zika/legacy/informacion-publica-dengue-zika-nacional-anio-2022.csv',
        format => 'csv',
        header => true,
        delimiter => ';',
        encoding => 'ISO-8859-1',
        schema => 'department_id STRING, department_name STRING, province_id STRING, province_name STRING, year_raw STRING, semanas_epidemiologicas STRING, evento_nombre STRING, grupo_edad_id STRING, grupo_edad_desc STRING, cantidad_casos STRING'
    )

),

comma_variants as (

    select
        department_id,
        department_name,
        province_id,
        province_name,
        cast(year_raw as int) as year,
        -- 2020 file bug (informacion-...-20201231_1): ~1,100 rows have
        -- semanas_epidemiologicas and evento_nombre swapped (semanas_epidemiologicas
        -- holds 'Dengue', evento_nombre holds the week number). Detect it by regex
        -- and swap the two columns back rather than dropping the rows.
        cast(
            case
                when semanas_epidemiologicas rlike '^[0-9]+$' then semanas_epidemiologicas
                else evento_nombre
            end as int
        ) as epi_week,
        case
            when semanas_epidemiologicas rlike '^[0-9]+$' then evento_nombre
            else semanas_epidemiologicas
        end as event,
        -- same 2020 file also has grupo_edad_id/grupo_edad_desc swapped for every row
        -- (a separate bug from the one above): the real id is always numeric (or the
        -- '-' placeholder used for "Sin Especificar"), the real desc is always text, so
        -- the same kind of regex check un-swaps it everywhere, not just the rows
        -- affected by the semanas_epidemiologicas/evento_nombre bug.
        case
            when grupo_edad_id rlike '^[0-9]+(\\.[0-9]+)?$'
                then regexp_replace(grupo_edad_id, '\\.0$', '')
            else grupo_edad_desc
        end as age_group_id,
        case
            when grupo_edad_id rlike '^[0-9]+(\\.[0-9]+)?$' then grupo_edad_desc
            else grupo_edad_id
        end as age_group_desc,
        cast(cast(cantidad_casos as double) as int) as case_count,
        _metadata.file_path as _source_file
    from
        read_files(
            '/Volumes/workspace/bronze/raw_data/dengue_zika/legacy/*.csv',
            format => 'csv',
            header => true,
            encoding => 'UTF-8',
            schema => 'department_id STRING, department_name STRING, province_id STRING, province_name STRING, year_raw STRING, semanas_epidemiologicas STRING, evento_nombre STRING, grupo_edad_id STRING, grupo_edad_desc STRING, cantidad_casos STRING'
        )
    -- the semicolon/latin1 file also matches this glob; excluded here since it's
    -- already covered above, parsed with its real delimiter and encoding
    where _metadata.file_path not like '%anio-2022.csv'

),

unioned as (

    select * from semicolon_latin1
    union all
    select * from comma_variants

)

select
    -- one 2021 file has province_id/province_name swapped for every row (province_id
    -- holds the name, province_name holds the numeric code) — same kind of bug as the
    -- 2020 file's columns, fixed the same way: province_id is always numeric.
    department_id,
    trim(department_name) as department_name,
    case when province_id rlike '^[0-9]+$' then province_id else province_name end
        as province_id,
    trim(case when province_id rlike '^[0-9]+$' then province_name else province_id end)
        as province_name,
    year,
    epi_week,
    trim(event) as event,
    age_group_id,
    trim(age_group_desc) as age_group_desc,
    case_count,
    _source_file,
    current_timestamp() as _loaded_at
from unioned
