{{
  config(
    materialized='view'
  )
}}

-- 4 published cuts of RENAPER's "identified population by department" (see
-- docs/data_profile.md), used as the incidence-rate denominator. Each cut is
-- broken down by sex/age/nationality in the source; that breakdown isn't needed
-- here (the mart only needs total population per department per cut), so it's
-- summed away in this same staging model rather than carried downstream.
--
-- `departamento_id` is zero-padded to 5 digits in some cuts (e.g. "06854") and not
-- in others (e.g. "2000" for CABA) — normalized here with `lpad` so every cut has
-- the same 4-or-5-digit-source shape before the intermediate layer resolves it
-- against Georef by name (same approach as `int_dengue_zika_unified`, since this
-- raw id doesn't reliably match Georef's own department numbering either).
--
-- Publish dates aren't in the filenames as a parseable date, just a month/year
-- (e.g. `_agosto_2024`) — mapped explicitly per file since there are only 4.

with source as (

    select
        provincia_id,
        nombre_provincia as province_name,
        departamento_id,
        nombre_departamento as department_name,
        cantidad as population,
        _metadata.file_path as _source_file
    from read_files(
        '/Volumes/workspace/bronze/raw_data/renaper_population/*.csv',
        format => 'csv',
        header => true,
        schema => 'provincia_id STRING, nombre_provincia STRING, departamento_id STRING, nombre_departamento STRING, sexo STRING, edad_quinquenal STRING, nacionalidad STRING, pais_nacimiento STRING, cantidad INT'
    )

),

with_publish_date as (

    select
        *,
        case
            when _source_file like '%departamento_agosto_2024.csv' then date('2024-08-01')
            when _source_file like '%departamento_enero_2025.csv' then date('2025-01-01')
            when _source_file like '%departamento_junio_2025.csv' then date('2025-06-01')
            when _source_file like '%departamento_junio_2026.csv' then date('2026-06-01')
        end as publish_date,
        lpad(departamento_id, 5, '0') as department_id_padded
    from source

)

select
    publish_date,
    provincia_id as province_id,
    trim(province_name) as province_name,
    department_id_padded as department_id,
    trim(department_name) as department_name,
    sum(population) as population,
    max(_source_file) as _source_file,
    current_timestamp() as _loaded_at
from with_publish_date
group by 1, 2, 3, 4, 5
