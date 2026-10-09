-- A name containing U+FFFD means a source file was read with the wrong encoding
-- (e.g. a new republication that isn't ISO-8859-1). Fails with the offending names.
select distinct
    'geo' as kind,
    department_name as value
from {{ ref('stg_dengue_zika__current') }}
where department_name like '%�%' or province_name like '%�%'

union all

select distinct
    'age' as kind,
    age_group_desc as value
from {{ ref('stg_dengue_zika__current') }}
where age_group_desc like '%�%'
