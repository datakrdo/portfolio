{{
  config(
    materialized='table'
  )
}}

-- Row-level security map for the BI semantic model: which provinces each user may
-- see. Fictitious users in the `rls_province_access` seed; in a real deployment this
-- would come from the identity provider. The "Provincial" role filters `dim_province`
-- by `USERPRINCIPALNAME()` against this table (see docs/powerbi.md).

select
    user_email,
    province_indec_id
from {{ ref('rls_province_access') }}
