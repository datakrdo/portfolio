{{ config(severity='warn') }}

-- The upstream feed publishes new cuts regularly; warn when the newest publication
-- we hold is older than 120 days (the weekly refresh job stopped or the source did).
select max(published_date) as latest_published_date
from {{ ref('int_dengue_zika_all_revisions') }}
having max(published_date) < date_sub(current_date(), 120)
