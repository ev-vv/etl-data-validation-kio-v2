{{ config(severity='error') }}

select *
from {{ source('dds', 'fact_match') }}
where run_id = '{{ var("run_id") }}'
  and season_id is null
