{{ config(severity='error') }}

select *
from {{ source('dds', 'fact_standing') }}
where run_id = '{{ var("run_id") }}'
  and points is not null
  and won is not null
  and draw is not null
  and points <> (won * 3 + draw)
