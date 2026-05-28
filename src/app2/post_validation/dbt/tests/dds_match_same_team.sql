{{ config(severity='error') }}

select *
from {{ source('dds', 'fact_match') }}
where run_id = '{{ var("run_id") }}'
  and home_team_id is not null
  and away_team_id is not null
  and home_team_id = away_team_id
