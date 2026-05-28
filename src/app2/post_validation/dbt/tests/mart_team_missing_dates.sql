{{ config(severity='error') }}

select *
from {{ source('mart', 'v_team_season_results') }}
where run_id = '{{ var("run_id") }}'
  and (start_date is null or end_date is null or season_year is null)
