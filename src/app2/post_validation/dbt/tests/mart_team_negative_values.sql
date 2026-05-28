{{ config(severity='error') }}

select *
from {{ source('mart', 'v_team_season_results') }}
where run_id = '{{ var("run_id") }}'
  and (points_calc < 0 or goals_for < 0 or goals_against < 0)
