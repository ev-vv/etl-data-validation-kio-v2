{{ config(severity='error') }}

select *
from {{ source('mart', 'v_team_season_results') }}
where run_id = '{{ var("run_id") }}'
  and points_calc <> (wins * 3 + draws)
