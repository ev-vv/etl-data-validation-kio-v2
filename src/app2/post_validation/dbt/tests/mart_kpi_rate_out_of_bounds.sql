{{ config(severity='error') }}

select *
from {{ source('mart', 'v_competition_season_kpi') }}
where run_id = '{{ var("run_id") }}'
  and (
    home_win_rate < 0 or home_win_rate > 1 or
    draw_rate < 0 or draw_rate > 1 or
    away_win_rate < 0 or away_win_rate > 1
  )
