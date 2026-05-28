{{ config(severity='error', tags=['stage_L', 'out_of_range']) }}
SELECT
  run_id,
  competition_id,
  season_id,
  home_win_rate,
  draw_rate,
  away_win_rate
FROM mart.v_competition_season_kpi
WHERE run_id = '{{ var("run_id") }}'
  AND (
    home_win_rate < 0 OR home_win_rate > 1 OR
    draw_rate < 0 OR draw_rate > 1 OR
    away_win_rate < 0 OR away_win_rate > 1
  )
