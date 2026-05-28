{{ config(severity='error', tags=['stage_L', 'missing_values']) }}
SELECT
  run_id,
  competition_id,
  season_id,
  start_date,
  end_date,
  season_year
FROM mart.v_competition_season_kpi
WHERE run_id = '{{ var("run_id") }}'
  AND (start_date IS NULL OR end_date IS NULL OR season_year IS NULL)
