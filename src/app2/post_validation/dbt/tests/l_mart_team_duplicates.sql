{{ config(severity='error', tags=['stage_L', 'duplicate_records']) }}
SELECT
  run_id,
  competition_id,
  season_id,
  team_id,
  COUNT(*) AS cnt
FROM mart.v_team_season_results
WHERE run_id = '{{ var("run_id") }}'
GROUP BY run_id, competition_id, season_id, team_id
HAVING COUNT(*) > 1
