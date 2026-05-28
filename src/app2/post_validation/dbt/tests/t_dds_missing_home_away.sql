{{ config(severity='error', tags=['stage_T', 'missing_values']) }}
SELECT
  run_id,
  match_id,
  home_team_id,
  away_team_id
FROM dds.fact_match
WHERE run_id = '{{ var("run_id") }}'
  AND (home_team_id IS NULL OR away_team_id IS NULL)
