{{ config(severity='error', tags=['stage_T', 'out_of_range']) }}
SELECT
  run_id,
  match_id,
  matchday
FROM dds.fact_match
WHERE run_id = '{{ var("run_id") }}'
  AND matchday IS NOT NULL
  AND (matchday < 0 OR matchday > 60)
