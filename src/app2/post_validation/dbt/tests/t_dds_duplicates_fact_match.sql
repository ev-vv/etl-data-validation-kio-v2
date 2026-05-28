{{ config(severity='error', tags=['stage_T', 'duplicate_records']) }}
SELECT
  run_id,
  match_id,
  COUNT(*) AS cnt
FROM dds.fact_match
WHERE run_id = '{{ var("run_id") }}'
GROUP BY run_id, match_id
HAVING COUNT(*) > 1
