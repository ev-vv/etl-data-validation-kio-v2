{{ config(severity='error', tags=['stage_T', 'referential_integrity_violation']) }}
SELECT 'competition' AS ref_type, fm.match_id
FROM dds.fact_match fm
LEFT JOIN dds.dim_competition dc
  ON dc.run_id = fm.run_id AND dc.competition_id = fm.competition_id
WHERE fm.run_id = '{{ var("run_id") }}' AND dc.competition_id IS NULL
UNION ALL
SELECT 'season' AS ref_type, fm.match_id
FROM dds.fact_match fm
LEFT JOIN dds.dim_season ds
  ON ds.run_id = fm.run_id AND ds.season_id = fm.season_id
WHERE fm.run_id = '{{ var("run_id") }}' AND ds.season_id IS NULL
UNION ALL
SELECT 'home_team' AS ref_type, fm.match_id
FROM dds.fact_match fm
LEFT JOIN dds.dim_team dt
  ON dt.run_id = fm.run_id AND dt.team_id = fm.home_team_id
WHERE fm.run_id = '{{ var("run_id") }}' AND fm.home_team_id IS NOT NULL AND dt.team_id IS NULL
UNION ALL
SELECT 'away_team' AS ref_type, fm.match_id
FROM dds.fact_match fm
LEFT JOIN dds.dim_team dt
  ON dt.run_id = fm.run_id AND dt.team_id = fm.away_team_id
WHERE fm.run_id = '{{ var("run_id") }}' AND fm.away_team_id IS NOT NULL AND dt.team_id IS NULL
