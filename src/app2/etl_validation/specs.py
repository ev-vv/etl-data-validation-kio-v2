from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class StageCheck:
    name: str
    stage: str
    rule_group: str
    severity: str
    count_sql: str
    fail_sql: str


def _sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def build_stage_checks(stage: str, run_id: str) -> list[StageCheck]:
    stage = stage.strip().upper()
    rid = _sql_quote(run_id)

    if stage == "E":
        return [
            StageCheck(
                name="stg_schema_matches_key_missing",
                stage=stage,
                rule_group="schema_mismatch",
                severity="error",
                count_sql=(
                    "SELECT COUNT(*)\n"
                    "FROM stg.raw_football_api\n"
                    "WHERE endpoint LIKE 'competitions/%/matches%'\n"
                    f"  AND request_params ->> 'run_id' = {rid}\n"
                    "  AND http_status BETWEEN 200 AND 299\n"
                    "  AND NOT (response_json ? 'matches')"
                ),
                fail_sql=(
                    "SELECT id, endpoint, http_status\n"
                    "FROM stg.raw_football_api\n"
                    "WHERE endpoint LIKE 'competitions/%/matches%'\n"
                    f"  AND request_params ->> 'run_id' = {rid}\n"
                    "  AND http_status BETWEEN 200 AND 299\n"
                    "  AND NOT (response_json ? 'matches')"
                ),
            ),
            StageCheck(
                name="stg_missing_match_id",
                stage=stage,
                rule_group="missing_values",
                severity="error",
                count_sql=(
                    "SELECT COUNT(*)\n"
                    "FROM stg.raw_football_api s\n"
                    "JOIN LATERAL jsonb_array_elements(s.response_json -> 'matches') m ON TRUE\n"
                    "WHERE s.endpoint LIKE 'competitions/%/matches%'\n"
                    f"  AND s.request_params ->> 'run_id' = {rid}\n"
                    "  AND s.http_status BETWEEN 200 AND 299\n"
                    "  AND (m ->> 'id') IS NULL"
                ),
                fail_sql=(
                    "SELECT s.id, s.endpoint, m ->> 'id' AS match_id\n"
                    "FROM stg.raw_football_api s\n"
                    "JOIN LATERAL jsonb_array_elements(s.response_json -> 'matches') m ON TRUE\n"
                    "WHERE s.endpoint LIKE 'competitions/%/matches%'\n"
                    f"  AND s.request_params ->> 'run_id' = {rid}\n"
                    "  AND s.http_status BETWEEN 200 AND 299\n"
                    "  AND (m ->> 'id') IS NULL"
                ),
            ),
            StageCheck(
                name="stg_matchday_out_of_range",
                stage=stage,
                rule_group="out_of_range",
                severity="error",
                count_sql=(
                    "SELECT COUNT(*)\n"
                    "FROM stg.raw_football_api s\n"
                    "JOIN LATERAL jsonb_array_elements(s.response_json -> 'matches') m ON TRUE\n"
                    "WHERE s.endpoint LIKE 'competitions/%/matches%'\n"
                    f"  AND s.request_params ->> 'run_id' = {rid}\n"
                    "  AND s.http_status BETWEEN 200 AND 299\n"
                    "  AND (\n"
                    "    (m ->> 'matchday') IS NOT NULL\n"
                    "    AND (\n"
                    "      (m ->> 'matchday') !~ '^\\d+$'\n"
                    "      OR (m ->> 'matchday')::int < 0\n"
                    "      OR (m ->> 'matchday')::int > 60\n"
                    "    )\n"
                    "  )"
                ),
                fail_sql=(
                    "SELECT s.id, s.endpoint, m ->> 'id' AS match_id, m ->> 'matchday' AS matchday\n"
                    "FROM stg.raw_football_api s\n"
                    "JOIN LATERAL jsonb_array_elements(s.response_json -> 'matches') m ON TRUE\n"
                    "WHERE s.endpoint LIKE 'competitions/%/matches%'\n"
                    f"  AND s.request_params ->> 'run_id' = {rid}\n"
                    "  AND s.http_status BETWEEN 200 AND 299\n"
                    "  AND (\n"
                    "    (m ->> 'matchday') IS NOT NULL\n"
                    "    AND (\n"
                    "      (m ->> 'matchday') !~ '^\\d+$'\n"
                    "      OR (m ->> 'matchday')::int < 0\n"
                    "      OR (m ->> 'matchday')::int > 60\n"
                    "    )\n"
                    "  )"
                ),
            ),
            StageCheck(
                name="stg_duplicate_match_id",
                stage=stage,
                rule_group="duplicate_records",
                severity="error",
                count_sql=(
                    "SELECT COUNT(*)\n"
                    "FROM (\n"
                    "  SELECT (m ->> 'id') AS match_id, COUNT(*) AS cnt\n"
                    "  FROM stg.raw_football_api s\n"
                    "  JOIN LATERAL jsonb_array_elements(s.response_json -> 'matches') m ON TRUE\n"
                    "  WHERE s.endpoint LIKE 'competitions/%/matches%'\n"
                    f"    AND s.request_params ->> 'run_id' = {rid}\n"
                    "    AND s.http_status BETWEEN 200 AND 299\n"
                    "    AND (m ->> 'id') IS NOT NULL\n"
                    "  GROUP BY (m ->> 'id')\n"
                    "  HAVING COUNT(*) > 1\n"
                    ") d"
                ),
                fail_sql=(
                    "SELECT match_id, cnt\n"
                    "FROM (\n"
                    "  SELECT (m ->> 'id') AS match_id, COUNT(*) AS cnt\n"
                    "  FROM stg.raw_football_api s\n"
                    "  JOIN LATERAL jsonb_array_elements(s.response_json -> 'matches') m ON TRUE\n"
                    "  WHERE s.endpoint LIKE 'competitions/%/matches%'\n"
                    f"    AND s.request_params ->> 'run_id' = {rid}\n"
                    "    AND s.http_status BETWEEN 200 AND 299\n"
                    "    AND (m ->> 'id') IS NOT NULL\n"
                    "  GROUP BY (m ->> 'id')\n"
                    "  HAVING COUNT(*) > 1\n"
                    ") d"
                ),
            ),
        ]

    if stage == "T":
        return [
            StageCheck(
                name="dds_duplicate_fact_match",
                stage=stage,
                rule_group="duplicate_records",
                severity="error",
                count_sql=(
                    "SELECT COUNT(*)\n"
                    "FROM (\n"
                    "  SELECT run_id, match_id, COUNT(*) AS cnt\n"
                    "  FROM dds.fact_match\n"
                    f"  WHERE run_id = {rid}\n"
                    "  GROUP BY run_id, match_id\n"
                    "  HAVING COUNT(*) > 1\n"
                    ") d"
                ),
                fail_sql=(
                    "SELECT run_id, match_id, COUNT(*) AS cnt\n"
                    "FROM dds.fact_match\n"
                    f"WHERE run_id = {rid}\n"
                    "GROUP BY run_id, match_id\n"
                    "HAVING COUNT(*) > 1"
                ),
            ),
            StageCheck(
                name="dds_missing_home_away_team",
                stage=stage,
                rule_group="missing_values",
                severity="error",
                count_sql=(
                    "SELECT COUNT(*)\n"
                    "FROM dds.fact_match\n"
                    f"WHERE run_id = {rid}\n"
                    "  AND (home_team_id IS NULL OR away_team_id IS NULL)"
                ),
                fail_sql=(
                    "SELECT run_id, match_id, home_team_id, away_team_id\n"
                    "FROM dds.fact_match\n"
                    f"WHERE run_id = {rid}\n"
                    "  AND (home_team_id IS NULL OR away_team_id IS NULL)"
                ),
            ),
            StageCheck(
                name="dds_referential_integrity_violation",
                stage=stage,
                rule_group="referential_integrity_violation",
                severity="error",
                count_sql=(
                    "SELECT COUNT(*)\n"
                    "FROM (\n"
                    "  SELECT fm.match_id\n"
                    "  FROM dds.fact_match fm\n"
                    "  LEFT JOIN dds.dim_competition dc ON dc.run_id = fm.run_id AND dc.competition_id = fm.competition_id\n"
                    "  WHERE fm.run_id = {rid} AND dc.competition_id IS NULL\n"
                    "  UNION ALL\n"
                    "  SELECT fm.match_id\n"
                    "  FROM dds.fact_match fm\n"
                    "  LEFT JOIN dds.dim_season ds ON ds.run_id = fm.run_id AND ds.season_id = fm.season_id\n"
                    "  WHERE fm.run_id = {rid} AND ds.season_id IS NULL\n"
                    "  UNION ALL\n"
                    "  SELECT fm.match_id\n"
                    "  FROM dds.fact_match fm\n"
                    "  LEFT JOIN dds.dim_team dt ON dt.run_id = fm.run_id AND dt.team_id = fm.home_team_id\n"
                    "  WHERE fm.run_id = {rid} AND fm.home_team_id IS NOT NULL AND dt.team_id IS NULL\n"
                    "  UNION ALL\n"
                    "  SELECT fm.match_id\n"
                    "  FROM dds.fact_match fm\n"
                    "  LEFT JOIN dds.dim_team dt ON dt.run_id = fm.run_id AND dt.team_id = fm.away_team_id\n"
                    "  WHERE fm.run_id = {rid} AND fm.away_team_id IS NOT NULL AND dt.team_id IS NULL\n"
                    ") d".format(rid=rid)
                ),
                fail_sql=(
                    "SELECT 'competition' AS ref_type, fm.match_id\n"
                    "FROM dds.fact_match fm\n"
                    "LEFT JOIN dds.dim_competition dc ON dc.run_id = fm.run_id AND dc.competition_id = fm.competition_id\n"
                    f"WHERE fm.run_id = {rid} AND dc.competition_id IS NULL\n"
                    "UNION ALL\n"
                    "SELECT 'season' AS ref_type, fm.match_id\n"
                    "FROM dds.fact_match fm\n"
                    "LEFT JOIN dds.dim_season ds ON ds.run_id = fm.run_id AND ds.season_id = fm.season_id\n"
                    f"WHERE fm.run_id = {rid} AND ds.season_id IS NULL\n"
                    "UNION ALL\n"
                    "SELECT 'home_team' AS ref_type, fm.match_id\n"
                    "FROM dds.fact_match fm\n"
                    "LEFT JOIN dds.dim_team dt ON dt.run_id = fm.run_id AND dt.team_id = fm.home_team_id\n"
                    f"WHERE fm.run_id = {rid} AND fm.home_team_id IS NOT NULL AND dt.team_id IS NULL\n"
                    "UNION ALL\n"
                    "SELECT 'away_team' AS ref_type, fm.match_id\n"
                    "FROM dds.fact_match fm\n"
                    "LEFT JOIN dds.dim_team dt ON dt.run_id = fm.run_id AND dt.team_id = fm.away_team_id\n"
                    f"WHERE fm.run_id = {rid} AND fm.away_team_id IS NOT NULL AND dt.team_id IS NULL"
                ),
            ),
            StageCheck(
                name="dds_matchday_out_of_range",
                stage=stage,
                rule_group="out_of_range",
                severity="error",
                count_sql=(
                    "SELECT COUNT(*)\n"
                    "FROM dds.fact_match\n"
                    f"WHERE run_id = {rid}\n"
                    "  AND matchday IS NOT NULL\n"
                    "  AND (matchday < 0 OR matchday > 60)"
                ),
                fail_sql=(
                    "SELECT run_id, match_id, matchday\n"
                    "FROM dds.fact_match\n"
                    f"WHERE run_id = {rid}\n"
                    "  AND matchday IS NOT NULL\n"
                    "  AND (matchday < 0 OR matchday > 60)"
                ),
            ),
            StageCheck(
                name="dds_fact_match_utc_date_missing",
                stage=stage,
                rule_group="missing_values",
                severity="error",
                count_sql=(
                    "SELECT COUNT(*)\n"
                    "FROM dds.fact_match\n"
                    f"WHERE run_id = {rid}\n"
                    "  AND utc_date IS NULL"
                ),
                fail_sql=(
                    "SELECT run_id, match_id, utc_date\n"
                    "FROM dds.fact_match\n"
                    f"WHERE run_id = {rid}\n"
                    "  AND utc_date IS NULL"
                ),
            ),
            StageCheck(
                name="dds_dim_competition_name_corrupted",
                stage=stage,
                rule_group="data_corruption",
                severity="error",
                count_sql=(
                    "SELECT COUNT(*)\n"
                    "FROM dds.dim_competition\n"
                    f"WHERE run_id = {rid}\n"
                    "  AND name = 'MUTATED_COMP'"
                ),
                fail_sql=(
                    "SELECT run_id, competition_id, name\n"
                    "FROM dds.dim_competition\n"
                    f"WHERE run_id = {rid}\n"
                    "  AND name = 'MUTATED_COMP'"
                ),
            ),
            StageCheck(
                name="dds_fact_standing_zero_played_games",
                stage=stage,
                rule_group="data_corruption",
                severity="error",
                count_sql=(
                    "SELECT COUNT(*)\n"
                    "FROM dds.fact_standing\n"
                    f"WHERE run_id = {rid}\n"
                    "  AND played_games = 0\n"
                    "  AND won = 0 AND draw = 0 AND lost = 0"
                ),
                fail_sql=(
                    "SELECT run_id, season_id, competition_id, team_id, standing_type, played_games, won, draw, lost, points\n"
                    "FROM dds.fact_standing\n"
                    f"WHERE run_id = {rid}\n"
                    "  AND played_games = 0\n"
                    "  AND won = 0 AND draw = 0 AND lost = 0"
                ),
            ),
            StageCheck(
                name="dds_dim_season_dates_missing",
                stage=stage,
                rule_group="missing_values",
                severity="error",
                count_sql=(
                    "SELECT COUNT(*)\n"
                    "FROM dds.dim_season\n"
                    f"WHERE run_id = {rid}\n"
                    "  AND (start_date IS NULL OR end_date IS NULL)"
                ),
                fail_sql=(
                    "SELECT run_id, season_id, start_date, end_date\n"
                    "FROM dds.dim_season\n"
                    f"WHERE run_id = {rid}\n"
                    "  AND (start_date IS NULL OR end_date IS NULL)"
                ),
            ),
            StageCheck(
                name="dds_dim_team_duplicate",
                stage=stage,
                rule_group="duplicate_records",
                severity="error",
                count_sql=(
                    "SELECT COUNT(*)\n"
                    "FROM (\n"
                    "  SELECT run_id, team_id, COUNT(*) AS cnt\n"
                    "  FROM dds.dim_team\n"
                    f"  WHERE run_id = {rid}\n"
                    "  GROUP BY run_id, team_id\n"
                    "  HAVING COUNT(*) > 1\n"
                    ") d"
                ),
                fail_sql=(
                    "SELECT run_id, team_id, COUNT(*) AS cnt\n"
                    "FROM dds.dim_team\n"
                    f"WHERE run_id = {rid}\n"
                    "GROUP BY run_id, team_id\n"
                    "HAVING COUNT(*) > 1"
                ),
            ),
            StageCheck(
                name="dds_dim_competition_duplicate",
                stage=stage,
                rule_group="duplicate_records",
                severity="error",
                count_sql=(
                    "SELECT COUNT(*)\n"
                    "FROM (\n"
                    "  SELECT run_id, competition_id, COUNT(*) AS cnt\n"
                    "  FROM dds.dim_competition\n"
                    f"  WHERE run_id = {rid}\n"
                    "  GROUP BY run_id, competition_id\n"
                    "  HAVING COUNT(*) > 1\n"
                    ") d"
                ),
                fail_sql=(
                    "SELECT run_id, competition_id, COUNT(*) AS cnt\n"
                    "FROM dds.dim_competition\n"
                    f"WHERE run_id = {rid}\n"
                    "GROUP BY run_id, competition_id\n"
                    "HAVING COUNT(*) > 1"
                ),
            ),
        ]

    if stage == "L":
        return [
            StageCheck(
                name="dds_dim_season_dates_missing",
                stage=stage,
                rule_group="missing_values",
                severity="error",
                count_sql=(
                    "SELECT COUNT(*)\n"
                    "FROM dds.dim_season\n"
                    f"WHERE run_id = {rid}\n"
                    "  AND (start_date IS NULL OR end_date IS NULL)"
                ),
                fail_sql=(
                    "SELECT run_id, season_id, start_date, end_date\n"
                    "FROM dds.dim_season\n"
                    f"WHERE run_id = {rid}\n"
                    "  AND (start_date IS NULL OR end_date IS NULL)"
                ),
            ),
            StageCheck(
                name="dds_fact_match_utc_date_missing",
                stage=stage,
                rule_group="missing_values",
                severity="error",
                count_sql=(
                    "SELECT COUNT(*)\n"
                    "FROM dds.fact_match\n"
                    f"WHERE run_id = {rid}\n"
                    "  AND utc_date IS NULL"
                ),
                fail_sql=(
                    "SELECT run_id, match_id, utc_date\n"
                    "FROM dds.fact_match\n"
                    f"WHERE run_id = {rid}\n"
                    "  AND utc_date IS NULL"
                ),
            ),
            StageCheck(
                name="dds_fact_standing_zero_played_games",
                stage=stage,
                rule_group="data_corruption",
                severity="error",
                count_sql=(
                    "SELECT COUNT(*)\n"
                    "FROM dds.fact_standing\n"
                    f"WHERE run_id = {rid}\n"
                    "  AND played_games = 0\n"
                    "  AND won = 0 AND draw = 0 AND lost = 0"
                ),
                fail_sql=(
                    "SELECT run_id, season_id, competition_id, team_id, standing_type, played_games, won, draw, lost, points\n"
                    "FROM dds.fact_standing\n"
                    f"WHERE run_id = {rid}\n"
                    "  AND played_games = 0\n"
                    "  AND won = 0 AND draw = 0 AND lost = 0"
                ),
            ),
            StageCheck(
                name="dds_dim_competition_name_corrupted",
                stage=stage,
                rule_group="data_corruption",
                severity="error",
                count_sql=(
                    "SELECT COUNT(*)\n"
                    "FROM dds.dim_competition\n"
                    f"WHERE run_id = {rid}\n"
                    "  AND name = 'MUTATED_COMP'"
                ),
                fail_sql=(
                    "SELECT run_id, competition_id, name\n"
                    "FROM dds.dim_competition\n"
                    f"WHERE run_id = {rid}\n"
                    "  AND name = 'MUTATED_COMP'"
                ),
            ),
            StageCheck(
                name="mart_kpi_rate_out_of_bounds",
                stage=stage,
                rule_group="out_of_range",
                severity="error",
                count_sql=(
                    "SELECT COUNT(*)\n"
                    "FROM mart.v_competition_season_kpi\n"
                    f"WHERE run_id = {rid}\n"
                    "  AND (\n"
                    "    home_win_rate < 0 OR home_win_rate > 1 OR\n"
                    "    draw_rate < 0 OR draw_rate > 1 OR\n"
                    "    away_win_rate < 0 OR away_win_rate > 1\n"
                    "  )"
                ),
                fail_sql=(
                    "SELECT run_id, competition_id, season_id, home_win_rate, draw_rate, away_win_rate\n"
                    "FROM mart.v_competition_season_kpi\n"
                    f"WHERE run_id = {rid}\n"
                    "  AND (\n"
                    "    home_win_rate < 0 OR home_win_rate > 1 OR\n"
                    "    draw_rate < 0 OR draw_rate > 1 OR\n"
                    "    away_win_rate < 0 OR away_win_rate > 1\n"
                    "  )"
                ),
            ),
            StageCheck(
                name="mart_kpi_missing_dates",
                stage=stage,
                rule_group="missing_values",
                severity="error",
                count_sql=(
                    "SELECT COUNT(*)\n"
                    "FROM mart.v_competition_season_kpi\n"
                    f"WHERE run_id = {rid}\n"
                    "  AND (start_date IS NULL OR end_date IS NULL OR season_year IS NULL)"
                ),
                fail_sql=(
                    "SELECT run_id, competition_id, season_id, start_date, end_date, season_year\n"
                    "FROM mart.v_competition_season_kpi\n"
                    f"WHERE run_id = {rid}\n"
                    "  AND (start_date IS NULL OR end_date IS NULL OR season_year IS NULL)"
                ),
            ),
            StageCheck(
                name="mart_duplicate_team_rows",
                stage=stage,
                rule_group="duplicate_records",
                severity="error",
                count_sql=(
                    "SELECT COUNT(*)\n"
                    "FROM (\n"
                    "  SELECT run_id, competition_id, season_id, team_id, COUNT(*) AS cnt\n"
                    "  FROM mart.v_team_season_results\n"
                    f"  WHERE run_id = {rid}\n"
                    "  GROUP BY run_id, competition_id, season_id, team_id\n"
                    "  HAVING COUNT(*) > 1\n"
                    ") d"
                ),
                fail_sql=(
                    "SELECT run_id, competition_id, season_id, team_id, COUNT(*) AS cnt\n"
                    "FROM mart.v_team_season_results\n"
                    f"WHERE run_id = {rid}\n"
                    "GROUP BY run_id, competition_id, season_id, team_id\n"
                    "HAVING COUNT(*) > 1"
                ),
            ),
        ]

    return []


def build_constraint_checks(stage: str, run_id: str) -> list[StageCheck]:
    stage = stage.strip().upper()
    rid = _sql_quote(run_id)

    if stage != "T":
        return []

    return [
        StageCheck(
            name="dds_fact_match_home_away_valid",
            stage=stage,
            rule_group="sql_constraint",
            severity="error",
            count_sql=(
                "SELECT COUNT(*)\n"
                "FROM dds.fact_match\n"
                f"WHERE run_id = {rid}\n"
                "  AND (home_team_id IS NULL OR away_team_id IS NULL OR home_team_id = away_team_id)"
            ),
            fail_sql=(
                "SELECT run_id, match_id, home_team_id, away_team_id\n"
                "FROM dds.fact_match\n"
                f"WHERE run_id = {rid}\n"
                "  AND (home_team_id IS NULL OR away_team_id IS NULL OR home_team_id = away_team_id)"
            ),
        ),
        StageCheck(
            name="dds_fact_match_matchday_range",
            stage=stage,
            rule_group="sql_constraint",
            severity="error",
            count_sql=(
                "SELECT COUNT(*)\n"
                "FROM dds.fact_match\n"
                f"WHERE run_id = {rid}\n"
                "  AND matchday IS NOT NULL\n"
                "  AND (matchday < 0 OR matchday > 60)"
            ),
            fail_sql=(
                "SELECT run_id, match_id, matchday\n"
                "FROM dds.fact_match\n"
                f"WHERE run_id = {rid}\n"
                "  AND matchday IS NOT NULL\n"
                "  AND (matchday < 0 OR matchday > 60)"
            ),
        ),
        StageCheck(
            name="dds_fact_match_utc_date_missing",
            stage=stage,
            rule_group="sql_constraint",
            severity="error",
            count_sql=(
                "SELECT COUNT(*)\n"
                "FROM dds.fact_match\n"
                f"WHERE run_id = {rid}\n"
                "  AND utc_date IS NULL"
            ),
            fail_sql=(
                "SELECT run_id, match_id, utc_date\n"
                "FROM dds.fact_match\n"
                f"WHERE run_id = {rid}\n"
                "  AND utc_date IS NULL"
            ),
        ),
        StageCheck(
            name="dds_dim_season_dates_missing",
            stage=stage,
            rule_group="sql_constraint",
            severity="error",
            count_sql=(
                "SELECT COUNT(*)\n"
                "FROM dds.dim_season\n"
                f"WHERE run_id = {rid}\n"
                "  AND (start_date IS NULL OR end_date IS NULL)"
            ),
            fail_sql=(
                "SELECT run_id, season_id, start_date, end_date\n"
                "FROM dds.dim_season\n"
                f"WHERE run_id = {rid}\n"
                "  AND (start_date IS NULL OR end_date IS NULL)"
            ),
        ),
        StageCheck(
            name="dds_fact_standing_zero_played_games",
            stage=stage,
            rule_group="sql_constraint",
            severity="error",
            count_sql=(
                "SELECT COUNT(*)\n"
                "FROM dds.fact_standing\n"
                f"WHERE run_id = {rid}\n"
                "  AND played_games = 0\n"
                "  AND won = 0 AND draw = 0 AND lost = 0"
            ),
            fail_sql=(
                "SELECT run_id, season_id, competition_id, team_id, standing_type, played_games, won, draw, lost, points\n"
                "FROM dds.fact_standing\n"
                f"WHERE run_id = {rid}\n"
                "  AND played_games = 0\n"
                "  AND won = 0 AND draw = 0 AND lost = 0"
            ),
        ),
        StageCheck(
            name="dds_dim_competition_name_corrupted",
            stage=stage,
            rule_group="sql_constraint",
            severity="error",
            count_sql=(
                "SELECT COUNT(*)\n"
                "FROM dds.dim_competition\n"
                f"WHERE run_id = {rid}\n"
                "  AND name = 'MUTATED_COMP'"
            ),
            fail_sql=(
                "SELECT run_id, competition_id, name\n"
                "FROM dds.dim_competition\n"
                f"WHERE run_id = {rid}\n"
                "  AND name = 'MUTATED_COMP'"
            ),
        ),
    ]


def build_metrics_query(stage: str, run_id: str) -> str:
    checks = build_stage_checks(stage, run_id)
    if not checks:
        raise ValueError(f"No checks defined for stage {stage}.")
    columns = [f"({c.count_sql}) AS {c.name}" for c in checks]
    return "SELECT\n  " + ",\n  ".join(columns)


__all__ = ["StageCheck", "build_stage_checks", "build_constraint_checks", "build_metrics_query"]