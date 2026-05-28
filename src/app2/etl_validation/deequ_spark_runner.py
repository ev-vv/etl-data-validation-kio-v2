"""Deequ проверки на Spark (через SQL, расширенный набор)."""

from __future__ import annotations

import logging, time, json
from datetime import datetime
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from app2.post_validation.paths import tool_output_dir

logger = logging.getLogger(__name__)

ALL_DEEQU_CHECKS = {
    "E": {
        # completeness
        "id_is_complete": "SELECT COUNT(*) FROM etl_kio.matches WHERE id IS NULL",
        "utcDate_is_complete": "SELECT COUNT(*) FROM etl_kio.matches WHERE utcDate IS NULL",
        "status_is_complete": "SELECT COUNT(*) FROM etl_kio.matches WHERE status IS NULL",
        "home_team_complete": "SELECT COUNT(*) FROM etl_kio.matches WHERE home_team_id IS NULL",
        "away_team_complete": "SELECT COUNT(*) FROM etl_kio.matches WHERE away_team_id IS NULL",
        # schema
        "matchday_range": "SELECT COUNT(*) FROM etl_kio.matches WHERE matchday IS NOT NULL AND (matchday < 0 OR matchday > 60)",
        "status_valid": "SELECT COUNT(*) FROM etl_kio.matches WHERE status NOT IN ('SCHEDULED','TIMED','IN_PLAY','PAUSED','FINISHED','POSTPONED','SUSPENDED','CANCELED')",
        # uniqueness
        "unique_match_id": "SELECT COUNT(*) FROM (SELECT id, COUNT(*) AS cnt FROM etl_kio.matches WHERE id IS NOT NULL GROUP BY id HAVING COUNT(*) > 1) d",
        "unique_competition_id": "SELECT COUNT(*) FROM (SELECT id, COUNT(*) AS cnt FROM etl_kio.competitions WHERE id IS NOT NULL GROUP BY id HAVING COUNT(*) > 1) d",
        "unique_team_id": "SELECT COUNT(*) FROM (SELECT id, COUNT(*) AS cnt FROM etl_kio.teams WHERE id IS NOT NULL GROUP BY id HAVING COUNT(*) > 1) d",
        "unique_area_id": "SELECT COUNT(*) FROM (SELECT id, COUNT(*) AS cnt FROM etl_kio.areas WHERE id IS NOT NULL GROUP BY id HAVING COUNT(*) > 1) d",
        # completeness of other entities
        "competitions_not_empty": "SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM etl_kio.competitions",
        "teams_not_empty": "SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM etl_kio.teams",
        "areas_not_empty": "SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM etl_kio.areas",
        "areas_id_complete": "SELECT COUNT(*) FROM etl_kio.areas WHERE id IS NULL OR name IS NULL",
        "competitions_id_complete": "SELECT COUNT(*) FROM etl_kio.competitions WHERE id IS NULL OR name IS NULL",
        "teams_id_complete": "SELECT COUNT(*) FROM etl_kio.teams WHERE id IS NULL OR name IS NULL",
    },
    "T": {
        # uniqueness
        "unique_match_id": "SELECT COUNT(*) FROM (SELECT id, COUNT(*) AS cnt FROM etl_kio.matches GROUP BY id HAVING COUNT(*) > 1) d",
        # missing values
        "missing_home_away": "SELECT COUNT(*) FROM etl_kio.matches WHERE home_team_id IS NULL OR away_team_id IS NULL",
        # business rules
        "home_away_diff": "SELECT COUNT(*) FROM etl_kio.matches WHERE home_team_id = away_team_id AND home_team_id IS NOT NULL",
        # referential integrity
        "referential_home": "SELECT COUNT(*) FROM etl_kio.matches m LEFT JOIN etl_kio.teams ht ON m.home_team_id = ht.id WHERE m.home_team_id IS NOT NULL AND ht.id IS NULL",
        "referential_away": "SELECT COUNT(*) FROM etl_kio.matches m LEFT JOIN etl_kio.teams at ON m.away_team_id = at.id WHERE m.away_team_id IS NOT NULL AND at.id IS NULL",
        "referential_competition": "SELECT COUNT(*) FROM etl_kio.matches m LEFT JOIN etl_kio.competitions c ON m.competition_id = c.id WHERE c.id IS NULL",
        # range checks
        "matchday_out_of_range": "SELECT COUNT(*) FROM etl_kio.matches WHERE matchday IS NOT NULL AND (matchday < 0 OR matchday > 60)",
        # status validity
        "status_valid": "SELECT COUNT(*) FROM etl_kio.matches WHERE status NOT IN ('SCHEDULED','TIMED','IN_PLAY','PAUSED','FINISHED','POSTPONED','SUSPENDED','CANCELED')",
        # standings rules
        "standings_points_consistency": "SELECT COUNT(*) FROM etl_kio.standings WHERE points != won*3 + draw AND points IS NOT NULL",
        "played_games_positive": "SELECT COUNT(*) FROM etl_kio.standings WHERE playedGames = 0",
    },
    "L": {
        "matches_dates_not_null": "SELECT COUNT(*) FROM etl_kio.matches WHERE utcDate IS NULL",
        "unique_standings": "SELECT COUNT(*) FROM (SELECT competition_id, season_id, team_id, COUNT(*) AS cnt FROM etl_kio.standings GROUP BY competition_id, season_id, team_id HAVING COUNT(*) > 1) d",
        "standings_positive_points": "SELECT COUNT(*) FROM etl_kio.standings WHERE points < 0",
        "rate_out_of_bounds": "SELECT COUNT(*) FROM etl_kio.standings WHERE won*1.0/playedGames < 0 OR won*1.0/playedGames > 1",
        "standings_not_empty": "SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM etl_kio.standings",
    },
}


def run_stage_validation_deequ_spark(
    *,
    spark: SparkSession,
    dag_id: str,
    stage: str,
    targets: list[Any],
    output_dir: Path,
    layer: str = "STG",
) -> list[dict[str, Any]]:
    stage = stage.strip().upper()
    output_dir = tool_output_dir(output_dir, "deequ_spark")
    output_dir.mkdir(parents=True, exist_ok=True)

    checks = ALL_DEEQU_CHECKS.get(stage, {})
    reports = []
    for t in targets:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        total_checks = len(checks)
        total_failed = 0
        check_results = []
        spark_sec_total = 0.0  

        for name, sql in checks.items():
            start = time.time()
            try:
                row = spark.sql(sql).collect()[0]
                failed_rows = int(row[0]) if row[0] is not None else 0
                status = "PASS" if failed_rows == 0 else "FAIL"
                if failed_rows > 0:
                    total_failed += 1
            except Exception as e:
                status = "FAIL"
                failed_rows = -1
                logger.warning("Deequ check %s failed: %s", name, e)
            duration = time.time() - start
            spark_sec_total += duration
            check_results.append({"check": name, "status": status, "failed_rows": failed_rows, "duration_sec": round(duration, 3)})

        overall = "SUCCESS" if total_failed == 0 else "FAILED"
        report = {
            "run_id": getattr(t, "run_id", "spark_run"),
            "stage": stage,
            "status": overall,
            "checks_total": total_checks,
            "checks_failed": total_failed,
            "spark_sec": round(spark_sec_total, 3),
            "results": check_results,
        }
        out_path = output_dir / f"deequ_spark_{stage.lower()}_{ts}.json"
        out_path.write_text(json.dumps(report, indent=2, ensure_ascii=False))
        reports.append({
            "run_id": getattr(t, "run_id", "spark_run"),
            "stage": stage,
            "kind": getattr(t, "kind", "baseline"),
            "status": overall,
            "checks_total": total_checks,
            "checks_failed": total_failed,
            "report_path": str(out_path),
            "error": None,
            "spark_sec": round(spark_sec_total, 3),
        })
    return reports