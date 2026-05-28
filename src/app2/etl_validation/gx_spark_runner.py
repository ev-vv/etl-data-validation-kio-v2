
from __future__ import annotations

import logging, time, json
from datetime import datetime
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from app2.post_validation.paths import tool_output_dir

logger = logging.getLogger(__name__)


def _now_tag() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


STG_CHECKS = {
    # Completeness
    "areas_completeness": "SELECT COUNT(*) FROM etl_kio.areas WHERE id IS NULL OR name IS NULL",
    "competitions_completeness": "SELECT COUNT(*) FROM etl_kio.competitions WHERE id IS NULL OR name IS NULL",
    "teams_completeness": "SELECT COUNT(*) FROM etl_kio.teams WHERE id IS NULL OR name IS NULL",
    "matches_completeness": "SELECT COUNT(*) FROM etl_kio.matches WHERE id IS NULL OR utcDate IS NULL OR home_team_id IS NULL OR away_team_id IS NULL",
    "standings_completeness": "SELECT COUNT(*) FROM etl_kio.standings WHERE season_id IS NULL OR competition_id IS NULL OR team_id IS NULL",

    # Uniqueness
    "areas_uniqueness": "SELECT COUNT(*) FROM (SELECT id, COUNT(*) AS cnt FROM etl_kio.areas WHERE id IS NOT NULL GROUP BY id HAVING COUNT(*) > 1) d",
    "competitions_uniqueness": "SELECT COUNT(*) FROM (SELECT id, COUNT(*) AS cnt FROM etl_kio.competitions WHERE id IS NOT NULL GROUP BY id HAVING COUNT(*) > 1) d",
    "teams_uniqueness": "SELECT COUNT(*) FROM (SELECT id, COUNT(*) AS cnt FROM etl_kio.teams WHERE id IS NOT NULL GROUP BY id HAVING COUNT(*) > 1) d",
    "matches_uniqueness": "SELECT COUNT(*) FROM (SELECT id, COUNT(*) AS cnt FROM etl_kio.matches WHERE id IS NOT NULL GROUP BY id HAVING COUNT(*) > 1) d",

    # Schema (обязательные поля)
    "matches_schema_id": "SELECT COUNT(*) FROM etl_kio.matches WHERE id IS NULL",
    "matches_schema_utcDate": "SELECT COUNT(*) FROM etl_kio.matches WHERE utcDate IS NULL",
    "matches_schema_status": "SELECT COUNT(*) FROM etl_kio.matches WHERE status IS NULL",

    # Consistency
    "matches_home_away_diff": "SELECT COUNT(*) FROM etl_kio.matches WHERE home_team_id IS NOT NULL AND away_team_id IS NOT NULL AND home_team_id = away_team_id",
    "matchday_out_of_range": "SELECT COUNT(*) FROM etl_kio.matches WHERE matchday IS NOT NULL AND (matchday < 0 OR matchday > 60)",
    "match_status_invalid": "SELECT COUNT(*) FROM etl_kio.matches WHERE status IS NOT NULL AND status NOT IN ('SCHEDULED','TIMED','IN_PLAY','PAUSED','FINISHED','POSTPONED','SUSPENDED','CANCELED')",
}

DDS_CHECKS = {
    # Referential integrity
    "fact_match_home_team_fk": """
        SELECT COUNT(*) FROM etl_kio.matches m
        LEFT JOIN etl_kio.teams ht ON m.home_team_id = ht.id
        WHERE m.home_team_id IS NOT NULL AND ht.id IS NULL
    """,
    "fact_match_away_team_fk": """
        SELECT COUNT(*) FROM etl_kio.matches m
        LEFT JOIN etl_kio.teams at ON m.away_team_id = at.id
        WHERE m.away_team_id IS NOT NULL AND at.id IS NULL
    """,
    "fact_match_competition_fk": """
        SELECT COUNT(*) FROM etl_kio.matches m
        LEFT JOIN etl_kio.competitions c ON m.competition_id = c.id
        WHERE c.id IS NULL
    """,

    # Rules
    "standings_points_consistency": """
        SELECT COUNT(*) FROM etl_kio.standings
        WHERE points IS NOT NULL AND won IS NOT NULL AND draw IS NOT NULL AND (won*3 + draw) != points
    """,
}

L_CHECKS = {
    "mart_kpi_rate_out_of_bounds": """
        SELECT COUNT(*) FROM etl_kio.matches WHERE matchday < 0 OR matchday > 60
    """,
    "mart_kpi_missing_dates": """
        SELECT COUNT(*) FROM etl_kio.matches WHERE utcDate IS NULL
    """,
    "mart_duplicate_standings": """
        SELECT COUNT(*) FROM (
            SELECT competition_id, season_id, team_id, COUNT(*) AS cnt
            FROM etl_kio.standings
            GROUP BY competition_id, season_id, team_id
            HAVING COUNT(*) > 1
        ) d
    """,
}


def run_stage_validation_gx_spark(
    *,
    spark: SparkSession,
    dag_id: str,
    stage: str,
    targets: list[Any],
    output_dir: Path,
    layer: str = "STG",
) -> list[dict[str, Any]]:
    stage = stage.strip().upper()
    output_dir = tool_output_dir(output_dir, "gx_spark")
    output_dir.mkdir(parents=True, exist_ok=True)

    check_map = {
        "E": STG_CHECKS,
        "T": {**STG_CHECKS, **DDS_CHECKS},
        "L": L_CHECKS,
    }.get(stage, {})

    reports = []
    for t in targets:
        run_id = getattr(t, "run_id", "spark_run")
        tag = _now_tag()
        results = []
        total_failed = 0
        total_checks = len(check_map)
        spark_sec_total = 0.0 

        for check_name, sql in check_map.items():
            start = time.time()
            try:
                row = spark.sql(sql).collect()[0]
                failed_rows = int(row[0]) if row[0] is not None else 0
                check_status = "PASS" if failed_rows == 0 else "FAIL"
                if failed_rows > 0:
                    total_failed += 1
            except Exception as e:
                check_status = "FAIL"
                failed_rows = -1
                logger.warning("GX Spark check %s failed: %s", check_name, e)
            duration = time.time() - start
            spark_sec_total += duration
            results.append({
                "check": check_name,
                "status": check_status,
                "failed_rows": failed_rows,
                "duration_sec": round(duration, 3),
            })

        overall = "SUCCESS" if total_failed == 0 else "FAILED"
        report_data = {
            "run_id": run_id,
            "stage": stage,
            "status": overall,
            "checks_total": total_checks,
            "checks_failed": total_failed,
            "spark_sec": round(spark_sec_total, 3),
            "results": results,
        }
        out_path = output_dir / f"gx_spark_{stage.lower()}_{tag}.json"
        out_path.write_text(json.dumps(report_data, indent=2, ensure_ascii=False))
        reports.append({
            "run_id": run_id,
            "stage": stage,
            "status": overall,
            "report_path": str(out_path),
            "error": None,
            "checks_total": total_checks,
            "checks_failed": total_failed,
            "spark_sec": round(spark_sec_total, 3),
        })
    return reports