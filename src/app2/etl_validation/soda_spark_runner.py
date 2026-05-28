
from __future__ import annotations

import logging, time, json
from datetime import datetime
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession
from app2.post_validation.paths import tool_output_dir

logger = logging.getLogger(__name__)

SODA_CHECKS = {
    "E": {
        "stg_missing_match_id": "SELECT COUNT(*) FROM etl_kio.matches WHERE id IS NULL",
        "stg_matchday_out_of_range": "SELECT COUNT(*) FROM etl_kio.matches WHERE matchday IS NOT NULL AND (matchday < 0 OR matchday > 60)",
        "stg_duplicate_match_id": "SELECT COUNT(*) FROM (SELECT id, COUNT(*) AS cnt FROM etl_kio.matches WHERE id IS NOT NULL GROUP BY id HAVING COUNT(*) > 1) d",
        "areas_completeness": "SELECT COUNT(*) FROM etl_kio.areas WHERE id IS NULL OR name IS NULL",
        "competitions_completeness": "SELECT COUNT(*) FROM etl_kio.competitions WHERE id IS NULL OR name IS NULL",
        "teams_completeness": "SELECT COUNT(*) FROM etl_kio.teams WHERE id IS NULL OR name IS NULL",
        "matches_completeness": "SELECT COUNT(*) FROM etl_kio.matches WHERE id IS NULL OR utcDate IS NULL OR home_team_id IS NULL OR away_team_id IS NULL",
    },
    "T": {
        "dds_duplicate_fact_match": "SELECT COUNT(*) FROM (SELECT id, COUNT(*) AS cnt FROM etl_kio.matches GROUP BY id HAVING COUNT(*) > 1) d",
        "dds_missing_home_away_team": "SELECT COUNT(*) FROM etl_kio.matches WHERE home_team_id IS NULL OR away_team_id IS NULL",
        "dds_referential_integrity_violation": """
            SELECT COUNT(*) FROM (
                SELECT m.id FROM etl_kio.matches m LEFT JOIN etl_kio.competitions c ON c.id = m.competition_id WHERE c.id IS NULL
                UNION ALL
                SELECT m.id FROM etl_kio.matches m LEFT JOIN etl_kio.teams ht ON ht.id = m.home_team_id WHERE m.home_team_id IS NOT NULL AND ht.id IS NULL
                UNION ALL
                SELECT m.id FROM etl_kio.matches m LEFT JOIN etl_kio.teams at ON at.id = m.away_team_id WHERE m.away_team_id IS NOT NULL AND at.id IS NULL
            ) d
        """,
        "match_home_away_diff": "SELECT COUNT(*) FROM etl_kio.matches WHERE home_team_id = away_team_id AND home_team_id IS NOT NULL",
        "match_status_invalid": "SELECT COUNT(*) FROM etl_kio.matches WHERE status NOT IN ('SCHEDULED','TIMED','IN_PLAY','PAUSED','FINISHED','POSTPONED','SUSPENDED','CANCELED')",
    },
    "L": {
        "mart_kpi_rate_out_of_bounds": "SELECT COUNT(*) FROM etl_kio.standings WHERE won*1.0/playedGames < 0 OR won*1.0/playedGames > 1",
        "mart_kpi_missing_dates": "SELECT COUNT(*) FROM etl_kio.matches WHERE utcDate IS NULL",
        "mart_duplicate_team_rows": "SELECT COUNT(*) FROM (SELECT competition_id, season_id, team_id, COUNT(*) AS cnt FROM etl_kio.standings GROUP BY competition_id, season_id, team_id HAVING COUNT(*) > 1) d",
    },
}


def run_stage_validation_soda_spark(
    *,
    spark: SparkSession,
    dag_id: str,
    stage: str,
    targets: list[Any],
    output_dir: Path,
    layer: str = "STG",
) -> list[dict[str, Any]]:
    stage = stage.strip().upper()
    output_dir = tool_output_dir(output_dir, "soda_spark")
    output_dir.mkdir(parents=True, exist_ok=True)

    checks = SODA_CHECKS.get(stage, {})
    reports = []
    for t in targets:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        total_failed = 0
        results = []
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
                status = "FAIL"; failed_rows = -1
                logger.warning("Soda Spark check %s failed: %s", name, e)
            duration = time.time() - start
            spark_sec_total += duration
            results.append({"check": name, "status": status, "failed_rows": failed_rows, "duration_sec": round(duration, 3)})

        overall = "SUCCESS" if total_failed == 0 else "FAILED"
        report = {
            "run_id": getattr(t, "run_id", "spark_run"), "stage": stage,
            "status": overall, "checks_total": len(checks), "checks_failed": total_failed,
            "spark_sec": round(spark_sec_total, 3),
            "results": results,
        }
        out_path = output_dir / f"soda_spark_{stage.lower()}_{ts}.json"
        json.dump(report, out_path.open("w", encoding="utf-8"), indent=2, ensure_ascii=False)
        reports.append({
            "run_id": getattr(t, "run_id", "spark_run"), "stage": stage, "status": overall,
            "report_path": str(out_path), "error": None,
            "checks_total": len(checks), "checks_failed": total_failed,
            "spark_sec": round(spark_sec_total, 3),
        })
    return reports