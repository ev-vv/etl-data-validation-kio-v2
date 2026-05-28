from __future__ import annotations

import json
import re
import time
import traceback
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from soda.scan import Scan

from app2.core.config import load_settings
from app2.db.batch import log_batch_status
from app2.db.connection import get_engine
from app2.db.validation_metrics import finish_validation_run, log_validation_check, start_validation_run
from app2.post_validation.discovery import PostValidationTarget
from app2.post_validation.paths import tool_output_dir


@dataclass(frozen=True)
class SodaPostValidationReport:
    dds_run_id: str
    stg_run_id: str
    kind: str
    status: str
    report_path: str | None
    error: str | None = None


def _now_tag() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _sanitize(value: str) -> str:
    v = re.sub(r"[^a-zA-Z0-9._-]+", "_", (value or "").strip())
    return v or "id"


def _sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _build_config_yaml() -> str:
    settings = load_settings()
    return (
        "data_source postgres:\n"
        "  type: postgres\n"
        f"  host: {settings.postgres_host}\n"
        f"  port: {settings.postgres_port}\n"
        f"  username: {settings.postgres_user}\n"
        f"  password: {settings.postgres_password}\n"
        f"  database: {settings.postgres_db}\n"
    )


def _build_checks_yaml(dds_run_id: str) -> str:
    rid = _sql_quote(dds_run_id)
    return f"""
checks:
  - failed rows:
      name: dds_match_same_team
      fail query: |
        SELECT *
        FROM dds.fact_match
        WHERE run_id = {rid}
          AND home_team_id IS NOT NULL
          AND away_team_id IS NOT NULL
          AND home_team_id = away_team_id
  - failed rows:
      name: dds_match_null_competition_id
      fail query: |
        SELECT *
        FROM dds.fact_match
        WHERE run_id = {rid}
          AND competition_id IS NULL
  - failed rows:
      name: dds_match_null_season_id
      fail query: |
        SELECT *
        FROM dds.fact_match
        WHERE run_id = {rid}
          AND season_id IS NULL
  - failed rows:
      name: dds_standings_points_inconsistent
      fail query: |
        SELECT *
        FROM dds.fact_standing
        WHERE run_id = {rid}
          AND points IS NOT NULL
          AND won IS NOT NULL
          AND draw IS NOT NULL
          AND points <> (won * 3 + draw)
  - failed rows:
      name: mart_kpi_rate_out_of_bounds
      fail query: |
        SELECT *
        FROM mart.v_competition_season_kpi
        WHERE run_id = {rid}
          AND (
            home_win_rate < 0 OR home_win_rate > 1 OR
            draw_rate < 0 OR draw_rate > 1 OR
            away_win_rate < 0 OR away_win_rate > 1
          )
  - failed rows:
      name: mart_kpi_missing_dates
      fail query: |
        SELECT *
        FROM mart.v_competition_season_kpi
        WHERE run_id = {rid}
          AND (start_date IS NULL OR end_date IS NULL OR season_year IS NULL)
  - failed rows:
      name: mart_team_missing_dates
      fail query: |
        SELECT *
        FROM mart.v_team_season_results
        WHERE run_id = {rid}
          AND (start_date IS NULL OR end_date IS NULL OR season_year IS NULL)
  - failed rows:
      name: mart_team_points_inconsistent
      fail query: |
        SELECT *
        FROM mart.v_team_season_results
        WHERE run_id = {rid}
          AND points_calc <> (wins * 3 + draws)
  - failed rows:
      name: mart_team_matches_inconsistent
      fail query: |
        SELECT *
        FROM mart.v_team_season_results
        WHERE run_id = {rid}
          AND matches_played <> (wins + draws + losses)
  - failed rows:
      name: mart_team_negative_values
      fail query: |
        SELECT *
        FROM mart.v_team_season_results
        WHERE run_id = {rid}
          AND (points_calc < 0 OR goals_for < 0 OR goals_against < 0)
""".strip()


def _map_outcome(outcome: str | None) -> str:
    if outcome == "pass":
        return "PASS"
    if outcome == "warn":
        return "WARN"
    if outcome == "fail":
        return "FAIL"
    return "ERROR"


def _map_severity(outcome: str | None) -> str | None:
    if outcome == "warn":
        return "warn"
    if outcome == "fail":
        return "error"
    return None


def run_post_validation_soda(
    *,
    dag_id: str,
    targets: list[PostValidationTarget],
    output_dir: Path,
    layer: str = "POST_SODA",
) -> list[SodaPostValidationReport]:
    engine = get_engine()
    output_dir = tool_output_dir(output_dir, "soda")
    output_dir.mkdir(parents=True, exist_ok=True)

    reports: list[SodaPostValidationReport] = []
    for t in targets:
        report_path = None
        run_started = time.time()
        validation_run_id = None
        try:
            tag = _now_tag()
            safe_dds = _sanitize(t.dds_run_id)
            safe_kind = _sanitize(t.kind)
            target_dir = output_dir / f"{safe_kind}_{safe_dds}_{tag}"
            target_dir.mkdir(parents=True, exist_ok=True)

            validation_run_id = start_validation_run(
                engine,
                dag_id=dag_id,
                run_id=t.dds_run_id,
                parent_run_id=t.stg_run_id,
                layer=layer,
                tool="soda",
                suite="post_validation",
                kind=t.kind,
            )

            log_batch_status(
                engine,
                dag_id=dag_id,
                run_id=t.dds_run_id,
                parent_run_id=t.stg_run_id,
                layer=layer,
                status="NEW",
            )
            log_batch_status(
                engine,
                dag_id=dag_id,
                run_id=t.dds_run_id,
                parent_run_id=t.stg_run_id,
                layer=layer,
                status="PROCESSING",
            )

            scan = Scan()
            scan.set_data_source_name("postgres")
            scan.disable_telemetry()
            scan.add_configuration_yaml_str(_build_config_yaml(), file_path=f"soda_config_{safe_dds}_{tag}.yml")
            scan.add_sodacl_yaml_str(_build_checks_yaml(t.dds_run_id), file_name=f"soda_checks_{safe_dds}_{tag}")

            exit_code = scan.execute()
            results = scan.get_scan_results() or {}

            results_path = target_dir / f"soda_post_validation_{safe_kind}_{safe_dds}_{tag}.json"
            results_path.write_text(
                json.dumps(results, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            report_path = str(results_path)

            logs_text = scan.get_logs_text()
            if logs_text:
                (target_dir / "soda_logs.txt").write_text(logs_text, encoding="utf-8")

            checks = results.get("checks") or []
            checks_total = len(checks)
            checks_failed = 0
            for check in checks:
                outcome_raw = str(check.get("outcome") or "").lower()
                status = _map_outcome(outcome_raw)
                if status == "FAIL":
                    checks_failed += 1

                diagnostics = check.get("diagnostics") or {}
                value = diagnostics.get("value") if isinstance(diagnostics, dict) else None
                rows_failed = int(value) if isinstance(value, (int, float)) else None

                log_validation_check(
                    engine,
                    validation_run_id=validation_run_id,
                    check_name=check.get("name") or check.get("definition") or "soda_check",
                    rule_type=check.get("type") or "failed_rows",
                    etl_stage="POST",
                    status=status,
                    severity=_map_severity(outcome_raw),
                    rows_failed=rows_failed,
                    observed_value=str(value) if value is not None else None,
                    expected_value="0",
                    message=None if status == "PASS" else "Failed rows check",
                    details_json={
                        "outcome": outcome_raw,
                        "table": check.get("table"),
                        "filter": check.get("filter"),
                        "definition": check.get("definition"),
                    },
                )

            status = "SUCCESS" if exit_code == 0 else "FAILED"
            log_batch_status(
                engine,
                dag_id=dag_id,
                run_id=t.dds_run_id,
                parent_run_id=t.stg_run_id,
                layer=layer,
                status=status,
                error_message=None if status == "SUCCESS" else "SodaCL post-validation failed",
            )

            reports.append(
                SodaPostValidationReport(
                    dds_run_id=t.dds_run_id,
                    stg_run_id=t.stg_run_id,
                    kind=t.kind,
                    status=status,
                    report_path=report_path,
                )
            )
            if validation_run_id is not None:
                finish_validation_run(
                    engine,
                    validation_run_id=validation_run_id,
                    status=status,
                    duration_ms=int((time.time() - run_started) * 1000),
                    checks_total=checks_total,
                    checks_failed=checks_failed,
                    report_path=report_path,
                    meta_json={
                        "soda_exit_code": exit_code,
                        "checks_total": checks_total,
                        "checks_failed": checks_failed,
                    },
                )
        except Exception:
            err = traceback.format_exc()
            try:
                log_batch_status(
                    engine,
                    dag_id=dag_id,
                    run_id=t.dds_run_id,
                    parent_run_id=t.stg_run_id,
                    layer=layer,
                    status="FAILED",
                    error_message="SodaCL post-validation error",
                )
            except Exception:
                pass
            reports.append(
                SodaPostValidationReport(
                    dds_run_id=t.dds_run_id,
                    stg_run_id=t.stg_run_id,
                    kind=t.kind,
                    status="FAILED",
                    report_path=report_path,
                    error=err,
                )
            )
            if validation_run_id is not None:
                finish_validation_run(
                    engine,
                    validation_run_id=validation_run_id,
                    status="FAILED",
                    duration_ms=int((time.time() - run_started) * 1000),
                    checks_total=0,
                    checks_failed=0,
                    report_path=report_path,
                    meta_json={"error": err},
                )
    return reports
