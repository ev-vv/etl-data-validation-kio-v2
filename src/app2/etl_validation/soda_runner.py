from __future__ import annotations

import json
import re
import time
import traceback
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from soda.scan import Scan
from sqlalchemy.engine import Engine

from app2.core.config import load_settings
from app2.db.batch import log_batch_status
from app2.db.connection import get_engine
from app2.db.validation_metrics import finish_validation_run, log_validation_check, start_validation_run
from app2.etl_validation.discovery import StageTarget
from app2.etl_validation.resource_metrics import build_resource_summary, capture_resource_snapshot
from app2.etl_validation.specs import build_stage_checks
from app2.post_validation.paths import tool_output_dir


@dataclass(frozen=True)
class SodaStageReport:
    run_id: str
    parent_run_id: str
    stage: str
    kind: str
    status: str
    report_path: str | None
    error: str | None = None


def _now_tag() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _sanitize(value: str) -> str:
    v = re.sub(r"[^a-zA-Z0-9._-]+", "_", (value or "").strip())
    return v or "id"


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


def _build_checks_yaml(stage: str, run_id: str) -> str:
    checks = build_stage_checks(stage, run_id)
    lines = ["checks:"]
    for spec in checks:
        lines.append("  - failed rows:")
        lines.append(f"      name: {spec.name}")
        lines.append("      fail query: |")
        for sql_line in spec.fail_sql.splitlines():
            lines.append(f"        {sql_line}")
    return "\n".join(lines)


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


def run_stage_validation_soda(
    *,
    dag_id: str,
    stage: str,
    targets: list[StageTarget],
    output_dir: Path,
    layer: str,
    engine: Engine | None = None,
) -> list[SodaStageReport]:
    stage = stage.strip().upper()
    if engine is None:
        engine = get_engine()
    output_dir = tool_output_dir(output_dir, "soda")
    output_dir.mkdir(parents=True, exist_ok=True)

    reports: list[SodaStageReport] = []
    for t in targets:
        report_path = None
        resource_start = capture_resource_snapshot()
        run_started = time.time()
        validation_run_id = None
        run_id = t.run_id
        parent_run_id = t.parent_run_id
        try:
            tag = _now_tag()
            safe_run = _sanitize(run_id)
            safe_kind = _sanitize(t.kind)
            target_dir = output_dir / f"{safe_kind}_{stage.lower()}_{safe_run}_{tag}"
            target_dir.mkdir(parents=True, exist_ok=True)

            validation_run_id = start_validation_run(
                engine,
                dag_id=dag_id,
                run_id=run_id,
                parent_run_id=parent_run_id,
                layer=layer,
                tool="soda",
                suite=f"{stage}_validation",
                kind=t.kind,
            )

            log_batch_status(
                engine,
                dag_id=dag_id,
                run_id=run_id,
                parent_run_id=parent_run_id,
                layer=layer,
                status="NEW",
            )
            log_batch_status(
                engine,
                dag_id=dag_id,
                run_id=run_id,
                parent_run_id=parent_run_id,
                layer=layer,
                status="PROCESSING",
            )

            scan = Scan()
            scan.set_data_source_name("postgres")
            scan.disable_telemetry()
            scan.add_configuration_yaml_str(_build_config_yaml(), file_path=f"soda_config_{safe_run}_{tag}.yml")
            scan.add_sodacl_yaml_str(_build_checks_yaml(stage, run_id), file_name=f"soda_checks_{safe_run}_{tag}")

            exit_code = scan.execute()
            results = scan.get_scan_results() or {}

            results_path = target_dir / f"soda_etl_{stage.lower()}_{safe_kind}_{safe_run}_{tag}.json"
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
                    etl_stage=stage,
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
                run_id=run_id,
                parent_run_id=parent_run_id,
                layer=layer,
                status=status,
                error_message=None if status == "SUCCESS" else f"SodaCL {stage} validation failed",
            )

            reports.append(
                SodaStageReport(
                    run_id=run_id,
                    parent_run_id=parent_run_id,
                    stage=stage,
                    kind=t.kind,
                    status=status,
                    report_path=report_path,
                )
            )
            if validation_run_id is not None:
                resource_summary = build_resource_summary(resource_start, capture_resource_snapshot())
                meta_json = {
                    "soda_exit_code": exit_code,
                    "checks_total": checks_total,
                    "checks_failed": checks_failed,
                }
                if resource_summary:
                    meta_json["resources"] = resource_summary
                finish_validation_run(
                    engine,
                    validation_run_id=validation_run_id,
                    status=status,
                    duration_ms=int((time.time() - run_started) * 1000),
                    checks_total=checks_total,
                    checks_failed=checks_failed,
                    report_path=report_path,
                    meta_json=meta_json,
                )
        except Exception:
            err = traceback.format_exc()
            try:
                log_batch_status(
                    engine,
                    dag_id=dag_id,
                    run_id=run_id,
                    parent_run_id=parent_run_id,
                    layer=layer,
                    status="FAILED",
                    error_message=f"SodaCL {stage} validation error",
                )
            except Exception:
                pass
            reports.append(
                SodaStageReport(
                    run_id=run_id,
                    parent_run_id=parent_run_id,
                    stage=stage,
                    kind=t.kind,
                    status="FAILED",
                    report_path=report_path,
                    error=err,
                )
            )
            if validation_run_id is not None:
                resource_summary = build_resource_summary(resource_start, capture_resource_snapshot())
                meta_json = {"error": err}
                if resource_summary:
                    meta_json["resources"] = resource_summary
                finish_validation_run(
                    engine,
                    validation_run_id=validation_run_id,
                    status="FAILED",
                    duration_ms=int((time.time() - run_started) * 1000),
                    checks_total=0,
                    checks_failed=0,
                    report_path=report_path,
                    meta_json=meta_json,
                )
    return reports


__all__ = ["SodaStageReport", "run_stage_validation_soda"]
