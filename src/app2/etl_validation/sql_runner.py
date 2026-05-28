from __future__ import annotations

import json
import re
import time
import traceback
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.engine import Engine

from app2.db.batch import log_batch_status
from app2.db.connection import get_engine
from app2.db.validation_metrics import finish_validation_run, log_validation_check, start_validation_run
from app2.etl_validation.discovery import StageTarget
from app2.etl_validation.resource_metrics import build_resource_summary, capture_resource_snapshot
from app2.etl_validation.specs import build_constraint_checks
from app2.post_validation.paths import tool_output_dir


@dataclass(frozen=True)
class SqlConstraintStageReport:
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


def _scalar_int(engine: Engine, sql: str) -> int:
    with engine.connect() as conn:
        value = conn.execute(text(sql)).scalar()
    return int(value or 0)


def run_stage_validation_sql(
    *,
    dag_id: str,
    stage: str,
    targets: list[StageTarget],
    output_dir: Path,
    layer: str,
    engine: Engine | None = None,
) -> list[SqlConstraintStageReport]:
    stage = stage.strip().upper()
    if engine is None:
        engine = get_engine()
    output_dir = tool_output_dir(output_dir, "sql")
    output_dir.mkdir(parents=True, exist_ok=True)

    reports: list[SqlConstraintStageReport] = []
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
                tool="sql",
                suite=f"{stage}_constraints",
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

            checks = build_constraint_checks(stage, run_id)
            results: list[dict[str, object]] = []
            checks_failed = 0
            for spec in checks:
                count = _scalar_int(engine, spec.count_sql)
                status = "PASS" if count == 0 else "FAIL"
                if status != "PASS":
                    checks_failed += 1
                results.append(
                    {
                        "name": spec.name,
                        "rule_group": spec.rule_group,
                        "severity": spec.severity,
                        "status": status,
                        "rows_failed": count,
                        "count_sql": spec.count_sql,
                        "fail_sql": spec.fail_sql,
                    }
                )
                log_validation_check(
                    engine,
                    validation_run_id=validation_run_id,
                    check_name=spec.name,
                    rule_type=spec.rule_group,
                    etl_stage=stage,
                    status=status,
                    severity=spec.severity,
                    rows_failed=count,
                    observed_value=str(count),
                    expected_value="0",
                    message=None if status == "PASS" else "Constraint violation",
                    details_json={"count_sql": spec.count_sql, "fail_sql": spec.fail_sql},
                )

            results_path = target_dir / f"sql_constraints_{stage.lower()}_{safe_kind}_{safe_run}_{tag}.json"
            results_path.write_text(
                json.dumps({"checks": results}, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            report_path = str(results_path)

            status = "SUCCESS" if checks_failed == 0 else "FAILED"
            log_batch_status(
                engine,
                dag_id=dag_id,
                run_id=run_id,
                parent_run_id=parent_run_id,
                layer=layer,
                status=status,
                error_message=None if status == "SUCCESS" else f"SQL constraints {stage} validation failed",
            )

            reports.append(
                SqlConstraintStageReport(
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
                    "checks_total": len(checks),
                    "checks_failed": checks_failed,
                }
                if resource_summary:
                    meta_json["resources"] = resource_summary
                finish_validation_run(
                    engine,
                    validation_run_id=validation_run_id,
                    status=status,
                    duration_ms=int((time.time() - run_started) * 1000),
                    checks_total=len(checks),
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
                    error_message=f"SQL constraints {stage} validation error",
                )
            except Exception:
                pass
            reports.append(
                SqlConstraintStageReport(
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


__all__ = ["SqlConstraintStageReport", "run_stage_validation_sql"]
