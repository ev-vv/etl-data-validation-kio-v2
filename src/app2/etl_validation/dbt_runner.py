from __future__ import annotations

import json
import os
import re
import subprocess
import traceback
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import sys

from sqlalchemy.engine import Engine

from app2.core.config import load_settings
from app2.db.batch import log_batch_status
from app2.db.connection import get_engine
from app2.db.validation_metrics import finish_validation_run, log_validation_check, start_validation_run
from app2.etl_validation.discovery import StageTarget
from app2.etl_validation.resource_metrics import build_resource_summary, capture_resource_snapshot
from app2.post_validation.paths import tool_output_dir


@dataclass(frozen=True)
class DbtStageReport:
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


def _build_env() -> dict[str, str]:
    settings = load_settings()
    env = os.environ.copy()
    env["POSTGRES_DB"] = settings.postgres_db
    env["POSTGRES_USER"] = settings.postgres_user
    env["POSTGRES_PASSWORD"] = settings.postgres_password
    env["POSTGRES_HOST"] = settings.postgres_host
    env["POSTGRES_PORT"] = settings.postgres_port
    return env


def _parse_run_results(path: Path) -> dict[str, object]:
    if not path.exists():
        return {"total": 0, "failed": 0, "failed_tests": []}
    data = json.loads(path.read_text(encoding="utf-8"))
    results = data.get("results", []) or []
    failed = [
        r
        for r in results
        if str(r.get("status", "")).lower() in {"fail", "error"}
    ]
    failed_tests = [r.get("unique_id") for r in failed if r.get("unique_id")]
    return {
        "total": len(results),
        "failed": len(failed),
        "failed_tests": failed_tests,
        "generated_at": data.get("metadata", {}).get("generated_at"),
        "invocation_id": data.get("metadata", {}).get("invocation_id"),
    }


def _run_dbt(args: list[str], *, env: dict[str, str]) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            ["dbt", *args],
            capture_output=True,
            text=True,
            env=env,
        )
    except FileNotFoundError:
        return subprocess.run(
            [sys.executable, "-m", "dbt.cli.main", *args],
            capture_output=True,
            text=True,
            env=env,
        )


def run_stage_validation_dbt(
    *,
    dag_id: str,
    stage: str,
    targets: list[StageTarget],
    output_dir: Path,
    layer: str,
    engine: Engine | None = None,
) -> list[DbtStageReport]:
    stage = stage.strip().upper()
    if engine is None:
        engine = get_engine()
    env = _build_env()

    project_dir = Path(__file__).resolve().parents[1] / "post_validation" / "dbt"
    profiles_dir = project_dir

    output_dir = tool_output_dir(output_dir, "dbt")
    output_dir.mkdir(parents=True, exist_ok=True)

    reports: list[DbtStageReport] = []
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
            log_dir = target_dir / "logs"
            target_dir.mkdir(parents=True, exist_ok=True)
            log_dir.mkdir(parents=True, exist_ok=True)

            validation_run_id = start_validation_run(
                engine,
                dag_id=dag_id,
                run_id=run_id,
                parent_run_id=parent_run_id,
                layer=layer,
                tool="dbt",
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

            args = [
                "test",
                "--project-dir",
                str(project_dir),
                "--profiles-dir",
                str(profiles_dir),
                "--target-path",
                str(target_dir),
                "--log-path",
                str(log_dir),
                "--vars",
                json.dumps({"run_id": run_id}),
                "--select",
                f"tag:stage_{stage}",
            ]
            result = _run_dbt(args, env=env)

            (target_dir / "dbt_stdout.log").write_text(result.stdout or "", encoding="utf-8")
            (target_dir / "dbt_stderr.log").write_text(result.stderr or "", encoding="utf-8")

            run_results_path = target_dir / "run_results.json"
            summary = _parse_run_results(run_results_path)
            (target_dir / "summary.json").write_text(
                json.dumps(summary, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            report_path = str(run_results_path) if run_results_path.exists() else str(target_dir)

            checks_total = int(summary.get("total", 0))
            checks_failed = int(summary.get("failed", 0))
            if run_results_path.exists():
                data = json.loads(run_results_path.read_text(encoding="utf-8"))
                for item in data.get("results", []) or []:
                    status_raw = str(item.get("status", "")).lower()
                    if status_raw == "pass":
                        check_status = "PASS"
                    elif status_raw == "warn":
                        check_status = "WARN"
                    elif status_raw == "fail":
                        check_status = "FAIL"
                    else:
                        check_status = "ERROR"
                    log_validation_check(
                        engine,
                        validation_run_id=validation_run_id,
                        check_name=item.get("unique_id") or item.get("name") or "dbt_test",
                        rule_type=item.get("resource_type"),
                        etl_stage=stage,
                        status=check_status,
                        severity=(item.get("config") or {}).get("severity"),
                        duration_ms=int(float(item.get("execution_time") or 0) * 1000),
                        rows_failed=item.get("failures"),
                        observed_value=str(item.get("failures")) if item.get("failures") is not None else None,
                        expected_value="0",
                        message=item.get("message"),
                        details_json={
                            "unique_id": item.get("unique_id"),
                            "name": item.get("name"),
                            "status": item.get("status"),
                            "failures": item.get("failures"),
                            "execution_time": item.get("execution_time"),
                        },
                    )

            status = "SUCCESS" if result.returncode == 0 else "FAILED"
            log_batch_status(
                engine,
                dag_id=dag_id,
                run_id=run_id,
                parent_run_id=parent_run_id,
                layer=layer,
                status=status,
                error_message=None if status == "SUCCESS" else f"dbt {stage} validation failed",
            )

            reports.append(
                DbtStageReport(
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
                meta_json = {"dbt_summary": summary}
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
                    error_message=f"dbt {stage} validation error",
                )
            except Exception:
                pass
            reports.append(
                DbtStageReport(
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


__all__ = ["DbtStageReport", "run_stage_validation_dbt"]
