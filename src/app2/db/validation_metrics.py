from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from sqlalchemy import bindparam, text
from sqlalchemy.engine import Engine


def resolve_validation_kind(run_id: str | None) -> str | None:
    if not run_id:
        return None
    return "experiment" if str(run_id).startswith("exp_") else "baseline"


def start_validation_run(
    engine: Engine,
    *,
    dag_id: str,
    run_id: str,
    parent_run_id: str,
    layer: str,
    tool: str,
    suite: str | None = None,
    kind: str | None = None,
    status: str = "PROCESSING",
    started_at: datetime | None = None,
    config_hash: str | None = None,
    meta_json: dict[str, Any] | None = None,
) -> int:
    with engine.begin() as conn:
        row = conn.execute(
            text(
                """
                INSERT INTO tech.validation_run (
                    dag_id, run_id, parent_run_id, layer, tool, suite, kind,
                    status, started_at, config_hash, meta_json
                )
                VALUES (
                    :dag_id, :run_id, :parent_run_id, :layer, :tool, :suite, :kind,
                    :status, :started_at, :config_hash, :meta_json
                )
                RETURNING validation_run_id
                """
            ),
            {
                "dag_id": dag_id,
                "run_id": run_id,
                "parent_run_id": parent_run_id,
                "layer": layer,
                "tool": tool,
                "suite": suite,
                "kind": kind,
                "status": status,
                "started_at": started_at or datetime.now(),
                "config_hash": config_hash,
                "meta_json": json.dumps(meta_json) if meta_json else None,
            },
        ).scalar_one()
    return int(row)


def finish_validation_run(
    engine: Engine,
    *,
    validation_run_id: int,
    status: str,
    finished_at: datetime | None = None,
    duration_ms: int | None = None,
    checks_total: int | None = None,
    checks_failed: int | None = None,
    rows_checked: int | None = None,
    rows_failed: int | None = None,
    report_path: str | None = None,
    meta_json: dict[str, Any] | None = None,
) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                UPDATE tech.validation_run
                SET status = :status,
                    finished_at = :finished_at,
                    duration_ms = :duration_ms,
                    checks_total = COALESCE(:checks_total, checks_total),
                    checks_failed = COALESCE(:checks_failed, checks_failed),
                    rows_checked = COALESCE(:rows_checked, rows_checked),
                    rows_failed = COALESCE(:rows_failed, rows_failed),
                    report_path = COALESCE(:report_path, report_path),
                    meta_json = COALESCE(:meta_json, meta_json)
                WHERE validation_run_id = :validation_run_id
                """
            ),
            {
                "validation_run_id": validation_run_id,
                "status": status,
                "finished_at": finished_at or datetime.now(),
                "duration_ms": duration_ms,
                "checks_total": checks_total,
                "checks_failed": checks_failed,
                "rows_checked": rows_checked,
                "rows_failed": rows_failed,
                "report_path": report_path,
                "meta_json": json.dumps(meta_json) if meta_json else None,
            },
        )


def log_validation_check(
    engine: Engine,
    *,
    validation_run_id: int,
    check_name: str,
    status: str,
    severity: str | None = None,
    rule_type: str | None = None,
    etl_stage: str | None = None,
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
    duration_ms: int | None = None,
    rows_failed: int | None = None,
    observed_value: str | None = None,
    expected_value: str | None = None,
    message: str | None = None,
    details_json: dict[str, Any] | None = None,
) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO tech.validation_check_result (
                    validation_run_id, check_name, rule_type, etl_stage,
                    status, severity, started_at, finished_at, duration_ms,
                    rows_failed, observed_value, expected_value, message, details_json
                )
                VALUES (
                    :validation_run_id, :check_name, :rule_type, :etl_stage,
                    :status, :severity, :started_at, :finished_at, :duration_ms,
                    :rows_failed, :observed_value, :expected_value, :message, :details_json
                )
                """
            ),
            {
                "validation_run_id": validation_run_id,
                "check_name": check_name,
                "rule_type": rule_type,
                "etl_stage": etl_stage,
                "status": status,
                "severity": severity,
                "started_at": started_at or datetime.now(),
                "finished_at": finished_at,
                "duration_ms": duration_ms,
                "rows_failed": rows_failed,
                "observed_value": observed_value,
                "expected_value": expected_value,
                "message": message,
                "details_json": json.dumps(details_json) if details_json else None,
            },
        )


def delete_validation_runs_for_layer(
    engine: Engine,
    *,
    dag_id: str,
    layer: str,
    run_ids: list[str] | None = None,
) -> None:
    with engine.begin() as conn:
        if run_ids:
            conn.execute(
                text(
                    """
                    DELETE FROM tech.validation_run
                    WHERE dag_id = :dag_id
                      AND layer = :layer
                      AND run_id IN :run_ids
                    """
                ).bindparams(bindparam("run_ids", expanding=True)),
                {
                    "dag_id": dag_id,
                    "layer": layer,
                    "run_ids": run_ids,
                },
            )
        else:
            conn.execute(
                text(
                    """
                    DELETE FROM tech.validation_run
                    WHERE dag_id = :dag_id
                      AND layer = :layer
                    """
                ),
                {
                    "dag_id": dag_id,
                    "layer": layer,
                },
            )
