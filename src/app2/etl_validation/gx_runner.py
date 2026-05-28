from __future__ import annotations

import re
import time
import traceback
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import great_expectations as gx
from great_expectations.render.renderer import ValidationResultsPageRenderer
from great_expectations.render.view import DefaultJinjaPageView

from sqlalchemy import text
from sqlalchemy.engine import Engine

from app2.core.config import load_settings
from app2.db.batch import log_batch_status
from app2.db.connection import get_engine
from app2.db.validation_metrics import finish_validation_run, log_validation_check, start_validation_run
from app2.etl_validation.discovery import StageTarget
from app2.etl_validation.resource_metrics import build_resource_summary, capture_resource_snapshot
from app2.etl_validation.specs import build_metrics_query, build_stage_checks
from app2.post_validation.paths import tool_output_dir


@dataclass(frozen=True)
class StageValidationReport:
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


def _get_expectation_type(item: Any) -> str | None:
    cfg = getattr(item, "expectation_config", None)
    if cfg is None:
        return None
    exp_type = getattr(cfg, "expectation_type", None)
    if exp_type:
        return exp_type
    try:
        cfg_dict = cfg.to_json_dict()
        return cfg_dict.get("expectation_type")
    except Exception:
        return None


def _add_postgres_datasource(ctx: Any, conn_str: str):
    if hasattr(ctx, "data_sources"):
        return ctx.data_sources.add_postgres(name="postgres", connection_string=conn_str)
    if hasattr(ctx, "sources"):
        return ctx.sources.add_postgres(name="postgres", connection_string=conn_str)
    datasources = getattr(ctx, "datasources", None)
    if datasources is not None and hasattr(datasources, "add_postgres"):
        return datasources.add_postgres(name="postgres", connection_string=conn_str)
    raise AttributeError("Great Expectations context has no datasource factory")


def _fetch_metrics_row(engine: Any, *, stage: str, run_id: str) -> dict[str, Any] | None:
    query = build_metrics_query(stage, run_id)
    with engine.connect() as conn:
        row = conn.execute(text(query)).mappings().first()
    return dict(row) if row else None


def run_stage_validation_gx(
    *,
    dag_id: str,
    stage: str,
    targets: list[StageTarget],
    output_dir: Path,
    layer: str,
    engine: Engine | None = None,
    gx_context: Any | None = None,
    gx_datasource: Any | None = None,
) -> list[StageValidationReport]:
    stage = stage.strip().upper()
    settings = load_settings()
    if engine is None:
        engine = get_engine()

    if gx_context is None or gx_datasource is None:
        conn_str = (
            f"postgresql+psycopg2://{settings.postgres_user}:{settings.postgres_password}"
            f"@{settings.postgres_host}:{settings.postgres_port}/{settings.postgres_db}"
        )
        ctx = gx.get_context(mode="ephemeral")
        datasource = _add_postgres_datasource(ctx, conn_str)
    else:
        ctx = gx_context
        datasource = gx_datasource
    
    output_dir = tool_output_dir(output_dir, "gx")
    output_dir.mkdir(parents=True, exist_ok=True)

    reports: list[StageValidationReport] = []
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

            validation_run_id = start_validation_run(
                engine,
                dag_id=dag_id,
                run_id=run_id,
                parent_run_id=parent_run_id,
                layer=layer,
                tool="gx",
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

            checks = build_stage_checks(stage, run_id)
            metrics_query = build_metrics_query(stage, run_id)
            asset = datasource.add_query_asset(name=f"metrics_{stage}_{safe_run}_{tag}", query=metrics_query)
            batch_request = asset.build_batch_request()
            suite_name = f"etl_{stage}_{safe_run}_{tag}"
            v = ctx.get_validator(batch_request=batch_request, create_expectation_suite_with_name=suite_name)

            for spec in checks:
                v.expect_column_values_to_be_in_set(spec.name, value_set=[0])

            result = v.validate()
            status = "SUCCESS" if result.success else "FAILED"
            metrics_row = _fetch_metrics_row(engine, stage=stage, run_id=run_id)

            results_by_metric: dict[str, Any] = {}
            for item in result.results:
                try:
                    column = item.expectation_config.kwargs.get("column")
                except Exception:
                    column = None
                if column:
                    results_by_metric[column] = item

            failed_checks = 0
            for spec in checks:
                item = results_by_metric.get(spec.name)
                ok = bool(item.success) if item else False
                if not ok:
                    failed_checks += 1
                row_value = metrics_row.get(spec.name) if metrics_row else None
                log_validation_check(
                    engine,
                    validation_run_id=validation_run_id,
                    check_name=spec.name,
                    rule_type=spec.rule_group,
                    etl_stage=stage,
                    status="PASS" if ok else "FAIL",
                    severity=spec.severity,
                    rows_failed=row_value if isinstance(row_value, int) else None,
                    observed_value=str(row_value) if row_value is not None else None,
                    expected_value="0",
                    message=None if ok else "Metric should be 0",
                    details_json={
                        "expectation_type": _get_expectation_type(item) if item else None,
                        "success": bool(item.success) if item else False,
                        "result": item.result if item else None,
                        "count_sql": spec.count_sql,
                    },
                )

            doc = ValidationResultsPageRenderer().render(result)
            html = DefaultJinjaPageView().render(doc)
            out_name = f"gx_etl_{stage.lower()}_{_sanitize(t.kind)}_{safe_run}_{tag}.html"
            out_path = output_dir / out_name
            out_path.write_text(html, encoding="utf-8")
            report_path = str(out_path)

            log_batch_status(
                engine,
                dag_id=dag_id,
                run_id=run_id,
                parent_run_id=parent_run_id,
                layer=layer,
                status=status,
                error_message=None if status == "SUCCESS" else f"GX {stage} validation failed",
            )

            reports.append(
                StageValidationReport(
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
                meta_json = {"gx_statistics": getattr(result, "statistics", None)}
                if resource_summary:
                    meta_json["resources"] = resource_summary
                finish_validation_run(
                    engine,
                    validation_run_id=validation_run_id,
                    status=status,
                    duration_ms=int((time.time() - run_started) * 1000),
                    checks_total=len(checks),
                    checks_failed=failed_checks,
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
                    error_message=f"GX {stage} validation error",
                )
            except Exception:
                pass
            reports.append(
                StageValidationReport(
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


__all__ = ["StageValidationReport", "run_stage_validation_gx"]
