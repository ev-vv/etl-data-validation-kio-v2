from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from app2.core.config import load_settings
from app2.db.batch import delete_batch_status_for_layer
from app2.db.connection import get_engine
from app2.db.validation_metrics import delete_validation_runs_for_layer
from app2.etl_validation.config import load_tools_experiment_config
from app2.etl_validation.discovery import discover_stage_targets
from app2.etl_validation.dbt_runner import run_stage_validation_dbt
from app2.etl_validation.gx_runner import _add_postgres_datasource, run_stage_validation_gx
from app2.etl_validation.sql_runner import run_stage_validation_sql
from app2.etl_validation.soda_runner import run_stage_validation_soda


def _layer_name(stage: str, tool: str) -> str:
    return f"{stage.upper()}_{tool.upper()}"


def _should_run_tool(tools_by_stage: dict[str, list[str]] | None, stage: str, tool: str) -> bool:
    if not tools_by_stage:
        return True
    tools = tools_by_stage.get(stage.upper())
    if not tools:
        return False
    return tool.lower() in {t.lower() for t in tools}


def run_stage_tool(
    *,
    stage: str,
    tool: str,
    config_path: str,
    output_dir: Path | None = None,
    dag_id: str | None = None,
) -> dict[str, Any]:
    stage = stage.strip().upper()
    tool = tool.strip().lower()
    cfg = load_tools_experiment_config(config_path)
    if not _should_run_tool(cfg.defaults.tools_by_stage, stage, tool):
        return {"stage": stage, "tool": tool, "status": "SKIPPED", "reason": "tool disabled in config"}

    engine = get_engine()
    layer = _layer_name(stage, tool)
    out_dir = Path(output_dir or cfg.defaults.output_dir)
    dag_id = dag_id or f"etl_stage_validation_{tool}"
    
    repeats = cfg.defaults.repeats
    override = os.environ.get("APP2_REPEATS_OVERRIDE")
    if override:
        try:
            repeats = int(override)
        except ValueError:
            pass
    if repeats < 1:
        repeats = 1
    
    only_unprocessed = cfg.defaults.only_unprocessed and repeats == 1
    targets = discover_stage_targets(
        engine,
        baseline_stg_run_id=cfg.baseline.stg_run_id,
        baseline_dds_run_id=cfg.baseline.dds_run_id,
        stage=stage,
        include_experiments=cfg.defaults.include_experiments,
        only_unprocessed=only_unprocessed,
        processed_layer=layer,
    )
    if not targets:
        return {"stage": stage, "tool": tool, "status": "EMPTY", "reason": "no targets"}

    run_ids = [t.run_id for t in targets]
    
    if repeats > 1:
        pass
    
    gx_context = None
    gx_datasource = None
    if tool == "gx":
        import great_expectations as gx
        settings = load_settings()
        conn_str = (
            f"postgresql+psycopg2://{settings.postgres_user}:{settings.postgres_password}"
            f"@{settings.postgres_host}:{settings.postgres_port}/{settings.postgres_db}"
        )
        gx_context = gx.get_context(mode="ephemeral")
        gx_datasource = _add_postgres_datasource(gx_context, conn_str)
    
    for repeat_num in range(1, repeats + 1):

        if tool == "gx":
            reports = run_stage_validation_gx(
                dag_id=dag_id,
                stage=stage,
                targets=targets,
                output_dir=out_dir,
                layer=layer,
                engine=engine,
                gx_context=gx_context,
                gx_datasource=gx_datasource,
            )
        elif tool == "soda":
            reports = run_stage_validation_soda(
                dag_id=dag_id,
                stage=stage,
                targets=targets,
                output_dir=out_dir,
                layer=layer,
                engine=engine,
            )
        elif tool == "dbt":
            reports = run_stage_validation_dbt(
                dag_id=dag_id,
                stage=stage,
                targets=targets,
                output_dir=out_dir,
                layer=layer,
                engine=engine,
            )
        elif tool == "sql":
            reports = run_stage_validation_sql(
                dag_id=dag_id,
                stage=stage,
                targets=targets,
                output_dir=out_dir,
                layer=layer,
                engine=engine,
            )
        else:
            raise ValueError(f"Unsupported tool: {tool}")

    success = sum(1 for r in reports if r.status == "SUCCESS")
    failed = sum(1 for r in reports if r.status != "SUCCESS")
    return {
        "stage": stage,
        "tool": tool,
        "status": "OK",
        "targets": len(reports),
        "success": success,
        "failed": failed,
        "repeats": repeats,
    }


__all__ = ["run_stage_tool"]
