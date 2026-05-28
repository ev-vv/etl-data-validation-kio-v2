import time
from datetime import datetime

from app2.db.audit import audit_log
from app2.db.validation_metrics import finish_validation_run, resolve_validation_kind, start_validation_run
from app2.validators import load_config


def _run_suite(engine, dag_id: str, run_id: str, parent_run_id: str, conn, suite_key: str, default_entity: str):
    from app2.validators.runner import run_validation

    cfg = load_config("DDS")
    layer_cfg = cfg.get("layers", {}).get("DDS", {}) if isinstance(cfg, dict) else {}
    suite_cfg = layer_cfg.get("suites", {}).get(suite_key, {}) if isinstance(layer_cfg, dict) else {}
    validations_cfg = layer_cfg.get("validations", {}) if isinstance(layer_cfg, dict) else {}
    if not suite_cfg.get("enabled", True):
        return

    entity_name = suite_cfg.get("entity", default_entity)
    suite_validations = suite_cfg.get("validations", []) if isinstance(suite_cfg, dict) else []
    start_dt = datetime.now()
    suite_started = time.time()
    validation_run_id = start_validation_run(
        engine,
        dag_id=dag_id,
        run_id=run_id,
        parent_run_id=parent_run_id,
        layer="DDS",
        tool="author",
        suite=suite_key,
        kind=resolve_validation_kind(run_id),
    )
    audit_log(engine, dag_id=dag_id, run_id=run_id, layer="DDS", entity_name=entity_name, status="STARTED", started_at=start_dt)
    count = 0
    failed = 0
    try:
        for validator_name in suite_validations:
            v_cfg = validations_cfg.get(validator_name, {}) if isinstance(validations_cfg, dict) else {}
            if not v_cfg.get("enabled", True):
                continue
            payload = {"engine": engine, "conn": conn, "run_id": run_id, "parent_run_id": parent_run_id}
            result = run_validation(
                engine=engine,
                layer="DDS",
                dag_id=dag_id,
                run_id=run_id,
                validator_name=validator_name,
                payload=payload,
                parent_run_id=parent_run_id,
                validation_run_id=validation_run_id,
            )
            count += 1
            if result and result.errors and str(v_cfg.get("severity", "error")).lower() != "warning":
                failed += 1
        audit_log(
            engine,
            dag_id=dag_id,
            run_id=run_id,
            layer="DDS",
            entity_name=entity_name,
            status="SUCCESS",
            message=f"{entity_name} completed, validations run: {count}",
            started_at=start_dt,
            finished_at=datetime.now(),
        )
        finish_validation_run(
            engine,
            validation_run_id=validation_run_id,
            status="SUCCESS" if failed == 0 else "FAILED",
            duration_ms=int((time.time() - suite_started) * 1000),
            checks_total=count,
            checks_failed=failed,
        )
    except Exception as e:
        audit_log(
            engine,
            dag_id=dag_id,
            run_id=run_id,
            layer="DDS",
            entity_name=entity_name,
            status="FAILED",
            message=f"{entity_name} failed: {e}",
            started_at=start_dt,
            finished_at=datetime.now(),
        )
        finish_validation_run(
            engine,
            validation_run_id=validation_run_id,
            status="FAILED",
            duration_ms=int((time.time() - suite_started) * 1000),
            checks_total=count,
            checks_failed=max(1, failed),
        )
        raise


def run_dds_source_completeness_suite(engine, dag_id: str, run_id: str, parent_run_id: str, conn=None):
    _run_suite(
        engine=engine,
        dag_id=dag_id,
        run_id=run_id,
        parent_run_id=parent_run_id,
        conn=conn,
        suite_key="source_completeness_suite",
        default_entity="DDS_source_completeness_validation_suite",
    )


def run_dds_source_exclusivity_suite(engine, dag_id: str, run_id: str, parent_run_id: str, conn=None):
    _run_suite(
        engine=engine,
        dag_id=dag_id,
        run_id=run_id,
        parent_run_id=parent_run_id,
        conn=conn,
        suite_key="source_exclusivity_suite",
        default_entity="DDS_source_exclusivity_validation_suite",
    )
