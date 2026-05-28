import time
from datetime import datetime

from app2.db.audit import audit_log
from app2.db.validation_metrics import finish_validation_run, resolve_validation_kind, start_validation_run
from app2.validators import load_config


def run_stg_ingestion_suite(engine, dag_id: str, run_id: str, parent_run_id: str):
    from app2.validators.runner import run_validation

    cfg = load_config("STG")
    layer_cfg = cfg.get("layers", {}).get("STG", {}) if isinstance(cfg, dict) else {}
    suite_cfg = layer_cfg.get("suites", {}).get("ingestion_suite", {}) if isinstance(layer_cfg, dict) else {}
    validations_cfg = layer_cfg.get("validations", {}) if isinstance(layer_cfg, dict) else {}
    if not suite_cfg.get("enabled", True):
        return

    entity_name = suite_cfg.get("entity", "STG_ingestion_validation_suite")
    suite_validations = suite_cfg.get("validations", []) if isinstance(suite_cfg, dict) else []
    start_dt = datetime.now()
    suite_started = time.time()
    validation_run_id = start_validation_run(
        engine,
        dag_id=dag_id,
        run_id=run_id,
        parent_run_id=parent_run_id,
        layer="STG",
        tool="author",
        suite="ingestion_suite",
        kind=resolve_validation_kind(run_id),
    )
    audit_log(engine, dag_id=dag_id, run_id=run_id, layer="STG", entity_name=entity_name, status="STARTED", started_at=start_dt)
    count = 0
    failed = 0
    try:
        payload = {"engine": engine, "run_id": run_id}
        for validator_name in suite_validations:
            v_cfg = validations_cfg.get(validator_name, {}) if isinstance(validations_cfg, dict) else {}
            if not v_cfg.get("enabled", True):
                continue
            result = run_validation(
                engine=engine,
                layer="STG",
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
            layer="STG",
            entity_name=entity_name,
            status="SUCCESS",
            message=f"Ingestion suite completed, validations run: {count}",
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
            layer="STG",
            entity_name=entity_name,
            status="FAILED",
            message=f"Ingestion suite failed: {e}",
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
