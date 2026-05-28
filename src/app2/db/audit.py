from sqlalchemy import text
from sqlalchemy.engine import Engine


def audit_log(
    engine: Engine,
    dag_id: str,
    run_id: str,
    layer: str,
    entity_name: str,
    status: str,
    task_id: str | None = None,
    message: str | None = None,
    rows_processed: int | None = None,
    started_at=None,
    finished_at=None,
):
    with engine.begin() as conn:
        started_ts = started_at if started_at is not None else None
        finished_ts = finished_at if finished_at is not None else None
        conn.execute(
            text(
                """
                INSERT INTO tech.etl_load_audit (
                    dag_id, run_id, task_id, layer, entity_name, status, message, rows_processed, started_at, finished_at
                )
                VALUES (
                    :dag_id, :run_id, :task_id, :layer, :entity_name, :status, :message,
                    CASE WHEN :status = 'SUCCESS' THEN CAST(:rows_processed AS int) ELSE NULL END,
                    COALESCE(:started_at, timezone('Europe/Moscow', now())),
                    CASE
                        WHEN :finished_at IS NOT NULL THEN :finished_at
                        WHEN :status IN ('SUCCESS','FAILED','ENDED') THEN timezone('Europe/Moscow', now())
                        ELSE NULL
                    END
                )
                """
            ),
            {
                "dag_id": dag_id,
                "run_id": run_id,
                "task_id": task_id,
                "layer": layer,
                "entity_name": entity_name,
                "status": status,
                "message": message,
                "rows_processed": rows_processed,
                "started_at": started_ts,
                "finished_at": finished_ts,
            },
        )

