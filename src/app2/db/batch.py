from sqlalchemy import text
from sqlalchemy.engine import Engine


def log_batch_status(
    engine: Engine,
    dag_id: str,
    run_id: str,
    layer: str,
    status: str,
    parent_run_id: str,
    error_message: str | None = None,
):
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO tech.etl_batch_status (dag_id, run_id, parent_run_id, layer, status, attempts, error_message, last_updated_at)
                VALUES (:dag_id, :run_id, :parent_run_id, :layer, :status,
                        CASE WHEN :status = 'PROCESSING' THEN 1 ELSE 0 END,
                        :error_message,
                        timezone('Europe/Moscow', now()))
                ON CONFLICT (layer, parent_run_id, run_id) DO UPDATE
                SET status = EXCLUDED.status,
                    dag_id = EXCLUDED.dag_id,
                    error_message = EXCLUDED.error_message,
                    attempts = CASE
                                   WHEN EXCLUDED.status = 'PROCESSING' THEN tech.etl_batch_status.attempts + 1
                                   ELSE tech.etl_batch_status.attempts
                               END,
                    last_updated_at = timezone('Europe/Moscow', now())
                """
            ),
            {
                "dag_id": dag_id,
                "run_id": run_id,
                "parent_run_id": parent_run_id,
                "layer": layer,
                "status": status,
                "error_message": error_message,
            },
        )


def claim_pending_dds_batches(engine: Engine, dag_id: str, dds_run_id: str):
    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """
                WITH stg_success AS (
                    SELECT run_id AS stg_run_id
                    FROM tech.etl_batch_status
                    WHERE layer = 'STG' AND status = 'SUCCESS'
                    FOR UPDATE SKIP LOCKED
                ),
                eligible AS (
                    SELECT s.stg_run_id
                    FROM stg_success s
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM tech.etl_batch_status d
                        WHERE d.layer = 'DDS'
                          AND d.parent_run_id = s.stg_run_id
                          AND d.status IN ('SUCCESS', 'PROCESSING')
                    )
                ),
                inserted AS (
                    INSERT INTO tech.etl_batch_status (dag_id, run_id, parent_run_id, layer, status, attempts, last_updated_at)
                    SELECT :dag_id, :dds_run_id, e.stg_run_id, 'DDS', 'NEW', 0, timezone('Europe/Moscow', now())
                    FROM eligible e
                    RETURNING parent_run_id
                )
                SELECT parent_run_id FROM inserted
                """
            ),
            {"dag_id": dag_id, "dds_run_id": dds_run_id},
        ).fetchall()
    return [r[0] for r in rows]


def delete_batch_status_for_layer(
    engine: Engine,
    *,
    layer: str,
    run_ids: list[str] | None = None,
) -> None:
    from sqlalchemy import bindparam

    with engine.begin() as conn:
        if run_ids:
            conn.execute(
                text(
                    """
                    DELETE FROM tech.etl_batch_status
                    WHERE layer = :layer
                      AND run_id IN :run_ids
                    """
                ).bindparams(bindparam("run_ids", expanding=True)),
                {
                    "layer": layer,
                    "run_ids": run_ids,
                },
            )
        else:
            conn.execute(
                text(
                    """
                    DELETE FROM tech.etl_batch_status
                    WHERE layer = :layer
                    """
                ),
                {
                    "layer": layer,
                },
            )