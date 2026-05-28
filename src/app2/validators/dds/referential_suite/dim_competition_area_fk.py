from sqlalchemy import text
from sqlalchemy.engine import Engine, Connection

from app2.validators.models import ValidationResult


def validate_dim_competition_area_fk(payload) -> ValidationResult:
    engine: Engine = payload.get("engine")
    external_conn: Connection | None = payload.get("conn")
    dds_run_id = payload.get("run_id")
    errors = []
    warnings = []
    infos = []
    missing = 0

    def _execute(conn: Connection):
        nonlocal missing
        missing = conn.execute(
            text(
                """
                SELECT count(*) FROM dds.dim_competition dc
                WHERE dc.run_id = :run_id
                  AND dc.area_id IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM dds.dim_area a
                      WHERE a.run_id = dc.run_id
                        AND a.area_id = dc.area_id
                  )
                """
            ),
            {"run_id": dds_run_id},
        ).scalar_one()

    if external_conn is not None:
        _execute(external_conn)
    elif engine is not None:
        with engine.begin() as conn:
            _execute(conn)

    if missing:
        errors.append(f"dim_competition: {missing} rows with missing area in dim_area")
    infos.append(f"dim_competition_area_fk_missing={missing}")
    status = "ERROR" if errors else ("WARNING" if warnings else "INFO")
    return ValidationResult(status=status, errors=errors, warnings=warnings, infos=infos, duration_ms=0)
