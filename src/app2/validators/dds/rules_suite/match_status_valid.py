from sqlalchemy import text
from sqlalchemy.engine import Engine, Connection

from app2.validators.models import ValidationResult


def validate_match_status_valid(payload) -> ValidationResult:
    engine: Engine = payload.get("engine")
    external_conn: Connection | None = payload.get("conn")
    dds_run_id = payload.get("run_id")

    errors = []
    warnings = []
    infos = []
    invalid = 0

    def _execute(conn: Connection):
        nonlocal invalid
        invalid = conn.execute(
            text(
                """
                SELECT count(*) FROM dds.fact_match
                WHERE run_id = :run_id
                  AND status IS NOT NULL
                  AND status NOT IN ('SCHEDULED','TIMED','IN_PLAY','PAUSED','FINISHED','POSTPONED','SUSPENDED','CANCELED')
                """
            ),
            {"run_id": dds_run_id},
        ).scalar_one()

    if external_conn is not None:
        _execute(external_conn)
    elif engine is not None:
        with engine.begin() as conn:
            _execute(conn)

    if invalid:
        warnings.append(f"Matches with invalid status: {invalid}")
    infos.append(f"Matches_invalid_status={invalid}")
    status = "ERROR" if errors else ("WARNING" if warnings else "INFO")
    return ValidationResult(status=status, errors=errors, warnings=warnings, infos=infos, duration_ms=0)
