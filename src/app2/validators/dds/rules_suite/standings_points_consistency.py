from sqlalchemy import text
from sqlalchemy.engine import Engine, Connection

from app2.validators.models import ValidationResult


def validate_standings_points_consistency(payload) -> ValidationResult:
    engine: Engine = payload.get("engine")
    external_conn: Connection | None = payload.get("conn")
    dds_run_id = payload.get("run_id")

    errors = []
    warnings = []
    infos = []
    mismatches = 0

    def _execute(conn: Connection):
        nonlocal mismatches
        mismatches = conn.execute(
            text(
                """
                SELECT count(*) FROM dds.fact_standing
                WHERE run_id = :run_id
                  AND points IS NOT NULL
                  AND won IS NOT NULL
                  AND draw IS NOT NULL
                  AND (won*3 + draw) <> points
                """
            ),
            {"run_id": dds_run_id},
        ).scalar_one()

    if external_conn is not None:
        _execute(external_conn)
    elif engine is not None:
        with engine.begin() as conn:
            _execute(conn)

    if mismatches:
        warnings.append(f"Standings with points mismatch: {mismatches}")
    infos.append(f"Standings_points_mismatch={mismatches}")
    status = "ERROR" if errors else ("WARNING" if warnings else "INFO")
    return ValidationResult(status=status, errors=errors, warnings=warnings, infos=infos, duration_ms=0)
