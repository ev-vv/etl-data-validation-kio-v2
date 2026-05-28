from sqlalchemy import text
from sqlalchemy.engine import Engine, Connection

from app2.validators.models import ValidationResult


def validate_fact_standing_fk(payload) -> ValidationResult:
    engine: Engine = payload.get("engine")
    external_conn: Connection | None = payload.get("conn")
    dds_run_id = payload.get("run_id")
    errors = []
    warnings = []
    infos = []
    missing_team = 0

    def _execute(conn: Connection):
        nonlocal missing_team
        missing_team = conn.execute(
            text(
                """
                SELECT count(*) FROM dds.fact_standing fs
                WHERE fs.run_id = :run_id
                  AND fs.team_id IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM dds.dim_team t
                      WHERE t.run_id = fs.run_id
                        AND t.team_id = fs.team_id
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

    if missing_team:
        warnings.append(f"fact_standing: {missing_team} rows with missing team in dim_team")
    infos.append(f"fact_standing_fk_checks: team_missing={missing_team}")
    status = "ERROR" if errors else ("WARNING" if warnings else "INFO")
    return ValidationResult(status=status, errors=errors, warnings=warnings, infos=infos, duration_ms=0)
