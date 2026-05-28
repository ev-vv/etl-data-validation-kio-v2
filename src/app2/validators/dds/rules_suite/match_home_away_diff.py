from sqlalchemy import text
from sqlalchemy.engine import Engine, Connection

from app2.validators.models import ValidationResult


def validate_match_home_away_diff(payload) -> ValidationResult:
    engine: Engine = payload.get("engine")
    external_conn: Connection | None = payload.get("conn")
    dds_run_id = payload.get("run_id")

    errors = []
    warnings = []
    infos = []
    bad = 0

    def _execute(conn: Connection):
        nonlocal bad
        bad = conn.execute(
            text(
                """
                SELECT count(*) FROM dds.fact_match
                WHERE run_id = :run_id
                  AND home_team_id IS NOT NULL
                  AND away_team_id IS NOT NULL
                  AND home_team_id = away_team_id
                """
            ),
            {"run_id": dds_run_id},
        ).scalar_one()

    if external_conn is not None:
        _execute(external_conn)
    elif engine is not None:
        with engine.begin() as conn:
            _execute(conn)

    if bad:
        errors.append(f"Matches with same home/away team: {bad}")
    status = "ERROR" if errors else "INFO"
    infos.append(f"Matches_same_team={bad}")
    return ValidationResult(status=status, errors=errors, warnings=warnings, infos=infos, duration_ms=0)
