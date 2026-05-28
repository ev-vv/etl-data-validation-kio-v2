from sqlalchemy import text
from sqlalchemy.engine import Engine, Connection

from app2.validators.models import ValidationResult


def validate_fact_match_fk(payload) -> ValidationResult:
    engine: Engine = payload.get("engine")
    external_conn: Connection | None = payload.get("conn")
    dds_run_id = payload.get("run_id")
    errors = []
    warnings = []
    infos = []
    missing_home = missing_away = missing_season = 0

    def _execute_checks(conn: Connection):
        nonlocal missing_home, missing_away, missing_season
        missing_home = conn.execute(
            text(
                """
                SELECT count(*) FROM dds.fact_match fm
                WHERE fm.run_id = :run_id
                  AND fm.home_team_id IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM dds.dim_team t
                      WHERE t.run_id = fm.run_id
                        AND t.team_id = fm.home_team_id
                  )
                """
            ),
            {"run_id": dds_run_id},
        ).scalar_one()
        missing_away = conn.execute(
            text(
                """
                SELECT count(*) FROM dds.fact_match fm
                WHERE fm.run_id = :run_id
                  AND fm.away_team_id IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM dds.dim_team t
                      WHERE t.run_id = fm.run_id
                        AND t.team_id = fm.away_team_id
                  )
                """
            ),
            {"run_id": dds_run_id},
        ).scalar_one()
        missing_season = conn.execute(
            text(
                """
                SELECT count(*) FROM dds.fact_match fm
                WHERE fm.run_id = :run_id
                  AND fm.season_id IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1 FROM dds.dim_season s
                      WHERE s.run_id = fm.run_id
                        AND s.season_id = fm.season_id
                  )
                """
            ),
            {"run_id": dds_run_id},
        ).scalar_one()

    if external_conn is not None:
        _execute_checks(external_conn)
    elif engine is not None:
        with engine.begin() as conn:
            _execute_checks(conn)

    if missing_home:
        errors.append(f"fact_match: {missing_home} rows with missing home_team in dim_team")
    if missing_away:
        errors.append(f"fact_match: {missing_away} rows with missing away_team in dim_team")
    if missing_season:
        errors.append(f"fact_match: {missing_season} rows with missing season in dim_season")
    infos.append(f"fact_match_fk_checks: home={missing_home}, away={missing_away}, season={missing_season}")
    status = "ERROR" if errors else ("WARNING" if warnings else "INFO")
    return ValidationResult(status=status, errors=errors, warnings=warnings, infos=infos, duration_ms=0)
