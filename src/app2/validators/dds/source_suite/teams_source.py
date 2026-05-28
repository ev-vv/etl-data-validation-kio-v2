from sqlalchemy import text
from sqlalchemy.engine import Engine, Connection

from app2.validators.models import ValidationResult


def validate_teams_source_completeness(payload) -> ValidationResult:
    engine: Engine = payload.get("engine")
    external_conn: Connection | None = payload.get("conn")
    dds_run_id = payload.get("run_id")
    parent_run_id = payload.get("parent_run_id")

    errors = []
    warnings = []
    infos = []
    missing = src_count = 0

    def _execute(conn: Connection):
        nonlocal missing, src_count
        missing = conn.execute(
            text(
                """
                WITH src AS (
                    SELECT DISTINCT (t ->> 'id')::int AS team_id
                    FROM stg.raw_football_api s
                    CROSS JOIN LATERAL jsonb_array_elements(s.response_json -> 'teams') t
                    WHERE s.endpoint LIKE 'competitions/%/teams%'
                      AND s.request_params ->> 'run_id' = :run_id
                      AND (t ->> 'id') IS NOT NULL
                )
                SELECT count(*) FROM src s
                LEFT JOIN dds.dim_team dt
                  ON dt.run_id = :dds_run_id
                 AND dt.team_id = s.team_id
                WHERE dt.team_id IS NULL
                """
            ),
            {"run_id": parent_run_id, "dds_run_id": dds_run_id},
        ).scalar_one()
        src_count = conn.execute(
            text(
                """
                SELECT count(DISTINCT (t ->> 'id')::int)
                FROM stg.raw_football_api s
                CROSS JOIN LATERAL jsonb_array_elements(s.response_json -> 'teams') t
                WHERE s.endpoint LIKE 'competitions/%/teams%'
                  AND s.request_params ->> 'run_id' = :run_id
                  AND (t ->> 'id') IS NOT NULL
                """
            ),
            {"run_id": parent_run_id},
        ).scalar_one()

    if external_conn is not None:
        _execute(external_conn)
    elif engine is not None:
        with engine.begin() as conn:
            _execute(conn)

    if missing:
        errors.append(f"Teams missing in DDS: {missing} of {src_count}")
    infos.append(f"Teams_src_count={src_count}")
    status = "ERROR" if errors else ("WARNING" if warnings else "INFO")
    return ValidationResult(status=status, errors=errors, warnings=warnings, infos=infos, duration_ms=0)


def validate_teams_source_exclusivity(payload) -> ValidationResult:
    engine: Engine = payload.get("engine")
    external_conn: Connection | None = payload.get("conn")
    dds_run_id = payload.get("run_id")
    parent_run_id = payload.get("parent_run_id")

    errors = []
    warnings = []
    infos = []
    extras = 0

    def _execute(conn: Connection):
        nonlocal extras
        extras = conn.execute(
            text(
                """
                WITH src AS (
                    SELECT DISTINCT (t ->> 'id')::int AS team_id
                    FROM stg.raw_football_api s
                    CROSS JOIN LATERAL jsonb_array_elements(s.response_json -> 'teams') t
                    WHERE s.endpoint LIKE 'competitions/%/teams%'
                      AND s.request_params ->> 'run_id' = :run_id
                      AND (t ->> 'id') IS NOT NULL
                )
                SELECT count(*) FROM dds.dim_team dt
                WHERE dt.run_id = :dds_run_id
                  AND NOT EXISTS (SELECT 1 FROM src s WHERE s.team_id = dt.team_id)
                """
            ),
            {"run_id": parent_run_id, "dds_run_id": dds_run_id},
        ).scalar_one()

    if external_conn is not None:
        _execute(external_conn)
    elif engine is not None:
        with engine.begin() as conn:
            _execute(conn)

    if extras:
        errors.append(f"Teams in DDS not found in source: {extras}")
    infos.append(f"Teams_extras={extras}")
    status = "ERROR" if errors else "INFO"
    return ValidationResult(status=status, errors=errors, warnings=warnings, infos=infos, duration_ms=0)
