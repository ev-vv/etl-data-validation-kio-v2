from sqlalchemy import text
from sqlalchemy.engine import Engine, Connection

from app2.validators.models import ValidationResult


def validate_matches_source_completeness(payload) -> ValidationResult:
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
                    SELECT DISTINCT (m ->> 'id')::int AS match_id
                    FROM stg.raw_football_api s
                    CROSS JOIN LATERAL jsonb_array_elements(s.response_json -> 'matches') m
                    WHERE s.endpoint LIKE 'competitions/%/matches%'
                      AND s.request_params ->> 'run_id' = :run_id
                      AND (m ->> 'id') IS NOT NULL
                )
                SELECT count(*) FROM src s
                LEFT JOIN dds.fact_match fm
                  ON fm.run_id = :dds_run_id
                 AND fm.match_id = s.match_id
                WHERE fm.match_id IS NULL
                """
            ),
            {"run_id": parent_run_id, "dds_run_id": dds_run_id},
        ).scalar_one()
        src_count = conn.execute(
            text(
                """
                SELECT count(DISTINCT (m ->> 'id')::int)
                FROM stg.raw_football_api s
                CROSS JOIN LATERAL jsonb_array_elements(s.response_json -> 'matches') m
                WHERE s.endpoint LIKE 'competitions/%/matches%'
                  AND s.request_params ->> 'run_id' = :run_id
                  AND (m ->> 'id') IS NOT NULL
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
        errors.append(f"Matches missing in DDS: {missing} of {src_count}")
    infos.append(f"Matches_src_count={src_count}")
    status = "ERROR" if errors else ("WARNING" if warnings else "INFO")
    return ValidationResult(status=status, errors=errors, warnings=warnings, infos=infos, duration_ms=0)


def validate_matches_source_exclusivity(payload) -> ValidationResult:
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
                    SELECT DISTINCT (m ->> 'id')::int AS match_id
                    FROM stg.raw_football_api s
                    CROSS JOIN LATERAL jsonb_array_elements(s.response_json -> 'matches') m
                    WHERE s.endpoint LIKE 'competitions/%/matches%'
                      AND s.request_params ->> 'run_id' = :run_id
                      AND (m ->> 'id') IS NOT NULL
                )
                SELECT count(*) FROM dds.fact_match fm
                WHERE fm.run_id = :dds_run_id
                  AND NOT EXISTS (SELECT 1 FROM src s WHERE s.match_id = fm.match_id)
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
        errors.append(f"Matches in DDS not found in source: {extras}")
    infos.append(f"Matches_extras={extras}")
    status = "ERROR" if errors else "INFO"
    return ValidationResult(status=status, errors=errors, warnings=warnings, infos=infos, duration_ms=0)
