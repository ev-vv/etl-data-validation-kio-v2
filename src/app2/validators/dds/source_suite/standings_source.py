from sqlalchemy import text
from sqlalchemy.engine import Engine, Connection

from app2.validators.models import ValidationResult


def validate_standings_source_completeness(payload) -> ValidationResult:
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
                    SELECT DISTINCT
                        (s.response_json -> 'season' ->> 'id')::int AS season_id,
                        (s.response_json -> 'competition' ->> 'id')::int AS competition_id,
                        (tbl -> 'team' ->> 'id')::int AS team_id,
                        st ->> 'type' AS standing_type
                    FROM stg.raw_football_api s
                    CROSS JOIN LATERAL jsonb_array_elements(s.response_json -> 'standings') st
                    CROSS JOIN LATERAL jsonb_array_elements(st -> 'table') tbl
                    WHERE s.endpoint LIKE 'competitions/%/standings%'
                      AND s.request_params ->> 'run_id' = :run_id
                      AND (s.response_json -> 'season' ->> 'id') IS NOT NULL
                      AND (tbl -> 'team' ->> 'id') IS NOT NULL
                )
                SELECT count(*) FROM src s
                LEFT JOIN dds.fact_standing fs
                  ON fs.run_id = :dds_run_id
                 AND fs.season_id = s.season_id
                 AND fs.competition_id = s.competition_id
                 AND fs.team_id = s.team_id
                 AND fs.standing_type = s.standing_type
                WHERE fs.team_id IS NULL
                """
            ),
            {"run_id": parent_run_id, "dds_run_id": dds_run_id},
        ).scalar_one()
        src_count = conn.execute(
            text(
                """
                SELECT count(DISTINCT (s.response_json -> 'season' ->> 'id', s.response_json -> 'competition' ->> 'id', tbl -> 'team' ->> 'id', st ->> 'type'))
                FROM stg.raw_football_api s
                CROSS JOIN LATERAL jsonb_array_elements(s.response_json -> 'standings') st
                CROSS JOIN LATERAL jsonb_array_elements(st -> 'table') tbl
                WHERE s.endpoint LIKE 'competitions/%/standings%'
                  AND s.request_params ->> 'run_id' = :run_id
                  AND (s.response_json -> 'season' ->> 'id') IS NOT NULL
                  AND (tbl -> 'team' ->> 'id') IS NOT NULL
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
        errors.append(f"Standings missing in DDS: {missing} of {src_count}")
    infos.append(f"Standings_src_count={src_count}")
    status = "ERROR" if errors else ("WARNING" if warnings else "INFO")
    return ValidationResult(status=status, errors=errors, warnings=warnings, infos=infos, duration_ms=0)


def validate_standings_source_exclusivity(payload) -> ValidationResult:
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
                    SELECT DISTINCT
                        (s.response_json -> 'season' ->> 'id')::int AS season_id,
                        (s.response_json -> 'competition' ->> 'id')::int AS competition_id,
                        (tbl -> 'team' ->> 'id')::int AS team_id,
                        st ->> 'type' AS standing_type
                    FROM stg.raw_football_api s
                    CROSS JOIN LATERAL jsonb_array_elements(s.response_json -> 'standings') st
                    CROSS JOIN LATERAL jsonb_array_elements(st -> 'table') tbl
                    WHERE s.endpoint LIKE 'competitions/%/standings%'
                      AND s.request_params ->> 'run_id' = :run_id
                      AND (s.response_json -> 'season' ->> 'id') IS NOT NULL
                      AND (tbl -> 'team' ->> 'id') IS NOT NULL
                )
                SELECT count(*) FROM dds.fact_standing fs
                WHERE fs.run_id = :dds_run_id
                  AND NOT EXISTS (
                    SELECT 1 FROM src s
                    WHERE s.season_id = fs.season_id
                      AND s.competition_id = fs.competition_id
                      AND s.team_id = fs.team_id
                      AND s.standing_type = fs.standing_type
                )
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
        errors.append(f"Standings in DDS not found in source: {extras}")
    infos.append(f"Standings_extras={extras}")
    status = "ERROR" if errors else "INFO"
    return ValidationResult(status=status, errors=errors, warnings=warnings, infos=infos, duration_ms=0)
