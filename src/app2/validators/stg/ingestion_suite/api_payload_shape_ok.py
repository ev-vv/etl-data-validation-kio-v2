from sqlalchemy import text
from sqlalchemy.engine import Engine

from app2.validators.models import ValidationResult


def validate_api_payload_shape_ok(payload) -> ValidationResult:
    engine: Engine = payload.get("engine")
    run_id = payload.get("run_id")

    errors: list[str] = []
    warnings: list[str] = []
    infos: list[str] = []

    if engine is None or not run_id:
        return ValidationResult(status="ERROR", errors=["Missing engine/run_id in payload"], warnings=[], infos=[], duration_ms=0)

    with engine.begin() as conn:
        bad = conn.execute(
            text(
                """
                WITH mapped AS (
                    SELECT
                        endpoint,
                        response_json,
                        CASE
                            WHEN endpoint = 'competitions' THEN 'competitions'
                            WHEN endpoint = 'areas' THEN 'areas'
                            WHEN endpoint LIKE 'competitions/%/teams%' THEN 'teams'
                            WHEN endpoint LIKE 'competitions/%/scorers%' THEN 'scorers'
                            WHEN endpoint LIKE 'competitions/%/matches%' THEN 'matches'
                            WHEN endpoint LIKE 'competitions/%/standings%' THEN 'standings'
                            ELSE NULL
                        END AS required_key
                    FROM stg.raw_football_api
                    WHERE request_params ->> 'run_id' = :run_id
                )
                SELECT count(*)
                FROM mapped
                WHERE required_key IS NOT NULL
                  AND NOT (response_json ? required_key)
                """
            ),
            {"run_id": run_id},
        ).scalar_one()

    infos.append(f"Bad_payload_shape_rows={bad}")
    if bad:
        errors.append(f"Found responses without required top-level keys in STG: {bad}")
    status = "ERROR" if errors else "INFO"
    return ValidationResult(status=status, errors=errors, warnings=warnings, infos=infos, duration_ms=0)

