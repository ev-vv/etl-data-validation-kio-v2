from sqlalchemy import text
from sqlalchemy.engine import Engine

from app2.validators.models import ValidationResult


def validate_api_http_status_ok(payload) -> ValidationResult:
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
                SELECT count(*)
                FROM stg.raw_football_api
                WHERE request_params ->> 'run_id' = :run_id
                  AND (http_status < 200 OR http_status >= 300)
                """
            ),
            {"run_id": run_id},
        ).scalar_one()

    infos.append(f"Bad_http_status_rows={bad}")
    if bad:
        errors.append(f"Found non-2xx API responses in STG: {bad}")
    status = "ERROR" if errors else "INFO"
    return ValidationResult(status=status, errors=errors, warnings=warnings, infos=infos, duration_ms=0)

