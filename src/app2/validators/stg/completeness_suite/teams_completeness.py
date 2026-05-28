import time
from typing import Any

from app2.validators.models import ValidationResult


def validate_teams_completeness(payload: Any) -> ValidationResult:
    start = time.time()
    errors: list[str] = []
    warnings: list[str] = []
    infos: list[str] = []

    teams = payload.get("teams") if isinstance(payload, dict) else None
    if not isinstance(teams, list) or len(teams) == 0:
        errors.append("Teams list is missing or empty.")
    else:
        count = payload.get("count")
        if isinstance(count, int) and count != len(teams):
            warnings.append(f"Teams count mismatch: count={count}, actual={len(teams)}")
        infos.append(f"Teams_actual_count: {len(teams)}")

    duration_ms = int((time.time() - start) * 1000)
    infos.append(f"Validator_status: {'ERROR' if errors else ('WARNING' if warnings else 'INFO')}")
    infos.append(f"Validation_duration_ms: {duration_ms}")

    return ValidationResult(
        status="ERROR" if errors else ("WARNING" if warnings else "INFO"),
        errors=errors,
        warnings=warnings,
        infos=infos,
        duration_ms=duration_ms,
    )
