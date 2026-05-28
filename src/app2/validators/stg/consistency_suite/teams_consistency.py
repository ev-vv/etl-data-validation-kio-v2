import time
from typing import Any
from app2.validators.models import ValidationResult


def validate_teams_consistency(payload: Any) -> ValidationResult:
    start = time.time()
    errors: list[str] = []
    warnings: list[str] = []
    infos: list[str] = []

    teams = payload.get("teams") if isinstance(payload, dict) else None
    if isinstance(teams, list):
        infos.append(f"Teams_checked: {len(teams)}")
    else:
        warnings.append("Teams payload missing or not a list; skipped consistency.")

    duration_ms = int((time.time() - start) * 1000)
    status = "ERROR" if errors else ("WARNING" if warnings else "INFO")
    infos.append(f"Validator_status: {status}")
    infos.append(f"Validation_duration_ms: {duration_ms}")

    return ValidationResult(status=status, errors=errors, warnings=warnings, infos=infos, duration_ms=duration_ms)
