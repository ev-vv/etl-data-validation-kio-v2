import time
from typing import Any

from app2.validators.models import ValidationResult
from app2.validators.stg.uniqueness_suite.areas_uniqueness import _find_duplicates


def validate_teams_uniqueness(payload: Any) -> ValidationResult:
    start = time.time()
    errors: list[str] = []
    warnings: list[str] = []
    infos: list[str] = []

    teams = payload.get("teams") if isinstance(payload, dict) else None
    ids = []
    if isinstance(teams, list):
        ids = [t.get("id") for t in teams if isinstance(t, dict)]
        dup = _find_duplicates(ids)
        if dup:
            errors.append(f"Duplicate team ids: {sorted(dup)}")
        infos.append(f"Teams_ids_checked: {len(ids)}")
    else:
        warnings.append("Teams payload missing or not a list; skipped uniqueness.")

    duration_ms = int((time.time() - start) * 1000)
    status = "ERROR" if errors else ("WARNING" if warnings else "INFO")
    infos.append(f"Validator_status: {status}")
    infos.append(f"Validation_duration_ms: {duration_ms}")

    return ValidationResult(status=status, errors=errors, warnings=warnings, infos=infos, duration_ms=duration_ms)
