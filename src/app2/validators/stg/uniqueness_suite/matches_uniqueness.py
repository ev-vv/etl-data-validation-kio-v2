import time
from typing import Any

from app2.validators.models import ValidationResult
from app2.validators.stg.uniqueness_suite.areas_uniqueness import _find_duplicates


def validate_matches_uniqueness(payload: Any) -> ValidationResult:
    start = time.time()
    errors: list[str] = []
    warnings: list[str] = []
    infos: list[str] = []

    matches = payload.get("matches") if isinstance(payload, dict) else None
    ids = []
    if isinstance(matches, list):
        ids = [m.get("id") for m in matches if isinstance(m, dict)]
        dup = _find_duplicates(ids)
        if dup:
            errors.append(f"Duplicate match ids: {sorted(dup)}")
        infos.append(f"Matches_ids_checked: {len(ids)}")
    else:
        warnings.append("Matches payload missing or not a list; skipped uniqueness.")

    duration_ms = int((time.time() - start) * 1000)
    status = "ERROR" if errors else ("WARNING" if warnings else "INFO")
    infos.append(f"Validator_status: {status}")
    infos.append(f"Validation_duration_ms: {duration_ms}")

    return ValidationResult(status=status, errors=errors, warnings=warnings, infos=infos, duration_ms=duration_ms)
