import time
from typing import Any

from app2.validators.models import ValidationResult
from app2.validators.stg.uniqueness_suite.areas_uniqueness import _find_duplicates


def validate_scorers_uniqueness(payload: Any) -> ValidationResult:
    start = time.time()
    errors: list[str] = []
    warnings: list[str] = []
    infos: list[str] = []

    scorers = payload.get("scorers") if isinstance(payload, dict) else None
    ids = []
    if isinstance(scorers, list):
        ids = [s.get("player", {}).get("id") for s in scorers if isinstance(s, dict) and isinstance(s.get("player"), dict)]
        dup = _find_duplicates(ids)
        if dup:
            errors.append(f"Duplicate scorer player ids: {sorted(dup)}")
        infos.append(f"Scorers_ids_checked: {len(ids)}")
    else:
        warnings.append("Scorers payload missing or not a list; skipped uniqueness.")

    duration_ms = int((time.time() - start) * 1000)
    status = "ERROR" if errors else ("WARNING" if warnings else "INFO")
    infos.append(f"Validator_status: {status}")
    infos.append(f"Validation_duration_ms: {duration_ms}")

    return ValidationResult(status=status, errors=errors, warnings=warnings, infos=infos, duration_ms=duration_ms)
