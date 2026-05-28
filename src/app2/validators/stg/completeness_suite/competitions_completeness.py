import time
from typing import Any

from app2.validators.models import ValidationResult


def validate_competitions_completeness(payload: Any) -> ValidationResult:
    start = time.time()
    errors: list[str] = []
    warnings: list[str] = []
    infos: list[str] = []

    comps = payload.get("competitions") if isinstance(payload, dict) else None
    if not isinstance(comps, list) or len(comps) == 0:
        errors.append("Competitions list is missing or empty.")
    else:
        count = payload.get("count")
        if isinstance(count, int) and count != len(comps):
            warnings.append(f"Competitions count mismatch: count={count}, actual={len(comps)}")
        infos.append(f"Competitions_actual_count: {len(comps)}")

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
