import time
from typing import Any

from app2.validators.models import ValidationResult


def validate_matches_completeness(payload: Any) -> ValidationResult:
    start = time.time()
    errors: list[str] = []
    warnings: list[str] = []
    infos: list[str] = []

    matches = payload.get("matches") if isinstance(payload, dict) else None
    if not isinstance(matches, list) or len(matches) == 0:
        errors.append("Matches list is missing or empty.")
    else:
        count = payload.get("count")
        if isinstance(count, int) and count != len(matches):
            warnings.append(f"Matches count mismatch: count={count}, actual={len(matches)}")
        infos.append(f"Matches_actual_count: {len(matches)}")

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
