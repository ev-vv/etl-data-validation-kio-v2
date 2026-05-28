import time
from typing import Any

from app2.validators.models import ValidationResult


def validate_scorers_completeness(payload: Any) -> ValidationResult:
    start = time.time()
    errors: list[str] = []
    warnings: list[str] = []
    infos: list[str] = []

    scorers = payload.get("scorers") if isinstance(payload, dict) else None
    if not isinstance(scorers, list) or len(scorers) == 0:
        errors.append("Scorers list is missing or empty.")
    else:
        count = payload.get("count")
        if isinstance(count, int) and count != len(scorers):
            warnings.append(f"Scorers count mismatch: count={count}, actual={len(scorers)}")
        infos.append(f"Scorers_actual_count: {len(scorers)}")

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
