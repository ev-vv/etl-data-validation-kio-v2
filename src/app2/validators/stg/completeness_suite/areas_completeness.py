import time
from typing import Any

from app2.validators.models import ValidationResult


def validate_areas_completeness(payload: Any) -> ValidationResult:
    start = time.time()
    errors: list[str] = []
    warnings: list[str] = []
    infos: list[str] = []

    areas = payload.get("areas") if isinstance(payload, dict) else None
    if not isinstance(areas, list) or len(areas) == 0:
        errors.append("Areas list is missing or empty.")
    else:
        count = payload.get("count")
        if isinstance(count, int) and count != len(areas):
            warnings.append(f"Areas count mismatch: count={count}, actual={len(areas)}")
        infos.append(f"Areas_actual_count: {len(areas)}")

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
