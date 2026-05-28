import time
from typing import Any

from app2.validators.models import ValidationResult


def validate_areas_uniqueness(payload: Any) -> ValidationResult:
    start = time.time()
    errors: list[str] = []
    warnings: list[str] = []
    infos: list[str] = []

    areas = payload.get("areas") if isinstance(payload, dict) else None
    ids = []
    if isinstance(areas, list):
        ids = [a.get("id") for a in areas if isinstance(a, dict)]
        dup = _find_duplicates(ids)
        if dup:
            errors.append(f"Duplicate area ids: {sorted(dup)}")
        infos.append(f"Areas_ids_checked: {len(ids)}")
    else:
        warnings.append("Areas payload missing or not a list; skipped uniqueness.")

    duration_ms = int((time.time() - start) * 1000)
    status = "ERROR" if errors else ("WARNING" if warnings else "INFO")
    infos.append(f"Validator_status: {status}")
    infos.append(f"Validation_duration_ms: {duration_ms}")

    return ValidationResult(status=status, errors=errors, warnings=warnings, infos=infos, duration_ms=duration_ms)


def _find_duplicates(values):
    seen = set()
    dup = set()
    for v in values:
        if v in seen and v is not None:
            dup.add(v)
        seen.add(v)
    return dup
