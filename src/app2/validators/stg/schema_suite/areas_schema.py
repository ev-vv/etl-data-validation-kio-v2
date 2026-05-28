import time
from typing import Any

from jsonschema import Draft7Validator

from app2.validators.models import ValidationResult


AREAS_SCHEMA = {
    "type": "object",
    "required": ["areas"],
    "properties": {
        "count": {"type": ["integer", "null"]},
        "filters": {"type": ["object", "null"]},
        "areas": {
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "object",
                "required": ["id", "name"],
                "properties": {
                    "id": {"type": "integer"},
                    "name": {"type": "string", "minLength": 1},
                    "countryCode": {"type": ["string", "null"], "minLength": 2},
                    "parentAreaId": {"type": ["integer", "null"]},
                    "parentArea": {"type": ["string", "null"]},
                    "flag": {"type": ["string", "null"], "format": "uri-reference"},
                },
                "additionalProperties": True,
            },
        }
    },
    "additionalProperties": True,
}

_VALIDATOR = Draft7Validator(AREAS_SCHEMA)


def validate_areas_schema(payload: Any) -> ValidationResult:
    start = time.time()
    errors: list[str] = []
    warnings: list[str] = []
    infos: list[str] = []

    for err in _VALIDATOR.iter_errors(payload or {}):
        errors.append(f"Schema violation: {err.message}")

    areas = payload.get("areas") if isinstance(payload, dict) else None
    if isinstance(areas, list):
        ids = [a.get("id") for a in areas if isinstance(a, dict)]
        seen = set()
        duplicate_ids = []
        for i in ids:
            if i in seen and i is not None:
                duplicate_ids.append(i)
            seen.add(i)
        if duplicate_ids:
            warnings.append(f"Duplicate area ids detected: {sorted(set(duplicate_ids))}")
        infos.append(f"Areas_count: {len(areas)}")
    else:
        errors.append("Field 'areas' is missing or not a list.")

    duration_ms = int((time.time() - start) * 1000)
    status = "ERROR" if errors else ("WARNING" if warnings else "INFO")
    infos.append(f"Validator_status: {status}")
    infos.append(f"Validation_duration_ms: {duration_ms}")

    return ValidationResult(status=status, errors=errors, warnings=warnings, infos=infos, duration_ms=duration_ms)
