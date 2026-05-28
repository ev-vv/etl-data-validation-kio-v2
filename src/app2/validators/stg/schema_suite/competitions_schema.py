import time
from typing import Any

from jsonschema import Draft7Validator

from app2.validators.models import ValidationResult


COMPETITIONS_SCHEMA = {
    "type": "object",
    "required": ["competitions"],
    "properties": {
        "count": {"type": ["integer", "null"]},
        "filters": {"type": ["object", "null"]},
        "competitions": {
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "object",
                "required": ["id", "name", "type", "plan"],
                "properties": {
                    "id": {"type": "integer"},
                    "name": {"type": "string", "minLength": 1},
                    "code": {"type": ["string", "null"]},
                    "type": {"type": ["string", "null"]},
                    "plan": {"type": ["string", "null"]},
                    "lastUpdated": {"type": ["string", "null"]},
                    "area": {
                        "type": "object",
                        "properties": {
                            "id": {"type": ["integer", "null"]},
                            "name": {"type": ["string", "null"]},
                            "code": {"type": ["string", "null"]},
                        },
                    },
                    "currentSeason": {
                        "type": ["object", "null"],
                        "properties": {
                            "id": {"type": ["integer", "null"]},
                            "startDate": {"type": ["string", "null"]},
                            "endDate": {"type": ["string", "null"]},
                        },
                    },
                },
                "additionalProperties": True,
            },
        },
    },
    "additionalProperties": True,
}

_VALIDATOR = Draft7Validator(COMPETITIONS_SCHEMA)


def validate_competitions_schema(payload: Any) -> ValidationResult:
    start = time.time()
    errors: list[str] = []
    warnings: list[str] = []
    infos: list[str] = []

    for err in _VALIDATOR.iter_errors(payload or {}):
        errors.append(f"Schema violation: {err.message}")

    comps = payload.get("competitions") if isinstance(payload, dict) else None
    if isinstance(comps, list):
        ids = [c.get("id") for c in comps if isinstance(c, dict)]
        seen = set()
        dup = []
        for i in ids:
            if i in seen and i is not None:
                dup.append(i)
            seen.add(i)
        if dup:
            warnings.append(f"Duplicate competition ids detected: {sorted(set(dup))}")
        infos.append(f"Competitions_count: {len(comps)}")
    else:
        errors.append("Field 'competitions' is missing or not a list.")

    duration_ms = int((time.time() - start) * 1000)
    status = "ERROR" if errors else ("WARNING" if warnings else "INFO")
    infos.append(f"Validator_status: {status}")
    infos.append(f"Validation_duration_ms: {duration_ms}")

    return ValidationResult(status=status, errors=errors, warnings=warnings, infos=infos, duration_ms=duration_ms)
