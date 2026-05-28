import time
from typing import Any

from jsonschema import Draft7Validator

from app2.validators.models import ValidationResult


MATCHES_SCHEMA = {
    "type": "object",
    "required": ["matches"],
    "properties": {
        "count": {"type": ["integer", "null"]},
        "filters": {"type": ["object", "null"]},
        "matches": {
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "object",
                "required": ["id", "utcDate", "status", "homeTeam", "awayTeam", "competition", "season"],
                "properties": {
                    "id": {"type": "integer"},
                    "utcDate": {"type": "string"},
                    "status": {"type": "string"},
                    "stage": {"type": ["string", "null"]},
                    "group": {"type": ["string", "null"]},
                    "matchday": {"type": ["integer", "null"]},
                    "competition": {
                        "type": "object",
                        "properties": {"id": {"type": ["integer", "null"]}, "name": {"type": ["string", "null"]}},
                    },
                    "season": {
                        "type": "object",
                        "properties": {
                            "id": {"type": ["integer", "null"]},
                            "startDate": {"type": ["string", "null"]},
                            "endDate": {"type": ["string", "null"]},
                        },
                    },
                    "homeTeam": {
                        "type": "object",
                        "properties": {"id": {"type": ["integer", "null"]}, "name": {"type": ["string", "null"]}},
                    },
                    "awayTeam": {
                        "type": "object",
                        "properties": {"id": {"type": ["integer", "null"]}, "name": {"type": ["string", "null"]}},
                    },
                },
                "additionalProperties": True,
            },
        },
    },
    "additionalProperties": True,
}

_VALIDATOR = Draft7Validator(MATCHES_SCHEMA)


def validate_matches_schema(payload: Any) -> ValidationResult:
    start = time.time()
    errors: list[str] = []
    warnings: list[str] = []
    infos: list[str] = []

    for err in _VALIDATOR.iter_errors(payload or {}):
        errors.append(f"Schema violation: {err.message}")

    matches = payload.get("matches") if isinstance(payload, dict) else None
    if isinstance(matches, list):
        ids = [m.get("id") for m in matches if isinstance(m, dict)]
        seen = set()
        dup = []
        for i in ids:
            if i in seen and i is not None:
                dup.append(i)
            seen.add(i)
        if dup:
            warnings.append(f"Duplicate match ids detected: {sorted(set(dup))}")
        infos.append(f"Matches_count: {len(matches)}")
    else:
        errors.append("Field 'matches' is missing or not a list.")

    duration_ms = int((time.time() - start) * 1000)
    status = "ERROR" if errors else ("WARNING" if warnings else "INFO")
    infos.append(f"Validator_status: {status}")
    infos.append(f"Validation_duration_ms: {duration_ms}")

    return ValidationResult(status=status, errors=errors, warnings=warnings, infos=infos, duration_ms=duration_ms)
