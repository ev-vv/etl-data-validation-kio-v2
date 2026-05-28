import time
from typing import Any

from jsonschema import Draft7Validator

from app2.validators.models import ValidationResult


TEAMS_SCHEMA = {
    "type": "object",
    "required": ["teams"],
    "properties": {
        "count": {"type": ["integer", "null"]},
        "filters": {"type": ["object", "null"]},
        "teams": {
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "object",
                "required": ["id", "name"],
                "properties": {
                    "id": {"type": "integer"},
                    "name": {"type": "string", "minLength": 1},
                    "tla": {"type": ["string", "null"]},
                    "area": {
                        "type": "object",
                        "properties": {"id": {"type": ["integer", "null"]}, "name": {"type": ["string", "null"]}},
                    },
                    "crest": {"type": ["string", "null"]},
                    "venue": {"type": ["string", "null"]},
                    "address": {"type": ["string", "null"]},
                },
                "additionalProperties": True,
            },
        },
    },
    "additionalProperties": True,
}

_VALIDATOR = Draft7Validator(TEAMS_SCHEMA)


def validate_teams_schema(payload: Any) -> ValidationResult:
    start = time.time()
    errors: list[str] = []
    warnings: list[str] = []
    infos: list[str] = []

    for err in _VALIDATOR.iter_errors(payload or {}):
        errors.append(f"Schema violation: {err.message}")

    teams = payload.get("teams") if isinstance(payload, dict) else None
    if isinstance(teams, list):
        ids = [t.get("id") for t in teams if isinstance(t, dict)]
        seen = set()
        dup = []
        for i in ids:
            if i in seen and i is not None:
                dup.append(i)
            seen.add(i)
        if dup:
            warnings.append(f"Duplicate team ids detected: {sorted(set(dup))}")
        infos.append(f"Teams_count: {len(teams)}")
    else:
        errors.append("Field 'teams' is missing or not a list.")

    duration_ms = int((time.time() - start) * 1000)
    status = "ERROR" if errors else ("WARNING" if warnings else "INFO")
    infos.append(f"Validator_status: {status}")
    infos.append(f"Validation_duration_ms: {duration_ms}")

    return ValidationResult(status=status, errors=errors, warnings=warnings, infos=infos, duration_ms=duration_ms)
