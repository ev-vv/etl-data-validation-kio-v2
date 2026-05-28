import time
from typing import Any

from jsonschema import Draft7Validator

from app2.validators.models import ValidationResult


SCORERS_SCHEMA = {
    "type": "object",
    "required": ["scorers"],
    "properties": {
        "count": {"type": ["integer", "null"]},
        "filters": {"type": ["object", "null"]},
        "season": {"type": ["object", "null"]},
        "scorers": {
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "object",
                "required": ["player", "team", "goals"],
                "properties": {
                    "goals": {"type": ["integer", "null"]},
                    "assists": {"type": ["integer", "null"]},
                    "penalties": {"type": ["integer", "null"]},
                    "team": {
                        "type": "object",
                        "properties": {
                            "id": {"type": ["integer", "null"]},
                            "name": {"type": ["string", "null"]},
                            "tla": {"type": ["string", "null"]},
                        },
                    },
                    "player": {
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

_VALIDATOR = Draft7Validator(SCORERS_SCHEMA)


def validate_scorers_schema(payload: Any) -> ValidationResult:
    start = time.time()
    errors: list[str] = []
    warnings: list[str] = []
    infos: list[str] = []

    for err in _VALIDATOR.iter_errors(payload or {}):
        errors.append(f"Schema violation: {err.message}")

    scorers = payload.get("scorers") if isinstance(payload, dict) else None
    if isinstance(scorers, list):
        ids = [s.get("player", {}).get("id") for s in scorers if isinstance(s, dict)]
        seen = set()
        dup = []
        for i in ids:
            if i in seen and i is not None:
                dup.append(i)
            seen.add(i)
        if dup:
            warnings.append(f"Duplicate player ids detected: {sorted(set(dup))}")
        infos.append(f"Scorers_count: {len(scorers)}")
    else:
        errors.append("Field 'scorers' is missing or not a list.")

    duration_ms = int((time.time() - start) * 1000)
    status = "ERROR" if errors else ("WARNING" if warnings else "INFO")
    infos.append(f"Validator_status: {status}")
    infos.append(f"Validation_duration_ms: {duration_ms}")

    return ValidationResult(status=status, errors=errors, warnings=warnings, infos=infos, duration_ms=duration_ms)
