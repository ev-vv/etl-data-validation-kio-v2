import time
from typing import Any

from jsonschema import Draft7Validator

from app2.validators.models import ValidationResult


STANDINGS_SCHEMA = {
    "type": "object",
    "required": ["standings"],
    "properties": {
        "filters": {"type": ["object", "null"]},
        "standings": {
            "type": "array",
            "minItems": 1,
            "items": {
                "type": "object",
                "required": ["type", "table"],
                "properties": {
                    "type": {"type": "string"},
                    "stage": {"type": ["string", "null"]},
                    "group": {"type": ["string", "null"]},
                    "table": {
                        "type": "array",
                        "minItems": 1,
                        "items": {
                            "type": "object",
                            "required": ["team", "position", "points", "playedGames"],
                            "properties": {
                                "position": {"type": ["integer", "null"]},
                                "points": {"type": ["integer", "null"]},
                                "playedGames": {"type": ["integer", "null"]},
                                "won": {"type": ["integer", "null"]},
                                "draw": {"type": ["integer", "null"]},
                                "lost": {"type": ["integer", "null"]},
                                "goalsFor": {"type": ["integer", "null"]},
                                "goalsAgainst": {"type": ["integer", "null"]},
                                "goalDifference": {"type": ["integer", "null"]},
                                "team": {
                                    "type": "object",
                                    "properties": {
                                        "id": {"type": ["integer", "null"]},
                                        "name": {"type": ["string", "null"]},
                                        "tla": {"type": ["string", "null"]},
                                    },
                                },
                            },
                            "additionalProperties": True,
                        },
                    },
                },
                "additionalProperties": True,
            },
        },
    },
    "additionalProperties": True,
}

_VALIDATOR = Draft7Validator(STANDINGS_SCHEMA)


def validate_standings_schema(payload: Any) -> ValidationResult:
    start = time.time()
    errors: list[str] = []
    warnings: list[str] = []
    infos: list[str] = []

    for err in _VALIDATOR.iter_errors(payload or {}):
        errors.append(f"Schema violation: {err.message}")

    standings = payload.get("standings") if isinstance(payload, dict) else None
    if isinstance(standings, list):
        table_entries = 0
        team_ids = []
        for st in standings:
            if isinstance(st, dict):
                table = st.get("table")
                if isinstance(table, list):
                    table_entries += len(table)
                    for row in table:
                        if isinstance(row, dict):
                            tid = row.get("team", {}).get("id")
                            if tid is not None:
                                team_ids.append(tid)
        dup = []
        seen = set()
        for tid in team_ids:
            if tid in seen:
                dup.append(tid)
            seen.add(tid)
        if dup:
            warnings.append(f"Duplicate team ids in standings: {sorted(set(dup))}")
        infos.append(f"Standings_groups: {len(standings)}")
        infos.append(f"Standings_rows: {table_entries}")
    else:
        errors.append("Field 'standings' is missing or not a list.")

    duration_ms = int((time.time() - start) * 1000)
    status = "ERROR" if errors else ("WARNING" if warnings else "INFO")
    infos.append(f"Validator_status: {status}")
    infos.append(f"Validation_duration_ms: {duration_ms}")

    return ValidationResult(status=status, errors=errors, warnings=warnings, infos=infos, duration_ms=duration_ms)
