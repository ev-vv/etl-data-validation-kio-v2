import time
from typing import Any

from app2.validators.models import ValidationResult


def validate_standings_completeness(payload: Any) -> ValidationResult:
    start = time.time()
    errors: list[str] = []
    warnings: list[str] = []
    infos: list[str] = []

    standings = payload.get("standings") if isinstance(payload, dict) else None
    if not isinstance(standings, list) or len(standings) == 0:
        errors.append("Standings list is missing or empty.")
    else:
        infos.append(f"Standings_groups: {len(standings)}")
        total_rows = 0
        for st in standings:
            if isinstance(st, dict) and isinstance(st.get("table"), list):
                total_rows += len(st["table"])
        infos.append(f"Standings_rows: {total_rows}")
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
