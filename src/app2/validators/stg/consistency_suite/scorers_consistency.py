import time
from typing import Any
from app2.validators.models import ValidationResult


def validate_scorers_consistency(payload: Any) -> ValidationResult:
    start = time.time()
    errors: list[str] = []
    warnings: list[str] = []
    infos: list[str] = []

    scorers = payload.get("scorers") if isinstance(payload, dict) else None
    if isinstance(scorers, list):
        missing_players = [s for s in scorers if not (isinstance(s, dict) and isinstance(s.get("player"), dict))]
        if missing_players:
            warnings.append(f"Scorers missing player details: {len(missing_players)}")
        infos.append(f"Scorers_checked: {len(scorers)}")
    else:
        warnings.append("Scorers payload missing or not a list; skipped consistency.")

    duration_ms = int((time.time() - start) * 1000)
    status = "ERROR" if errors else ("WARNING" if warnings else "INFO")
    infos.append(f"Validator_status: {status}")
    infos.append(f"Validation_duration_ms: {duration_ms}")

    return ValidationResult(status=status, errors=errors, warnings=warnings, infos=infos, duration_ms=duration_ms)
