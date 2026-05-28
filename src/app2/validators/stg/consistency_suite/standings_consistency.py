import time
from typing import Any

from app2.validators.models import ValidationResult
from app2.utils.dates import parse_date


def validate_standings_consistency(payload: Any) -> ValidationResult:
    start = time.time()
    errors: list[str] = []
    warnings: list[str] = []
    infos: list[str] = []

    season = payload.get("season") if isinstance(payload, dict) else None
    if isinstance(season, dict):
        sd = parse_date(season.get("startDate"))
        ed = parse_date(season.get("endDate"))
        if sd and ed and sd > ed:
            errors.append("Standings season startDate > endDate")

    standings = payload.get("standings") if isinstance(payload, dict) else None
    if isinstance(standings, list):
        infos.append(f"Standings_checked: {len(standings)}")
    else:
        warnings.append("Standings payload missing or not a list; skipped consistency.")

    duration_ms = int((time.time() - start) * 1000)
    status = "ERROR" if errors else ("WARNING" if warnings else "INFO")
    infos.append(f"Validator_status: {status}")
    infos.append(f"Validation_duration_ms: {duration_ms}")

    return ValidationResult(status=status, errors=errors, warnings=warnings, infos=infos, duration_ms=duration_ms)
