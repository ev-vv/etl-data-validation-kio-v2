import time
from typing import Any
from datetime import date

from app2.validators.models import ValidationResult
from app2.utils.dates import parse_date


def validate_competitions_consistency(payload: Any) -> ValidationResult:
    start = time.time()
    errors: list[str] = []
    warnings: list[str] = []
    infos: list[str] = []

    comps = payload.get("competitions") if isinstance(payload, dict) else None
    if isinstance(comps, list):
        for comp in comps:
            if not isinstance(comp, dict):
                continue
            season = comp.get("currentSeason")
            if isinstance(season, dict):
                sd = parse_date(season.get("startDate"))
                ed = parse_date(season.get("endDate"))
                if sd and ed and sd > ed:
                    errors.append(f"Competition {comp.get('id')}: startDate > endDate")
                elif sd and not ed:
                    warnings.append(f"Competition {comp.get('id')}: startDate present, endDate missing")
        infos.append(f"Competitions_checked: {len(comps)}")
    else:
        warnings.append("Competitions payload missing or not a list; skipped consistency.")

    duration_ms = int((time.time() - start) * 1000)
    status = "ERROR" if errors else ("WARNING" if warnings else "INFO")
    infos.append(f"Validator_status: {status}")
    infos.append(f"Validation_duration_ms: {duration_ms}")

    return ValidationResult(status=status, errors=errors, warnings=warnings, infos=infos, duration_ms=duration_ms)
