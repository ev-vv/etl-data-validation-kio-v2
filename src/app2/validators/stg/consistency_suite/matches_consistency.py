import time
from typing import Any
from datetime import datetime

from app2.validators.models import ValidationResult
from app2.utils.dates import parse_date


def _parse_ts(ts: str):
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None


def validate_matches_consistency(payload: Any) -> ValidationResult:
    start = time.time()
    errors: list[str] = []
    warnings: list[str] = []
    infos: list[str] = []

    matches = payload.get("matches") if isinstance(payload, dict) else None
    if isinstance(matches, list):
        for m in matches:
            if not isinstance(m, dict):
                continue
            season = m.get("season")
            sd = ed = None
            if isinstance(season, dict):
                sd = parse_date(season.get("startDate"))
                ed = parse_date(season.get("endDate"))
                if sd and ed and sd > ed:
                    errors.append(f"Match {m.get('id')}: season startDate > endDate")
            ts = _parse_ts(m.get("utcDate"))
            if ts and sd and ed and not (sd <= ts.date() <= ed):
                warnings.append(f"Match {m.get('id')}: utcDate outside season range")
            if m.get("homeTeam", {}).get("id") == m.get("awayTeam", {}).get("id"):
                errors.append(f"Match {m.get('id')}: homeTeam equals awayTeam")
        infos.append(f"Matches_checked: {len(matches)}")
    else:
        warnings.append("Matches payload missing or not a list; skipped consistency.")

    duration_ms = int((time.time() - start) * 1000)
    status = "ERROR" if errors else ("WARNING" if warnings else "INFO")
    infos.append(f"Validator_status: {status}")
    infos.append(f"Validation_duration_ms: {duration_ms}")

    return ValidationResult(status=status, errors=errors, warnings=warnings, infos=infos, duration_ms=duration_ms)
