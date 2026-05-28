import time
from typing import Any

from app2.validators.models import ValidationResult
from app2.validators.stg.uniqueness_suite.areas_uniqueness import _find_duplicates


def validate_standings_uniqueness(payload: Any) -> ValidationResult:
    start = time.time()
    errors: list[str] = []
    warnings: list[str] = []
    infos: list[str] = []

    standings = payload.get("standings") if isinstance(payload, dict) else None
    team_ids = []
    if isinstance(standings, list):
        for st in standings:
            if isinstance(st, dict) and isinstance(st.get("table"), list):
                for row in st["table"]:
                    if isinstance(row, dict):
                        tid = row.get("team", {}).get("id") if isinstance(row.get("team"), dict) else None
                        team_ids.append(tid)
        dup = _find_duplicates(team_ids)
        if dup:
            warnings.append(f"Duplicate team ids in standings: {sorted(dup)}")
        infos.append(f"Standings_team_ids_checked: {len(team_ids)}")
    else:
        warnings.append("Standings payload missing or not a list; skipped uniqueness.")

    duration_ms = int((time.time() - start) * 1000)
    status = "ERROR" if errors else ("WARNING" if warnings else "INFO")
    infos.append(f"Validator_status: {status}")
    infos.append(f"Validation_duration_ms: {duration_ms}")

    return ValidationResult(status=status, errors=errors, warnings=warnings, infos=infos, duration_ms=duration_ms)
