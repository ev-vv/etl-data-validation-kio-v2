from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from app2.validators import load_config


def _payload_for_entity(engine: Engine, run_id: str, entity: str) -> Any:
    entity = entity.strip().lower()
    where = None
    if entity == "competitions":
        where = "endpoint = 'competitions'"
    elif entity == "areas":
        where = "endpoint = 'areas'"
    elif entity == "teams":
        where = "endpoint LIKE 'competitions/%/teams%'"
    elif entity == "scorers":
        where = "endpoint LIKE 'competitions/%/scorers%'"
    elif entity == "matches":
        where = "endpoint LIKE 'competitions/%/matches%'"
    elif entity == "standings":
        where = "endpoint LIKE 'competitions/%/standings%'"
    if where is None:
        return None

    with engine.begin() as conn:
        row = conn.execute(
            text(
                f"""
                SELECT response_json
                FROM stg.raw_football_api
                WHERE {where}
                  AND request_params ->> 'run_id' = :run_id
                  AND http_status BETWEEN 200 AND 299
                ORDER BY id DESC
                LIMIT 1
                """
            ),
            {"run_id": run_id},
        ).fetchone()
    return row[0] if row else None


def build_stg_payloads(engine: Engine, run_id: str) -> dict[str, Any]:
    cfg = load_config("STG")
    layer_cfg = cfg.get("layers", {}).get("STG", {}) if isinstance(cfg, dict) else {}
    validations_cfg = layer_cfg.get("validations", {}) if isinstance(layer_cfg, dict) else {}
    validator_names = [k for k, v in validations_cfg.items() if isinstance(v, dict) and v.get("enabled", True)]

    payloads: dict[str, Any] = {}
    cache: dict[str, Any] = {}
    for validator_name in validator_names:
        entity = validator_name.split("_", 1)[0]
        if entity not in cache:
            cache[entity] = _payload_for_entity(engine, run_id, entity)
        if cache[entity] is None:
            continue
        payloads[validator_name] = cache[entity]
    return payloads
