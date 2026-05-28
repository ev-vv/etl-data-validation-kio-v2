import copy
import copy
import os
import random
from pathlib import Path
import yaml

from app2.db.audit import audit_log

MUTATION_CONFIG_PATH = Path(__file__).resolve().parent / "configs" / "stg_mutations.yml"


def load_mutation_config():
    override = os.environ.get("APP2_STG_MUTATIONS_CONFIG")
    if override:
        p = Path(override)
        if not p.is_absolute():
            p = Path(__file__).resolve().parents[2] / p  
        config_path = p
    else:
        config_path = MUTATION_CONFIG_PATH
    if not config_path.exists():
        return {}
    with config_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _mutate_list(payload: dict, list_key: str, action: str, *, rng: random.Random | None = None):
    if action == "drop_matches_key" and list_key == "matches":
        if isinstance(payload, dict) and "matches" in payload:
            payload.pop("matches", None)
            return "matches: removed key 'matches'"
    arr = payload.get(list_key) if isinstance(payload, dict) else None
    if not isinstance(arr, list) or not arr:
        return None
    if action == "duplicate_first":
        arr.append(copy.deepcopy(arr[0]))
        return f"{list_key}: duplicated first element"
    if action == "drop_required":
        if isinstance(arr[0], dict):
            for field in ("id", "name", "utcDate"):
                if field in arr[0]:
                    arr[0].pop(field, None)
                    return f"{list_key}: removed field '{field}' from first element"
        return None
    if action == "corrupt_id":
        if isinstance(arr[0], dict) and "id" in arr[0]:
            arr[0]["id"] = "abc"
            return f"{list_key}: corrupted id to string"
    if action == "matchday_out_of_range" and list_key == "matches":
        if isinstance(arr[0], dict):
            arr[0]["matchday"] = "999"
            return f"{list_key}: set matchday to out-of-range value"
    if action == "swap_teams" and list_key == "matches":
        try:
            count = int(os.environ.get("APP2_STG_SWAP_TEAMS_COUNT", "5"))
        except Exception:
            count = 5
        count = max(1, count)
        count = min(count, len(arr))
        rng = rng or random.Random()

        mutated: list[str] = []
        for i in rng.sample(range(len(arr)), k=count):
            match = arr[i]
            if not isinstance(match, dict):
                continue
            home = match.get("homeTeam")
            away = match.get("awayTeam")
            if not isinstance(home, dict) or not isinstance(away, dict):
                continue

            match_id = match.get("id")
            home_id = home.get("id")
            away_id = away.get("id")
            home_name = home.get("name")
            away_name = away.get("name")

            match["homeTeam"], match["awayTeam"] = away, home

            if match_id is not None:
                desc = f"id={match_id}"
            else:
                desc = f"index={i}"
            if home_id is not None and away_id is not None:
                desc += f" ({home_id}<->{away_id})"
            elif home_name and away_name:
                desc += f" ({home_name}<->{away_name})"
            mutated.append(desc)

        if mutated:
            return f"matches: swapped home/away teams for {len(mutated)} random matches: " + ", ".join(mutated)
    return None


def mutate_payload(engine, layer: str, dag_id: str | None, run_id: str | None, kind: str, payload):
    cfg = load_mutation_config()
    layer_cfg = cfg.get("layers", {}).get(layer, {}) if isinstance(cfg, dict) else {}
    mut_cfg = layer_cfg.get("mutations", {}).get(kind, {}) if isinstance(layer_cfg, dict) else {}
    if not mut_cfg.get("enabled", False):
        return payload, False

    actions = mut_cfg.get("actions", []) if isinstance(mut_cfg, dict) else []
    mutated = copy.deepcopy(payload)
    performed = []
    for action in actions:
        seed = f"{run_id or ''}:{layer}:{kind}:{action}"
        desc = _mutate_list(mutated, kind if kind != "matches" else "matches", action, rng=random.Random(seed))
        if desc:
            performed.append(desc)

    if performed and engine and dag_id and run_id:
        audit_log(
            engine,
            dag_id=dag_id,
            run_id=run_id,
            layer=layer,
            entity_name=f"{layer}_mutation_{kind}",
            status="MUTATED",
            message="; ".join(performed),
        )
    return (mutated if performed else payload), bool(performed)
