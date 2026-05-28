from __future__ import annotations

import os
import re
import tempfile
import traceback
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml
from sqlalchemy import bindparam, text

from app2.db.connection import get_engine
from app2.db.batch import log_batch_status
from app2.dds.load_dds import run_dds_load
from app2.experiments.config import ExperimentConfig, load_experiment_config
from app2.experiments.db_ops import delete_dds_run, fetch_view_rows
from app2.experiments.report import ExperimentResult, IterationResult, StepResult, render_html_report
from app2.experiments.stg_copy import copy_stg_run_with_mutations
from app2.experiments.stg_payloads import build_stg_payloads
from app2.mutators.dds_mutations import mutate_dds
from app2.validators.dds.referential_suite import run_dds_referential_suite
from app2.validators.dds.rules_suite import run_dds_rules_suite
from app2.validators.dds.source_suite import run_dds_source_completeness_suite, run_dds_source_exclusivity_suite
from app2.validators.stg.completeness_suite import run_stg_completeness_suite
from app2.validators.stg.consistency_suite import run_stg_consistency_suite
from app2.validators.stg.ingestion_suite import run_stg_ingestion_suite
from app2.validators.stg.schema_suite import run_stg_schema_suite
from app2.validators.stg.uniqueness_suite import run_stg_uniqueness_suite


def _now_tag() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _make_run_id(prefix: str, iteration_no: int, layer: str) -> str:
    return f"{prefix}_i{iteration_no:02d}_{layer.lower()}_{_now_tag()}"


def _set_env(key: str, value: str | None):
    if value is None or not str(value).strip():
        os.environ.pop(key, None)
        return
    os.environ[key] = str(value)


def _yaml_search_roots() -> list[Path]:
    src_root = Path(__file__).resolve().parents[2]
    repo_root = Path(os.environ.get("APP2_REPO_ROOT", Path(__file__).resolve().parents[3]))
    roots: list[Path] = [src_root, repo_root, repo_root / "src"]
    seen: set[str] = set()
    unique: list[Path] = []
    for root in roots:
        key = str(root.resolve())
        if key in seen:
            continue
        seen.add(key)
        unique.append(root)
    return unique


def _resolve_yaml_path(path: str | None) -> Path | None:
    if not path:
        return None
    raw = Path(path)
    if raw.is_absolute():
        return raw

    candidates: list[Path] = []
    for root in _yaml_search_roots():
        candidates.append(root / raw)
        if raw.parts and raw.parts[0] == "app2":
            candidates.append(root / "src" / raw)

    for candidate in candidates:
        if candidate.exists():
            return candidate
    return candidates[0] if candidates else raw


def _read_yaml_summary(path: str | None) -> dict[str, Any] | None:
    if not path:
        return None
    p = _resolve_yaml_path(path)
    if p is None:
        return None
    if not p.exists():
        return {"path": str(path), "resolved_path": str(p), "error": "file not found"}
    try:
        data = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
        return {"path": str(path), "resolved_path": str(p), "data": data}
    except Exception as e:
        return {"path": str(path), "resolved_path": str(p), "error": str(e)}


def _load_yaml_file(path: str | None) -> dict[str, Any]:
    p = _resolve_yaml_path(path)
    if p is None or not p.exists():
        return {}
    return yaml.safe_load(p.read_text(encoding="utf-8")) or {}


def _write_yaml_file(path: Path, data: dict[str, Any]):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(data, allow_unicode=True, sort_keys=False), encoding="utf-8")


def _sanitize_filename(value: str) -> str:
    v = re.sub(r"[^a-zA-Z0-9._-]+", "_", value.strip())
    return v or "cfg"


def _materialize_stg_mutations(
    *,
    base_cfg_path: str | None,
    enable: dict[str, list[str]],
    out_dir: Path,
    run_tag: str,
) -> str:
    cfg = _load_yaml_file(base_cfg_path)
    layers = cfg.setdefault("layers", {})
    stg = layers.setdefault("STG", {})
    muts = stg.setdefault("mutations", {})
    if isinstance(muts, dict):
        for k, v in muts.items():
            if isinstance(v, dict):
                v["enabled"] = False
    for entity, actions in enable.items():
        entry = muts.get(entity)
        if not isinstance(entry, dict):
            entry = {}
            muts[entity] = entry
        entry["enabled"] = True
        if actions:
            entry["actions"] = actions
    out_path = out_dir / f"stg_mut_{run_tag}.yml"
    _write_yaml_file(out_path, cfg)
    return str(out_path)


def _materialize_dds_mutations(
    *,
    base_cfg_path: str | None,
    enable: list[str],
    out_dir: Path,
    run_tag: str,
    baseline_stg_run_id: str | None = None,
    baseline_dds_run_id: str | None = None,
) -> str:
    cfg = _load_yaml_file(base_cfg_path)
    layers = cfg.setdefault("layers", {})
    dds = layers.setdefault("DDS", {})
    muts = dds.setdefault("mutations", {})
    if isinstance(muts, dict):
        for k, v in muts.items():
            if isinstance(v, dict):
                v["enabled"] = False
    for key in enable:
        entry = muts.get(key)
        if not isinstance(entry, dict):
            entry = {}
            muts[key] = entry
        entry["enabled"] = True
    if baseline_stg_run_id or baseline_dds_run_id:
        exp = cfg.setdefault("experiment", {})
        bl = exp.setdefault("baseline", {})
        if baseline_stg_run_id:
            bl["stg_run_id"] = baseline_stg_run_id
        if baseline_dds_run_id:
            bl["dds_run_id"] = baseline_dds_run_id
    out_path = out_dir / f"dds_mut_{run_tag}.yml"
    _write_yaml_file(out_path, cfg)
    return str(out_path)


def _materialize_validations(
    *,
    base_cfg_path: str | None,
    layer: str,
    overrides: dict[str, bool],
    out_dir: Path,
    run_tag: str,
) -> str:
    cfg = _load_yaml_file(base_cfg_path)
    layers = cfg.setdefault("layers", {})
    layer_cfg = layers.setdefault(layer, {})
    validations = layer_cfg.setdefault("validations", {})
    if isinstance(validations, dict):
        for name, enabled in overrides.items():
            v = validations.get(name)
            if not isinstance(v, dict):
                v = {}
                validations[name] = v
            v["enabled"] = bool(enabled)
    out_path = out_dir / f"{layer.lower()}_val_{run_tag}.yml"
    _write_yaml_file(out_path, cfg)
    return str(out_path)


def _summarize_stg_mutations(path: str | None) -> dict[str, Any] | None:
    raw = _read_yaml_summary(path)
    if not raw or "data" not in raw:
        return raw
    data = raw["data"]
    layer = (data.get("layers", {}) or {}).get("STG", {}) if isinstance(data, dict) else {}
    muts = (layer.get("mutations", {}) or {}) if isinstance(layer, dict) else {}
    enabled = {}
    for k, v in muts.items():
        if isinstance(v, dict) and v.get("enabled"):
            enabled[k] = v.get("actions", [])
    raw["enabled"] = enabled
    return raw


def _summarize_dds_mutations(path: str | None) -> dict[str, Any] | None:
    raw = _read_yaml_summary(path)
    if not raw or "data" not in raw:
        return raw
    data = raw["data"]
    layer = (data.get("layers", {}) or {}).get("DDS", {}) if isinstance(data, dict) else {}
    muts = (layer.get("mutations", {}) or {}) if isinstance(layer, dict) else {}
    enabled = [k for k, v in muts.items() if isinstance(v, dict) and v.get("enabled")]
    raw["enabled"] = enabled
    return raw


def _summarize_validations(path: str | None, layer_name: str) -> dict[str, Any] | None:
    raw = _read_yaml_summary(path)
    if not raw or "data" not in raw:
        return raw
    data = raw["data"]
    layer = (data.get("layers", {}) or {}).get(layer_name, {}) if isinstance(data, dict) else {}
    vals = (layer.get("validations", {}) or {}) if isinstance(layer, dict) else {}
    enabled = {}
    for k, v in vals.items():
        if isinstance(v, dict) and v.get("enabled", True):
            enabled[k] = str(v.get("severity", "error"))
    raw["enabled"] = enabled
    return raw


def _snapshot(engine, views: list[str], limit: int, *, run_id: str | None) -> dict[str, Any]:
    snapshots: dict[str, Any] = {}
    for view in views:
        try:
            snapshots[view] = fetch_view_rows(engine, view, limit=limit, run_id=run_id)
        except Exception as e:
            snapshots[view] = {"error": str(e)}
    return snapshots


def _resolve_from_stg_run_id(value: str | None, baseline: str) -> str:
    if value is None or value.strip().lower() == "baseline":
        return baseline
    return value.strip()


def _run_stg_validations(engine, dag_id: str, run_id: str, validation_cfg_path: str | None):
    _set_env("APP2_VALIDATION_CONFIG_STG", validation_cfg_path)
    run_stg_ingestion_suite(engine=engine, dag_id=dag_id, run_id=run_id, parent_run_id=run_id)
    payloads = build_stg_payloads(engine, run_id)
    run_stg_schema_suite(engine=engine, dag_id=dag_id, run_id=run_id, parent_run_id=run_id, payloads=payloads)
    run_stg_completeness_suite(engine=engine, dag_id=dag_id, run_id=run_id, parent_run_id=run_id, payloads=payloads)
    run_stg_uniqueness_suite(engine=engine, dag_id=dag_id, run_id=run_id, parent_run_id=run_id, payloads=payloads)
    run_stg_consistency_suite(engine=engine, dag_id=dag_id, run_id=run_id, parent_run_id=run_id, payloads=payloads)


def _run_dds_validations(engine, dag_id: str, dds_run_id: str, parent_run_id: str, validation_cfg_path: str | None, conn):
    _set_env("APP2_VALIDATION_CONFIG_DDS", validation_cfg_path)
    run_dds_referential_suite(engine=engine, dag_id=dag_id, run_id=dds_run_id, parent_run_id=parent_run_id, conn=conn)
    run_dds_source_completeness_suite(engine=engine, dag_id=dag_id, run_id=dds_run_id, parent_run_id=parent_run_id, conn=conn)
    run_dds_source_exclusivity_suite(engine=engine, dag_id=dag_id, run_id=dds_run_id, parent_run_id=parent_run_id, conn=conn)
    run_dds_rules_suite(engine=engine, dag_id=dag_id, run_id=dds_run_id, parent_run_id=parent_run_id, conn=conn)


def _build_capabilities(
    *,
    stg_validation_config: str | None,
    dds_validation_config: str | None,
    stg_mutations_config: str | None,
    dds_mutations_config: str | None,
) -> dict[str, Any]:
    out: dict[str, Any] = {"validations": {}, "mutations": {}}

    def _suites(cfg: dict[str, Any], layer: str) -> list[dict[str, Any]]:
        layer_cfg = (cfg.get("layers", {}) or {}).get(layer, {}) if isinstance(cfg, dict) else {}
        suites = (layer_cfg.get("suites", {}) or {}) if isinstance(layer_cfg, dict) else {}
        if not isinstance(suites, dict):
            return []
        out_suites: list[dict[str, Any]] = []
        for suite_name, suite_cfg in suites.items():
            if not str(suite_name).strip():
                continue
            desc = ""
            entity = ""
            validations = []
            if isinstance(suite_cfg, dict):
                desc = str(suite_cfg.get("description") or "").strip()
                entity = str(suite_cfg.get("entity") or "").strip()
                validations = suite_cfg.get("validations", [])
            if not isinstance(validations, list):
                validations = []
            out_suites.append(
                {
                    "name": str(suite_name),
                    "entity": entity or str(suite_name),
                    "description": desc,
                    "validations": [str(v) for v in validations if isinstance(v, (str, int)) and str(v).strip()],
                }
            )
        return sorted(out_suites, key=lambda x: x.get("name", ""))

    def _validations(cfg: dict[str, Any], layer: str) -> dict[str, Any]:
        layer_cfg = (cfg.get("layers", {}) or {}).get(layer, {}) if isinstance(cfg, dict) else {}
        validations = (layer_cfg.get("validations", {}) or {}) if isinstance(layer_cfg, dict) else {}
        if not isinstance(validations, dict):
            return {}
        out_vals: dict[str, Any] = {}
        for name, item in validations.items():
            if not str(name).strip() or not isinstance(item, dict):
                continue
            out_vals[str(name)] = {
                "entity": str(item.get("entity") or "").strip(),
                "severity": str(item.get("severity") or "").strip(),
                "type": str(item.get("type") or "").strip(),
                "description": str(item.get("description") or "").strip(),
            }
        return out_vals

    def _stg_mutations(cfg: dict[str, Any]) -> list[dict[str, Any]]:
        layer_cfg = (cfg.get("layers", {}) or {}).get("STG", {}) if isinstance(cfg, dict) else {}
        muts = (layer_cfg.get("mutations", {}) or {}) if isinstance(layer_cfg, dict) else {}
        action_desc = (layer_cfg.get("action_descriptions", {}) or {}) if isinstance(layer_cfg, dict) else {}
        if not isinstance(action_desc, dict):
            action_desc = {}
        if not isinstance(muts, dict):
            return []
        out_entities: list[dict[str, Any]] = []
        for entity, item in muts.items():
            if not isinstance(item, dict):
                continue
            actions = item.get("actions", [])
            if isinstance(actions, list):
                act_list = [str(a) for a in actions if str(a).strip()]
            else:
                act_list = []
            out_entities.append(
                {
                    "name": str(entity),
                    "description": str(item.get("description") or "").strip(),
                    "actions": [{"name": a, "description": str(action_desc.get(a) or "").strip()} for a in act_list],
                }
            )
        return sorted(out_entities, key=lambda x: x.get("name", ""))

    def _dds_mutations(cfg: dict[str, Any]) -> list[dict[str, Any]]:
        layer_cfg = (cfg.get("layers", {}) or {}).get("DDS", {}) if isinstance(cfg, dict) else {}
        muts = (layer_cfg.get("mutations", {}) or {}) if isinstance(layer_cfg, dict) else {}
        if not isinstance(muts, dict):
            return []
        out_entities: list[dict[str, Any]] = []
        for key, item in muts.items():
            if not str(key).strip() or not isinstance(item, dict):
                continue
            out_entities.append({"name": str(key), "description": str(item.get("description") or "").strip()})
        return sorted(out_entities, key=lambda x: x.get("name", ""))

    stg_val = _load_yaml_file(stg_validation_config)
    dds_val = _load_yaml_file(dds_validation_config)
    stg_mut = _load_yaml_file(stg_mutations_config)
    dds_mut = _load_yaml_file(dds_mutations_config)

    out["validations"]["STG"] = {"suites": _suites(stg_val, "STG"), "items": _validations(stg_val, "STG")}
    out["validations"]["DDS"] = {"suites": _suites(dds_val, "DDS"), "items": _validations(dds_val, "DDS")}
    out["mutations"]["STG"] = {"entities": _stg_mutations(stg_mut)}
    out["mutations"]["DDS"] = {"entities": _dds_mutations(dds_mut)}
    return out


def _format_mutation_messages(rows: list[tuple[str, str | None]], limit: int = 8) -> str | None:
    swap_teams_re = re.compile(r"swapped home/away teams for (\d+) matches", re.IGNORECASE)

    items: list[str] = []
    seen = set()
    for entity, msg in rows:
        entity = str(entity or "").strip()
        if entity.startswith("STG_mutation_"):
            entity = entity.removeprefix("STG_mutation_")
        elif entity.startswith("DDS_mutation_"):
            entity = entity.removeprefix("DDS_mutation_")
        msg = str(msg or "").strip()
        if not msg:
            continue

        if entity and msg.lower().startswith(f"{entity.lower()}:"):
            msg = msg[len(entity) + 1 :].strip()

        m = swap_teams_re.search(msg)
        if entity == "matches" and m:
            msg = f"swap_teams ({m.group(1)} матчей)"
        key = (entity, msg)
        if key in seen:
            continue
        seen.add(key)
        items.append(f"{entity}: {msg}")
    if not items:
        return None
    more = ""
    if len(items) > limit:
        more = f" (+{len(items) - limit} еще)"
        items = items[:limit]
    return "; ".join(items) + more


def _collect_validation_time_summary(
    *,
    engine,
    capabilities: dict[str, Any] | None,
    iterations: list[IterationResult],
) -> list[dict[str, Any]]:
    if not capabilities:
        return []

    suite_map: dict[tuple[str, str], str] = {}
    suite_entities: dict[str, list[str]] = {"STG": [], "DDS": []}
    for layer in ("STG", "DDS"):
        suites = (capabilities.get("validations", {}) or {}).get(layer, {}).get("suites", []) if isinstance(capabilities, dict) else []
        if not isinstance(suites, list):
            continue
        for s in suites:
            if not isinstance(s, dict):
                continue
            suite_name = str(s.get("name") or "").strip()
            entity = str(s.get("entity") or "").strip()
            if not suite_name or not entity:
                continue
            suite_map[(layer, entity)] = suite_name
            suite_entities[layer].append(entity)

    stmt = (
        text(
            """
            SELECT run_id,
                   entity_name,
                   SUM(EXTRACT(EPOCH FROM (finished_at - started_at))) AS seconds_sum
            FROM tech.etl_load_audit
            WHERE layer = :layer
              AND status IN ('SUCCESS','FAILED')
              AND started_at IS NOT NULL
              AND finished_at IS NOT NULL
              AND run_id = :run_id
              AND entity_name IN :entities
            GROUP BY run_id, entity_name
            """
        )
        .bindparams(bindparam("entities", expanding=True))
    )

    out: list[dict[str, Any]] = []
    with engine.begin() as conn:
        for it in iterations:
            for layer, run_id in (("STG", it.stg_run_id), ("DDS", it.dds_run_id)):
                if not run_id:
                    continue
                entities = suite_entities.get(layer) or []
                if not entities:
                    continue
                rows = conn.execute(stmt, {"layer": layer, "run_id": run_id, "entities": entities}).mappings().all()
                for r in rows:
                    entity = str(r.get("entity_name") or "")
                    seconds_sum = float(r.get("seconds_sum") or 0.0)
                    out.append(
                        {
                            "iteration_no": it.iteration_no,
                            "iteration_name": it.name,
                            "layer": layer,
                            "suite": suite_map.get((layer, entity), entity),
                            "entity": entity,
                            "run_id": run_id,
                            "seconds_sum": round(seconds_sum, 3),
                        }
                    )
    return sorted(out, key=lambda x: (int(x.get("iteration_no") or 0), x.get("layer", ""), x.get("suite", "")))


def run_experiment(cfg: ExperimentConfig, output_dir: Path) -> Path:
    engine = get_engine()
    gen_dir = Path(tempfile.mkdtemp(prefix=f"app2_experiment_{_sanitize_filename(cfg.name)}_"))

    snapshot_views = cfg.baseline.snapshot_views or [
        "mart.v_competition_season_kpi",
        "mart.v_team_season_results",
    ]

    if not cfg.baseline.dds_run_id:
        raise ValueError("experiment.baseline.dds_run_id is required for mart snapshot filtering by run_id.")

    baseline = IterationResult(
        iteration_no=0,
        name="baseline",
        kind="baseline",
        stg_run_id=cfg.baseline.stg_run_id,
        dds_run_id=cfg.baseline.dds_run_id,
        status="SUCCESS",
        error_message=None,
        configs={"stg_run_id": cfg.baseline.stg_run_id, "dds_run_id": cfg.baseline.dds_run_id},
        snapshots=_snapshot(engine, snapshot_views, limit=cfg.defaults.snapshot_limit, run_id=cfg.baseline.dds_run_id),
        steps=[StepResult(name="MART: снимок витрин", status="SUCCESS", details=f"snapshot_limit={cfg.defaults.snapshot_limit}")],
    )

    iterations: list[IterationResult] = []
    prefix = f"exp_{cfg.name.replace(' ', '_')}"
    capabilities = _build_capabilities(
        stg_validation_config=cfg.defaults.stg_validation_config,
        dds_validation_config=cfg.defaults.dds_validation_config,
        stg_mutations_config=cfg.defaults.stg_mutations_config,
        dds_mutations_config=cfg.defaults.dds_mutations_config,
    )

    for idx, it_cfg in enumerate(cfg.iterations, start=1):
        stg_run_id: str | None = None
        dds_run_id: str | None = None
        status = "SUCCESS"
        err: str | None = None
        steps: list[StepResult] = []
        snapshots: dict[str, Any] = {}
        expected: list[tuple[str, str | None]] = []

        iter_views = it_cfg.snapshot_views or snapshot_views

        stg_mut_cfg = it_cfg.stg_mutations_config or cfg.defaults.stg_mutations_config
        dds_mut_cfg = it_cfg.dds_mutations_config or cfg.defaults.dds_mutations_config
        stg_val_cfg = it_cfg.stg_validation_config or cfg.defaults.stg_validation_config
        dds_val_cfg = it_cfg.dds_validation_config or cfg.defaults.dds_validation_config

        run_tag = f"i{idx:02d}_{_now_tag()}"
        if it_cfg.stg_mutations_enable:
            stg_mut_cfg = _materialize_stg_mutations(
                base_cfg_path=stg_mut_cfg,
                enable=it_cfg.stg_mutations_enable,
                out_dir=gen_dir,
                run_tag=run_tag,
            )
        if it_cfg.dds_mutations_enable:
            dds_mut_cfg = _materialize_dds_mutations(
                base_cfg_path=dds_mut_cfg,
                enable=it_cfg.dds_mutations_enable,
                out_dir=gen_dir,
                run_tag=run_tag,
                baseline_stg_run_id=cfg.baseline.stg_run_id,
                baseline_dds_run_id=cfg.baseline.dds_run_id,
            )
        if it_cfg.stg_validation_overrides:
            stg_val_cfg = _materialize_validations(
                base_cfg_path=stg_val_cfg,
                layer="STG",
                overrides=it_cfg.stg_validation_overrides,
                out_dir=gen_dir,
                run_tag=run_tag,
            )
        if it_cfg.dds_validation_overrides:
            dds_val_cfg = _materialize_validations(
                base_cfg_path=dds_val_cfg,
                layer="DDS",
                overrides=it_cfg.dds_validation_overrides,
                out_dir=gen_dir,
                run_tag=run_tag,
            )

        configs_used: dict[str, Any] = {
            "stg_mutations": _summarize_stg_mutations(stg_mut_cfg),
            "dds_mutations": _summarize_dds_mutations(dds_mut_cfg),
            "stg_validations": _summarize_validations(stg_val_cfg, "STG"),
            "dds_validations": _summarize_validations(dds_val_cfg, "DDS"),
        }

        def _enabled_stg_mutations() -> str:
            cfg0 = configs_used.get("stg_mutations") or {}
            enabled = (cfg0.get("enabled") or {}) if isinstance(cfg0, dict) else {}
            if not isinstance(enabled, dict) or not enabled:
                return "мутации отключены"
            parts = []
            for entity, actions in enabled.items():
                if isinstance(actions, list) and actions:
                    parts.append(f"{entity}: {', '.join(map(str, actions))}")
                else:
                    parts.append(str(entity))
            return "; ".join(parts)

        def _enabled_dds_mutations() -> str:
            cfg0 = configs_used.get("dds_mutations") or {}
            enabled = cfg0.get("enabled") if isinstance(cfg0, dict) else None
            if not isinstance(enabled, list) or not enabled:
                return "мутации отключены"
            return ", ".join(map(str, enabled))

        def _enabled_validations(layer_key: str) -> str:
            cfg0 = configs_used.get(layer_key) or {}
            enabled = cfg0.get("enabled") if isinstance(cfg0, dict) else None
            if not isinstance(enabled, dict) or not enabled:
                return "валидации отключены"
            return f"проверок: {len(enabled)}"

        def _run_step(name: str, fn, *, skipped: bool = False, details: str | None = None):
            if skipped:
                steps.append(StepResult(name=name, status="SKIPPED", details=details))
                return None
            try:
                result0 = fn()
                steps.append(StepResult(name=name, status="SUCCESS", details=details))
                return result0
            except Exception:
                steps.append(StepResult(name=name, status="FAILED", details=details, error=traceback.format_exc()))
                raise

        def _append_step_details(step_name: str, extra: str):
            extra = (extra or "").strip()
            if not extra:
                return
            for s in steps:
                if s.name == step_name:
                    if s.details:
                        s.details = f"{s.details}; {extra}"
                    else:
                        s.details = extra
                    return

        prev_env: dict[str, str | None] = {}
        try:
            if it_cfg.env:
                for k, v in it_cfg.env.items():
                    prev_env[k] = os.environ.get(k)
                    os.environ[k] = str(v)
        except Exception:
            prev_env = {}

        def _finalize_steps():
            existing = {s.name for s in steps}
            failed = any(s.status == "FAILED" for s in steps)
            for step_name, step_details in expected:
                if step_name in existing:
                    continue
                if failed:
                    steps.append(StepResult(name=step_name, status="SKIPPED", details=step_details or "не выполнено из-за ошибки на предыдущем шаге"))
                else:
                    steps.append(StepResult(name=step_name, status="SKIPPED", details=step_details))

        try:
            kind = it_cfg.kind.strip().lower()
            if kind == "snapshot":
                expected = [
                    ("STG: raw слой", f"используется baseline stg_run_id={cfg.baseline.stg_run_id}"),
                    ("DDS: загрузка", "используется baseline"),
                    ("MART: снимок витрин", f"snapshot_limit={cfg.defaults.snapshot_limit}"),
                ]
                _run_step("STG: raw слой", lambda: None, skipped=True, details=expected[0][1])
                _run_step("DDS: загрузка", lambda: None, skipped=True, details=expected[1][1])
                snapshots = _run_step(
                    "MART: снимок витрин",
                    lambda: _snapshot(engine, iter_views, limit=cfg.defaults.snapshot_limit, run_id=cfg.baseline.dds_run_id),
                    details=expected[2][1],
                )
                _finalize_steps()

            elif kind == "stg_mutation":
                src_run = _resolve_from_stg_run_id(it_cfg.from_stg_run_id, cfg.baseline.stg_run_id)
                stg_run_id = _make_run_id(prefix, idx, "stg")
                dds_run_id = _make_run_id(prefix, idx, "dds")

                expected = [
                    ("STG: raw слой", f"используется baseline stg_run_id={src_run}"),
                    ("STG: мутация", f"копирование run_id={src_run} -> {stg_run_id}; {_enabled_stg_mutations()}"),
                    ("STG: валидация", _enabled_validations("stg_validations")),
                    ("DDS: подготовка", f"очистка версии dds_run_id={dds_run_id}"),
                    ("DDS: загрузка", f"parent_run_id={stg_run_id}"),
                    ("DDS: мутация", _enabled_dds_mutations()),
                    ("DDS: валидация", _enabled_validations("dds_validations")),
                    ("MART: снимок витрин", f"snapshot_limit={cfg.defaults.snapshot_limit}"),
                ]

                _run_step("STG: raw слой", lambda: None, skipped=True, details=expected[0][1])
                _set_env("APP2_STG_MUTATIONS_CONFIG", stg_mut_cfg)
                _run_step(
                    "STG: мутация",
                    lambda: copy_stg_run_with_mutations(
                        engine=engine,
                        dag_id=cfg.defaults.dag_id_stg,
                        source_run_id=src_run,
                        target_run_id=stg_run_id,
                        parent_run_id=src_run,
                        apply_mutations=True,
                    ),
                    details=expected[1][1],
                )
                try:
                    with engine.begin() as conn:
                        rows = conn.execute(
                            text(
                                """
                                SELECT entity_name, message
                                FROM tech.etl_load_audit
                                WHERE run_id = :run_id
                                  AND layer = 'STG'
                                  AND status = 'MUTATED'
                                  AND entity_name LIKE 'STG_mutation_%'
                                ORDER BY audit_id DESC
                                LIMIT 50
                                """
                            ),
                            {"run_id": stg_run_id},
                        ).fetchall()
                    msg = _format_mutation_messages([(r[0], r[1]) for r in rows])
                    if msg:
                        _append_step_details("STG: мутация", msg)
                except Exception:
                    pass
                _run_step(
                    "STG: валидация",
                    lambda: _run_stg_validations(engine, cfg.defaults.dag_id_stg, stg_run_id, stg_val_cfg),
                    skipped=not it_cfg.run_stg_validation,
                    details=expected[2][1] if it_cfg.run_stg_validation else "отключено в конфиге эксперимента",
                )
                log_batch_status(engine, dag_id=cfg.defaults.dag_id_stg, run_id=stg_run_id, parent_run_id=src_run, layer="STG", status="SUCCESS")

                _run_step("DDS: подготовка", lambda: delete_dds_run(engine, dds_run_id), details=expected[3][1])

                log_batch_status(engine, dag_id=cfg.defaults.dag_id_dds, run_id=dds_run_id, parent_run_id=stg_run_id, layer="DDS", status="PROCESSING")
                with engine.begin() as conn:
                    _run_step(
                        "DDS: загрузка",
                        lambda: run_dds_load(conn=conn, dag_id=cfg.defaults.dag_id_dds, dds_run_id=dds_run_id, parent_run_id=stg_run_id),
                        details=expected[4][1],
                    )
                    _set_env("APP2_DDS_MUTATIONS_CONFIG", dds_mut_cfg)
                    _run_step(
                        "DDS: мутация",
                        lambda: mutate_dds(engine, dag_id=cfg.defaults.dag_id_dds, run_id=dds_run_id, conn=conn),
                        skipped=not bool(dds_mut_cfg),
                        details=expected[5][1] if dds_mut_cfg else "мутации не заданы",
                    )
                    try:
                        msg = conn.execute(
                            text(
                                """
                                SELECT message
                                FROM tech.etl_load_audit
                                WHERE run_id = :run_id
                                  AND layer = 'DDS'
                                  AND entity_name = 'DDS_mutation'
                                  AND status = 'MUTATED'
                                ORDER BY audit_id DESC
                                LIMIT 1
                                """
                            ),
                            {"run_id": dds_run_id},
                        ).scalar()
                        if msg:
                            _append_step_details("DDS: мутация", str(msg))
                    except Exception:
                        pass
                    _run_step(
                        "DDS: валидация",
                        lambda: _run_dds_validations(engine, cfg.defaults.dag_id_dds, dds_run_id, stg_run_id, dds_val_cfg, conn),
                        skipped=not it_cfg.run_dds_validation,
                        details=expected[6][1] if it_cfg.run_dds_validation else "отключено в конфиге эксперимента",
                    )
                log_batch_status(engine, dag_id=cfg.defaults.dag_id_dds, run_id=dds_run_id, parent_run_id=stg_run_id, layer="DDS", status="SUCCESS")

                snapshots = _run_step(
                    "MART: снимок витрин",
                    lambda: _snapshot(engine, iter_views, limit=cfg.defaults.snapshot_limit, run_id=dds_run_id),
                    details=expected[7][1],
                )
                _finalize_steps()

            elif kind == "dds_mutation":
                src_run = _resolve_from_stg_run_id(it_cfg.from_stg_run_id, cfg.baseline.stg_run_id)
                dds_run_id = _make_run_id(prefix, idx, "dds")

                expected = [
                    ("STG: raw слой", f"используется baseline stg_run_id={src_run}"),
                    ("DDS: подготовка", f"очистка версии dds_run_id={dds_run_id}"),
                    ("DDS: загрузка", f"parent_run_id={src_run}"),
                    ("DDS: мутация", _enabled_dds_mutations()),
                    ("DDS: валидация", _enabled_validations("dds_validations")),
                    ("MART: снимок витрин", f"snapshot_limit={cfg.defaults.snapshot_limit}"),
                ]

                _run_step("STG: raw слой", lambda: None, skipped=True, details=expected[0][1])
                _run_step("DDS: подготовка", lambda: delete_dds_run(engine, dds_run_id), details=expected[1][1])

                log_batch_status(engine, dag_id=cfg.defaults.dag_id_dds, run_id=dds_run_id, parent_run_id=src_run, layer="DDS", status="PROCESSING")
                with engine.begin() as conn:
                    _run_step(
                        "DDS: загрузка",
                        lambda: run_dds_load(conn=conn, dag_id=cfg.defaults.dag_id_dds, dds_run_id=dds_run_id, parent_run_id=src_run),
                        details=expected[2][1],
                    )
                    _set_env("APP2_DDS_MUTATIONS_CONFIG", dds_mut_cfg)
                    _run_step(
                        "DDS: мутация",
                        lambda: mutate_dds(engine, dag_id=cfg.defaults.dag_id_dds, run_id=dds_run_id, conn=conn),
                        skipped=not bool(dds_mut_cfg),
                        details=expected[3][1] if dds_mut_cfg else "мутации не заданы",
                    )
                    try:
                        msg = conn.execute(
                            text(
                                """
                                SELECT message
                                FROM tech.etl_load_audit
                                WHERE run_id = :run_id
                                  AND layer = 'DDS'
                                  AND entity_name = 'DDS_mutation'
                                  AND status = 'MUTATED'
                                ORDER BY audit_id DESC
                                LIMIT 1
                                """
                            ),
                            {"run_id": dds_run_id},
                        ).scalar()
                        if msg:
                            _append_step_details("DDS: мутация", str(msg))
                    except Exception:
                        pass
                    _run_step(
                        "DDS: валидация",
                        lambda: _run_dds_validations(engine, cfg.defaults.dag_id_dds, dds_run_id, src_run, dds_val_cfg, conn),
                        skipped=not it_cfg.run_dds_validation,
                        details=expected[4][1] if it_cfg.run_dds_validation else "отключено в конфиге эксперимента",
                    )
                log_batch_status(engine, dag_id=cfg.defaults.dag_id_dds, run_id=dds_run_id, parent_run_id=src_run, layer="DDS", status="SUCCESS")

                snapshots = _run_step(
                    "MART: снимок витрин",
                    lambda: _snapshot(engine, iter_views, limit=cfg.defaults.snapshot_limit, run_id=dds_run_id),
                    details=expected[5][1],
                )
                _finalize_steps()

            else:
                raise ValueError(f"Unknown iteration kind: {it_cfg.kind}")

        except Exception:
            status = "FAILED"
            err = traceback.format_exc()
            try:
                if stg_run_id:
                    parent = _resolve_from_stg_run_id(it_cfg.from_stg_run_id, cfg.baseline.stg_run_id)
                    log_batch_status(
                        engine,
                        dag_id=cfg.defaults.dag_id_stg,
                        run_id=stg_run_id,
                        parent_run_id=parent,
                        layer="STG",
                        status="FAILED",
                        error_message="Experiment iteration failed",
                    )
                if dds_run_id:
                    parent = stg_run_id or _resolve_from_stg_run_id(it_cfg.from_stg_run_id, cfg.baseline.stg_run_id)
                    log_batch_status(
                        engine,
                        dag_id=cfg.defaults.dag_id_dds,
                        run_id=dds_run_id,
                        parent_run_id=parent,
                        layer="DDS",
                        status="FAILED",
                        error_message="Experiment iteration failed",
                    )
            except Exception:
                pass

            snapshots = {}
            _finalize_steps()
        finally:
            if prev_env:
                for k, prev in prev_env.items():
                    if prev is None:
                        os.environ.pop(k, None)
                    else:
                        os.environ[k] = prev

        iterations.append(
            IterationResult(
                iteration_no=idx,
                name=it_cfg.name,
                kind=it_cfg.kind,
                stg_run_id=stg_run_id,
                dds_run_id=dds_run_id,
                status=status,
                error_message=err,
                configs=configs_used,
                snapshots=snapshots,
                steps=steps,
            )
        )

    validation_time_summary = _collect_validation_time_summary(engine=engine, capabilities=capabilities, iterations=iterations)
    result = ExperimentResult(
        name=cfg.name,
        created_at=datetime.now(),
        baseline=baseline,
        iterations=iterations,
        capabilities=capabilities,
        validation_time_summary=validation_time_summary,
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / f"experiment_{cfg.name.replace(' ', '_')}_{_now_tag()}.html"
    render_html_report(result, out_path)
    return out_path


__all__ = ["run_experiment"]
