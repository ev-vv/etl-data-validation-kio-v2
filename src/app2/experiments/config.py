from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class BaselineConfig:
    stg_run_id: str
    dds_run_id: str | None = None
    snapshot_views: list[str] | None = None


@dataclass(frozen=True)
class IterationConfig:
    name: str
    kind: str
    from_stg_run_id: str | None = None

    stg_mutations_config: str | None = None
    dds_mutations_config: str | None = None
    stg_validation_config: str | None = None
    dds_validation_config: str | None = None

    stg_mutations_enable: dict[str, list[str]] | None = None
    dds_mutations_enable: list[str] | None = None
    stg_validation_overrides: dict[str, bool] | None = None
    dds_validation_overrides: dict[str, bool] | None = None
    env: dict[str, str] | None = None

    run_stg_validation: bool = True
    run_dds_validation: bool = True
    truncate_dds: bool | None = None

    snapshot_views: list[str] | None = None


@dataclass(frozen=True)
class DefaultsConfig:
    dag_id_stg: str = "stg_football_raw_app2"
    dag_id_dds: str = "dds_football_load_app2"
    snapshot_limit: int = 200
    truncate_dds_before_iteration: bool = True

    stg_mutations_config: str | None = None
    dds_mutations_config: str | None = None
    stg_validation_config: str | None = None
    dds_validation_config: str | None = None


@dataclass(frozen=True)
class ExperimentConfig:
    name: str
    baseline: BaselineConfig
    defaults: DefaultsConfig
    iterations: list[IterationConfig]


def _as_str(value: Any, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Config field '{field}' must be a non-empty string.")
    return value.strip()


def _as_bool(value: Any, *, field: str, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    raise ValueError(f"Config field '{field}' must be boolean.")


def _as_int(value: Any, *, field: str, default: int) -> int:
    if value is None:
        return default
    if isinstance(value, int):
        return value
    raise ValueError(f"Config field '{field}' must be integer.")


def _as_list_str(value: Any, *, field: str) -> list[str] | None:
    if value is None:
        return None
    if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
        raise ValueError(f"Config field '{field}' must be a list of strings.")
    items = [v.strip() for v in value if v.strip()]
    return items or None


def _as_dict_str_bool(value: Any, *, field: str) -> dict[str, bool] | None:
    if value is None:
        return None
    if not isinstance(value, dict) or not all(isinstance(k, str) for k in value.keys()):
        raise ValueError(f"Config field '{field}' must be a mapping of string keys.")
    out: dict[str, bool] = {}
    for k, v in value.items():
        if not isinstance(v, bool):
            raise ValueError(f"Config field '{field}.{k}' must be boolean.")
        kk = k.strip()
        if kk:
            out[kk] = v
    return out or None


def _as_dict_str_list_str(value: Any, *, field: str) -> dict[str, list[str]] | None:
    if value is None:
        return None
    if not isinstance(value, dict) or not all(isinstance(k, str) for k in value.keys()):
        raise ValueError(f"Config field '{field}' must be a mapping of string keys to list of strings.")
    out: dict[str, list[str]] = {}
    for k, v in value.items():
        if not isinstance(v, list) or not all(isinstance(item, str) for item in v):
            raise ValueError(f"Config field '{field}.{k}' must be a list of strings.")
        kk = k.strip()
        vv = [item.strip() for item in v if item.strip()]
        if kk:
            out[kk] = vv
    return out or None


def _as_dict_str_str(value: Any, *, field: str) -> dict[str, str] | None:
    if value is None:
        return None
    if not isinstance(value, dict) or not all(isinstance(k, str) for k in value.keys()):
        raise ValueError(f"Config field '{field}' must be a mapping of string keys.")
    out: dict[str, str] = {}
    for k, v in value.items():
        if v is None:
            continue
        kk = k.strip()
        if not kk:
            continue
        out[kk] = str(v)
    return out or None


def load_experiment_config(path: str | Path) -> ExperimentConfig:
    path = Path(path)
    if not path.is_absolute():
        base_dir = Path(__file__).resolve().parents[2]  
        path = base_dir / path
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    exp = raw.get("experiment") if isinstance(raw, dict) else None
    if not isinstance(exp, dict):
        raise ValueError("Top-level key 'experiment' must be a mapping.")

    name = _as_str(exp.get("name"), field="experiment.name")

    baseline_raw = exp.get("baseline", {})
    if not isinstance(baseline_raw, dict):
        raise ValueError("Field 'experiment.baseline' must be a mapping.")
    baseline = BaselineConfig(
        stg_run_id=_as_str(baseline_raw.get("stg_run_id"), field="experiment.baseline.stg_run_id"),
        dds_run_id=baseline_raw.get("dds_run_id") if isinstance(baseline_raw.get("dds_run_id"), str) else None,
        snapshot_views=_as_list_str(baseline_raw.get("snapshot_views"), field="experiment.baseline.snapshot_views"),
    )

    defaults_raw = exp.get("defaults", {})
    if defaults_raw is None:
        defaults_raw = {}
    if not isinstance(defaults_raw, dict):
        raise ValueError("Field 'experiment.defaults' must be a mapping.")
    defaults = DefaultsConfig(
        dag_id_stg=str(defaults_raw.get("dag_id_stg", DefaultsConfig.dag_id_stg)),
        dag_id_dds=str(defaults_raw.get("dag_id_dds", DefaultsConfig.dag_id_dds)),
        snapshot_limit=_as_int(defaults_raw.get("snapshot_limit"), field="experiment.defaults.snapshot_limit", default=DefaultsConfig.snapshot_limit),
        truncate_dds_before_iteration=_as_bool(
            defaults_raw.get("truncate_dds_before_iteration"),
            field="experiment.defaults.truncate_dds_before_iteration",
            default=DefaultsConfig.truncate_dds_before_iteration,
        ),
        stg_mutations_config=defaults_raw.get("stg_mutations_config") if isinstance(defaults_raw.get("stg_mutations_config"), str) else None,
        dds_mutations_config=defaults_raw.get("dds_mutations_config") if isinstance(defaults_raw.get("dds_mutations_config"), str) else None,
        stg_validation_config=defaults_raw.get("stg_validation_config") if isinstance(defaults_raw.get("stg_validation_config"), str) else None,
        dds_validation_config=defaults_raw.get("dds_validation_config") if isinstance(defaults_raw.get("dds_validation_config"), str) else None,
    )

    iterations_raw = exp.get("iterations", [])
    if not isinstance(iterations_raw, list):
        raise ValueError("Field 'experiment.iterations' must be a list.")
    iterations: list[IterationConfig] = []
    for idx, it in enumerate(iterations_raw, start=1):
        if not isinstance(it, dict):
            raise ValueError(f"Iteration #{idx} must be a mapping.")
        iterations.append(
            IterationConfig(
                name=_as_str(it.get("name", f"iteration_{idx}"), field=f"experiment.iterations[{idx}].name"),
                kind=_as_str(it.get("kind", "snapshot"), field=f"experiment.iterations[{idx}].kind"),
                from_stg_run_id=it.get("from_stg_run_id") if isinstance(it.get("from_stg_run_id"), str) else None,
                stg_mutations_config=it.get("stg_mutations_config") if isinstance(it.get("stg_mutations_config"), str) else None,
                dds_mutations_config=it.get("dds_mutations_config") if isinstance(it.get("dds_mutations_config"), str) else None,
                stg_validation_config=it.get("stg_validation_config") if isinstance(it.get("stg_validation_config"), str) else None,
                dds_validation_config=it.get("dds_validation_config") if isinstance(it.get("dds_validation_config"), str) else None,
                stg_mutations_enable=_as_dict_str_list_str(it.get("stg_mutations_enable"), field=f"experiment.iterations[{idx}].stg_mutations_enable"),
                dds_mutations_enable=_as_list_str(it.get("dds_mutations_enable"), field=f"experiment.iterations[{idx}].dds_mutations_enable"),
                stg_validation_overrides=_as_dict_str_bool(it.get("stg_validation_overrides"), field=f"experiment.iterations[{idx}].stg_validation_overrides"),
                dds_validation_overrides=_as_dict_str_bool(it.get("dds_validation_overrides"), field=f"experiment.iterations[{idx}].dds_validation_overrides"),
                env=_as_dict_str_str(it.get("env"), field=f"experiment.iterations[{idx}].env"),
                run_stg_validation=_as_bool(it.get("run_stg_validation"), field=f"experiment.iterations[{idx}].run_stg_validation", default=True),
                run_dds_validation=_as_bool(it.get("run_dds_validation"), field=f"experiment.iterations[{idx}].run_dds_validation", default=True),
                truncate_dds=it.get("truncate_dds") if isinstance(it.get("truncate_dds"), bool) else None,
                snapshot_views=_as_list_str(it.get("snapshot_views"), field=f"experiment.iterations[{idx}].snapshot_views"),
            )
        )

    return ExperimentConfig(name=name, baseline=baseline, defaults=defaults, iterations=iterations)
