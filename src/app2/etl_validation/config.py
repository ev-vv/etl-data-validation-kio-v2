from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class ToolsBaselineConfig:
    stg_run_id: str
    dds_run_id: str


@dataclass(frozen=True)
class ToolsDefaultsConfig:
    output_dir: str = "/opt/airflow/post_validation_reports/etl_stage"
    include_experiments: bool = True
    only_unprocessed: bool = True
    repeats: int = 1
    tools_by_stage: dict[str, list[str]] | None = None


@dataclass(frozen=True)
class ToolsExperimentConfig:
    name: str
    baseline: ToolsBaselineConfig
    defaults: ToolsDefaultsConfig


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
        if value < 1:
            raise ValueError(f"Config field '{field}' must be a positive integer.")
        return value
    raise ValueError(f"Config field '{field}' must be integer.")


def _as_dict_str_list_str(value: Any, *, field: str) -> dict[str, list[str]] | None:
    if value is None:
        return None
    if not isinstance(value, dict) or not all(isinstance(k, str) for k in value.keys()):
        raise ValueError(f"Config field '{field}' must be a mapping of string keys to list of strings.")
    out: dict[str, list[str]] = {}
    for k, v in value.items():
        if not isinstance(v, list) or not all(isinstance(item, str) for item in v):
            raise ValueError(f"Config field '{field}.{k}' must be a list of strings.")
        kk = k.strip().upper()
        vv = [item.strip().lower() for item in v if item.strip()]
        if kk:
            out[kk] = vv
    return out or None


def load_tools_experiment_config(path: str | Path) -> ToolsExperimentConfig:
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
    baseline = ToolsBaselineConfig(
        stg_run_id=_as_str(baseline_raw.get("stg_run_id"), field="experiment.baseline.stg_run_id"),
        dds_run_id=_as_str(baseline_raw.get("dds_run_id"), field="experiment.baseline.dds_run_id"),
    )

    defaults_raw = exp.get("defaults", {})
    if defaults_raw is None:
        defaults_raw = {}
    if not isinstance(defaults_raw, dict):
        raise ValueError("Field 'experiment.defaults' must be a mapping.")
    defaults = ToolsDefaultsConfig(
        output_dir=str(defaults_raw.get("output_dir", ToolsDefaultsConfig.output_dir)),
        include_experiments=_as_bool(
            defaults_raw.get("include_experiments"),
            field="experiment.defaults.include_experiments",
            default=ToolsDefaultsConfig.include_experiments,
        ),
        only_unprocessed=_as_bool(
            defaults_raw.get("only_unprocessed"),
            field="experiment.defaults.only_unprocessed",
            default=ToolsDefaultsConfig.only_unprocessed,
        ),
        repeats=_as_int(
            defaults_raw.get("repeats"),
            field="experiment.defaults.repeats",
            default=ToolsDefaultsConfig.repeats,
        ),
        tools_by_stage=_as_dict_str_list_str(
            defaults_raw.get("tools_by_stage"),
            field="experiment.defaults.tools_by_stage",
        ),
    )

    return ToolsExperimentConfig(name=name, baseline=baseline, defaults=defaults)


__all__ = ["ToolsExperimentConfig", "load_tools_experiment_config"]
