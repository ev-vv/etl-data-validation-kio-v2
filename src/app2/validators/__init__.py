from pathlib import Path
import os
import yaml


def _config_path_for_layer(layer: str) -> Path:
    layer_norm = str(layer or "").strip().upper()
    override = None
    if layer_norm == "STG":
        override = os.environ.get("APP2_VALIDATION_CONFIG_STG")
    elif layer_norm == "DDS":
        override = os.environ.get("APP2_VALIDATION_CONFIG_DDS")

    if override:
        p = Path(override)
        if not p.is_absolute():
            p = Path(__file__).resolve().parents[2] / p  
        return p
    return Path(__file__).resolve().parent / "configs" / f"{layer.lower()}_validation.yml"


def load_config(layer: str = "STG"):
    path = _config_path_for_layer(layer)
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}
