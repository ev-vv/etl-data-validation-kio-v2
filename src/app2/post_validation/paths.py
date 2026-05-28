from __future__ import annotations

from pathlib import Path


def normalize_output_dir(output_dir: Path) -> Path:
    if output_dir.is_absolute():
        return output_dir

    repo_root = Path(__file__).resolve().parents[4]
    if output_dir.parts and output_dir.parts[0] == "pipline_vkr":
        return (repo_root / output_dir).resolve()

    pipline_root = repo_root / "pipline_vkr"
    return (pipline_root / output_dir).resolve()


def tool_output_dir(output_dir: Path, tool_name: str) -> Path:
    normalized = normalize_output_dir(output_dir)
    return normalized if normalized.name == tool_name else normalized / tool_name
