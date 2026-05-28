from __future__ import annotations

import json
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine


def delete_dds_run(engine: Engine, run_id: str) -> None:
    run_id = (run_id or "").strip()
    if not run_id:
        return
    ordered_tables = [
        "dds.fact_match_score",
        "dds.fact_match",
        "dds.fact_standing",
        "dds.dim_season",
        "dds.dim_team",
        "dds.dim_competition",
        "dds.dim_area",
    ]
    with engine.begin() as conn:
        for table in ordered_tables:
            conn.execute(text(f"DELETE FROM {table} WHERE run_id = :run_id"), {"run_id": run_id})


def fetch_view_rows(engine: Engine, view_name: str, limit: int = 200, *, run_id: str | None = None) -> list[dict[str, Any]]:
    view = view_name.strip()
    order_by = ""
    if view.lower() == "mart.v_competition_season_kpi":
        order_by = " ORDER BY matches_total DESC NULLS LAST"
    elif view.lower() == "mart.v_team_season_results":
        order_by = " ORDER BY points_calc DESC NULLS LAST"

    with engine.begin() as conn:
        if run_id is not None:
            rows = conn.execute(
                text(f"SELECT * FROM {view} WHERE run_id = :run_id{order_by} LIMIT :limit"),
                {"run_id": run_id, "limit": limit},
            ).mappings().all()
        else:
            rows = conn.execute(text(f"SELECT * FROM {view}{order_by} LIMIT :limit"), {"limit": limit}).mappings().all()
    out: list[dict[str, Any]] = []
    for r in rows:
        row = dict(r)
        if run_id is not None and "run_id" in row:
            row.pop("run_id", None)
        out.append(row)
    return out


def json_dumps_safe(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, default=str, indent=2)
