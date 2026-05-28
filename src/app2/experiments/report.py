from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from jinja2 import BaseLoader, Environment, select_autoescape

from app2.experiments.db_ops import json_dumps_safe


@dataclass
class StepResult:
    name: str
    status: str
    details: str | None = None
    error: str | None = None


@dataclass
class IterationResult:
    iteration_no: int
    name: str
    kind: str
    stg_run_id: str | None
    dds_run_id: str | None
    status: str
    error_message: str | None
    configs: dict[str, Any]
    snapshots: dict[str, list[dict[str, Any]] | dict[str, Any]]
    steps: list[StepResult] = field(default_factory=list)
    comparisons: dict[str, Any] | None = None
    stop_at: str | None = None


@dataclass
class ExperimentResult:
    name: str
    created_at: datetime
    baseline: IterationResult
    iterations: list[IterationResult]
    capabilities: dict[str, Any] | None = None
    validation_time_summary: list[dict[str, Any]] | None = None


_BUSINESS_VIEWS = [
    "mart.v_competition_season_kpi",
    "mart.v_team_season_results",
]

_VIEW_TITLES: dict[str, str] = {
    "mart.v_competition_season_kpi": "Витрина KPI по сезонам",
    "mart.v_team_season_results": "Результаты команд в сезоне",
}

_VIEW_COLUMNS: dict[str, list[str]] = {
    "mart.v_competition_season_kpi": [
        "competition_name",
        "season_year",
        "start_date",
        "end_date",
        "matches_total",
        "matches_finished",
        "teams_distinct",
        "home_win_rate",
        "draw_rate",
        "away_win_rate",
    ],
    "mart.v_team_season_results": [
        "competition_name",
        "season_year",
        "start_date",
        "end_date",
        "team_name",
        "matches_played",
        "wins",
        "draws",
        "losses",
        "goals_for",
        "goals_against",
        "goal_difference",
        "points_calc",
    ],
}

_VIEW_KEY_FIELDS: dict[str, list[str]] = {
    "mart.v_competition_season_kpi": ["competition_id", "season_id"],
    "mart.v_team_season_results": ["competition_id", "season_id", "team_id"],
}


_TEMPLATE = """<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Отчет эксперимента: {{ result.name }}</title>
  <style>
    body { font-family: -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 24px; color: #111; }
    h1 { margin: 0 0 8px; }
    h2 { margin: 18px 0 8px; }
    h3 { margin: 14px 0 8px; }
    h4 { margin: 12px 0 8px; }
    .meta { color: #555; margin-bottom: 16px; }
    .card { border: 1px solid #e5e5e5; border-radius: 10px; padding: 14px; margin: 14px 0; }
    .badge { display: inline-block; padding: 2px 8px; border-radius: 999px; font-size: 12px; border: 1px solid #ddd; }
    .ok { background: #e8f7ee; border-color: #bfe6cc; }
    .fail { background: #fdecec; border-color: #f3c1c1; }
    .skip { background: #f4f4f4; border-color: #d9d9d9; }
    pre { background: #0b1020; color: #e6e6e6; padding: 10px; border-radius: 8px; overflow-x: auto; font-size: 12px; }
    table { border-collapse: collapse; width: 100%; font-size: 12px; }
    th, td { border: 1px solid #e5e5e5; padding: 6px 8px; vertical-align: top; }
    th { background: #fafafa; text-align: left; }
    .small { font-size: 12px; color: #555; }
    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
    details > summary { cursor: pointer; }
    .steps { margin-top: 8px; }
    .step-row { display: grid; grid-template-columns: 90px 1fr; gap: 10px; padding: 6px 0; border-top: 1px solid #f0f0f0; }
    .step-row:first-child { border-top: none; }
    .step-name { font-weight: 600; }
    .muted { color: #666; }
    .grid2 { display: grid; grid-template-columns: 1fr 1fr; gap: 14px; }
    .diff-cell { background: #fff3cd; }
    .row-added td { background: #e8f7ee; }
    .row-removed td { background: #fdecec; }
  </style>
</head>
<body>
  <h1>Отчет эксперимента</h1>
  <div class="meta">
    Название: <span class="mono">{{ result.name }}</span><br/>
    Дата: <span class="mono">{{ result.created_at.strftime("%Y-%m-%d %H:%M:%S") }}</span>
  </div>

  {% if result.capabilities %}
    <div class="card">
      <h2>Доступные проверки и мутации</h2>
      {% set stg_v = result.capabilities.get("validations", {}).get("STG", {}) %}
      {% set dds_v = result.capabilities.get("validations", {}).get("DDS", {}) %}
      {% set stg_m = result.capabilities.get("mutations", {}).get("STG", {}).get("entities", []) %}
      {% set dds_m = result.capabilities.get("mutations", {}).get("DDS", {}).get("entities", []) %}

      <div class="grid2">
        <div>
          <h3>STG валидации</h3>
          <table>
            <thead>
              <tr>
                <th>Проверка</th>
                <th>Группа</th>
                <th>Тип</th>
                <th>Важность</th>
                <th>Описание</th>
              </tr>
            </thead>
            <tbody>
              {% set suite_map = {} %}
              {% for s in (stg_v.get("suites", []) or []) %}
                {% for vn in (s.get("validations", []) or []) %}
                  {% set _ = suite_map.__setitem__(vn, s.get("name")) %}
                {% endfor %}
              {% endfor %}
              {% for name, item in (stg_v.get("items", {}) or {}).items() %}
                <tr>
                  <td class="mono">{{ name }}</td>
                  <td class="mono">{{ suite_map.get(name, "-") }}</td>
                  <td class="mono">{{ item.get("type") or "-" }}</td>
                  <td class="mono">{{ item.get("severity") or "-" }}</td>
                  <td>{{ item.get("description") or "-" }}</td>
                </tr>
              {% endfor %}
            </tbody>
          </table>
        </div>
        <div>
          <h3>DDS валидации</h3>
          <table>
            <thead>
              <tr>
                <th>Проверка</th>
                <th>Группа</th>
                <th>Тип</th>
                <th>Важность</th>
                <th>Описание</th>
              </tr>
            </thead>
            <tbody>
              {% set suite_map = {} %}
              {% for s in (dds_v.get("suites", []) or []) %}
                {% for vn in (s.get("validations", []) or []) %}
                  {% set _ = suite_map.__setitem__(vn, s.get("name")) %}
                {% endfor %}
              {% endfor %}
              {% for name, item in (dds_v.get("items", {}) or {}).items() %}
                <tr>
                  <td class="mono">{{ name }}</td>
                  <td class="mono">{{ suite_map.get(name, "-") }}</td>
                  <td class="mono">{{ item.get("type") or "-" }}</td>
                  <td class="mono">{{ item.get("severity") or "-" }}</td>
                  <td>{{ item.get("description") or "-" }}</td>
                </tr>
              {% endfor %}
            </tbody>
          </table>
        </div>
      </div>

      <div class="grid2" style="margin-top: 14px;">
        <div>
          <h3>STG мутации</h3>
          <table>
            <thead>
              <tr>
                <th>Сущность</th>
                <th>Деиствие</th>
                <th>Описание</th>
              </tr>
            </thead>
            <tbody>
              {% for e in stg_m %}
                {% set actions = e.get("actions", []) or [] %}
                {% if actions|length == 0 %}
                  <tr>
                    <td class="mono">{{ e.get("name") }}</td>
                    <td class="mono">-</td>
                    <td>{{ e.get("description") or "-" }}</td>
                  </tr>
                {% else %}
                  {% for a in actions %}
                    <tr>
                      <td class="mono">{{ e.get("name") }}</td>
                      <td class="mono">{{ a.get("name") }}</td>
                      <td>{{ a.get("description") or e.get("description") or "-" }}</td>
                    </tr>
                  {% endfor %}
                {% endif %}
              {% endfor %}
            </tbody>
          </table>
        </div>
        <div>
          <h3>DDS мутации</h3>
          <table>
            <thead>
              <tr>
                <th>Сущность</th>
                <th>Описание</th>
              </tr>
            </thead>
            <tbody>
              {% for e in dds_m %}
                <tr>
                  <td class="mono">{{ e.get("name") }}</td>
                  <td>{{ e.get("description") or "-" }}</td>
                </tr>
              {% endfor %}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  {% endif %}

  <div class="card">
    <h2>Эталонная загрузка</h2>
    <div class="small">
      STG run_id: <span class="mono">{{ result.baseline.stg_run_id }}</span><br/>
      DDS run_id: <span class="mono">{{ result.baseline.dds_run_id }}</span>
    </div>
    <div class="small" style="margin-top: 10px;">
      Ниже приведены эталонные снимки бизнес-витрин.
    </div>
    {% for view in business_views %}
      {% set data = result.baseline.snapshots.get(view) %}
      <h3>{{ view_titles.get(view, view) }}</h3>
      {% if data is mapping and data.get("error") %}
        <div class="small">Ошибка получения витрины: <span class="mono">{{ data.get("error") }}</span></div>
      {% elif data is sequence and data|length > 0 %}
        <table>
          <thead>
            <tr>
              {% for col in view_columns.get(view, []) %}
                <th class="mono">{{ col }}</th>
              {% endfor %}
            </tr>
          </thead>
          <tbody>
            {% for row in data %}
              <tr>
                {% for col in view_columns.get(view, []) %}
                  <td>{{ row.get(col) }}</td>
                {% endfor %}
              </tr>
            {% endfor %}
          </tbody>
        </table>
      {% else %}
        <div class="small">Нет строк в витрине.</div>
      {% endif %}
    {% endfor %}
  </div>

  <div class="card">
    <h2>Итерационные прогоны эксперимента</h2>
    <table>
      <thead>
        <tr>
          <th>#</th>
          <th>Название</th>
          <th>STG run_id</th>
          <th>DDS run_id</th>
          <th>Статус</th>
          <th>Остановилось на шаге</th>
        </tr>
      </thead>
      <tbody>
        {% for it in result.iterations %}
          <tr>
            <td class="mono">{{ it.iteration_no }}</td>
            <td>{{ it.name }}</td>
            <td class="mono">{{ it.stg_run_id or "-" }}</td>
            <td class="mono">{{ it.dds_run_id or "-" }}</td>
            <td>
              {% if it.status == "SUCCESS" %}
                <span class="badge ok">SUCCESS</span>
              {% else %}
                <span class="badge fail">FAILED</span>
              {% endif %}
            </td>
            <td>{{ it.stop_at or "-" }}</td>
          </tr>
        {% endfor %}
      </tbody>
    </table>
  </div>

  {% for it in result.iterations %}
    <div class="card" id="iter-{{ it.iteration_no }}">
      <h2>Итерация {{ it.iteration_no }}: {{ it.name }}</h2>
      <div class="small">
        Статус:
        {% if it.status == "SUCCESS" %}
          <span class="badge ok">SUCCESS</span>
        {% else %}
          <span class="badge fail">FAILED</span>
        {% endif %}
        <br/>
        STG run_id: <span class="mono">{{ it.stg_run_id or "-" }}</span><br/>
        DDS run_id: <span class="mono">{{ it.dds_run_id or "-" }}</span><br/>
      </div>

      <h3>Ход выполнения</h3>
      <div class="steps">
        {% for step in it.steps %}
          {% if step.name != "DDS: подготовка" %}
            <div class="step-row">
              <div>
                {% if step.status == "SUCCESS" %}
                  <span class="badge ok">OK</span>
                {% elif step.status == "FAILED" %}
                  <span class="badge fail">FAIL</span>
                {% else %}
                  <span class="badge skip">SKIP</span>
                {% endif %}
              </div>
              <div>
                <div class="step-name">{{ step.name }}</div>
                {% if step.details %}<div class="small muted">{{ step.details }}</div>{% endif %}
                {% if step.error %}
                  <details class="small" style="margin-top: 6px;">
                    <summary>Показать ошибку</summary>
                    <pre>{{ step.error }}</pre>
                  </details>
                {% endif %}
              </div>
            </div>
          {% endif %}
        {% endfor %}
      </div>

      {% if it.status == "SUCCESS" %}
        <h3>Влияние на выходные витрины</h3>
        {% if it.comparisons is not none %}
          {% if it.comparisons|length == 0 %}
            <div class="small">Изменений относительно эталона не обнаружено.</div>
          {% else %}
            {% for view in business_views %}
              {% set diff = it.comparisons.get(view) %}
              {% if diff %}
                <h4>{{ view_titles.get(view, view) }}</h4>

                {% if diff.get("added") and diff.get("added")|length > 0 %}
                  <div class="small">Добавлено строк: {{ diff.get("added")|length }}</div>
                  <table>
                    <thead>
                      <tr>
                        {% for col in view_columns.get(view, []) %}
                          <th class="mono">{{ col }}</th>
                        {% endfor %}
                      </tr>
                    </thead>
                    <tbody>
                      {% for row in diff.get("added") %}
                        <tr>
                          {% for col in view_columns.get(view, []) %}
                            <td>{{ row.get(col) }}</td>
                          {% endfor %}
                        </tr>
                      {% endfor %}
                    </tbody>
                  </table>
                {% endif %}

                {% if diff.get("removed") and diff.get("removed")|length > 0 %}
                  <div class="small" style="margin-top: 10px;">Удалено строк: {{ diff.get("removed")|length }}</div>
                  <table>
                    <thead>
                      <tr>
                        {% for col in view_columns.get(view, []) %}
                          <th class="mono">{{ col }}</th>
                        {% endfor %}
                      </tr>
                    </thead>
                    <tbody>
                      {% for row in diff.get("removed") %}
                        <tr>
                          {% for col in view_columns.get(view, []) %}
                            <td>{{ row.get(col) }}</td>
                          {% endfor %}
                        </tr>
                      {% endfor %}
                    </tbody>
                  </table>
                {% endif %}

                {% if diff.get("changed") and diff.get("changed")|length > 0 %}
                  <div class="small" style="margin-top: 10px;">Изменено строк: {{ diff.get("changed")|length }}</div>
                  {% if view|lower == "mart.v_team_season_results" and diff.get("table") %}
                    <table>
                      <thead>
                        <tr>
                          {% for col in diff.get("table", {}).get("columns", []) %}
                            <th class="mono">{{ col }}</th>
                          {% endfor %}
                        </tr>
                      </thead>
                      <tbody>
                        {% for r in diff.get("table", {}).get("rows", []) %}
                          <tr class="{{ 'row-added' if r.get('row_status') == 'added' else ('row-removed' if r.get('row_status') == 'removed' else '') }}">
                            {% for c in r.get("cells", []) %}
                              <td class="{{ 'diff-cell' if c.get('changed') else '' }}">{{ c.get("text") }}</td>
                            {% endfor %}
                          </tr>
                        {% endfor %}
                      </tbody>
                    </table>
                  {% else %}
                    <table>
                      <thead>
                        <tr>
                          <th>Объект</th>
                          <th>Изменения</th>
                        </tr>
                      </thead>
                      <tbody>
                        {% for ch in diff.get("changed") %}
                          <tr>
                            <td class="mono">{{ ch.get("key_label") or ch.get("key") }}</td>
                            <td>
                              {% for item in ch.get("changes", []) %}
                                <div class="small">
                                  <span class="mono">{{ item.get("field") }}</span>:
                                  <span class="mono">{{ item.get("before") }}</span> → <span class="mono">{{ item.get("after") }}</span>
                                </div>
                              {% endfor %}
                            </td>
                          </tr>
                        {% endfor %}
                      </tbody>
                    </table>
                  {% endif %}
                {% endif %}
              {% endif %}
            {% endfor %}
          {% endif %}
        {% endif %}
      {% else %}
        <div class="small" style="margin-top: 10px;">
          Витрины <span class="mono">mart.*</span> не отображаются, так как итерация не дошла до финального шага.
        </div>
      {% endif %}
    </div>
  {% endfor %}

  {% if result.validation_time_summary %}
    <div class="card">
      <h2>Итоги по времени валидации (по группам)</h2>
      <div class="small">
        Время рассчитано по журналу <span class="mono">tech.etl_load_audit</span> как разница <span class="mono">finished_at - started_at</span> для записей групповых suite.
      </div>
      <table style="margin-top: 10px;">
        <thead>
          <tr>
            <th>#</th>
            <th>Итерация</th>
            <th>Слои</th>
            <th>run_id</th>
            <th>Группа</th>
            <th>Время, сек</th>
          </tr>
        </thead>
        <tbody>
          {% for row in result.validation_time_summary %}
            <tr>
              <td class="mono">{{ row.get("iteration_no") }}</td>
              <td>{{ row.get("iteration_name") }}</td>
              <td class="mono">{{ row.get("layer") }}</td>
              <td class="mono">{{ row.get("run_id") }}</td>
              <td class="mono">{{ row.get("suite") }}</td>
              <td class="mono">{{ row.get("seconds_sum") }}</td>
            </tr>
          {% endfor %}
        </tbody>
      </table>
    </div>
  {% endif %}
</body>
</html>
"""


def _stable_row_json(row: dict[str, Any]) -> str:
    return json.dumps(row, ensure_ascii=False, sort_keys=True, default=str)


def _row_key(view: str, row: dict[str, Any]) -> tuple[Any, ...] | None:
    v = (view or "").strip().lower()
    fields = _VIEW_KEY_FIELDS.get(v)
    if not fields:
        return None
    return tuple(row.get(f) for f in fields)


def _diff_view_rows(
    view: str,
    baseline_rows: list[dict[str, Any]],
    iteration_rows: list[dict[str, Any]],
    *,
    sample_limit: int = 50,
) -> dict[str, Any]:
    base_keyed: dict[tuple[Any, ...], dict[str, Any]] = {}
    iter_keyed: dict[tuple[Any, ...], dict[str, Any]] = {}
    key_supported = True

    for row in baseline_rows:
        key = _row_key(view, row)
        if key is None:
            key_supported = False
            break
        base_keyed[key] = row

    if key_supported:
        for row in iteration_rows:
            key = _row_key(view, row)
            if key is None:
                key_supported = False
                break
            iter_keyed[key] = row

    if key_supported:
        base_keys = set(base_keyed.keys())
        iter_keys = set(iter_keyed.keys())

        added_keys = sorted(iter_keys - base_keys, key=str)
        removed_keys = sorted(base_keys - iter_keys, key=str)
        common_keys = base_keys & iter_keys

        changed = []
        for key in sorted(common_keys, key=str):
            if _stable_row_json(base_keyed[key]) != _stable_row_json(iter_keyed[key]):
                changed.append({"key": key, "baseline": base_keyed[key], "iteration": iter_keyed[key]})

        view_lower = view.lower()
        key_fields = set(_VIEW_KEY_FIELDS.get(view_lower) or [])
        columns = _VIEW_COLUMNS.get(view_lower) or []

        def _fmt_cell(before: Any, after: Any) -> str:
            if before == after:
                return "—" if after is None else str(after)
            if before is None and after is None:
                return "—"
            if before is None:
                a = "—" if after is None else str(after)
                return f"{a} (— → {a})"
            if after is None:
                b = "—" if before is None else str(before)
                return f"— ({b} → —)"
            return f"{after} ({before} → {after})"

        out_changed: list[dict[str, Any]] = []
        for item in changed[:sample_limit]:
            before = item["baseline"]
            after = item["iteration"]
            key_label = None
            if view_lower == "mart.v_team_season_results":
                name = (before.get("team_name") or after.get("team_name") or "").strip()
                comp = (before.get("competition_name") or after.get("competition_name") or "").strip()
                start = before.get("start_date") or after.get("start_date")
                end = before.get("end_date") or after.get("end_date")
                period = f"{start} --- {end}" if start or end else ""
                parts = [p for p in [comp, period, name] if str(p).strip()]
                key_label = " / ".join(map(str, parts))
            elif view_lower == "mart.v_competition_season_kpi":
                comp = (before.get("competition_name") or after.get("competition_name") or "").strip()
                start = before.get("start_date") or after.get("start_date")
                end = before.get("end_date") or after.get("end_date")
                period = f"{start} --- {end}" if start or end else ""
                parts = [p for p in [comp, period] if str(p).strip()]
                key_label = " / ".join(map(str, parts))
            changes = []
            fields = columns or sorted(set(before.keys()) | set(after.keys()))
            for k in fields:
                if k in key_fields:
                    continue
                if before.get(k) != after.get(k):
                    changes.append({"field": k, "before": before.get(k), "after": after.get(k)})
            out_changed.append({"key": item["key"], "key_label": key_label, "changes": changes})

        out: dict[str, Any] = {
            "added": [iter_keyed[k] for k in added_keys[:sample_limit]],
            "removed": [base_keyed[k] for k in removed_keys[:sample_limit]],
            "changed": out_changed,
        }

        if view_lower == "mart.v_team_season_results" and columns:
            table_rows: list[dict[str, Any]] = []

            for item in changed[:sample_limit]:
                before = item["baseline"]
                after = item["iteration"]
                cells = []
                for col in columns:
                    b = before.get(col)
                    a = after.get(col)
                    cells.append({"text": _fmt_cell(b, a), "changed": b != a})
                table_rows.append({"key": item["key"], "row_status": "changed", "cells": cells})

            out["table"] = {"columns": columns, "rows": table_rows}

        return out

    base_set = {_stable_row_json(r) for r in baseline_rows}
    iter_set = {_stable_row_json(r) for r in iteration_rows}
    added = list(iter_set - base_set)
    removed = list(base_set - iter_set)
    return {
        "added": [json.loads(s) for s in added[:sample_limit]],
        "removed": [json.loads(s) for s in removed[:sample_limit]],
        "changed": [],
    }


def _compute_stop_at(it: IterationResult) -> str | None:
    for s in it.steps:
        if s.status == "FAILED":
            if s.name == "DDS: подготовка":
                continue
            return s.name
    return None


def _build_comparisons(result: ExperimentResult) -> None:
    base_snaps = result.baseline.snapshots or {}
    for it in result.iterations:
        it.stop_at = _compute_stop_at(it)
        if it.status != "SUCCESS" or not it.snapshots:
            it.comparisons = None
            continue

        comparisons: dict[str, Any] = {}
        for view in _BUSINESS_VIEWS:
            data = it.snapshots.get(view)
            base_data = base_snaps.get(view)

            if isinstance(data, dict) and data.get("error"):
                continue
            if isinstance(base_data, dict) and base_data.get("error"):
                continue

            if not isinstance(data, list) or not isinstance(base_data, list):
                continue

            try:
                diff = _diff_view_rows(view, base_data, data)
                if diff.get("added") or diff.get("removed") or diff.get("changed"):
                    comparisons[view] = diff
            except Exception as e:
                comparisons[view] = {
                    "added": [],
                    "removed": [],
                    "changed": [{"key": "error", "changes": [{"field": "error", "before": None, "after": str(e)}]}],
                }

        it.comparisons = comparisons


def render_html_report(result: ExperimentResult, output_path: Path):
    _build_comparisons(result)
    env = Environment(loader=BaseLoader(), autoescape=select_autoescape(["html", "xml"]))
    template = env.from_string(_TEMPLATE)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    html = template.render(
        result=result,
        json_dumps_safe=json_dumps_safe,
        business_views=_BUSINESS_VIEWS,
        view_titles=_VIEW_TITLES,
        view_columns=_VIEW_COLUMNS,
    )
    output_path.write_text(html, encoding="utf-8")
