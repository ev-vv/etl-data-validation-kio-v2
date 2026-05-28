from __future__ import annotations

import html as html_lib
import re
import time
import traceback
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import great_expectations as gx
from great_expectations.render.renderer import ValidationResultsPageRenderer
from great_expectations.render.view import DefaultJinjaPageView

from app2.core.config import load_settings
from app2.db.connection import get_engine
from app2.db.batch import log_batch_status
from app2.db.validation_metrics import finish_validation_run, log_validation_check, start_validation_run
from app2.post_validation.discovery import PostValidationTarget
from app2.post_validation.paths import tool_output_dir


@dataclass(frozen=True)
class PostValidationReport:
    dds_run_id: str
    stg_run_id: str
    kind: str
    status: str
    report_path: str | None
    error: str | None = None


@dataclass(frozen=True)
class MetricSpec:
    name: str
    description: str
    drilldown_sql: str | None


def _now_tag() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _sanitize(value: str) -> str:
    v = re.sub(r"[^a-zA-Z0-9._-]+", "_", (value or "").strip())
    return v or "id"


def _sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _build_metrics_query(dds_run_id: str) -> str:
    rid = _sql_quote(dds_run_id)
    return f"""
    SELECT
      (SELECT COUNT(*)
       FROM dds.fact_match
       WHERE run_id = {rid}
         AND home_team_id IS NOT NULL
         AND away_team_id IS NOT NULL
         AND home_team_id = away_team_id) AS dds_match_same_team,

      (SELECT COUNT(*)
       FROM dds.fact_standing
       WHERE run_id = {rid}
         AND points IS NOT NULL
         AND won IS NOT NULL
         AND draw IS NOT NULL
         AND points <> (won * 3 + draw)) AS dds_standings_points_inconsistent,

      (SELECT COUNT(*)
       FROM dds.fact_match
       WHERE run_id = {rid}
         AND competition_id IS NULL) AS dds_match_null_competition_id,

      (SELECT COUNT(*)
       FROM dds.fact_match
       WHERE run_id = {rid}
         AND season_id IS NULL) AS dds_match_null_season_id,

      (SELECT COUNT(*)
       FROM mart.v_competition_season_kpi
       WHERE run_id = {rid}
         AND (
           home_win_rate < 0 OR home_win_rate > 1 OR
           draw_rate < 0 OR draw_rate > 1 OR
           away_win_rate < 0 OR away_win_rate > 1
         )) AS mart_kpi_rate_out_of_bounds,

      (SELECT COUNT(*)
       FROM mart.v_competition_season_kpi
       WHERE run_id = {rid}
         AND (start_date IS NULL OR end_date IS NULL OR season_year IS NULL)) AS mart_kpi_missing_dates,

      (SELECT COUNT(*)
       FROM mart.v_team_season_results
       WHERE run_id = {rid}
         AND (start_date IS NULL OR end_date IS NULL OR season_year IS NULL)) AS mart_team_missing_dates,

      (SELECT COUNT(*)
       FROM mart.v_team_season_results
       WHERE run_id = {rid}
         AND points_calc <> (wins * 3 + draws)) AS mart_team_points_inconsistent,

      (SELECT COUNT(*)
       FROM mart.v_team_season_results
       WHERE run_id = {rid}
         AND matches_played <> (wins + draws + losses)) AS mart_team_matches_inconsistent,

      (SELECT COUNT(*)
       FROM mart.v_team_season_results
       WHERE run_id = {rid}
         AND (points_calc < 0 OR goals_for < 0 OR goals_against < 0)) AS mart_team_negative_values,

      (SELECT COUNT(*) FROM mart.v_competition_season_kpi WHERE run_id = {rid}) AS mart_kpi_rows,
      (SELECT COUNT(*) FROM mart.v_team_season_results WHERE run_id = {rid}) AS mart_team_rows
    """


def _metric_specs(*, dds_run_id: str) -> list[MetricSpec]:
    rid = _sql_quote(dds_run_id)
    return [
        MetricSpec(
            name="dds_match_same_team",
            description="Количество матчей, где home_team_id = away_team_id (одна и та же команда играет сама с собой). Ожидается 0.",
            drilldown_sql=(
                "SELECT *\n"
                "FROM dds.fact_match\n"
                f"WHERE run_id = {rid}\n"
                "  AND home_team_id IS NOT NULL\n"
                "  AND away_team_id IS NOT NULL\n"
                "  AND home_team_id = away_team_id\n"
                "LIMIT 20"
            ),
        ),
        MetricSpec(
            name="dds_standings_points_inconsistent",
            description="Количество строк в standings, где points != won*3 + draw. Ожидается 0.",
            drilldown_sql=(
                "SELECT *\n"
                "FROM dds.fact_standing\n"
                f"WHERE run_id = {rid}\n"
                "  AND points IS NOT NULL\n"
                "  AND won IS NOT NULL\n"
                "  AND draw IS NOT NULL\n"
                "  AND points <> (won * 3 + draw)\n"
                "LIMIT 20"
            ),
        ),
        MetricSpec(
            name="dds_match_null_competition_id",
            description="Количество матчей без competition_id. Ожидается 0.",
            drilldown_sql=(
                "SELECT *\n"
                "FROM dds.fact_match\n"
                f"WHERE run_id = {rid}\n"
                "  AND competition_id IS NULL\n"
                "LIMIT 20"
            ),
        ),
        MetricSpec(
            name="dds_match_null_season_id",
            description="Количество матчей без season_id. Ожидается 0.",
            drilldown_sql=(
                "SELECT *\n"
                "FROM dds.fact_match\n"
                f"WHERE run_id = {rid}\n"
                "  AND season_id IS NULL\n"
                "LIMIT 20"
            ),
        ),
        MetricSpec(
            name="mart_kpi_rate_out_of_bounds",
            description="Количество KPI-строк, где доли (win/draw/away) выходят за диапазон [0, 1]. Ожидается 0.",
            drilldown_sql=(
                "SELECT *\n"
                "FROM mart.v_competition_season_kpi\n"
                f"WHERE run_id = {rid}\n"
                "  AND (\n"
                "    home_win_rate < 0 OR home_win_rate > 1 OR\n"
                "    draw_rate < 0 OR draw_rate > 1 OR\n"
                "    away_win_rate < 0 OR away_win_rate > 1\n"
                "  )\n"
                "LIMIT 20"
            ),
        ),
        MetricSpec(
            name="mart_kpi_missing_dates",
            description="Количество KPI-строк с отсутствующими датами/годом сезона. Ожидается 0.",
            drilldown_sql=(
                "SELECT *\n"
                "FROM mart.v_competition_season_kpi\n"
                f"WHERE run_id = {rid}\n"
                "  AND (start_date IS NULL OR end_date IS NULL OR season_year IS NULL)\n"
                "LIMIT 20"
            ),
        ),
        MetricSpec(
            name="mart_team_missing_dates",
            description="Количество team-результатов с отсутствующими датами/годом сезона. Ожидается 0.",
            drilldown_sql=(
                "SELECT *\n"
                "FROM mart.v_team_season_results\n"
                f"WHERE run_id = {rid}\n"
                "  AND (start_date IS NULL OR end_date IS NULL OR season_year IS NULL)\n"
                "LIMIT 20"
            ),
        ),
        MetricSpec(
            name="mart_team_points_inconsistent",
            description="Количество team-результатов, где points_calc != wins*3 + draws. Ожидается 0.",
            drilldown_sql=(
                "SELECT *\n"
                "FROM mart.v_team_season_results\n"
                f"WHERE run_id = {rid}\n"
                "  AND points_calc <> (wins * 3 + draws)\n"
                "LIMIT 20"
            ),
        ),
        MetricSpec(
            name="mart_team_matches_inconsistent",
            description="Количество team-результатов, где matches_played != wins + draws + losses. Ожидается 0.",
            drilldown_sql=(
                "SELECT *\n"
                "FROM mart.v_team_season_results\n"
                f"WHERE run_id = {rid}\n"
                "  AND matches_played <> (wins + draws + losses)\n"
                "LIMIT 20"
            ),
        ),
        MetricSpec(
            name="mart_team_negative_values",
            description="Количество team-результатов с отрицательными значениями (очки/забито/пропущено). Ожидается 0.",
            drilldown_sql=(
                "SELECT *\n"
                "FROM mart.v_team_season_results\n"
                f"WHERE run_id = {rid}\n"
                "  AND (points_calc < 0 OR goals_for < 0 OR goals_against < 0)\n"
                "LIMIT 20"
            ),
        ),
    ]


def _fetch_metrics_row(engine: Any, *, dds_run_id: str) -> dict[str, Any] | None:
    query = _build_metrics_query(dds_run_id)
    with engine.connect() as conn:
        row = conn.exec_driver_sql(query).mappings().first()
    return dict(row) if row else None


def _inject_after_body_open(html: str, snippet: str) -> str:
    m = re.search(r"<body[^>]*>", html, flags=re.IGNORECASE)
    if not m:
        return html
    insert_at = m.end()
    return html[:insert_at] + snippet + html[insert_at:]


def _render_summary_html(
    *,
    dag_id: str,
    dds_run_id: str,
    stg_run_id: str,
    kind: str,
    status: str,
    metrics: dict[str, Any] | None,
    specs: list[MetricSpec],
) -> str:
    safe_dag_id = html_lib.escape(dag_id)
    safe_dds_run_id = html_lib.escape(dds_run_id)
    safe_stg_run_id = html_lib.escape(stg_run_id or "")
    safe_kind = html_lib.escape(kind)

    if metrics is None:
        metrics = {}

    failed_specs: list[MetricSpec] = []
    rows_html: list[str] = []
    for spec in specs:
        raw_value = metrics.get(spec.name)
        value = raw_value if raw_value is not None else "—"
        ok = raw_value == 0
        if raw_value is not None and not ok:
            failed_specs.append(spec)

        icon = "fa-check-circle text-success" if ok else "fa-times text-danger"
        badge = "badge-success" if ok else "badge-danger"
        safe_metric = html_lib.escape(spec.name)
        safe_desc = html_lib.escape(spec.description)

        sql_html = ""
        if spec.drilldown_sql:
            safe_sql = html_lib.escape(spec.drilldown_sql)
            sql_html = (
                "<details class=\"mt-1\">"
                "<summary class=\"small\">SQL для детализации</summary>"
                f"<pre class=\"mt-2 mb-0\" style=\"white-space:pre-wrap;\">{safe_sql}</pre>"
                "</details>"
            )

        rows_html.append(
            "<tr>"
            f"<td style=\"white-space:nowrap;\"><i class=\"fas {icon}\"></i> <code>{safe_metric}</code></td>"
            f"<td><span class=\"badge {badge}\" style=\"font-size: 90%;\">{html_lib.escape(str(value))}</span></td>"
            "<td><span class=\"badge badge-secondary\" style=\"font-size: 90%;\">0</span></td>"
            f"<td>{safe_desc}{sql_html}</td>"
            "</tr>"
        )

    kpi_rows = metrics.get("mart_kpi_rows")
    team_rows = metrics.get("mart_team_rows")
    info_rows = []
    if kpi_rows is not None:
        info_rows.append(f"<code>mart_kpi_rows</code>: <strong>{html_lib.escape(str(kpi_rows))}</strong>")
    if team_rows is not None:
        info_rows.append(f"<code>mart_team_rows</code>: <strong>{html_lib.escape(str(team_rows))}</strong>")
    info_line = " · ".join(info_rows) if info_rows else "—"

    if status == "SUCCESS":
        alert_class = "alert-success"
        title = "Пост-валидация: SUCCESS"
    else:
        alert_class = "alert-danger"
        title = "Пост-валидация: FAILED"

    failed_list_html = ""
    if failed_specs:
        failed_names = ", ".join(f"<code>{html_lib.escape(s.name)}</code>" for s in failed_specs)
        failed_list_html = f"<div class=\"small mt-1\">Провалившиеся проверки: {failed_names}</div>"

    return (
        "<div class=\"container-fluid mt-3\">"
        f"<div class=\"alert {alert_class}\" role=\"alert\" style=\"border-radius: .25rem;\">"
        f"<div class=\"h5 mb-1\">{title}</div>"
        f"<div class=\"small\">dag_id: <code>{safe_dag_id}</code> · kind: <code>{safe_kind}</code> · dds_run_id: <code>{safe_dds_run_id}</code>"
        f"{(' · stg_run_id: <code>' + safe_stg_run_id + '</code>') if safe_stg_run_id else ''}</div>"
        f"<div class=\"small mt-1\">Смысл метрик: это количество нарушений правила (ожидается 0). Доп. инфо: {info_line}</div>"
        f"{failed_list_html}"
        "</div>"
        "<div class=\"card mb-3\">"
        "<div class=\"card-header\"><strong>Сводка по правилам</strong></div>"
        "<div class=\"card-body p-0\">"
        "<div class=\"table-responsive\">"
        "<table class=\"table table-sm mb-0\">"
        "<thead><tr>"
        "<th>Метрика</th><th>Значение</th><th>Ожидается</th><th>Описание / как чинить</th>"
        "</tr></thead>"
        "<tbody>"
        + "".join(rows_html)
        + "</tbody></table></div></div></div></div>"
    )


def _get_expectation_type(item: Any) -> str | None:
    cfg = getattr(item, "expectation_config", None)
    if cfg is None:
        return None
    exp_type = getattr(cfg, "expectation_type", None)
    if exp_type:
        return exp_type
    try:
        cfg_dict = cfg.to_json_dict()
        return cfg_dict.get("expectation_type")
    except Exception:
        return None


def _add_postgres_datasource(ctx: Any, conn_str: str):
    if hasattr(ctx, "data_sources"):
        return ctx.data_sources.add_postgres(name="postgres", connection_string=conn_str)
    if hasattr(ctx, "sources"):
        return ctx.sources.add_postgres(name="postgres", connection_string=conn_str)
    datasources = getattr(ctx, "datasources", None)
    if datasources is not None and hasattr(datasources, "add_postgres"):
        return datasources.add_postgres(name="postgres", connection_string=conn_str)
    raise AttributeError("Great Expectations context has no datasource factory")


def run_post_validation_gx(
    *,
    dag_id: str,
    targets: list[PostValidationTarget],
    output_dir: Path,
    layer: str = "POST_GX",
) -> list[PostValidationReport]:
    settings = load_settings()
    conn_str = (
        f"postgresql+psycopg2://{settings.postgres_user}:{settings.postgres_password}"
        f"@{settings.postgres_host}:{settings.postgres_port}/{settings.postgres_db}"
    )
    engine = get_engine()

    ctx = gx.get_context(mode="ephemeral")
    datasource = _add_postgres_datasource(ctx, conn_str)
    output_dir = tool_output_dir(output_dir, "gx")
    output_dir.mkdir(parents=True, exist_ok=True)

    reports: list[PostValidationReport] = []
    for t in targets:
        report_path = None
        run_started = time.time()
        validation_run_id = None
        try:
            tag = _now_tag()
            safe_dds = _sanitize(t.dds_run_id)

            validation_run_id = start_validation_run(
                engine,
                dag_id=dag_id,
                run_id=t.dds_run_id,
                parent_run_id=t.stg_run_id,
                layer=layer,
                tool="gx",
                suite="post_validation",
                kind=t.kind,
            )

            log_batch_status(
                engine,
                dag_id=dag_id,
                run_id=t.dds_run_id,
                parent_run_id=t.stg_run_id,
                layer=layer,
                status="NEW",
            )
            log_batch_status(
                engine,
                dag_id=dag_id,
                run_id=t.dds_run_id,
                parent_run_id=t.stg_run_id,
                layer=layer,
                status="PROCESSING",
            )

            suite_name = f"post_validation_metrics_{safe_dds}_{tag}"
            metrics_query = _build_metrics_query(t.dds_run_id)
            asset = datasource.add_query_asset(name=f"metrics_{safe_dds}_{tag}", query=metrics_query)
            batch_request = asset.build_batch_request()
            v = ctx.get_validator(batch_request=batch_request, create_expectation_suite_with_name=suite_name)

            specs = _metric_specs(dds_run_id=t.dds_run_id)
            for spec in specs:
                v.expect_column_values_to_be_in_set(spec.name, value_set=[0])

            result = v.validate()
            status = "SUCCESS" if result.success else "FAILED"
            metrics_row = _fetch_metrics_row(engine, dds_run_id=t.dds_run_id)

            results_by_metric: dict[str, Any] = {}
            for item in result.results:
                try:
                    column = item.expectation_config.kwargs.get("column")
                except Exception:
                    column = None
                if column:
                    results_by_metric[column] = item

            failed_checks = 0
            for spec in specs:
                item = results_by_metric.get(spec.name)
                ok = bool(item.success) if item else False
                if not ok:
                    failed_checks += 1
                row_value = metrics_row.get(spec.name) if metrics_row else None
                log_validation_check(
                    engine,
                    validation_run_id=validation_run_id,
                    check_name=spec.name,
                    rule_type="metric",
                    etl_stage="POST",
                    status="PASS" if ok else "FAIL",
                    severity="error",
                    rows_failed=row_value if isinstance(row_value, int) else None,
                    observed_value=str(row_value) if row_value is not None else None,
                    expected_value="0",
                    message=None if ok else "Metric should be 0",
                    details_json={
                        "expectation_type": _get_expectation_type(item) if item else None,
                        "success": bool(item.success) if item else False,
                        "result": item.result if item else None,
                    },
                )

            doc = ValidationResultsPageRenderer().render(result)
            html = DefaultJinjaPageView().render(doc)

            try:
                summary = _render_summary_html(
                    dag_id=dag_id,
                    dds_run_id=t.dds_run_id,
                    stg_run_id=t.stg_run_id,
                    kind=t.kind,
                    status=status,
                    metrics=metrics_row,
                    specs=specs,
                )
                html = _inject_after_body_open(html, summary)
            except Exception:
                pass
            out_name = f"gx_post_validation_{_sanitize(t.kind)}_{safe_dds}_{tag}.html"
            out_path = output_dir / out_name
            out_path.write_text(html, encoding="utf-8")
            report_path = str(out_path)

            log_batch_status(
                engine,
                dag_id=dag_id,
                run_id=t.dds_run_id,
                parent_run_id=t.stg_run_id,
                layer=layer,
                status=status,
                error_message=None if status == "SUCCESS" else "GX post-validation failed",
            )

            reports.append(
                PostValidationReport(
                    dds_run_id=t.dds_run_id,
                    stg_run_id=t.stg_run_id,
                    kind=t.kind,
                    status=status,
                    report_path=report_path,
                )
            )
            if validation_run_id is not None:
                finish_validation_run(
                    engine,
                    validation_run_id=validation_run_id,
                    status=status,
                    duration_ms=int((time.time() - run_started) * 1000),
                    checks_total=len(specs),
                    checks_failed=failed_checks,
                    report_path=report_path,
                    meta_json={"gx_statistics": getattr(result, "statistics", None)},
                )
        except Exception:
            err = traceback.format_exc()
            try:
                log_batch_status(
                    engine,
                    dag_id=dag_id,
                    run_id=t.dds_run_id,
                    parent_run_id=t.stg_run_id,
                    layer=layer,
                    status="FAILED",
                    error_message="GX post-validation error",
                )
            except Exception:
                pass
            reports.append(
                PostValidationReport(
                    dds_run_id=t.dds_run_id,
                    stg_run_id=t.stg_run_id,
                    kind=t.kind,
                    status="FAILED",
                    report_path=report_path,
                    error=err,
                )
            )
            if validation_run_id is not None:
                finish_validation_run(
                    engine,
                    validation_run_id=validation_run_id,
                    status="FAILED",
                    duration_ms=int((time.time() - run_started) * 1000),
                    checks_total=0,
                    checks_failed=0,
                    report_path=report_path,
                    meta_json={"error": err},
                )
    return reports
