from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import subprocess
import sys
import tempfile
import time         
import psutil                           
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
import yaml
from dotenv import load_dotenv
from sqlalchemy import bindparam, text


REPO_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = REPO_ROOT / "src"
os.environ.setdefault("APP2_REPO_ROOT", str(REPO_ROOT))
os.environ.setdefault("PYTHONPATH", str(SRC_ROOT))
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

try:
    from app2.db.audit import audit_log
    from app2.db.batch import log_batch_status
    from app2.db.connection import get_engine
    from app2.dds.load_dds import run_dds_load
    from app2.dds.parallel_loader import load_dds_parallel
    from app2.etl_validation.config import load_tools_experiment_config
    from app2.etl_validation.runner import run_stage_tool
    from app2.experiments.config import load_experiment_config
    from app2.experiments.run import run_experiment
    from app2.loaders.raw_staging import load_raw
    from app2.loaders.batch_staging import load_all_batches_parallel, load_all_batches_sequential
    from pyspark.sql import SparkSession
    from app2.spark.loader import load_batches_to_spark
    from app2.mutators.spark_mutations import MUTATION_MAP
    from app2.etl_validation.gx_spark_runner import run_stage_validation_gx_spark
    from app2.etl_validation.sql_spark_runner import run_stage_validation_sql_spark
    from app2.etl_validation.soda_spark_runner import run_stage_validation_soda_spark
    from app2.etl_validation.deequ_spark_runner import run_stage_validation_deequ_spark
except ModuleNotFoundError as exc:
    raise ModuleNotFoundError(
        f"Cannot import '{exc.name}'. Ensure project root is '{REPO_ROOT}' and 'src/app2' exists."
    ) from exc

@dataclass(frozen=True)
class BaselineRuns:
    stg_run_id: str
    dds_run_id: str


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run manual experiment pipeline from local input payloads (without Airflow)."
    )
    parser.add_argument(
        "--input-dir",
        default="input/raw_football_api",
        help="Root input directory (contains exported run folders).",
    )
    parser.add_argument(
        "--input-run-dir",
        default=None,
        help="Specific input run directory. If omitted, the latest subdirectory under --input-dir is used.",
    )
    parser.add_argument("--tools-config", default="config/tools_pg_all.yml")
    parser.add_argument("--mutation-config", default="config/mutation_safe.yml")
    parser.add_argument("--output-dir", default="output")
    parser.add_argument("--logs-dir", default="logs")
    parser.add_argument(
        "--engine",
        choices=["postgres", "spark"],
        default="postgres",
        help="Database engine to use for the experiment.",
    )
    parser.add_argument("--start-temp-db", action="store_true", help="Start/reset temp postgres in Docker before run.")
    parser.add_argument(
        "--keep-temp-db",
        action="store_true",
        help="Keep temporary postgres container after run (default: stop and remove volume).",
    )
    parser.add_argument("--skip-mutations", action="store_true")
    parser.add_argument("--skip-tools", action="store_true")
    parser.add_argument(
        "--persist-mutation-report",
        action="store_true",
        help="Store mutation HTML report in logs/mutation_reports (default: disable report artifacts).",
    )
    parser.add_argument(
        "--persist-tool-reports",
        action="store_true",
        help="Store tool reports in logs/etl_stage_reports (default: disable report artifacts).",
    )
    return parser.parse_args()


def _setup_logging(logs_dir: Path) -> Path:
    logs_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = logs_dir / f"manual_experiment_{ts}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
        handlers=[
            logging.FileHandler(log_path, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
        force=True,
    )
    return log_path


def _resolve_input_run_dir(input_root: Path, explicit: str | None) -> Path:
    if explicit:
        candidate = Path(explicit)
        if not candidate.is_absolute():
            candidate = REPO_ROOT / candidate
        if not candidate.exists():
            raise FileNotFoundError(f"Input run directory does not exist: {candidate}")
        return candidate

    if not input_root.exists():
        raise FileNotFoundError(f"Input root does not exist: {input_root}")
    children = [p for p in input_root.iterdir() if p.is_dir()]
    if not children:
        raise FileNotFoundError(f"No run subdirectories found in {input_root}")
    return max(children, key=lambda p: p.stat().st_mtime)


def _load_payload_files(input_run_dir: Path) -> list[Path]:
    payload_dir = input_run_dir / "payloads"
    if not payload_dir.exists():
        raise FileNotFoundError(f"Missing payloads directory: {payload_dir}")
    files = sorted(payload_dir.glob("*.json"))
    if not files:
        raise FileNotFoundError(f"No payload JSON files found in {payload_dir}")
    return files


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _build_run_id(prefix: str) -> str:
    return f"{prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"


def _load_stg_from_input(
    *,
    payload_files: list[Path],
    dag_id: str,
    stg_run_id: str,
    input_run_dir: Path | None = None,
    parallel: bool = True,
    max_workers: int = 4,
) -> int:
    logger = logging.getLogger(__name__) 
    
    engine = get_engine()
    total_rows = 0
    
    batches = []
    if input_run_dir and input_run_dir.exists():
        batches = sorted(input_run_dir.glob("batch_*"))
    
    if batches:
        logger.info("Detected batched dataset: %s batches", len(batches))
        
        audit_log(
            engine, dag_id=dag_id, run_id=stg_run_id,
            layer="STG", entity_name="raw_football_api",
            status="STARTED",
            message=f"Batched load: {len(batches)} batches",
        )
        
        if parallel:
            results = load_all_batches_parallel(
                engine, input_run_dir, stg_run_id, max_workers=max_workers
            )
        else:
            results = load_all_batches_sequential(
                engine, input_run_dir, stg_run_id
            )
        
        total_rows = sum(r["rows_loaded"] for r in results)
        failed = [r for r in results if r["status"] == "FAILED"]
        
        if failed:
            logger.error("%s batches failed", len(failed))
            audit_log(
                engine, dag_id=dag_id, run_id=stg_run_id,
                layer="STG", entity_name="raw_football_api",
                status="FAILED",
                message=f"{len(failed)} batches failed",
            )
            raise RuntimeError(f"STG load failed: {len(failed)} batches failed")
        
        audit_log(
            engine, dag_id=dag_id, run_id=stg_run_id,
            layer="STG", entity_name="raw_football_api",
            status="SUCCESS", rows_processed=total_rows,
        )
        return total_rows
    
    logger.info("Detected flat dataset (legacy format)")
    
    audit_log(
        engine, dag_id=dag_id, run_id=stg_run_id,
        layer="STG", entity_name="raw_football_api",
        status="STARTED",
    )
    
    try:
        for payload_path in payload_files:
            payload = _read_json(payload_path)
            endpoint = str(payload["endpoint"])
            http_status = int(payload.get("http_status", 200))
            response_json = payload["response_json"]
            metadata = dict(payload.get("request_params") or {})
            metadata["run_id"] = stg_run_id
            metadata["source_file"] = str(payload_path.name)
            total_rows += load_raw(
                engine,
                endpoint=endpoint,
                status_code=http_status,
                payload=response_json,
                metadata=metadata,
            )
        
        audit_log(
            engine, dag_id=dag_id, run_id=stg_run_id,
            layer="STG", entity_name="raw_football_api",
            status="SUCCESS", rows_processed=total_rows,
        )
        return total_rows
    
    except Exception:
        audit_log(
            engine, dag_id=dag_id, run_id=stg_run_id,
            layer="STG", entity_name="raw_football_api",
            status="FAILED",
        )
        raise

def _run_dds(
    *,
    dag_id: str,
    stg_run_id: str,
    dds_run_id: str,
    parallel: bool = True,
    max_workers: int = 4,
) -> None:
    engine = get_engine()
    logger = logging.getLogger(__name__)
    
    log_batch_status(
        engine, dag_id=dag_id, run_id=dds_run_id,
        parent_run_id=stg_run_id, layer="DDS", status="NEW",
    )
    log_batch_status(
        engine, dag_id=dag_id, run_id=dds_run_id,
        parent_run_id=stg_run_id, layer="DDS", status="PROCESSING",
    )
    
    try:
        batch_count = 0
        try:
            with engine.begin() as conn:
                result = conn.execute(
                    text("""
                        SELECT COUNT(DISTINCT batch_id) as batch_count
                        FROM stg.raw_football_api
                        WHERE request_params ->> 'run_id' = :run_id
                          AND batch_id IS NOT NULL
                    """),
                    {"run_id": stg_run_id},
                ).scalar()
                batch_count = int(result or 0)
        except Exception:
            logger.info("batch_id column not found, falling back to sequential DDS load")
            batch_count = 0
        
        if parallel and batch_count > 1:
            logger.info("Using sequential DDS load (parallel DDS under development)")
            with engine.begin() as conn:
                run_dds_load(
                    conn=conn, dag_id=dag_id,
                    dds_run_id=dds_run_id, parent_run_id=stg_run_id,
                )
        
        log_batch_status(
            engine, dag_id=dag_id, run_id=dds_run_id,
            parent_run_id=stg_run_id, layer="DDS", status="SUCCESS",
        )
    
    except Exception as exc:
        log_batch_status(
            engine, dag_id=dag_id, run_id=dds_run_id,
            parent_run_id=stg_run_id, layer="DDS",
            status="FAILED", error_message=str(exc),
        )
        raise

def _set_baseline_ids(config_path: Path, baseline: BaselineRuns) -> None:
    data = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    exp = data.setdefault("experiment", {})
    base = exp.setdefault("baseline", {})
    base["stg_run_id"] = baseline.stg_run_id
    base["dds_run_id"] = baseline.dds_run_id
    config_path.write_text(yaml.safe_dump(data, allow_unicode=True, sort_keys=False), encoding="utf-8")


def _resolve_existing_path(path_str: str) -> Path:
    candidate = Path(path_str)
    if candidate.is_absolute() and candidate.exists():
        return candidate

    candidates = []
    if not candidate.is_absolute():
        candidates.append(REPO_ROOT / candidate)
        candidates.append(SRC_ROOT / candidate)
        if not str(candidate).startswith("src\\") and not str(candidate).startswith("src/"):
            candidates.append(REPO_ROOT / "src" / candidate)
    for item in candidates:
        if item.exists():
            return item.resolve()
    raise FileNotFoundError(f"Cannot resolve config path: {path_str}")


def _validate_mutation_defaults_paths(config_path: Path) -> None:
    data = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    defaults = (data.get("experiment", {}) or {}).get("defaults", {})
    if not isinstance(defaults, dict):
        return

    keys = [
        "stg_mutations_config",
        "dds_mutations_config",
        "stg_validation_config",
        "dds_validation_config",
    ]
    for key in keys:
        value = defaults.get(key)
        if not isinstance(value, str) or not value.strip():
            continue
        _resolve_existing_path(value.strip())


def _to_repo_relative_path_str(path: Path | None) -> str | None:
    if path is None:
        return None
    resolved = path.resolve()
    try:
        return resolved.relative_to(REPO_ROOT).as_posix()
    except ValueError:
        return resolved.as_posix()


def _run_tools(config_path: Path, output_dir: Path, dag_id: str) -> list[dict[str, Any]]:
    cfg = load_tools_experiment_config(config_path)
    output_dir.mkdir(parents=True, exist_ok=True)

    ordered_stages = ["E", "T", "L"]
    results: list[dict[str, Any]] = []
    for stage in ordered_stages:
        tools = (cfg.defaults.tools_by_stage or {}).get(stage, [])
        for tool in tools:
            try:
                result = run_stage_tool(
                    stage=stage,
                    tool=tool,
                    config_path=str(config_path),
                    output_dir=output_dir,
                    dag_id=dag_id,
                )
            except Exception as exc:
                result = {
                    "stage": stage,
                    "tool": tool,
                    "status": "FAILED",
                    "reason": str(exc),
                }
            results.append(result)
    return results


def _export_validation_summary(*, output_dir: Path, dag_id: str) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / f"validation_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    engine = get_engine()
    exported_at = datetime.now().isoformat()
    query = text(
        """
        SELECT
            split_part(layer, '_', 1) AS stage,
            layer,
            tool,
            COALESCE(kind, 'unknown') AS kind,
            COUNT(*) AS runs,
            SUM(checks_total) AS checks_total,
            SUM(checks_failed) AS checks_failed,
            ROUND(AVG(duration_ms)::numeric, 3) AS avg_duration_ms,
            ROUND(COALESCE(STDDEV_POP(duration_ms), 0)::numeric, 3) AS std_duration_ms,
            ROUND(AVG(NULLIF(meta_json->'resources'->>'cpu_percent_avg', '')::numeric)::numeric, 3) AS avg_cpu_percent,
            ROUND(COALESCE(STDDEV_POP(NULLIF(meta_json->'resources'->>'cpu_percent_avg', '')::numeric), 0)::numeric, 3) AS std_cpu_percent,
            ROUND(AVG(NULLIF(meta_json->'resources'->>'rss_kb', '')::numeric)::numeric, 3) AS avg_rss_kb,
            ROUND(COALESCE(STDDEV_POP(NULLIF(meta_json->'resources'->>'rss_kb', '')::numeric), 0)::numeric, 3) AS std_rss_kb,
            MIN(started_at) AS first_started_at,
            MAX(finished_at) AS last_finished_at
        FROM tech.validation_run
        WHERE dag_id = :dag_id
        GROUP BY layer, tool, COALESCE(kind, 'unknown')
        ORDER BY layer, tool, kind
        """
    )
    with engine.begin() as conn:
        rows = conn.execute(query, {"dag_id": dag_id}).mappings().all()

    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "exported_at",
                "tools_dag_id",
                "stage",
                "layer",
                "tool",
                "kind",
                "runs",
                "checks_total",
                "checks_failed",
                "avg_duration_ms",
                "std_duration_ms",
                "avg_cpu_percent",
                "avg_rss_kb",
                "std_cpu_percent",
                "std_rss_kb",
                "first_started_at",
                "last_finished_at",
            ],
        )
        writer.writeheader()
        for row in rows:
            row_out = dict(row)
            row_out["exported_at"] = exported_at
            row_out["tools_dag_id"] = dag_id
            writer.writerow(row_out)
    return out_path


def _start_temp_db_if_requested(enabled: bool) -> None:
    if not enabled:
        return
    script = REPO_ROOT / "scripts" / "start_temp_db.py"
    if not script.exists():
        raise FileNotFoundError(f"Missing helper script: {script}")
    subprocess.run([sys.executable, str(script)], check=True, cwd=REPO_ROOT)


def _stop_temp_db_if_requested(enabled: bool) -> None:
    if not enabled:
        return
    compose_file = REPO_ROOT / "docker-compose.experiments.yml"
    if not compose_file.exists():
        raise FileNotFoundError(f"Missing compose file: {compose_file}")
    subprocess.run(
        ["docker", "compose", "-f", str(compose_file), "down", "-v"],
        check=True,
        cwd=REPO_ROOT,
    )


def _collect_related_run_ids(*, stg_run_id: str, dds_run_id: str) -> list[str]:
    engine = get_engine()
    query = text(
        """
        SELECT DISTINCT run_id
        FROM tech.etl_batch_status
        WHERE run_id = :stg_run_id
           OR run_id = :dds_run_id
           OR parent_run_id = :stg_run_id
        ORDER BY run_id
        """
    )
    with engine.begin() as conn:
        rows = conn.execute(query, {"stg_run_id": stg_run_id, "dds_run_id": dds_run_id}).scalars().all()
    return [str(v) for v in rows]


def _export_db_logs(
    *,
    logs_dir: Path,
    related_run_ids: list[str],
    tools_dag_id: str | None,
) -> Path:
    logs_dir.mkdir(parents=True, exist_ok=True)
    out_path = logs_dir / f"db_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    engine = get_engine()

    audit_rows: list[dict[str, Any]] = []
    batch_rows: list[dict[str, Any]] = []
    validation_rows: list[dict[str, Any]] = []

    with engine.begin() as conn:
        if related_run_ids:
            audit_stmt = (
                text(
                    """
                    SELECT audit_id, dag_id, run_id, layer, entity_name, status, started_at, finished_at, rows_processed, message
                    FROM tech.etl_load_audit
                    WHERE run_id IN :run_ids
                    ORDER BY audit_id
                    """
                ).bindparams(bindparam("run_ids", expanding=True))
            )
            audit_rows = [dict(row) for row in conn.execute(audit_stmt, {"run_ids": related_run_ids}).mappings().all()]

            batch_stmt = (
                text(
                    """
                    SELECT batch_id, dag_id, run_id, parent_run_id, layer, status, attempts, created_at, last_updated_at, error_message
                    FROM tech.etl_batch_status
                    WHERE run_id IN :run_ids OR parent_run_id IN :run_ids
                    ORDER BY batch_id
                    """
                ).bindparams(bindparam("run_ids", expanding=True))
            )
            batch_rows = [dict(row) for row in conn.execute(batch_stmt, {"run_ids": related_run_ids}).mappings().all()]

        if tools_dag_id:
            validation_stmt = text(
                """
                SELECT
                    validation_run_id, dag_id, run_id, parent_run_id, layer, tool, kind,
                    status, started_at, finished_at, duration_ms, checks_total, checks_failed
                FROM tech.validation_run
                WHERE dag_id = :dag_id
                ORDER BY validation_run_id
                """
            )
            validation_rows = [dict(row) for row in conn.execute(validation_stmt, {"dag_id": tools_dag_id}).mappings().all()]

    with out_path.open("w", encoding="utf-8") as f:
        f.write("=== ETL LOAD AUDIT ===\n")
        for row in audit_rows:
            f.write(
                f"[audit_id={row['audit_id']}] dag={row['dag_id']} run={row['run_id']} "
                f"layer={row['layer']} entity={row['entity_name']} status={row['status']} "
                f"started={row['started_at']} finished={row['finished_at']} rows={row['rows_processed']} "
                f"message={row['message']}\n"
            )

        f.write("\n=== ETL BATCH STATUS ===\n")
        for row in batch_rows:
            f.write(
                f"[batch_id={row['batch_id']}] dag={row['dag_id']} run={row['run_id']} parent={row['parent_run_id']} "
                f"layer={row['layer']} status={row['status']} attempts={row['attempts']} "
                f"created={row['created_at']} updated={row['last_updated_at']} error={row['error_message']}\n"
            )

        f.write("\n=== VALIDATION RUNS ===\n")
        for row in validation_rows:
            f.write(
                f"[validation_run_id={row['validation_run_id']}] dag={row['dag_id']} run={row['run_id']} "
                f"parent={row['parent_run_id']} layer={row['layer']} tool={row['tool']} kind={row['kind']} "
                f"status={row['status']} started={row['started_at']} finished={row['finished_at']} "
                f"duration_ms={row['duration_ms']} checks_total={row['checks_total']} checks_failed={row['checks_failed']}\n"
            )
    return out_path


def _run_spark_experiment(
    input_run_dir: Path,
    output_dir: Path,
    logs_dir: Path,
    tools_config_path: Path,
    mutation_config_path: Path | None = None,
    skip_mutations: bool = False,
    skip_tools: bool = False,
) -> None:
    logger = logging.getLogger(__name__)
    logger.info("Starting Spark experiment...")

    spark = (
        SparkSession.builder
        .appName("ETL-Validation-Spark")
        .master("local[*]")
        .getOrCreate()
    )

    try:
        warehouse_base = Path("spark-warehouse")
        if warehouse_base.exists():
            import shutil
            shutil.rmtree(warehouse_base)

        spark.sql("CREATE DATABASE IF NOT EXISTS etl_kio")

        start = time.time()
        stats = load_batches_to_spark(spark, input_run_dir, database="etl_kio")
        load_sec = time.time() - start
        logger.info("Spark load completed in %.2f sec", load_sec)
        logger.info(
            "Tables: areas=%d, competitions=%d, teams=%d, matches=%d, standings=%d",
            stats.get("areas", 0),
            stats.get("competitions", 0),
            stats.get("teams", 0),
            stats.get("matches", 0),
            stats.get("standings", 0),
        )
        snapshot_dir = Path("temp_parquet")
        if snapshot_dir.exists():
            import shutil
            shutil.rmtree(snapshot_dir)
        snapshot_dir.mkdir(parents=True, exist_ok=True)

        for tbl in ["areas", "competitions", "teams", "matches", "standings"]:
            spark.table(f"etl_kio.{tbl}").write.mode("overwrite").parquet(str(snapshot_dir / tbl))
        validation_stats = {}
        if not skip_tools:
            validation_stats = _run_spark_validation(
                spark, output_dir, tools_config_path, kind="baseline"
            )
        else:
            validation_stats["status"] = "SKIPPED"
        mutation_results = []
        if not skip_mutations and mutation_config_path:
            from app2.experiments.config import load_experiment_config
            from app2.mutators.spark_mutations import MUTATION_MAP

            exp_cfg = load_experiment_config(mutation_config_path)
            for idx, it_cfg in enumerate(exp_cfg.iterations, start=1):
                logger.info("=== Mutation iteration %d: %s ===", idx, it_cfg.name)
                for tbl in ["areas", "competitions", "teams", "matches", "standings"]:
                    spark.read.parquet(str(snapshot_dir / tbl)).write.mode("overwrite").saveAsTable(f"etl_kio.{tbl}")

                if it_cfg.stg_mutations_enable:
                    for entity, actions in it_cfg.stg_mutations_enable.items():
                        for action in actions:
                            fn = MUTATION_MAP.get(action)
                            if fn:
                                fn(spark)
                if it_cfg.dds_mutations_enable:
                    for action in it_cfg.dds_mutations_enable:
                        fn = MUTATION_MAP.get(action)
                        if fn:
                            fn(spark)

                iter_output = output_dir / f"mutation_{idx:02d}"
                iter_output.mkdir(parents=True, exist_ok=True)
                iter_stats = _run_spark_validation(
                    spark, iter_output, tools_config_path, kind=it_cfg.name
                )
                mutation_results.append({
                    "iteration": idx,
                    "name": it_cfg.name,
                    "stats": iter_stats,
                })
                logger.info("Iteration %d finished", idx)

        output_dir.mkdir(parents=True, exist_ok=True)
        ts_tag = datetime.now().strftime('%Y%m%d_%H%M%S')
        summary_path = output_dir / f"spark_summary_{ts_tag}.csv"
        with open(summary_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["stage", "metric", "value"])
            writer.writerow(["load", "duration_sec", round(load_sec, 3)])
            for table, rows in stats.items():
                writer.writerow(["load", f"{table}_rows", rows])
            def _is_legacy_metric(key: str) -> bool:
                return not (key.endswith("_spark_sec") or key.endswith("_spark_sec_std"))

            for stage, info in validation_stats.items():
                if isinstance(info, dict):
                    for k, v in info.items():
                        if _is_legacy_metric(k):
                            writer.writerow([f"validation_{stage}", k, v])
                else:
                    writer.writerow(["validation", "status", info])
            for mres in mutation_results:
                idx = mres["iteration"]
                for stage, info in mres["stats"].items():
                    if isinstance(info, dict):
                        for k, v in info.items():
                            if _is_legacy_metric(k):
                                writer.writerow([f"mutation_{idx}_validation_{stage}", k, v])
                    else:
                        writer.writerow([f"mutation_{idx}_validation", "status", info])
        logger.info("Spark summary saved to %s", summary_path)
        flat_path = output_dir / f"validation_summary_{ts_tag}.csv"
        with open(flat_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f,
                fieldnames=[
                    "exported_at", "tools_dag_id",
                    "stage", "layer", "tool", "kind",
                    "runs", "checks_total", "checks_failed",
                    "avg_duration_ms", "std_duration_ms",
                    "avg_cpu_percent", "std_cpu_percent",
                    "avg_rss_kb", "std_rss_kb",
                    "avg_spark_duration_ms", "std_spark_duration_ms",
                ],
            )
            writer.writeheader()
            exported_at = datetime.now().isoformat()

            def _emit_block(block_kind: str, block_stats: dict):
                for stage, info in block_stats.items():
                    if not isinstance(info, dict):
                        continue
                    tools_in_stage = sorted({k.rsplit("_", 1)[0] for k in info.keys() if "_" in k})
                    tools_set = set()
                    for k in info.keys():
                        for t in ("gx", "sql", "soda", "deequ"):
                            if k.startswith(f"{t}_"):
                                tools_set.add(t)
                    for tool in sorted(tools_set):
                        writer.writerow({
                            "exported_at": exported_at,
                            "tools_dag_id": f"spark_{block_kind}",
                            "stage": stage,
                            "layer": f"{stage}_{tool.upper()}",
                            "tool": tool,
                            "kind": block_kind,
                            "runs": info.get(f"{tool}_repeats", 1),
                            "checks_total": info.get(f"{tool}_checks", 0),
                            "checks_failed": info.get(f"{tool}_failed", 0),
                            "avg_duration_ms": round(float(info.get(f"{tool}_sec", 0.0)) * 1000.0, 3),
                            "std_duration_ms": round(float(info.get(f"{tool}_sec_std", 0.0)) * 1000.0, 3),
                            "avg_cpu_percent": info.get(f"{tool}_cpu", 0.0),
                            "std_cpu_percent": info.get(f"{tool}_cpu_std", 0.0),
                            "avg_rss_kb": info.get(f"{tool}_ram_kb", 0.0),
                            "std_rss_kb": info.get(f"{tool}_ram_kb_std", 0.0),
                            "avg_spark_duration_ms": round(float(info.get(f"{tool}_spark_sec", 0.0)) * 1000.0, 3),
                            "std_spark_duration_ms": round(float(info.get(f"{tool}_spark_sec_std", 0.0)) * 1000.0, 3),
                        })

            _emit_block("baseline", validation_stats)
            for mres in mutation_results:
                _emit_block(mres.get("name") or f"mutation_{mres.get('iteration')}", mres["stats"])
        logger.info("Spark flat validation_summary saved to %s", flat_path)

    except Exception as e:
        logger.exception("Spark experiment failed")
        raise
    finally:
        spark.stop()


def _run_spark_validation(spark, output_dir, tools_config_path, kind: str) -> dict:
    import statistics
    logger = logging.getLogger(__name__)
    from app2.etl_validation.config import load_tools_experiment_config
    cfg = load_tools_experiment_config(tools_config_path)
    tools_by_stage = (cfg.defaults.tools_by_stage or {})
    repeats = int(cfg.defaults.repeats or 1)
    override = os.environ.get("APP2_REPEATS_OVERRIDE")
    if override:
        try:
            repeats = int(override)
        except ValueError:
            pass
    repeats = max(1, repeats)
    spark_tools = {"gx", "sql", "soda", "deequ"}

    def _agg(values: list[float]) -> tuple[float, float]:
        if not values:
            return 0.0, 0.0
        mean_v = round(statistics.mean(values), 3)
        std_v = round(statistics.pstdev(values), 3) if len(values) > 1 else 0.0
        return mean_v, std_v

    stats: dict[str, dict] = {}
    for stage in ["E", "T", "L"]:
        tools = [t for t in tools_by_stage.get(stage, []) if t in spark_tools]
        if not tools:
            continue
        logger.info("Spark validation [%s] stage %s: %s (repeats=%d)", kind, stage, tools, repeats)
        stage_stats: dict = {}
        for tool in tools:
            sec_runs: list[float] = []
            cpu_runs: list[float] = []
            ram_runs: list[float] = []
            spark_sec_runs: list[float] = []  
            checks_total = 0
            checks_failed = 0

            for repeat_num in range(1, repeats + 1):
                proc = psutil.Process()
                proc.cpu_percent(interval=None)
                mem_start = proc.memory_info().rss / 1024 
                t0 = time.time()

                rep_total = 0
                rep_failed = 0
                rep_spark_sec = 0.0
                try:
                    if tool == "gx":
                        reports = run_stage_validation_gx_spark(
                            spark=spark, dag_id=f"spark_{kind}_gx", stage=stage,
                            targets=[type('t', (), {"run_id": "spark_run", "parent_run_id": "spark_run", "kind": kind})()],
                            output_dir=output_dir, layer=stage)
                    elif tool == "sql":
                        reports = run_stage_validation_sql_spark(
                            spark=spark, dag_id=f"spark_{kind}_sql", stage=stage,
                            run_id="spark_run", parent_run_id="spark_run",
                            output_dir=output_dir, layer=stage)
                    elif tool == "soda":
                        reports = run_stage_validation_soda_spark(
                            spark=spark, dag_id=f"spark_{kind}_soda", stage=stage,
                            targets=[type('t', (), {"run_id": "spark_run", "parent_run_id": "spark_run", "kind": kind})()],
                            output_dir=output_dir, layer=stage)
                    elif tool == "deequ":
                        reports = run_stage_validation_deequ_spark(
                            spark=spark, dag_id=f"spark_{kind}_deequ", stage=stage,
                            targets=[type('t', (), {"run_id": "spark_run", "parent_run_id": "spark_run", "kind": kind})()],
                            output_dir=output_dir, layer=stage)
                    else:
                        reports = []

                    for r in reports:
                        if isinstance(r, dict):
                            rep_total += r.get("checks_total", 0)
                            rep_failed += r.get("checks_failed", 0)
                            rep_spark_sec += float(r.get("spark_sec", 0.0) or 0.0)

                    cpu_after = proc.cpu_percent(interval=None)
                    mem_end = proc.memory_info().rss / 1024
                    duration = time.time() - t0

                    sec_runs.append(round(duration, 3))
                    cpu_runs.append(round(cpu_after, 2))
                    ram_runs.append(round((mem_start + mem_end) / 2, 0))
                    spark_sec_runs.append(round(rep_spark_sec, 3))

                    if repeat_num == 1:
                        checks_total = rep_total
                        checks_failed = rep_failed

                except Exception as e:
                    logger.error("%s on Spark stage %s repeat %d failed: %s", tool, stage, repeat_num, e)

            sec_mean, sec_std = _agg(sec_runs)
            cpu_mean, cpu_std = _agg(cpu_runs)
            ram_mean, ram_std = _agg(ram_runs)
            spark_sec_mean, spark_sec_std = _agg(spark_sec_runs)

            stage_stats[f"{tool}_sec"] = sec_mean
            stage_stats[f"{tool}_sec_std"] = sec_std
            stage_stats[f"{tool}_cpu"] = cpu_mean
            stage_stats[f"{tool}_cpu_std"] = cpu_std
            stage_stats[f"{tool}_ram_kb"] = ram_mean
            stage_stats[f"{tool}_ram_kb_std"] = ram_std
            stage_stats[f"{tool}_spark_sec"] = spark_sec_mean
            stage_stats[f"{tool}_spark_sec_std"] = spark_sec_std
            stage_stats[f"{tool}_checks"] = checks_total
            stage_stats[f"{tool}_failed"] = checks_failed
            stage_stats[f"{tool}_repeats"] = len(sec_runs)

        stats[stage] = stage_stats
    return stats

def main() -> None:
    load_dotenv(dotenv_path=REPO_ROOT / ".env")
    args = _parse_args()

    output_dir = Path(args.output_dir)
    logs_dir = Path(args.logs_dir)
    tools_config_path = Path(args.tools_config)
    mutation_config_path = Path(args.mutation_config)
    if not tools_config_path.is_absolute():
        tools_config_path = REPO_ROOT / tools_config_path
    if not mutation_config_path.is_absolute():
        mutation_config_path = REPO_ROOT / mutation_config_path
    if not output_dir.is_absolute():
        output_dir = REPO_ROOT / output_dir
    if not logs_dir.is_absolute():
        logs_dir = REPO_ROOT / logs_dir

    log_path = _setup_logging(logs_dir)
    logging.info("Logs: %s", log_path)
    logging.info("Local input mode enabled. External football API client is not used in this run.")

    input_root = Path(args.input_dir)
    if not input_root.is_absolute():
        input_root = REPO_ROOT / input_root
    input_run_dir = _resolve_input_run_dir(input_root, args.input_run_dir)

    if args.engine == "spark":
        _run_spark_experiment(
            input_run_dir=input_run_dir,
            output_dir=output_dir,
            logs_dir=logs_dir,
            tools_config_path=tools_config_path,
            mutation_config_path=mutation_config_path,   
            skip_mutations=args.skip_mutations,  
            skip_tools=args.skip_tools,
        )
        return
    stg_run_id: str | None = None
    dds_run_id: str | None = None
    mutation_report_path: Path | None = None
    tools_dag_id: str | None = None
    tools_results: list[dict[str, Any]] = []
    summary_csv: Path | None = None
    db_log_path: Path | None = None
    temp_db_started = False

    try:
        _start_temp_db_if_requested(args.start_temp_db)
        if args.start_temp_db:
            temp_db_started = True

        is_batched = any(input_run_dir.glob("batch_*"))

        if is_batched:
            payload_files = []
        else:
            payload_files = _load_payload_files(input_run_dir)

        stg_run_id = _build_run_id("manual_input_stg")
        dds_run_id = _build_run_id("manual_input_dds")
        baseline = BaselineRuns(stg_run_id=stg_run_id, dds_run_id=dds_run_id)

        rows_loaded = _load_stg_from_input(
            payload_files=payload_files,
            dag_id="manual_stg_input",
            stg_run_id=stg_run_id,
            input_run_dir=input_run_dir,
            parallel=True,
            max_workers=4,
        )

        _run_dds(
            dag_id="manual_dds_input",
            stg_run_id=stg_run_id,
            dds_run_id=dds_run_id,
            parallel=True,
            max_workers=4,
        )
        logging.info("DDS load finished: %s", dds_run_id)

        _set_baseline_ids(tools_config_path, baseline)
        _set_baseline_ids(mutation_config_path, baseline)
        _validate_mutation_defaults_paths(mutation_config_path)
        logging.info("Baseline IDs updated in config files")

        if not args.skip_mutations:
            exp_cfg = load_experiment_config(mutation_config_path)
            if args.persist_mutation_report:
                mutation_report_path = run_experiment(exp_cfg, output_dir=logs_dir / "mutation_reports")
                logging.info("Mutation experiment report: %s", mutation_report_path)
            else:
                with tempfile.TemporaryDirectory(prefix="kio_mutation_reports_") as tmp_dir:
                    run_experiment(exp_cfg, output_dir=Path(tmp_dir))
                logging.info("Mutation experiment completed (report artifacts disabled)")

        tools_dag_id = f"manual_stage_tools_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        if not args.skip_tools:
            if args.persist_tool_reports:
                tool_reports_dir = logs_dir / "etl_stage_reports"
                tools_results = _run_tools(
                    config_path=tools_config_path,
                    output_dir=tool_reports_dir,
                    dag_id=tools_dag_id,
                )
                logging.info("Stage tool reports saved to: %s", tool_reports_dir)
            else:
                with tempfile.TemporaryDirectory(prefix="kio_stage_reports_") as tmp_dir:
                    tools_results = _run_tools(
                        config_path=tools_config_path,
                        output_dir=Path(tmp_dir),
                        dag_id=tools_dag_id,
                    )
                logging.info("Stage tool runs completed (report artifacts disabled)")

            logging.info("Stage tool runs: %s", len(tools_results))
            summary_csv = _export_validation_summary(output_dir=output_dir, dag_id=tools_dag_id)

        related_run_ids = _collect_related_run_ids(stg_run_id=stg_run_id, dds_run_id=dds_run_id)
        db_log_path = _export_db_logs(logs_dir=logs_dir, related_run_ids=related_run_ids, tools_dag_id=tools_dag_id)
        logging.info("DB logs exported to: %s", db_log_path)

        run_context = {
            "input_run_dir": _to_repo_relative_path_str(input_run_dir),
            "stg_run_id": stg_run_id,
            "dds_run_id": dds_run_id,
            "tools_dag_id": tools_dag_id if not args.skip_tools else None,
            "mutation_report_path": _to_repo_relative_path_str(mutation_report_path),
            "summary_csv": _to_repo_relative_path_str(summary_csv),
            "db_log_path": _to_repo_relative_path_str(db_log_path),
            "tools_results": tools_results,
            "timestamp": datetime.now().isoformat(),
        }
        context_path = REPO_ROOT / "config" / "last_run_context.json"
        context_path.write_text(json.dumps(run_context, ensure_ascii=False, indent=2), encoding="utf-8")

        print("STG run_id:", stg_run_id)
        print("DDS run_id:", dds_run_id)
        print("Input run dir:", input_run_dir)
        if mutation_report_path:
            print("Mutation report:", mutation_report_path)
        if summary_csv:
            print("Summary CSV:", summary_csv)
        if db_log_path:
            print("DB logs:", db_log_path)
        print("Context file:", context_path)
    finally:
        if temp_db_started and not args.keep_temp_db:
            try:
                _stop_temp_db_if_requested(True)
                logging.info("Temporary DB container and volume removed")
            except Exception:
                logging.exception("Failed to stop temporary DB")


if __name__ == "__main__":
    main()