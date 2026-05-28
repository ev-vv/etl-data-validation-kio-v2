
from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from app2.dds.load_dds import (
    SQL_DIM_AREA,
    SQL_DIM_COMPETITION,
    SQL_DIM_TEAM,
    SQL_DIM_SEASON,
    SQL_FACT_MATCH,
    SQL_FACT_MATCH_SCORE,
    SQL_FACT_STANDING,
)

logger = logging.getLogger(__name__)


def _add_batch_filter(sql: str, batch_id: str) -> str:
    if not batch_id:
        return sql

    if "ON CONFLICT" in sql.upper():
        idx = sql.upper().index("ON CONFLICT")
        return sql[:idx] + f"AND s.batch_id = :batch_id " + sql[idx:]
    else:
        if sql.rstrip().endswith(";"):
            return sql.rstrip()[:-1].rstrip() + f" AND s.batch_id = :batch_id;"
        else:
            return sql.rstrip() + f" AND s.batch_id = :batch_id"


def _load_dds_for_batch(
    conn,
    dds_run_id: str,
    parent_run_id: str,
    batch_id: str | None = None,
    skip_dimensions: bool = False,
) -> dict[str, Any]:
    start_time = time.time()
    steps_result = {}
    
    params: dict[str, Any] = {
        "stg_run_id": parent_run_id,
        "dds_run_id": dds_run_id,
    }
    
    if batch_id:
        params["batch_id"] = batch_id
    
    try:
        if not skip_dimensions:
            dim_sqls = [
                ("dim_area", SQL_DIM_AREA),
                ("dim_competition", SQL_DIM_COMPETITION),
                ("dim_team", SQL_DIM_TEAM),
                ("dim_season", SQL_DIM_SEASON),
            ]
            
            for name, sql in dim_sqls:
                step_start = time.time()
                result = conn.execute(
                    text(sql),
                    {"stg_run_id": parent_run_id, "dds_run_id": dds_run_id},
                )
                steps_result[name] = {
                    "rows": result.rowcount,
                    "duration": round(time.time() - step_start, 3),
                }
        
        fact_sqls = [
            ("fact_match", SQL_FACT_MATCH),
            ("fact_match_score", SQL_FACT_MATCH_SCORE),
            ("fact_standing", SQL_FACT_STANDING),
        ]
        
        for name, sql in fact_sqls:
            step_start = time.time()
            if batch_id:
                filtered_sql = _add_batch_filter(sql, batch_id)
            else:
                filtered_sql = sql
            result = conn.execute(text(filtered_sql), params)
            steps_result[name] = {
                "rows": result.rowcount,
                "duration": round(time.time() - step_start, 3),
            }
        
        total_duration = time.time() - start_time
        return {
            "batch_id": batch_id or "all",
            "status": "SUCCESS",
            "duration": round(total_duration, 3),
            "steps": steps_result,
        }
    
    except Exception as e:
        total_duration = time.time() - start_time
        return {
            "batch_id": batch_id or "all",
            "status": "FAILED",
            "duration": round(total_duration, 3),
            "error": str(e),
            "steps": steps_result,
        }


def load_dds_parallel(
    engine: Engine,
    dds_run_id: str,
    parent_run_id: str,
    batch_ids: list[str],
    max_workers: int = 4,
) -> dict[str, Any]:
    logger.info(
        "Starting parallel DDS load: %d batches, %d workers",
        len(batch_ids), max_workers
    )
    
    start_time = time.time()
    
    logger.info("Loading dimensions (single worker)...")
    with engine.begin() as conn:
        dim_result = _load_dds_for_batch(
            conn, dds_run_id, parent_run_id,
            batch_id=None, skip_dimensions=False
        )
    
    if dim_result["status"] == "FAILED":
        logger.error("Dimension load failed: %s", dim_result.get("error"))
        return {
            "status": "FAILED",
            "dimensions": dim_result,
            "facts": [],
            "total_duration": round(time.time() - start_time, 3),
        }
    
    logger.info(
        "Dimensions loaded: dim_area=%s, dim_competition=%s, dim_team=%s, dim_season=%s",
        dim_result["steps"].get("dim_area", {}).get("rows", 0),
        dim_result["steps"].get("dim_competition", {}).get("rows", 0),
        dim_result["steps"].get("dim_team", {}).get("rows", 0),
        dim_result["steps"].get("dim_season", {}).get("rows", 0),
    )
    
    logger.info("Loading facts (parallel workers)...")
    
    fact_results = []
    actual_workers = min(max_workers, len(batch_ids))
    
    with ThreadPoolExecutor(max_workers=actual_workers) as executor:
        futures = {}
        for batch_id in batch_ids:
            future = executor.submit(
                _load_dds_single_batch_facts,
                engine, dds_run_id, parent_run_id, batch_id
            )
            futures[future] = batch_id
        
        for future in as_completed(futures):
            batch_id = futures[future]
            try:
                result = future.result()
                fact_results.append(result)
                
                if result["status"] == "SUCCESS":
                    logger.info(
                        "  Batch %s: fact_match=%s, fact_score=%s, fact_standing=%s (%.2fs)",
                        batch_id,
                        result["steps"].get("fact_match", {}).get("rows", 0),
                        result["steps"].get("fact_match_score", {}).get("rows", 0),
                        result["steps"].get("fact_standing", {}).get("rows", 0),
                        result["duration"],
                    )
                else:
                    logger.error(
                        "  Batch %s FAILED: %s", batch_id, result.get("error")
                    )
            except Exception as e:
                logger.error("  Batch %s exception: %s", batch_id, e)
                fact_results.append({
                    "batch_id": batch_id,
                    "status": "FAILED",
                    "error": str(e),
                    "steps": {},
                })
    
    total_duration = time.time() - start_time
    
    total_fact_match = sum(
        r["steps"].get("fact_match", {}).get("rows", 0)
        for r in fact_results
    )
    total_fact_score = sum(
        r["steps"].get("fact_match_score", {}).get("rows", 0)
        for r in fact_results
    )
    total_fact_standing = sum(
        r["steps"].get("fact_standing", {}).get("rows", 0)
        for r in fact_results
    )
    
    failed = [r for r in fact_results if r["status"] == "FAILED"]
    
    logger.info(
        "Parallel DDS load completed: %d/%d batches, "
        "fact_match=%d, fact_score=%d, fact_standing=%d, "
        "total duration=%.2fs",
        len(fact_results) - len(failed),
        len(batch_ids),
        total_fact_match,
        total_fact_score,
        total_fact_standing,
        total_duration,
    )
    
    return {
        "status": "SUCCESS" if not failed else "PARTIAL",
        "dimensions": dim_result,
        "facts": fact_results,
        "failed_batches": len(failed),
        "total_duration": round(total_duration, 3),
        "totals": {
            "fact_match": total_fact_match,
            "fact_match_score": total_fact_score,
            "fact_standing": total_fact_standing,
        },
    }


def _load_dds_single_batch_facts(
    engine: Engine,
    dds_run_id: str,
    parent_run_id: str,
    batch_id: str,
) -> dict[str, Any]:
    with engine.begin() as conn:
        return _load_dds_for_batch(
            conn, dds_run_id, parent_run_id,
            batch_id=batch_id, skip_dimensions=True
        )