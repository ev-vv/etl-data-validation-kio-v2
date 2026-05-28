
from __future__ import annotations

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from app2.db.audit import audit_log
from app2.loaders.raw_staging import load_raw

logger = logging.getLogger(__name__)


def _load_payload_files_from_dir(payload_dir: Path) -> list[Path]:
    if not payload_dir.exists():
        raise FileNotFoundError(f"Missing payloads directory: {payload_dir}")
    files = sorted(payload_dir.glob("*.json"))
    if not files:
        raise FileNotFoundError(f"No payload JSON files found in {payload_dir}")
    return files


def load_single_batch(
    engine: Engine,
    batch_dir: Path,
    stg_run_id: str,
    batch_id: str,
) -> dict[str, Any]:
    start_time = time.time()
    payload_files = _load_payload_files_from_dir(batch_dir / "payloads")
    
    total_rows = 0
    files_processed = 0
    
    try:
        for payload_path in payload_files:
            with payload_path.open("r", encoding="utf-8") as f:
                payload = json.load(f)
            
            endpoint = str(payload.get("endpoint", ""))
            http_status = int(payload.get("http_status", 200))
            response_json = payload.get("response_json", {})
            metadata = dict(payload.get("request_params") or {})
            metadata["run_id"] = stg_run_id
            metadata["batch_id"] = batch_id
            metadata["source_file"] = str(payload_path.name)
            
            rows = load_raw(
                engine,
                endpoint=endpoint,
                status_code=http_status,
                payload=response_json,
                metadata=metadata,
            )
            total_rows += rows
            files_processed += 1
        
        duration = time.time() - start_time
        
        audit_log(
            engine,
            dag_id="batch_stg_input",
            run_id=stg_run_id,
            layer="STG",
            entity_name=f"batch_{batch_id}",
            status="SUCCESS",
            rows_processed=total_rows,
            message=f"Batch {batch_id}: {files_processed} files, {total_rows} rows, {duration:.2f}s",
        )
        
        return {
            "batch_id": batch_id,
            "rows_loaded": total_rows,
            "duration_sec": round(duration, 3),
            "files_processed": files_processed,
            "status": "SUCCESS",
        }
    
    except Exception as e:
        duration = time.time() - start_time
        
        audit_log(
            engine,
            dag_id="batch_stg_input",
            run_id=stg_run_id,
            layer="STG",
            entity_name=f"batch_{batch_id}",
            status="FAILED",
            message=str(e),
        )
        
        return {
            "batch_id": batch_id,
            "rows_loaded": total_rows,
            "duration_sec": round(duration, 3),
            "files_processed": files_processed,
            "status": "FAILED",
            "error": str(e),
        }


def load_all_batches_sequential(
    engine: Engine,
    dataset_dir: Path,
    stg_run_id: str,
) -> list[dict[str, Any]]:
    batches = sorted(dataset_dir.glob("batch_*"))
    
    if not batches:
        payloads_dir = dataset_dir / "payloads"
        if payloads_dir.exists():
            logger.info("Flat structure detected, loading as single batch")
            result = load_single_batch(engine, dataset_dir, stg_run_id, f"{stg_run_id}_b0001")
            return [result]
        else:
            raise FileNotFoundError(f"No batches or payloads found in {dataset_dir}")
    
    results = []
    for i, batch_dir in enumerate(batches):
        batch_id = f"{stg_run_id}_b{i+1:04d}"
        logger.info(f"Loading batch {i+1}/{len(batches)}: {batch_id}")
        
        result = load_single_batch(engine, batch_dir, stg_run_id, batch_id)
        results.append(result)
        
        if result["status"] == "SUCCESS":
            logger.info(
                f"  Batch {batch_id}: {result['rows_loaded']} rows, "
                f"{result['duration_sec']}s"
            )
        else:
            logger.error(f"  Batch {batch_id} FAILED: {result.get('error')}")
    
    return results


def load_all_batches_parallel(
    engine: Engine,
    dataset_dir: Path,
    stg_run_id: str,
    max_workers: int = 4,
) -> list[dict[str, Any]]:
    batches = sorted(dataset_dir.glob("batch_*"))
    
    if not batches:
        payloads_dir = dataset_dir / "payloads"
        if payloads_dir.exists():
            logger.info("Flat structure detected, loading as single batch")
            result = load_single_batch(engine, dataset_dir, stg_run_id, f"{stg_run_id}_b0001")
            return [result]
        else:
            raise FileNotFoundError(f"No batches or payloads found in {dataset_dir}")
    
    logger.info(
        f"Starting parallel load: {len(batches)} batches, {max_workers} workers"
    )
    
    results = []
    start_time = time.time()
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for i, batch_dir in enumerate(batches):
            batch_id = f"{stg_run_id}_b{i+1:04d}"
            future = executor.submit(
                load_single_batch, engine, batch_dir, stg_run_id, batch_id
            )
            futures[future] = batch_id
        
        for future in as_completed(futures):
            batch_id = futures[future]
            try:
                result = future.result()
                results.append(result)
                
                if result["status"] == "SUCCESS":
                    logger.info(
                        f"  Batch {batch_id}: {result['rows_loaded']} rows, "
                        f"{result['duration_sec']}s"
                    )
                else:
                    logger.error(
                        f"  Batch {batch_id} FAILED: {result.get('error')}"
                    )
            except Exception as e:
                logger.error(f"  Batch {batch_id} raised exception: {e}")
                results.append({
                    "batch_id": batch_id,
                    "rows_loaded": 0,
                    "duration_sec": 0,
                    "files_processed": 0,
                    "status": "FAILED",
                    "error": str(e),
                })
    
    total_duration = time.time() - start_time
    total_rows = sum(r["rows_loaded"] for r in results)
    successful = sum(1 for r in results if r["status"] == "SUCCESS")
    
    logger.info(
        f"Parallel load completed: {successful}/{len(batches)} batches, "
        f"{total_rows} total rows, {total_duration:.2f}s"
    )
    
    return results