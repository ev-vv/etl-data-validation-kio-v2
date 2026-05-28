import json
import re
import time
from datetime import datetime
from typing import Any

from app2.db.audit import audit_log
from app2.db.batch import log_batch_status
from app2.validators import load_config
from app2.db.validation_metrics import log_validation_check
from app2.validators.models import ValidationResult
from app2.validators.stg.schema_suite.areas_schema import validate_areas_schema
from app2.validators.stg.schema_suite.competitions_schema import validate_competitions_schema
from app2.validators.stg.schema_suite.matches_schema import validate_matches_schema
from app2.validators.stg.schema_suite.scorers_schema import validate_scorers_schema
from app2.validators.stg.schema_suite.standings_schema import validate_standings_schema
from app2.validators.stg.schema_suite.teams_schema import validate_teams_schema
from app2.validators.stg.ingestion_suite.api_http_status_ok import validate_api_http_status_ok
from app2.validators.stg.ingestion_suite.api_payload_shape_ok import validate_api_payload_shape_ok
from app2.validators.stg.uniqueness_suite.areas_uniqueness import validate_areas_uniqueness
from app2.validators.stg.uniqueness_suite.competitions_uniqueness import validate_competitions_uniqueness
from app2.validators.stg.uniqueness_suite.teams_uniqueness import validate_teams_uniqueness
from app2.validators.stg.uniqueness_suite.scorers_uniqueness import validate_scorers_uniqueness
from app2.validators.stg.uniqueness_suite.matches_uniqueness import validate_matches_uniqueness
from app2.validators.stg.uniqueness_suite.standings_uniqueness import validate_standings_uniqueness
from app2.validators.stg.completeness_suite.areas_completeness import validate_areas_completeness
from app2.validators.stg.completeness_suite.competitions_completeness import validate_competitions_completeness
from app2.validators.stg.completeness_suite.teams_completeness import validate_teams_completeness
from app2.validators.stg.completeness_suite.scorers_completeness import validate_scorers_completeness
from app2.validators.stg.completeness_suite.matches_completeness import validate_matches_completeness
from app2.validators.stg.completeness_suite.standings_completeness import validate_standings_completeness
from app2.validators.stg.consistency_suite.competitions_consistency import validate_competitions_consistency
from app2.validators.stg.consistency_suite.matches_consistency import validate_matches_consistency
from app2.validators.stg.consistency_suite.standings_consistency import validate_standings_consistency
from app2.validators.stg.consistency_suite.teams_consistency import validate_teams_consistency
from app2.validators.stg.consistency_suite.scorers_consistency import validate_scorers_consistency
from app2.validators.dds.referential_suite.fact_match_fk import validate_fact_match_fk
from app2.validators.dds.referential_suite.fact_standing_fk import validate_fact_standing_fk
from app2.validators.dds.referential_suite.dim_competition_area_fk import validate_dim_competition_area_fk
from app2.validators.dds.source_suite.competitions_source import validate_competitions_source_completeness, validate_competitions_source_exclusivity
from app2.validators.dds.source_suite.teams_source import validate_teams_source_completeness, validate_teams_source_exclusivity
from app2.validators.dds.source_suite.matches_source import validate_matches_source_completeness, validate_matches_source_exclusivity
from app2.validators.dds.source_suite.standings_source import validate_standings_source_completeness, validate_standings_source_exclusivity
from app2.validators.dds.rules_suite.match_home_away_diff import validate_match_home_away_diff
from app2.validators.dds.rules_suite.match_status_valid import validate_match_status_valid
from app2.validators.dds.rules_suite.standings_points_consistency import validate_standings_points_consistency
from app2.validators.dds.rules_suite.season_round_robin import validate_season_round_robin


VALIDATOR_REGISTRY = {
    ("STG", "api_http_status_ok"): validate_api_http_status_ok,
    ("STG", "api_payload_shape_ok"): validate_api_payload_shape_ok,
    ("STG", "areas_schema"): validate_areas_schema,
    ("STG", "competitions_schema"): validate_competitions_schema,
    ("STG", "teams_schema"): validate_teams_schema,
    ("STG", "scorers_schema"): validate_scorers_schema,
    ("STG", "matches_schema"): validate_matches_schema,
    ("STG", "standings_schema"): validate_standings_schema,
    ("STG", "areas_completeness"): validate_areas_completeness,
    ("STG", "competitions_completeness"): validate_competitions_completeness,
    ("STG", "teams_completeness"): validate_teams_completeness,
    ("STG", "scorers_completeness"): validate_scorers_completeness,
    ("STG", "matches_completeness"): validate_matches_completeness,
    ("STG", "standings_completeness"): validate_standings_completeness,
    ("STG", "areas_uniqueness"): validate_areas_uniqueness,
    ("STG", "competitions_uniqueness"): validate_competitions_uniqueness,
    ("STG", "teams_uniqueness"): validate_teams_uniqueness,
    ("STG", "scorers_uniqueness"): validate_scorers_uniqueness,
    ("STG", "matches_uniqueness"): validate_matches_uniqueness,
    ("STG", "standings_uniqueness"): validate_standings_uniqueness,
    ("STG", "competitions_consistency"): validate_competitions_consistency,
    ("STG", "matches_consistency"): validate_matches_consistency,
    ("STG", "standings_consistency"): validate_standings_consistency,
    ("STG", "teams_consistency"): validate_teams_consistency,
    ("STG", "scorers_consistency"): validate_scorers_consistency,
    ("DDS", "fact_match_fk"): validate_fact_match_fk,
    ("DDS", "fact_standing_fk"): validate_fact_standing_fk,
    ("DDS", "dim_competition_area_fk"): validate_dim_competition_area_fk,
    ("DDS", "competitions_source_completeness"): validate_competitions_source_completeness,
    ("DDS", "teams_source_completeness"): validate_teams_source_completeness,
    ("DDS", "matches_source_completeness"): validate_matches_source_completeness,
    ("DDS", "standings_source_completeness"): validate_standings_source_completeness,
    ("DDS", "competitions_source_exclusivity"): validate_competitions_source_exclusivity,
    ("DDS", "teams_source_exclusivity"): validate_teams_source_exclusivity,
    ("DDS", "matches_source_exclusivity"): validate_matches_source_exclusivity,
    ("DDS", "standings_source_exclusivity"): validate_standings_source_exclusivity,
    ("DDS", "match_home_away_diff"): validate_match_home_away_diff,
    ("DDS", "match_status_valid"): validate_match_status_valid,
    ("DDS", "standings_points_consistency"): validate_standings_points_consistency,
    ("DDS", "season_round_robin"): validate_season_round_robin,
}


def _load_layer_config(layer: str) -> dict:
    cfg = load_config(layer)
    layers_cfg = cfg.get("layers", {}) if isinstance(cfg, dict) else {}
    layer_cfg = layers_cfg.get(layer, {}) if isinstance(layers_cfg, dict) else {}
    return layer_cfg.get("validations", {}) if isinstance(layer_cfg, dict) else {}


def _extract_rows_failed(infos: list[str]) -> int | None:
    for info in infos:
        match = re.search(r"=\s*(\d+)\s*$", info)
        if match:
            return int(match.group(1))
    return None


def run_validation(
    engine,
    layer: str,
    dag_id: str,
    run_id: str,
    validator_name: str,
    payload: Any,
    parent_run_id: str,
    validation_run_id: int | None = None,
):
    validations_cfg = _load_layer_config(layer)
    cfg = validations_cfg.get(validator_name, {}) if isinstance(validations_cfg, dict) else {}
    if not cfg or not cfg.get("enabled", True):
        return
    severity = str(cfg.get("severity", "error")).lower()

    validator = VALIDATOR_REGISTRY.get((layer, validator_name))
    if validator is None:
        return

    rule_type = cfg.get("type") if isinstance(cfg, dict) else None
    entity_name = f"{layer}_validation_{validator_name}"
    start_dt = datetime.now()
    audit_log(engine, dag_id=dag_id, run_id=run_id, layer=layer, entity_name=entity_name, status="STARTED", started_at=start_dt)

    started = time.time()
    try:
        result: ValidationResult = validator(payload)
    except Exception as exc:
        duration_ms = int((time.time() - started) * 1000)
        if validation_run_id is not None:
            log_validation_check(
                engine,
                validation_run_id=validation_run_id,
                check_name=validator_name,
                rule_type=rule_type,
                etl_stage=layer,
                status="ERROR",
                severity=severity,
                started_at=start_dt,
                finished_at=datetime.now(),
                duration_ms=duration_ms,
                message=str(exc),
                details_json={"exception": str(exc)},
            )
        raise

    duration_ms = int((time.time() - started) * 1000)
    result.duration_ms = duration_ms

    infos = list(result.infos)
    infos.append(f"Duration_ms: {duration_ms}")
    try:
        infos.append(f"Payload_size_bytes: {len(json.dumps(payload or {}))}")
    except Exception:
        infos.append("Payload_size_bytes: n/a")
    info_text = "\n".join(infos)
    if info_text:
        audit_log(engine, dag_id=dag_id, run_id=run_id, layer=layer, entity_name=entity_name, status="INFO", message=info_text)

    if result.warnings:
        warning_text = "\n".join(result.warnings)
        audit_log(engine, dag_id=dag_id, run_id=run_id, layer=layer, entity_name=entity_name, status="WARNING", message=warning_text)

    if result.errors:
        error_text = "\n".join(result.errors)
        if severity == "warning":
            audit_log(engine, dag_id=dag_id, run_id=run_id, layer=layer, entity_name=entity_name, status="WARNING", message=error_text)
        else:
            audit_log(engine, dag_id=dag_id, run_id=run_id, layer=layer, entity_name=entity_name, status="ERROR", message=error_text)
            log_batch_status(
                engine,
                dag_id=dag_id,
                run_id=run_id,
                parent_run_id=parent_run_id,
                layer=layer,
                status="FAILED",
                error_message=f"{entity_name}: {result.errors[0]}",
            )
            audit_log(engine, dag_id=dag_id, run_id=run_id, layer=layer, entity_name=entity_name, status="ENDED", started_at=start_dt, finished_at=datetime.now(), message=error_text)
            if validation_run_id is not None:
                log_validation_check(
                    engine,
                    validation_run_id=validation_run_id,
                    check_name=validator_name,
                    rule_type=rule_type,
                    etl_stage=layer,
                    status="FAIL",
                    severity=severity,
                    started_at=start_dt,
                    finished_at=datetime.now(),
                    duration_ms=duration_ms,
                    rows_failed=_extract_rows_failed(infos),
                    message=result.errors[0],
                    details_json={"infos": infos, "warnings": result.warnings, "errors": result.errors},
                )
            raise ValueError(f"Validation {entity_name} failed: {result.errors[0]}")

    if validation_run_id is not None:
        status = "WARN" if result.errors and severity == "warning" else "PASS"
        if result.status == "ERROR":
            status = "ERROR"
        log_validation_check(
            engine,
            validation_run_id=validation_run_id,
            check_name=validator_name,
            rule_type=rule_type,
            etl_stage=layer,
            status=status,
            severity=severity,
            started_at=start_dt,
            finished_at=datetime.now(),
            duration_ms=duration_ms,
            rows_failed=_extract_rows_failed(infos) if status != "PASS" else None,
            message=result.warnings[0] if result.warnings else None,
            details_json={"infos": infos, "warnings": result.warnings, "errors": result.errors},
        )

    audit_log(engine, dag_id=dag_id, run_id=run_id, layer=layer, entity_name=entity_name, status="ENDED", started_at=start_dt, finished_at=datetime.now())
    return result
