from __future__ import annotations

import re
from typing import Any

from sqlalchemy import Integer, JSON, Text, bindparam, text
from sqlalchemy.engine import Engine

from app2.db.audit import audit_log
from app2.db.batch import log_batch_status
from app2.mutators.stg_mutations import mutate_payload


_KIND_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    ("competitions", re.compile(r"^competitions$")),
    ("areas", re.compile(r"^areas$")),
    ("teams", re.compile(r"^competitions/\d+/teams")),
    ("scorers", re.compile(r"^competitions/\d+/scorers")),
    ("matches", re.compile(r"^competitions/\d+/matches")),
    ("standings", re.compile(r"^competitions/\d+/standings")),
]

_INSERT_RAW = (
    text(
        """
        INSERT INTO stg.raw_football_api (endpoint, request_params, http_status, response_json)
        VALUES (:endpoint, :request_params, :http_status, :response_json)
        """
    )
    .bindparams(
        bindparam("endpoint", type_=Text),
        bindparam("request_params", type_=JSON),
        bindparam("http_status", type_=Integer),
        bindparam("response_json", type_=JSON),
    )
)


def _infer_kind(endpoint: str) -> str | None:
    ep = (endpoint or "").strip()
    for kind, pat in _KIND_PATTERNS:
        if pat.search(ep):
            return kind
    return None


def copy_stg_run_with_mutations(
    *,
    engine: Engine,
    dag_id: str,
    source_run_id: str,
    target_run_id: str,
    parent_run_id: str,
    apply_mutations: bool,
) -> int:
    log_batch_status(engine, dag_id=dag_id, run_id=target_run_id, parent_run_id=parent_run_id, layer="STG", status="NEW")
    log_batch_status(engine, dag_id=dag_id, run_id=target_run_id, parent_run_id=parent_run_id, layer="STG", status="PROCESSING")
    audit_log(engine, dag_id=dag_id, run_id=target_run_id, layer="STG", entity_name="raw_football_api_copy", status="STARTED")

    with engine.begin() as conn:
        rows = conn.execute(
            text(
                """
                SELECT endpoint, http_status, response_json
                FROM stg.raw_football_api
                WHERE request_params ->> 'run_id' = :run_id
                  AND http_status BETWEEN 200 AND 299
                ORDER BY id
                """
            ),
            {"run_id": source_run_id},
        ).mappings().all()

        inserted = 0
        for r in rows:
            endpoint = str(r.get("endpoint") or "")
            status = int(r.get("http_status") or 0)
            payload: Any = r.get("response_json")
            kind = _infer_kind(endpoint)
            if kind and (not isinstance(payload, dict) or kind not in payload):
                continue
            if apply_mutations and kind:
                payload, _ = mutate_payload(engine, "STG", dag_id, target_run_id, kind, payload)

            conn.execute(
                _INSERT_RAW,
                {
                    "endpoint": endpoint,
                    "request_params": {"dag_id": dag_id, "run_id": target_run_id, "source_run_id": source_run_id},
                    "http_status": status,
                    "response_json": payload,
                },
            )
            inserted += 1

    audit_log(engine, dag_id=dag_id, run_id=target_run_id, layer="STG", entity_name="raw_football_api_copy", status="SUCCESS", rows_processed=inserted)
    return inserted
