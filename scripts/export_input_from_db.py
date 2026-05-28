from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, text


ENDPOINT_PATTERN = re.compile(
    r"^competitions/(?P<competition_id>\d+)/(?P<entity>teams|scorers|matches|standings)\?season=(?P<season>2023|2024|2025)(?:&limit=\d+)?$"
)


@dataclass
class ExportRow:
    source_id: int
    endpoint: str
    request_params: dict[str, Any] | None
    http_status: int
    response_json: dict[str, Any]


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export static input JSON payloads from stg.raw_football_api into input/raw_football_api/."
    )
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--db", default="vkr_data")
    parser.add_argument("--user", default="admin")
    parser.add_argument("--password", default="pass")
    parser.add_argument("--run-id", default=None, help="Source STG run_id. If omitted, the latest manual__ run is used.")
    parser.add_argument("--output-dir", default="input/raw_football_api")
    parser.add_argument("--no-manifest", action="store_true", help="Do not write manifest.json.")
    return parser.parse_args()


def _slugify_endpoint(endpoint: str) -> str:
    slug = endpoint.replace("/", "__").replace("?", "__").replace("&", "__").replace("=", "_")
    slug = re.sub(r"[^a-zA-Z0-9_.-]+", "_", slug)
    return slug.strip("_")


def _resolve_source_run_id(engine, explicit_run_id: str | None) -> str:
    if explicit_run_id:
        return explicit_run_id

    query_manual = text(
        """
        SELECT request_params->>'run_id' AS run_id
        FROM stg.raw_football_api
        WHERE request_params ? 'run_id'
          AND request_params->>'run_id' LIKE 'manual__%'
        GROUP BY request_params->>'run_id'
        ORDER BY COUNT(*) DESC, MAX(load_dttm) DESC
        LIMIT 1
        """
    )
    query_any = text(
        """
        SELECT request_params->>'run_id' AS run_id
        FROM stg.raw_football_api
        WHERE request_params ? 'run_id'
        GROUP BY request_params->>'run_id'
        ORDER BY COUNT(*) DESC, MAX(load_dttm) DESC
        LIMIT 1
        """
    )
    with engine.begin() as conn:
        run_id = conn.execute(query_manual).scalar()
        if run_id:
            return str(run_id)
        run_id = conn.execute(query_any).scalar()
        if run_id:
            return str(run_id)
    raise RuntimeError("Could not resolve source run_id from stg.raw_football_api.")


def _load_rows(engine, run_id: str) -> list[ExportRow]:
    query = text(
        """
        SELECT
            id,
            endpoint,
            request_params,
            http_status,
            response_json
        FROM stg.raw_football_api
        WHERE request_params->>'run_id' = :run_id
          AND (
              endpoint IN ('competitions', 'areas')
              OR endpoint ~ '^competitions/[0-9]+/(teams|scorers|matches|standings)\\?season=(2023|2024|2025)(&limit=[0-9]+)?$'
          )
        ORDER BY id
        """
    )
    result: list[ExportRow] = []
    with engine.begin() as conn:
        rows = conn.execute(query, {"run_id": run_id}).mappings().all()
    for row in rows:
        response_json = row["response_json"]
        if not isinstance(response_json, dict):
            continue
        request_params = row["request_params"]
        if request_params is not None and not isinstance(request_params, dict):
            request_params = {}
        result.append(
            ExportRow(
                source_id=int(row["id"]),
                endpoint=str(row["endpoint"]),
                request_params=request_params or {},
                http_status=int(row["http_status"] or 0),
                response_json=response_json,
            )
        )
    return result


def _write_export(rows: list[ExportRow], source_run_id: str, output_root: Path, write_manifest: bool) -> Path:
    run_tag = re.sub(r"[^a-zA-Z0-9_.-]+", "_", source_run_id)
    export_dir = output_root / run_tag
    payload_dir = export_dir / "payloads"
    payload_dir.mkdir(parents=True, exist_ok=True)

    entity_counts: Counter[str] = Counter()
    seasons: set[int] = set()
    competition_ids: set[int] = set()
    payload_files: list[dict[str, Any]] = []

    for row in rows:
        match = ENDPOINT_PATTERN.match(row.endpoint)
        if match:
            entity_counts[match.group("entity")] += 1
            competition_ids.add(int(match.group("competition_id")))
            seasons.add(int(match.group("season")))
        elif row.endpoint in {"competitions", "areas"}:
            entity_counts[row.endpoint] += 1

        filename = f"{row.source_id:06d}_{_slugify_endpoint(row.endpoint)}.json"
        file_path = payload_dir / filename
        payload = {
            "source_id": row.source_id,
            "endpoint": row.endpoint,
            "http_status": row.http_status,
            "request_params": row.request_params or {},
            "response_json": row.response_json,
        }
        file_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        payload_files.append(
            {
                "file": f"payloads/{filename}",
                "endpoint": row.endpoint,
                "http_status": row.http_status,
            }
        )

    if write_manifest:
        manifest = {
            "source_run_id": source_run_id,
            "exported_at_utc": datetime.now(timezone.utc).isoformat(),
            "total_payloads": len(rows),
            "competition_ids": sorted(competition_ids),
            "seasons": sorted(seasons),
            "counts_by_entity": dict(sorted(entity_counts.items())),
            "files": payload_files,
        }
        (export_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return export_dir


def main() -> None:
    args = _parse_args()
    db_uri = f"postgresql+psycopg2://{args.user}:{args.password}@{args.host}:{args.port}/{args.db}"
    engine = create_engine(db_uri)

    source_run_id = _resolve_source_run_id(engine, args.run_id)
    rows = _load_rows(engine, source_run_id)
    if not rows:
        raise RuntimeError(f"No rows found for run_id={source_run_id}.")

    export_dir = _write_export(
        rows,
        source_run_id=source_run_id,
        output_root=Path(args.output_dir),
        write_manifest=not args.no_manifest,
    )
    print(f"Exported {len(rows)} payloads")
    print(f"Source run_id: {source_run_id}")
    print(f"Output: {export_dir}")


if __name__ == "__main__":
    main()
