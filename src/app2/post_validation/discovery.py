from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from sqlalchemy import bindparam, text
from sqlalchemy.engine import Engine


@dataclass(frozen=True)
class PostValidationTarget:
    baseline_stg_run_id: str
    stg_run_id: str
    dds_run_id: str
    kind: str


def _fetch_scalar(conn, stmt, params) -> str | None:
    return conn.execute(stmt, params).scalar()


def discover_post_validation_targets(
    engine: Engine,
    *,
    only_unprocessed: bool = True,
    baseline_stg_run_id: str | None = None,
    processed_layer: str = "POST",
) -> list[PostValidationTarget]:
    with engine.begin() as conn:
        if baseline_stg_run_id:
            baseline_stg = baseline_stg_run_id
        else:
            baseline_stg = _fetch_scalar(
                conn,
                text(
                    """
                    SELECT run_id
                    FROM tech.etl_batch_status
                    WHERE layer = 'STG'
                      AND status = 'SUCCESS'
                      AND run_id = parent_run_id
                      AND run_id NOT LIKE 'exp_%'
                    ORDER BY created_at DESC
                    LIMIT 1
                    """
                ),
                {},
            )
            if not baseline_stg:
                return []

        baseline_dds = conn.execute(
            text(
                """
                SELECT run_id
                FROM tech.etl_batch_status
                WHERE layer = 'DDS'
                  AND status = 'SUCCESS'
                  AND parent_run_id = :stg_run_id
                  AND run_id NOT LIKE 'exp_%'
                ORDER BY created_at DESC
                LIMIT 1
                """
            ),
            {"stg_run_id": baseline_stg},
        ).scalar()

        candidates: list[PostValidationTarget] = []
        if baseline_dds:
            candidates.append(
                PostValidationTarget(
                    baseline_stg_run_id=baseline_stg,
                    stg_run_id=baseline_stg,
                    dds_run_id=str(baseline_dds),
                    kind="baseline",
                )
            )

        exp_stg_rows = conn.execute(
            text(
                """
                SELECT run_id
                FROM tech.etl_batch_status
                WHERE layer = 'STG'
                  AND status = 'SUCCESS'
                  AND parent_run_id = :baseline_stg
                  AND run_id LIKE 'exp_%'
                ORDER BY created_at ASC
                """
            ),
            {"baseline_stg": baseline_stg},
        ).fetchall()
        exp_stg_ids = [r[0] for r in exp_stg_rows]

        exp_dds_rows: list[tuple[str, str]] = []
        if exp_stg_ids:
            exp_dds_rows.extend(
                conn.execute(
                    text(
                        """
                        SELECT parent_run_id, run_id
                        FROM tech.etl_batch_status
                        WHERE layer = 'DDS'
                          AND status = 'SUCCESS'
                          AND parent_run_id IN :parents
                          AND run_id LIKE 'exp_%'
                        ORDER BY created_at ASC
                        """
                    ).bindparams(bindparam("parents", expanding=True)),
                    {"parents": exp_stg_ids},
                ).fetchall()
            )

        exp_dds_rows.extend(
            conn.execute(
                text(
                    """
                    SELECT parent_run_id, run_id
                    FROM tech.etl_batch_status
                    WHERE layer = 'DDS'
                      AND status = 'SUCCESS'
                      AND parent_run_id = :baseline_stg
                      AND run_id LIKE 'exp_%'
                    ORDER BY created_at ASC
                    """
                ),
                {"baseline_stg": baseline_stg},
            ).fetchall()
        )

        seen_dds: set[str] = set()
        for parent_stg, dds_run in exp_dds_rows:
            dds_run_id = str(dds_run)
            if dds_run_id in seen_dds:
                continue
            seen_dds.add(dds_run_id)
            candidates.append(
                PostValidationTarget(
                    baseline_stg_run_id=baseline_stg,
                    stg_run_id=str(parent_stg),
                    dds_run_id=dds_run_id,
                    kind="experiment",
                )
            )

        if not only_unprocessed:
            return candidates

        dds_ids = [c.dds_run_id for c in candidates]
        if not dds_ids:
            return []

        processed_rows = conn.execute(
            text(
                """
                SELECT run_id
                FROM tech.etl_batch_status
                WHERE layer = :processed_layer
                  AND status IN ('SUCCESS','PROCESSING')
                  AND run_id IN :run_ids
                """
            ).bindparams(bindparam("run_ids", expanding=True)),
            {"run_ids": dds_ids, "processed_layer": processed_layer},
        ).fetchall()
        processed = {r[0] for r in processed_rows}
        return [c for c in candidates if c.dds_run_id not in processed]
