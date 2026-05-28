from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import bindparam, text
from sqlalchemy.engine import Engine


@dataclass(frozen=True)
class StageTarget:
    stage: str
    run_id: str
    parent_run_id: str
    stg_run_id: str | None
    dds_run_id: str | None
    kind: str


def _fetch_scalar(conn, stmt, params) -> str | None:
    return conn.execute(stmt, params).scalar()


def _dedupe(items: list[StageTarget]) -> list[StageTarget]:
    seen: set[tuple[str, str]] = set()
    out: list[StageTarget] = []
    for item in items:
        key = (item.stage, item.run_id)
        if key in seen:
            continue
        seen.add(key)
        out.append(item)
    return out


def discover_stage_targets(
    engine: Engine,
    *,
    baseline_stg_run_id: str,
    baseline_dds_run_id: str | None,
    stage: str,
    include_experiments: bool = True,
    only_unprocessed: bool = True,
    processed_layer: str = "E_GX",
) -> list[StageTarget]:
    stage = stage.strip().upper()
    if stage not in {"E", "T", "L"}:
        raise ValueError(f"Unsupported stage: {stage}")

    with engine.begin() as conn:
        baseline_stg = baseline_stg_run_id
        baseline_dds = baseline_dds_run_id
        if not baseline_dds:
            baseline_dds = _fetch_scalar(
                conn,
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
            )

        candidates: list[StageTarget] = []

        if stage == "E":
            candidates.append(
                StageTarget(
                    stage=stage,
                    run_id=baseline_stg,
                    parent_run_id=baseline_stg,
                    stg_run_id=baseline_stg,
                    dds_run_id=None,
                    kind="baseline",
                )
            )
            if include_experiments:
                exp_stg_rows = conn.execute(
                    text(
                        """
                        SELECT run_id
                        FROM tech.etl_batch_status
                        WHERE layer = 'STG'
                        AND status IN ('SUCCESS','FAILED')
                        AND parent_run_id = :baseline_stg
                        AND run_id LIKE 'exp_%'
                        ORDER BY created_at ASC
                        """
                    ),
                    {"baseline_stg": baseline_stg},
                ).fetchall()
                for (stg_run,) in exp_stg_rows:
                    candidates.append(
                        StageTarget(
                            stage=stage,
                            run_id=str(stg_run),
                            parent_run_id=baseline_stg,
                            stg_run_id=str(stg_run),
                            dds_run_id=None,
                            kind="experiment",
                        )
                    )
        else:
            if baseline_dds:
                candidates.append(
                    StageTarget(
                        stage=stage,
                        run_id=str(baseline_dds),
                        parent_run_id=baseline_stg,
                        stg_run_id=baseline_stg,
                        dds_run_id=str(baseline_dds),
                        kind="baseline",
                    )
                )

            if include_experiments:
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

                for parent_stg, dds_run in exp_dds_rows:
                    candidates.append(
                        StageTarget(
                            stage=stage,
                            run_id=str(dds_run),
                            parent_run_id=str(parent_stg),
                            stg_run_id=str(parent_stg),
                            dds_run_id=str(dds_run),
                            kind="experiment",
                        )
                    )

        candidates = _dedupe(candidates)

        if not only_unprocessed:
            return candidates

        run_ids = [c.run_id for c in candidates]
        if not run_ids:
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
            {"run_ids": run_ids, "processed_layer": processed_layer},
        ).fetchall()
        processed = {r[0] for r in processed_rows}
        return [c for c in candidates if c.run_id not in processed]


__all__ = ["StageTarget", "discover_stage_targets"]