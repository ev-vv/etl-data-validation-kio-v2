from sqlalchemy import text
from sqlalchemy.engine import Engine, Connection

from app2.validators.models import ValidationResult


def validate_season_round_robin(payload) -> ValidationResult:
    engine: Engine = payload.get("engine")
    external_conn: Connection | None = payload.get("conn")
    dds_run_id = payload.get("run_id")

    errors = []
    warnings = []
    infos = []
    offending = 0

    def _execute(conn: Connection):
        nonlocal offending
        offending = conn.execute(
            text(
                """
                WITH team_matches AS (
                    SELECT fm.competition_id,
                           fm.season_id,
                           fm.home_team_id AS team_id,
                           COUNT(*) AS home_matches,
                           0 AS away_matches
                    FROM dds.fact_match fm
                    JOIN dds.dim_season ds ON ds.run_id = fm.run_id AND ds.season_id = fm.season_id
                    WHERE fm.run_id = :run_id
                      AND ds.start_date < '2025-01-01'
                    GROUP BY fm.competition_id, fm.season_id, fm.home_team_id

                    UNION ALL

                    SELECT fm.competition_id,
                           fm.season_id,
                           fm.away_team_id AS team_id,
                           0 AS home_matches,
                           COUNT(*) AS away_matches
                    FROM dds.fact_match fm
                    JOIN dds.dim_season ds ON ds.run_id = fm.run_id AND ds.season_id = fm.season_id
                    WHERE fm.run_id = :run_id
                      AND ds.start_date < '2025-01-01'
                    GROUP BY fm.competition_id, fm.season_id, fm.away_team_id
                ),
                agg AS (
                    SELECT competition_id,
                           season_id,
                           team_id,
                           SUM(home_matches) AS home_matches,
                           SUM(away_matches) AS away_matches
                    FROM team_matches
                    GROUP BY competition_id, season_id, team_id
                )
                SELECT count(*) FROM agg
                WHERE home_matches <> away_matches
                """
            ),
            {"run_id": dds_run_id},
        ).scalar_one()

    if external_conn is not None:
        _execute(external_conn)
    elif engine is not None:
        with engine.begin() as conn:
            _execute(conn)

    if offending:
        warnings.append(f"Teams with unequal home/away matches (seasons before 2025): {offending}")
    infos.append(f"Round_robin_offending={offending}")
    status = "ERROR" if errors else ("WARNING" if warnings else "INFO")
    return ValidationResult(status=status, errors=errors, warnings=warnings, infos=infos, duration_ms=0)
