import logging
import os
from pathlib import Path
import yaml
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from app2.db.audit import audit_log
from app2.db.batch import log_batch_status          

logger = logging.getLogger(__name__)

MUTATION_CONFIG_PATH = Path(__file__).resolve().parent / "configs" / "dds_mutations.yml"
_DDS_CHECK_CONSTRAINTS = [
    ("dds.fact_match", "ck_fact_match_home_away_valid"),
    ("dds.fact_match", "ck_fact_match_matchday_range"),
    ("dds.fact_match", "ck_fact_match_utc_date_not_null"),
    ("dds.dim_season", "ck_dim_season_dates_not_null"),
]

_DDS_CHECK_DEFINITIONS = [
    ("dds.fact_match", "ck_fact_match_home_away_valid",
     "home_team_id IS NOT NULL AND away_team_id IS NOT NULL AND home_team_id <> away_team_id"),
    ("dds.fact_match", "ck_fact_match_matchday_range",
     "matchday IS NULL OR (matchday >= 0 AND matchday <= 60)"),
    ("dds.fact_match", "ck_fact_match_utc_date_not_null",
     "utc_date IS NOT NULL"),
    ("dds.dim_season", "ck_dim_season_dates_not_null",
     "start_date IS NOT NULL AND end_date IS NOT NULL"),
]


def _drop_check_constraints(conn) -> None:
    for table, constraint in _DDS_CHECK_CONSTRAINTS:
        conn.execute(text(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {constraint}"))


def _restore_check_constraints_not_valid(conn) -> None:
    for table, constraint, check_clause in _DDS_CHECK_DEFINITIONS:
        conn.execute(
            text(f"ALTER TABLE {table} ADD CONSTRAINT {constraint} CHECK ({check_clause}) NOT VALID")
        )


def load_dds_mutation_config():
    override = os.environ.get("APP2_DDS_MUTATIONS_CONFIG")
    if override:
        p = Path(override)
        if not p.is_absolute():
            p = Path(__file__).resolve().parents[2] / p
        config_path = p
    else:
        config_path = MUTATION_CONFIG_PATH
    if not config_path.exists():
        return {}
    with config_path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def mutate_dds(engine, dag_id: str, run_id: str, conn=None):
    cfg = load_dds_mutation_config()              
    layer_cfg = cfg.get("layers", {}).get("DDS", {}) if isinstance(cfg, dict) else {}
    mutations = layer_cfg.get("mutations", {}) if isinstance(layer_cfg, dict) else {}
    if not mutations:
        return False

    baseline_stg = cfg.get("experiment", {}).get("baseline", {}).get("stg_run_id") if isinstance(cfg, dict) else None

    needs_constraint_drop = any(
        isinstance(mutations.get(k), dict) and mutations[k].get("enabled")
        for k in ("fact_match", "fact_standing", "dim_competition", "season_dates_missing")
    )

    performed = []

    def _apply(exec_conn):
        if needs_constraint_drop:
            _drop_check_constraints(exec_conn)
            logger.info("Dropped DDS CHECK constraints for mutations (run_id=%s)", run_id)

        try:
            comp_id = season_id = team_id = None
            try:
                comp_id = exec_conn.execute(
                    text(
                        """
                        SELECT competition_id
                        FROM dds.fact_match
                        WHERE run_id = :run_id
                          AND competition_id IS NOT NULL
                        GROUP BY competition_id
                        ORDER BY COUNT(*) DESC
                        LIMIT 1
                        """
                    ),
                    {"run_id": run_id},
                ).scalar()

                if comp_id:
                    row = exec_conn.execute(
                        text(
                            """
                            SELECT season_id,
                                   COALESCE(home_team_id, away_team_id) AS team_id
                            FROM dds.fact_match
                            WHERE run_id = :run_id
                              AND competition_id = :comp_id
                              AND season_id IS NOT NULL
                              AND (home_team_id IS NOT NULL OR away_team_id IS NOT NULL)
                            ORDER BY match_id
                            LIMIT 1
                            """
                        ),
                        {"run_id": run_id, "comp_id": comp_id},
                    ).mappings().first()
                    if row:
                        season_id = row.get("season_id")
                        team_id = row.get("team_id")

                if not comp_id:
                    comp_id = exec_conn.execute(
                        text("SELECT min(competition_id) FROM dds.dim_competition WHERE run_id = :run_id"),
                        {"run_id": run_id},
                    ).scalar()
                if not season_id:
                    season_id = exec_conn.execute(
                        text("SELECT min(season_id) FROM dds.dim_season WHERE run_id = :run_id"),
                        {"run_id": run_id},
                    ).scalar()
                if not team_id:
                    team_id = exec_conn.execute(
                        text("SELECT min(team_id) FROM dds.dim_team WHERE run_id = :run_id"),
                        {"run_id": run_id},
                    ).scalar()
            except Exception:
                comp_id = season_id = team_id = None

            if mutations.get("fact_match", {}).get("enabled") and comp_id and season_id and team_id:
                try:
                    exec_conn.execute(
                        text(
                            """
                            INSERT INTO dds.fact_match (run_id, match_id, competition_id, season_id, utc_date, status, stage, matchday, home_team_id, away_team_id)
                            VALUES (:run_id, 99999901, :comp_id, :season_id, now(), 'MUTATED', 'MUTATED', 0, :team_id, :team_id)
                            ON CONFLICT (run_id, match_id) DO NOTHING
                            """
                        ),
                        {"run_id": run_id, "comp_id": comp_id, "season_id": season_id, "team_id": team_id},
                    )
                    exec_conn.execute(
                        text(
                            """
                            UPDATE dds.fact_match
                            SET matchday = 999,
                                home_team_id = NULL,
                                away_team_id = NULL
                            WHERE run_id = :run_id AND match_id = 99999901
                            """
                        ),
                        {"run_id": run_id},
                    )
                    performed.append("Inserted mutated fact_match with missing team ids and out-of-range matchday")
                except IntegrityError as ie:
                    performed.append(f"Skipped fact_match mutation (constraint): {ie}")
            if mutations.get("fact_standing", {}).get("enabled") and comp_id and season_id and team_id:
                try:
                    exec_conn.execute(
                        text(
                            """
                            INSERT INTO dds.fact_standing (run_id, season_id, competition_id, team_id, standing_type, stage, position, played_games, won, draw, lost, goals_for, goals_against, goal_difference, points, form)
                            VALUES (:run_id, :season_id, :comp_id, :team_id, 'MUTATED', 'MUTATED', 0,0,0,0,0,0,0,0,0,NULL)
                            ON CONFLICT (run_id, season_id, competition_id, team_id, standing_type) DO NOTHING
                            """
                        ),
                        {"run_id": run_id, "season_id": season_id, "comp_id": comp_id, "team_id": team_id},
                    )
                    performed.append("Inserted mutated fact_standing with zero stats")
                except IntegrityError as ie:
                    performed.append(f"Skipped fact_standing mutation (constraint): {ie}")
            if mutations.get("dim_competition", {}).get("enabled") and comp_id:
                try:
                    exec_conn.execute(
                        text(
                            """
                            UPDATE dds.dim_competition
                            SET name = 'MUTATED_COMP',
                                code = COALESCE(code, 'MUT'),
                                type = COALESCE(type, 'MUTATED'),
                                plan = COALESCE(plan, 'MUTATED')
                            WHERE run_id = :run_id AND competition_id = :comp_id
                            """
                        ),
                        {"run_id": run_id, "comp_id": comp_id},
                    )
                    performed.append(f"Updated dim_competition name for competition_id={comp_id}")
                except IntegrityError as ie:
                    performed.append(f"Skipped dim_competition mutation (constraint): {ie}")
            if mutations.get("season_dates_missing", {}).get("enabled"):
                try:
                    exec_conn.execute(
                        text(
                            """
                            UPDATE dds.dim_season
                            SET start_date = NULL,
                                end_date = NULL
                            WHERE run_id = :run_id
                            """
                        ),
                        {"run_id": run_id},
                    )
                    exec_conn.execute(
                        text(
                            """
                            UPDATE dds.fact_match
                            SET utc_date = NULL
                            WHERE run_id = :run_id
                            """
                        ),
                        {"run_id": run_id},
                    )
                    performed.append("Nullified dim_season dates and fact_match utc_date for missing date checks")
                except IntegrityError as ie:
                    performed.append(f"Skipped season_dates_missing mutation (constraint): {ie}")
        finally:
            if needs_constraint_drop:
                _restore_check_constraints_not_valid(exec_conn)
                logger.info("Restored DDS CHECK constraints as NOT VALID (run_id=%s)", run_id)

    if conn is not None:
        _apply(conn)
    else:
        with engine.begin() as temp_conn:
            _apply(temp_conn)

    if performed:
        if baseline_stg:
            log_batch_status(
                engine,
                dag_id=dag_id,
                run_id=run_id,
                parent_run_id=baseline_stg,
                layer="DDS",
                status="SUCCESS",
            )
        audit_log(
            engine,
            dag_id=dag_id,
            run_id=run_id,
            layer="DDS",
            entity_name="DDS_mutation",
            status="MUTATED",
            message="; ".join(performed),
        )
    return bool(performed)