import logging

from sqlalchemy import text

from app2.db.audit import audit_log
from app2.db.connection import get_engine
from app2.db.batch import claim_pending_dds_batches, log_batch_status
from app2.utils.timezone import set_moscow_timezone

set_moscow_timezone()
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S %z",
)

SQL_DIM_AREA = """
INSERT INTO dds.dim_area (run_id, area_id, name, country_code, flag_url, parent_area_id)
SELECT DISTINCT
    :dds_run_id                    AS run_id,
    (a ->> 'id')::int               AS area_id,
    a ->> 'name'                    AS name,
    a ->> 'countryCode'             AS country_code,
    a ->> 'flag'                    AS flag_url,
    (a ->> 'parentAreaId')::int     AS parent_area_id
FROM stg.raw_football_api s
CROSS JOIN LATERAL
    jsonb_array_elements(s.response_json -> 'areas') a
WHERE s.endpoint = 'areas'
  AND s.request_params ->> 'run_id' = :stg_run_id
ON CONFLICT (run_id, area_id) DO NOTHING;
"""

SQL_DIM_COMPETITION = """
INSERT INTO dds.dim_competition (run_id, competition_id, area_id, code, name, type, plan)
SELECT DISTINCT
    :dds_run_id,
    (c ->> 'id')::int,
    (c -> 'area' ->> 'id')::int,
    c ->> 'code',
    c ->> 'name',
    c ->> 'type',
    c ->> 'plan'
FROM stg.raw_football_api s
CROSS JOIN LATERAL jsonb_array_elements(s.response_json -> 'competitions') c
WHERE s.endpoint = 'competitions'
  AND s.request_params ->> 'run_id' = :stg_run_id
  AND (c -> 'area' ->> 'id') IS NOT NULL
ON CONFLICT (run_id, competition_id) DO NOTHING;
"""

SQL_DIM_TEAM = """
INSERT INTO dds.dim_team (run_id, team_id, area_id, name, short_name, tla, crest_url, venue, address, founded, website, club_colors)
SELECT DISTINCT
    :dds_run_id,
    (t ->> 'id')::int,
    (t -> 'area' ->> 'id')::int,
    t ->> 'name',
    t ->> 'shortName',
    t ->> 'tla',
    t ->> 'crest',
    t ->> 'venue',
    t ->> 'address',
    NULLIF(t ->> 'founded','')::int,
    t ->> 'website',
    t ->> 'clubColors'
FROM stg.raw_football_api s
CROSS JOIN LATERAL jsonb_array_elements(s.response_json -> 'teams') t
WHERE s.endpoint LIKE 'competitions/%/teams%'
  AND s.request_params ->> 'run_id' = :stg_run_id
  AND (t -> 'area' ->> 'id') IS NOT NULL
ON CONFLICT (run_id, team_id) DO NOTHING;
"""

SQL_DIM_SEASON = """
WITH seasons AS (
    SELECT DISTINCT
        (s.response_json -> 'season' ->> 'id')::int AS season_id,
        (s.response_json -> 'competition' ->> 'id')::int AS competition_id,
        (s.response_json -> 'season' ->> 'startDate')::date AS start_date,
        (s.response_json -> 'season' ->> 'endDate')::date AS end_date,
        NULLIF(s.response_json -> 'season' -> 'winner' ->> 'id','')::int AS winner_team_id
    FROM stg.raw_football_api s
    WHERE s.endpoint LIKE 'competitions/%/standings%'
      AND s.request_params ->> 'run_id' = :stg_run_id
      AND (s.response_json -> 'season' ->> 'id') IS NOT NULL

    UNION

    SELECT DISTINCT
        (m -> 'season' ->> 'id')::int AS season_id,
        NULLIF(m -> 'competition' ->> 'id','')::int AS competition_id,
        (m -> 'season' ->> 'startDate')::date AS start_date,
        (m -> 'season' ->> 'endDate')::date AS end_date,
        NULLIF(m -> 'season' -> 'winner' ->> 'id','')::int AS winner_team_id
    FROM stg.raw_football_api s
    CROSS JOIN LATERAL jsonb_array_elements(s.response_json -> 'matches') m
    WHERE s.endpoint LIKE 'competitions/%/matches%'
      AND s.request_params ->> 'run_id' = :stg_run_id
      AND (m -> 'season' ->> 'id') IS NOT NULL
)
INSERT INTO dds.dim_season (run_id, season_id, competition_id, start_date, end_date, winner_team_id)
SELECT DISTINCT :dds_run_id, season_id, competition_id, start_date, end_date, winner_team_id
FROM seasons
WHERE competition_id IS NOT NULL
ON CONFLICT (run_id, season_id) DO NOTHING;
"""

SQL_FACT_MATCH = """
INSERT INTO dds.fact_match (run_id, match_id, competition_id, season_id, utc_date, status, stage, matchday, home_team_id, away_team_id)
SELECT DISTINCT
    :dds_run_id,
    (m ->> 'id')::int,
    (m -> 'competition' ->> 'id')::int,
    (m -> 'season' ->> 'id')::int,
    (m ->> 'utcDate')::timestamp,
    m ->> 'status',
    m ->> 'stage',
    NULLIF(m ->> 'matchday','')::int,
    NULLIF(m -> 'homeTeam' ->> 'id','')::int,
    NULLIF(m -> 'awayTeam' ->> 'id','')::int
FROM stg.raw_football_api s
CROSS JOIN LATERAL jsonb_array_elements(s.response_json -> 'matches') m
WHERE s.endpoint LIKE 'competitions/%/matches%'
  AND s.request_params ->> 'run_id' = :stg_run_id
  AND (m ->> 'id') IS NOT NULL
ON CONFLICT (run_id, match_id) DO NOTHING;
"""

SQL_FACT_MATCH_SCORE = """
INSERT INTO dds.fact_match_score (run_id, match_id, winner, duration, half_time_home, half_time_away, full_time_home, full_time_away)
SELECT DISTINCT
    :dds_run_id,
    (m ->> 'id')::int,
    m -> 'score' ->> 'winner',
    m -> 'score' ->> 'duration',
    NULLIF(m -> 'score' -> 'halfTime' ->> 'home','')::int,
    NULLIF(m -> 'score' -> 'halfTime' ->> 'away','')::int,
    NULLIF(m -> 'score' -> 'fullTime' ->> 'home','')::int,
    NULLIF(m -> 'score' -> 'fullTime' ->> 'away','')::int
FROM stg.raw_football_api s
CROSS JOIN LATERAL jsonb_array_elements(s.response_json -> 'matches') m
WHERE s.endpoint LIKE 'competitions/%/matches%'
  AND s.request_params ->> 'run_id' = :stg_run_id
  AND (m ->> 'id') IS NOT NULL
ON CONFLICT (run_id, match_id) DO NOTHING;
"""

SQL_FACT_STANDING = """
INSERT INTO dds.fact_standing (run_id, season_id, competition_id, team_id, standing_type, stage, position, played_games, won, draw, lost, goals_for, goals_against, goal_difference, points, form)
SELECT DISTINCT
    :dds_run_id,
    (s.response_json -> 'season' ->> 'id')::int,
    (s.response_json -> 'competition' ->> 'id')::int,
    (tbl -> 'team' ->> 'id')::int,
    st ->> 'type',
    st ->> 'stage',
    NULLIF(tbl ->> 'position','')::int,
    NULLIF(tbl ->> 'playedGames','')::int,
    NULLIF(tbl ->> 'won','')::int,
    NULLIF(tbl ->> 'draw','')::int,
    NULLIF(tbl ->> 'lost','')::int,
    NULLIF(tbl ->> 'goalsFor','')::int,
    NULLIF(tbl ->> 'goalsAgainst','')::int,
    NULLIF(tbl ->> 'goalDifference','')::int,
    NULLIF(tbl ->> 'points','')::int,
    tbl ->> 'form'
FROM stg.raw_football_api s
CROSS JOIN LATERAL jsonb_array_elements(s.response_json -> 'standings') st
CROSS JOIN LATERAL jsonb_array_elements(st -> 'table') tbl
WHERE s.endpoint LIKE 'competitions/%/standings%'
  AND s.request_params ->> 'run_id' = :stg_run_id
  AND (s.response_json -> 'season' ->> 'id') IS NOT NULL
ON CONFLICT (run_id, season_id, competition_id, team_id, standing_type) DO NOTHING;
"""


def run_dds_load(conn, dag_id: str, dds_run_id: str, parent_run_id: str):
    audit_log(conn.engine, dag_id=dag_id, run_id=dds_run_id, layer="DDS", entity_name="ALL", status="STARTED")
    steps = [
        ("dim_area", SQL_DIM_AREA),
        ("dim_competition", SQL_DIM_COMPETITION),
        ("dim_team", SQL_DIM_TEAM),
        ("dim_season", SQL_DIM_SEASON),
        ("fact_match", SQL_FACT_MATCH),
        ("fact_match_score", SQL_FACT_MATCH_SCORE),
        ("fact_standing", SQL_FACT_STANDING),
    ]
    try:
        for name, sql in steps:
            logger.info("Loading %s", name)
            result = conn.execute(text(sql), {"stg_run_id": parent_run_id, "dds_run_id": dds_run_id})
            rows = result.rowcount
            audit_log(conn.engine, dag_id=dag_id, run_id=dds_run_id, layer="DDS", entity_name=name, status="SUCCESS", rows_processed=rows)
        audit_log(conn.engine, dag_id=dag_id, run_id=dds_run_id, layer="DDS", entity_name="ALL", status="SUCCESS")
    except Exception as e:
        logger.exception("DDS load failed on %s", name)
        audit_log(conn.engine, dag_id=dag_id, run_id=dds_run_id, layer="DDS", entity_name="ALL", status="FAILED", message=str(e))
        raise


def run_dds_from_pending(dag_id: str, run_id: str):
    engine = get_engine()
    pending = claim_pending_dds_batches(engine, dag_id, dds_run_id=run_id)
    if not pending:
        logger.info("No pending STG runs found for DDS load")
        return

    logger.info("Found %s pending STG runs: %s", len(pending), pending)
    for parent_run_id in pending:
        logger.info("Starting DDS load dds_run_id=%s stg_run_id=%s", run_id, parent_run_id)
        try:
            log_batch_status(engine, dag_id=dag_id, run_id=run_id, parent_run_id=parent_run_id, layer="DDS", status="PROCESSING")
            with engine.begin() as conn:
                run_dds_load(conn=conn, dag_id=dag_id, dds_run_id=run_id, parent_run_id=parent_run_id)
            log_batch_status(engine, dag_id=dag_id, run_id=run_id, parent_run_id=parent_run_id, layer="DDS", status="SUCCESS")
        except Exception as e:
            log_batch_status(engine, dag_id=dag_id, run_id=run_id, parent_run_id=parent_run_id, layer="DDS", status="FAILED", error_message=str(e))
            raise
