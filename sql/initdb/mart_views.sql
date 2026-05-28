CREATE SCHEMA IF NOT EXISTS mart;

CREATE OR REPLACE VIEW mart.v_competition_season_kpi AS
WITH matches AS (
    SELECT
        fm.run_id,
        fm.match_id,
        fm.competition_id,
        fm.season_id,
        fm.utc_date,
        fm.status,
        fm.home_team_id,
        fm.away_team_id,
        fms.full_time_home,
        fms.full_time_away,
        fms.winner
    FROM dds.fact_match fm
    LEFT JOIN dds.fact_match_score fms ON fms.run_id = fm.run_id AND fms.match_id = fm.match_id
),
season_dates AS (
    SELECT
        run_id,
        competition_id,
        season_id,
        MIN(utc_date)::date AS start_date_inferred,
        MAX(utc_date)::date AS end_date_inferred
    FROM matches
    WHERE utc_date IS NOT NULL
    GROUP BY run_id, competition_id, season_id
),
teams AS (
    SELECT
        run_id,
        competition_id,
        season_id,
        COUNT(DISTINCT team_id) AS teams_distinct
    FROM (
        SELECT run_id, competition_id, season_id, home_team_id AS team_id
        FROM dds.fact_match
        WHERE home_team_id IS NOT NULL
        UNION
        SELECT run_id, competition_id, season_id, away_team_id AS team_id
        FROM dds.fact_match
        WHERE away_team_id IS NOT NULL
    ) t
    GROUP BY run_id, competition_id, season_id
),
outcomes AS (
    SELECT
        m.*,
        CASE
            WHEN m.full_time_home IS NOT NULL AND m.full_time_away IS NOT NULL AND m.full_time_home > m.full_time_away THEN 'HOME'
            WHEN m.full_time_home IS NOT NULL AND m.full_time_away IS NOT NULL AND m.full_time_home < m.full_time_away THEN 'AWAY'
            WHEN m.full_time_home IS NOT NULL AND m.full_time_away IS NOT NULL AND m.full_time_home = m.full_time_away THEN 'DRAW'
            WHEN m.winner = 'HOME_TEAM' THEN 'HOME'
            WHEN m.winner = 'AWAY_TEAM' THEN 'AWAY'
            WHEN m.winner = 'DRAW' THEN 'DRAW'
            ELSE 'UNKNOWN'
        END AS outcome
    FROM matches m
)
SELECT
    o.run_id,
    c.competition_id,
    c.name AS competition_name,
    s.season_id,
    COALESCE(s.start_date, sd.start_date_inferred) AS start_date,
    COALESCE(s.end_date, sd.end_date_inferred) AS end_date,
    COALESCE(
        EXTRACT(YEAR FROM s.start_date)::int,
        EXTRACT(YEAR FROM s.end_date)::int,
        EXTRACT(YEAR FROM sd.start_date_inferred)::int,
        EXTRACT(YEAR FROM sd.end_date_inferred)::int
    ) AS season_year,
    COUNT(*) AS matches_total,
    COUNT(*) FILTER (WHERE o.status = 'FINISHED') AS matches_finished,
    COALESCE(t.teams_distinct, 0) AS teams_distinct,
    ROUND(COUNT(*) FILTER (WHERE o.outcome = 'HOME')::numeric / NULLIF(COUNT(*) FILTER (WHERE o.outcome IN ('HOME','AWAY','DRAW')), 0), 4) AS home_win_rate,
    ROUND(COUNT(*) FILTER (WHERE o.outcome = 'DRAW')::numeric / NULLIF(COUNT(*) FILTER (WHERE o.outcome IN ('HOME','AWAY','DRAW')), 0), 4) AS draw_rate,
    ROUND(COUNT(*) FILTER (WHERE o.outcome = 'AWAY')::numeric / NULLIF(COUNT(*) FILTER (WHERE o.outcome IN ('HOME','AWAY','DRAW')), 0), 4) AS away_win_rate
FROM outcomes o
JOIN dds.dim_competition c ON c.run_id = o.run_id AND c.competition_id = o.competition_id
JOIN dds.dim_season s ON s.run_id = o.run_id AND s.season_id = o.season_id
LEFT JOIN season_dates sd ON sd.run_id = o.run_id AND sd.competition_id = o.competition_id AND sd.season_id = o.season_id
LEFT JOIN teams t ON t.run_id = o.run_id AND t.competition_id = o.competition_id AND t.season_id = o.season_id
GROUP BY
    o.run_id,
    c.competition_id,
    c.name,
    s.season_id,
    s.start_date,
    s.end_date,
    sd.start_date_inferred,
    sd.end_date_inferred,
    t.teams_distinct;

CREATE OR REPLACE VIEW mart.v_team_season_results AS
WITH finished AS (
    SELECT
        fm.run_id,
        fm.match_id,
        fm.competition_id,
        fm.season_id,
        fm.utc_date,
        fm.home_team_id,
        fm.away_team_id,
        fms.full_time_home,
        fms.full_time_away
    FROM dds.fact_match fm
    JOIN dds.fact_match_score fms ON fms.run_id = fm.run_id AND fms.match_id = fm.match_id
    WHERE fm.status = 'FINISHED'
      AND fms.full_time_home IS NOT NULL
      AND fms.full_time_away IS NOT NULL
),
season_dates AS (
    SELECT
        run_id,
        competition_id,
        season_id,
        MIN(utc_date)::date AS start_date_inferred,
        MAX(utc_date)::date AS end_date_inferred
    FROM finished
    WHERE utc_date IS NOT NULL
    GROUP BY run_id, competition_id, season_id
),
team_rows AS (
    SELECT
        run_id,
        season_id,
        competition_id,
        home_team_id AS team_id,
        full_time_home AS goals_for,
        full_time_away AS goals_against,
        CASE WHEN full_time_home > full_time_away THEN 1 ELSE 0 END AS win,
        CASE WHEN full_time_home = full_time_away THEN 1 ELSE 0 END AS draw,
        CASE WHEN full_time_home < full_time_away THEN 1 ELSE 0 END AS loss
    FROM finished
    UNION ALL
    SELECT
        run_id,
        season_id,
        competition_id,
        away_team_id AS team_id,
        full_time_away AS goals_for,
        full_time_home AS goals_against,
        CASE WHEN full_time_away > full_time_home THEN 1 ELSE 0 END AS win,
        CASE WHEN full_time_away = full_time_home THEN 1 ELSE 0 END AS draw,
        CASE WHEN full_time_away < full_time_home THEN 1 ELSE 0 END AS loss
    FROM finished
)
SELECT
    tr.run_id,
    tr.competition_id,
    c.name AS competition_name,
    tr.season_id,
    COALESCE(s.start_date, sd.start_date_inferred) AS start_date,
    COALESCE(s.end_date, sd.end_date_inferred) AS end_date,
    COALESCE(
        EXTRACT(YEAR FROM s.start_date)::int,
        EXTRACT(YEAR FROM s.end_date)::int,
        EXTRACT(YEAR FROM sd.start_date_inferred)::int,
        EXTRACT(YEAR FROM sd.end_date_inferred)::int
    ) AS season_year,
    tr.team_id,
    t.name AS team_name,
    COUNT(*) AS matches_played,
    SUM(tr.win) AS wins,
    SUM(tr.draw) AS draws,
    SUM(tr.loss) AS losses,
    SUM(tr.goals_for) AS goals_for,
    SUM(tr.goals_against) AS goals_against,
    SUM(tr.goals_for) - SUM(tr.goals_against) AS goal_difference,
    (SUM(tr.win) * 3 + SUM(tr.draw)) AS points_calc
FROM team_rows tr
JOIN dds.dim_competition c ON c.run_id = tr.run_id AND c.competition_id = tr.competition_id
JOIN dds.dim_team t ON t.run_id = tr.run_id AND t.team_id = tr.team_id
JOIN dds.dim_season s ON s.run_id = tr.run_id AND s.season_id = tr.season_id
LEFT JOIN season_dates sd ON sd.run_id = tr.run_id AND sd.competition_id = tr.competition_id AND sd.season_id = tr.season_id
GROUP BY
    tr.run_id,
    tr.competition_id,
    c.name,
    tr.season_id,
    s.start_date,
    s.end_date,
    sd.start_date_inferred,
    sd.end_date_inferred,
    tr.team_id,
    t.name;
