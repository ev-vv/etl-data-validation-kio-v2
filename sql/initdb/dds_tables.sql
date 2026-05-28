-- DROP SCHEMA dds;

CREATE SCHEMA dds AUTHORIZATION "admin";

-- Drop table

-- DROP TABLE dds.dim_area;

CREATE TABLE dds.dim_area (
    run_id text NOT NULL,
    area_id int4 NOT NULL,
    "name" text NOT NULL,
    country_code text NULL,
    flag_url text NULL,
    parent_area_id int4 NULL,
    CONSTRAINT dim_area_pkey PRIMARY KEY (run_id, area_id),
    CONSTRAINT fk_dim_area_parent FOREIGN KEY (run_id, parent_area_id) REFERENCES dds.dim_area(run_id, area_id) ON DELETE SET NULL
);

-- Drop table

-- DROP TABLE dds.dim_competition;

CREATE TABLE dds.dim_competition (
    run_id text NOT NULL,
    competition_id int4 NOT NULL,
    area_id int4 NOT NULL,
    code text NULL,
    "name" text NOT NULL,
    "type" text NULL,
    plan text NULL,
    CONSTRAINT dim_competition_pkey PRIMARY KEY (run_id, competition_id),
    CONSTRAINT fk_dim_competition_area FOREIGN KEY (run_id, area_id) REFERENCES dds.dim_area(run_id, area_id) ON DELETE RESTRICT
);

-- Drop table

-- DROP TABLE dds.dim_season;

CREATE TABLE dds.dim_season (
    run_id text NOT NULL,
    season_id int4 NOT NULL,
    competition_id int4 NOT NULL,
    start_date date NULL,
    end_date date NULL,
    winner_team_id int4 NULL,
    CONSTRAINT dim_season_pkey PRIMARY KEY (run_id, season_id),
    CONSTRAINT fk_dim_season_competition FOREIGN KEY (run_id, competition_id) REFERENCES dds.dim_competition(run_id, competition_id) ON DELETE CASCADE
);

-- Drop table

-- DROP TABLE dds.dim_team;

CREATE TABLE dds.dim_team (
    run_id text NOT NULL,
    team_id int4 NOT NULL,
    area_id int4 NOT NULL,
    "name" text NOT NULL,
    short_name text NULL,
    tla text NULL,
    crest_url text NULL,
    venue text NULL,
    address text NULL,
    founded int4 NULL,
    website text NULL,
    club_colors text NULL,
    CONSTRAINT dim_team_pkey PRIMARY KEY (run_id, team_id),
    CONSTRAINT fk_team_area FOREIGN KEY (run_id, area_id) REFERENCES dds.dim_area(run_id, area_id)
);

-- Drop table

-- DROP TABLE dds.fact_match;

CREATE TABLE dds.fact_match (
    run_id text NOT NULL,
    match_id int4 NOT NULL,
    competition_id int4 NOT NULL,
    season_id int4 NOT NULL,
    utc_date timestamp NULL,
    status text NULL,
    stage text NULL,
    matchday int4 NULL,
    home_team_id int4 NULL,
    away_team_id int4 NULL,
    CONSTRAINT fact_match_pkey PRIMARY KEY (run_id, match_id),
    CONSTRAINT fact_match_away_team_id_fkey FOREIGN KEY (run_id, away_team_id) REFERENCES dds.dim_team(run_id, team_id),
    CONSTRAINT fact_match_competition_id_fkey FOREIGN KEY (run_id, competition_id) REFERENCES dds.dim_competition(run_id, competition_id),
    CONSTRAINT fact_match_home_team_id_fkey FOREIGN KEY (run_id, home_team_id) REFERENCES dds.dim_team(run_id, team_id),
    CONSTRAINT fact_match_season_id_fkey FOREIGN KEY (run_id, season_id) REFERENCES dds.dim_season(run_id, season_id)
);

-- Drop table

-- DROP TABLE dds.fact_match_score;

CREATE TABLE dds.fact_match_score (
    run_id text NOT NULL,
    match_id int4 NOT NULL,
    winner text NULL,
    duration text NULL,
    half_time_home int4 NULL,
    half_time_away int4 NULL,
    full_time_home int4 NULL,
    full_time_away int4 NULL,
    CONSTRAINT fact_match_score_pkey PRIMARY KEY (run_id, match_id),
    CONSTRAINT fact_match_score_match_id_fkey FOREIGN KEY (run_id, match_id) REFERENCES dds.fact_match(run_id, match_id)
);

-- Drop table

-- DROP TABLE dds.fact_standing;

CREATE TABLE dds.fact_standing (
    run_id text NOT NULL,
    season_id int4 NOT NULL,
    competition_id int4 NOT NULL,
    team_id int4 NOT NULL,
    standing_type text NOT NULL,
    stage text NULL,
    "position" int4 NULL,
    played_games int4 NULL,
    won int4 NULL,
    draw int4 NULL,
    lost int4 NULL,
    goals_for int4 NULL,
    goals_against int4 NULL,
    goal_difference int4 NULL,
    points int4 NULL,
    form text NULL,
    CONSTRAINT fact_standing_pkey PRIMARY KEY (run_id, season_id, competition_id, team_id, standing_type),
    CONSTRAINT fact_standing_competition_id_fkey FOREIGN KEY (run_id, competition_id) REFERENCES dds.dim_competition(run_id, competition_id),
    CONSTRAINT fact_standing_season_id_fkey FOREIGN KEY (run_id, season_id) REFERENCES dds.dim_season(run_id, season_id),
    CONSTRAINT fact_standing_team_id_fkey FOREIGN KEY (run_id, team_id) REFERENCES dds.dim_team(run_id, team_id)
);

ALTER TABLE dds.fact_match
    ADD CONSTRAINT ck_fact_match_home_away_valid
    CHECK (home_team_id IS NOT NULL AND away_team_id IS NOT NULL AND home_team_id <> away_team_id) NOT VALID;

ALTER TABLE dds.fact_match
    ADD CONSTRAINT ck_fact_match_matchday_range
    CHECK (matchday IS NULL OR (matchday >= 0 AND matchday <= 60)) NOT VALID;

ALTER TABLE dds.fact_match
    ADD CONSTRAINT ck_fact_match_utc_date_not_null
    CHECK (utc_date IS NOT NULL) NOT VALID;

ALTER TABLE dds.dim_season
    ADD CONSTRAINT ck_dim_season_dates_not_null
    CHECK (start_date IS NOT NULL AND end_date IS NOT NULL) NOT VALID;
