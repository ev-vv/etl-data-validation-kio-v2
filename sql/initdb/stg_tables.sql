CREATE SCHEMA IF NOT EXISTS stg;

CREATE TABLE stg.raw_football_api (
    id SERIAL PRIMARY KEY,
    endpoint TEXT,
    request_params JSONB,
    http_status INT,
    response_json JSONB,
    load_dttm TIMESTAMPTZ DEFAULT NOW()
);
