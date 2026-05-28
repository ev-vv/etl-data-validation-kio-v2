
CREATE SCHEMA IF NOT EXISTS stg;

ALTER TABLE stg.raw_football_api ADD COLUMN IF NOT EXISTS batch_id TEXT;

CREATE INDEX IF NOT EXISTS idx_raw_batch_id ON stg.raw_football_api(batch_id);

CREATE INDEX IF NOT EXISTS idx_raw_run_batch ON stg.raw_football_api((request_params->>'run_id'), batch_id);