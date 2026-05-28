-- tech.etl_load_audit определение

-- Drop table

-- DROP TABLE tech.etl_load_audit;

CREATE SCHEMA IF NOT EXISTS tech;

CREATE TABLE tech.etl_load_audit (
	audit_id bigserial NOT NULL,
	dag_id text NOT NULL,
	run_id text NOT NULL,
	task_id text NULL,
	layer text NOT NULL,
	entity_name text NOT NULL,
	status text NOT NULL,
	started_at timestamp DEFAULT timezone('Europe/Moscow', now()) NOT NULL,
	finished_at timestamp NULL,
	rows_processed int4 NULL,
	message text NULL,
	CONSTRAINT etl_load_audit_pkey PRIMARY KEY (audit_id)
);

CREATE TABLE tech.etl_batch_status (
    batch_id bigserial PRIMARY KEY,
    dag_id text NOT NULL,
    run_id text NOT NULL,
    parent_run_id text NOT NULL,
    layer text NOT NULL,
    status text NOT NULL CHECK (status IN ('NEW', 'PROCESSING', 'SUCCESS', 'FAILED')),
    attempts int NOT NULL DEFAULT 0,
    last_updated_at timestamp NOT NULL DEFAULT timezone('Europe/Moscow', now()),
    created_at timestamp NOT NULL DEFAULT timezone('Europe/Moscow', now()),
    error_message text NULL
);

CREATE INDEX etl_batch_status_layer_status_idx ON tech.etl_batch_status (layer, status);
CREATE INDEX etl_batch_status_dag_id_idx ON tech.etl_batch_status (dag_id);
CREATE INDEX etl_batch_status_run_id_idx ON tech.etl_batch_status (run_id);
CREATE INDEX etl_batch_status_parent_run_id_idx ON tech.etl_batch_status (parent_run_id);
CREATE UNIQUE INDEX etl_batch_status_layer_parent_run_id_run_id_uk ON tech.etl_batch_status (layer, parent_run_id, run_id);

CREATE TABLE IF NOT EXISTS tech.validation_run (
    validation_run_id bigserial PRIMARY KEY,
    dag_id text NOT NULL,
    run_id text NOT NULL,
    parent_run_id text NOT NULL,
    layer text NOT NULL,
    tool text NOT NULL,
    suite text NULL,
    kind text NULL,
    status text NOT NULL CHECK (status IN ('NEW','PROCESSING','SUCCESS','FAILED')),
    started_at timestamp NOT NULL DEFAULT timezone('Europe/Moscow', now()),
    finished_at timestamp NULL,
    duration_ms int NULL,
    checks_total int NOT NULL DEFAULT 0,
    checks_failed int NOT NULL DEFAULT 0,
    rows_checked int NULL,
    rows_failed int NULL,
    report_path text NULL,
    config_hash text NULL,
    meta_json jsonb NULL
);

CREATE TABLE IF NOT EXISTS tech.validation_check_result (
    validation_check_id bigserial PRIMARY KEY,
    validation_run_id bigint NOT NULL REFERENCES tech.validation_run(validation_run_id) ON DELETE CASCADE,
    check_name text NOT NULL,
    rule_type text NULL,
    etl_stage text NULL,
    status text NOT NULL CHECK (status IN ('PASS','WARN','FAIL','ERROR','SKIP')),
    severity text NULL,
    started_at timestamp NOT NULL DEFAULT timezone('Europe/Moscow', now()),
    finished_at timestamp NULL,
    duration_ms int NULL,
    rows_failed int NULL,
    observed_value text NULL,
    expected_value text NULL,
    message text NULL,
    details_json jsonb NULL
);

CREATE INDEX IF NOT EXISTS validation_run_idx ON tech.validation_run (run_id, layer, tool);
CREATE INDEX IF NOT EXISTS validation_run_parent_idx ON tech.validation_run (parent_run_id);
CREATE INDEX IF NOT EXISTS validation_run_dag_idx ON tech.validation_run (dag_id);
CREATE INDEX IF NOT EXISTS validation_check_run_idx ON tech.validation_check_result (validation_run_id);
