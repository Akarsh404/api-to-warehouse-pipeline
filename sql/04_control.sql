CREATE TABLE IF NOT EXISTS rejected_records (
    run_id VARCHAR NOT NULL,
    entity VARCHAR NOT NULL,
    reason VARCHAR NOT NULL,
    payload JSON NOT NULL
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id VARCHAR PRIMARY KEY,
    started_at VARCHAR NOT NULL,
    ended_at VARCHAR,
    status VARCHAR NOT NULL,
    rows_fetched INTEGER DEFAULT 0,
    rows_loaded INTEGER DEFAULT 0,
    rows_rejected INTEGER DEFAULT 0,
    duplicates_removed INTEGER DEFAULT 0,
    retries INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS watermarks (
    endpoint VARCHAR PRIMARY KEY,
    last_updated_at VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS run_summaries (
    run_id VARCHAR PRIMARY KEY,
    summary VARCHAR NOT NULL,
    provider VARCHAR NOT NULL,
    model VARCHAR NOT NULL,
    prompt_version VARCHAR NOT NULL,
    validated BOOLEAN NOT NULL
);
