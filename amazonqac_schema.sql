-- Independent historical keyword library. No application tables are changed.
CREATE SCHEMA IF NOT EXISTS amazonqac;
CREATE TABLE IF NOT EXISTS amazonqac.dataset (
    source_id smallint PRIMARY KEY,
    dataset_name text NOT NULL,
    revision text NOT NULL UNIQUE,
    source_url text NOT NULL,
    split text NOT NULL CHECK (split = 'train'),
    market text NOT NULL,
    period_start date NOT NULL,
    period_end date NOT NULL,
    popularity_definition text NOT NULL,
    import_status text NOT NULL DEFAULT 'in_progress',
    raw_records bigint,
    unique_keywords bigint,
    anomalous_records bigint,
    conflict_keywords bigint,
    started_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz
);
CREATE TABLE IF NOT EXISTS amazonqac.source_files (
    source_id smallint NOT NULL REFERENCES amazonqac.dataset,
    file_index smallint NOT NULL,
    source_path text NOT NULL,
    source_bytes bigint NOT NULL,
    source_lfs_sha256 text NOT NULL,
    raw_records bigint NOT NULL,
    row_groups integer NOT NULL,
    PRIMARY KEY(source_id, file_index)
);
-- The primary key is built after the initial COPY, before marking completion.
-- Exact text equality under C collation: no trimming, case folding or rewriting.
CREATE TABLE IF NOT EXISTS amazonqac.keywords (
    final_search_term text COLLATE "C" NOT NULL,
    popularity bigint NOT NULL CHECK (popularity >= 0),
    source_id smallint NOT NULL REFERENCES amazonqac.dataset,
    source_file smallint NOT NULL,
    source_row bigint NOT NULL,
    observations bigint NOT NULL,
    popularity_conflict boolean NOT NULL
);
CREATE TABLE IF NOT EXISTS amazonqac.popularity_conflicts (
    final_search_term text COLLATE "C" NOT NULL,
    popularity bigint NOT NULL,
    source_id smallint NOT NULL REFERENCES amazonqac.dataset,
    source_file smallint NOT NULL,
    source_row bigint NOT NULL,
    observations bigint NOT NULL
);
CREATE TABLE IF NOT EXISTS amazonqac.anomalies (
    final_search_term_utf8_hex text,
    popularity bigint,
    source_id smallint NOT NULL REFERENCES amazonqac.dataset,
    source_file smallint NOT NULL,
    source_row bigint NOT NULL,
    reason text NOT NULL
);
CREATE TABLE IF NOT EXISTS amazonqac.import_batches (
    stage text NOT NULL,
    batch_index integer NOT NULL,
    artifact_sha256 text NOT NULL,
    rows_copied bigint NOT NULL,
    committed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(stage, batch_index)
);
