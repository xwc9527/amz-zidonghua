-- Run once with psql (autocommit), against the existing AmazonQAC database.
-- Baseline EXPLAIN ANALYZE showed a parallel full scan for '%storage%'.
-- CONCURRENTLY keeps existing reads/writes available; do not wrap in BEGIN.
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX CONCURRENTLY amazonqac_keywords_term_trgm_idx
    ON amazonqac.keywords USING gin (final_search_term gin_trgm_ops);
ANALYZE amazonqac.keywords;
-- If interrupted, check pg_index.indisvalid before retrying. No raw data changes.
