-- One-time PostgreSQL migration for canonical category edges.
BEGIN;
LOCK TABLE categories IN ACCESS EXCLUSIVE MODE;

ALTER TABLE categories ADD COLUMN IF NOT EXISTS na_valid INTEGER;
UPDATE categories SET parent_node_id = '' WHERE parent_node_id IS NULL;

CREATE TEMP TABLE category_dedup_merged ON COMMIT DROP AS
SELECT MIN(id) AS keep_id,
       site, node_id, parent_node_id,
       MAX(COALESCE(explored, 0)) AS explored,
       MIN(true_depth) FILTER (WHERE true_depth IS NOT NULL) AS true_depth,
       MAX(nr_valid) FILTER (WHERE nr_valid IS NOT NULL) AS nr_valid,
       MAX(bs_valid) FILTER (WHERE bs_valid IS NOT NULL) AS bs_valid,
       MAX(ms_valid) FILTER (WHERE ms_valid IS NOT NULL) AS ms_valid,
       MAX(mw_valid) FILTER (WHERE mw_valid IS NOT NULL) AS mw_valid,
       MAX(COALESCE(breadcrumb_checked, 0)) AS breadcrumb_checked,
       MAX(NULLIF(slug, '')) AS fallback_slug,
       MAX(na_valid) FILTER (WHERE na_valid IS NOT NULL) AS na_valid
FROM categories
GROUP BY site, node_id, parent_node_id;

UPDATE categories AS c
SET explored = m.explored,
    true_depth = COALESCE(c.true_depth, m.true_depth),
    nr_valid = m.nr_valid,
    bs_valid = m.bs_valid,
    ms_valid = m.ms_valid,
    mw_valid = m.mw_valid,
    breadcrumb_checked = m.breadcrumb_checked,
    slug = COALESCE(NULLIF(c.slug, ''), m.fallback_slug, ''),
    na_valid = m.na_valid
FROM category_dedup_merged AS m
WHERE c.id = m.keep_id;

DELETE FROM categories AS c
USING category_dedup_merged AS m
WHERE c.site = m.site
  AND c.node_id IS NOT DISTINCT FROM m.node_id
  AND c.parent_node_id = m.parent_node_id
  AND c.id <> m.keep_id;

-- URL is ranking-page metadata, not the category-edge identity.
ALTER TABLE categories DROP CONSTRAINT IF EXISTS categories_url_key;
ALTER TABLE categories ALTER COLUMN parent_node_id SET DEFAULT '';
ALTER TABLE categories ALTER COLUMN parent_node_id SET NOT NULL;

DROP INDEX IF EXISTS idx_categories_node_parent;
CREATE UNIQUE INDEX idx_categories_node_parent
    ON categories(site, node_id, parent_node_id);
CREATE INDEX IF NOT EXISTS idx_categories_node_site
    ON categories(node_id, site, parent_node_id);
CREATE INDEX IF NOT EXISTS idx_categories_parent_site
    ON categories(parent_node_id, site);

UPDATE categories SET child_count = 0;
WITH child_counts AS (
    SELECT site, parent_node_id AS node_id,
           COUNT(DISTINCT node_id) AS child_count
    FROM categories
    WHERE parent_node_id != ''
    GROUP BY site, parent_node_id
)
UPDATE categories AS c
SET child_count = counts.child_count
FROM child_counts AS counts
WHERE c.site = counts.site AND c.node_id = counts.node_id;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM categories
        GROUP BY site, node_id, parent_node_id HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION 'category edge dedup verification failed';
    END IF;
    IF EXISTS (SELECT 1 FROM categories WHERE parent_node_id IS NULL) THEN
        RAISE EXCEPTION 'category parent normalization failed';
    END IF;
END
$$;

COMMIT;
