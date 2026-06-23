-- pg_schema.sql — PostgreSQL schema for amz_selection
-- 需要扩展: ltree, pg_trgm

CREATE EXTENSION IF NOT EXISTS ltree;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- ── 类目表 ──
CREATE TABLE IF NOT EXISTS categories (
    id              SERIAL PRIMARY KEY,
    name            TEXT NOT NULL,
    url             TEXT UNIQUE NOT NULL,
    node_id         TEXT,
    depth           INTEGER DEFAULT 0,
    source          TEXT DEFAULT 'sidebar',
    explored        INTEGER DEFAULT 0,
    created_at      TIMESTAMPTZ DEFAULT now(),
    parent_node_id  TEXT,
    true_depth      INTEGER,
    nr_valid        INTEGER,
    bs_valid        INTEGER,
    ms_valid        INTEGER,
    mw_valid        INTEGER,
    breadcrumb_checked INTEGER DEFAULT 0,
    slug            TEXT DEFAULT '',
    child_count     INTEGER DEFAULT 0,
    site            TEXT DEFAULT 'US',
    path            ltree
);

CREATE INDEX IF NOT EXISTS idx_cat_url        ON categories(url);
CREATE INDEX IF NOT EXISTS idx_cat_node_id    ON categories(node_id);
CREATE INDEX IF NOT EXISTS idx_cat_depth      ON categories(depth);
CREATE INDEX IF NOT EXISTS idx_cat_explored   ON categories(explored);
CREATE INDEX IF NOT EXISTS idx_cat_parent_nid ON categories(parent_node_id);
CREATE INDEX IF NOT EXISTS idx_cat_site       ON categories(site);
CREATE INDEX IF NOT EXISTS idx_cat_path       ON categories USING gist(path);
CREATE INDEX IF NOT EXISTS idx_cat_name_trgm  ON categories USING gin(name gin_trgm_ops);

-- child_count 自动维护触发器
CREATE OR REPLACE FUNCTION fn_child_inc() RETURNS trigger AS $$
BEGIN
    IF NEW.parent_node_id IS NOT NULL AND NEW.parent_node_id != '' THEN
        UPDATE categories SET child_count = child_count + 1
        WHERE node_id = NEW.parent_node_id AND site = NEW.site;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION fn_child_dec() RETURNS trigger AS $$
BEGIN
    IF OLD.parent_node_id IS NOT NULL AND OLD.parent_node_id != '' THEN
        UPDATE categories SET child_count = child_count - 1
        WHERE node_id = OLD.parent_node_id AND site = OLD.site;
    END IF;
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_child_inc ON categories;
CREATE TRIGGER trg_child_inc AFTER INSERT ON categories
FOR EACH ROW EXECUTE FUNCTION fn_child_inc();

DROP TRIGGER IF EXISTS trg_child_dec ON categories;
CREATE TRIGGER trg_child_dec AFTER DELETE ON categories
FOR EACH ROW EXECUTE FUNCTION fn_child_dec();

-- ── 商品快照表 ──
CREATE TABLE IF NOT EXISTS product_sightings (
    id            SERIAL PRIMARY KEY,
    name          TEXT,
    asin          TEXT,
    price         REAL,
    review_count  INTEGER,
    rank          INTEGER,
    rating        REAL,
    image_url     TEXT,
    product_url   TEXT,
    list_type     TEXT,
    category_name TEXT,
    scraped_at    TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_ps_asin       ON product_sightings(asin);
CREATE INDEX IF NOT EXISTS idx_ps_scraped    ON product_sightings(scraped_at DESC);
CREATE INDEX IF NOT EXISTS idx_ps_list_type  ON product_sightings(list_type);

-- ── 选品清单 ──
CREATE TABLE IF NOT EXISTS watchlist (
    id         SERIAL PRIMARY KEY,
    asin       TEXT UNIQUE NOT NULL,
    name       TEXT,
    added_at   TIMESTAMPTZ DEFAULT now(),
    notes      TEXT DEFAULT ''
);

-- ── 每日追踪快照 ──
CREATE TABLE IF NOT EXISTS tracking (
    id         SERIAL PRIMARY KEY,
    asin       TEXT NOT NULL,
    price      REAL,
    rank       INTEGER,
    rating     REAL,
    review_count INTEGER,
    snapshot_date DATE DEFAULT CURRENT_DATE,
    UNIQUE(asin, snapshot_date)
);

CREATE INDEX IF NOT EXISTS idx_track_asin ON tracking(asin);

-- ── 运行状态 ──
CREATE TABLE IF NOT EXISTS run_status (
    id         INTEGER PRIMARY KEY CHECK(id=1),
    phase      TEXT,
    paused     INTEGER DEFAULT 0,
    total      INTEGER DEFAULT 0,
    queue      INTEGER DEFAULT 0,
    updated_at TIMESTAMPTZ DEFAULT now()
);

INSERT INTO run_status(id, phase, paused) VALUES(1, 'idle', 0)
ON CONFLICT(id) DO NOTHING;
