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
    id                    SERIAL PRIMARY KEY,
    name                  TEXT,
    asin                  TEXT,
    price                 REAL,
    price_raw             TEXT,
    original_price        REAL,
    discount_pct          REAL,
    review_count          INTEGER,
    rank                  INTEGER,
    rating                REAL,
    image_url             TEXT,
    product_url           TEXT,
    has_video             INTEGER DEFAULT 0,
    is_amazon_choice      INTEGER DEFAULT 0,
    list_type             TEXT,
    list_total            INTEGER,
    category_name         TEXT,
    category_slug         TEXT,
    category_depth        INTEGER,
    node_id               TEXT,
    site                  TEXT DEFAULT 'US',
    scraped_at            TIMESTAMPTZ DEFAULT now(),
    -- 详情页字段（与 fetch_products._DETAIL_COLS / api_server SELECT 对齐）
    bsr_main_rank         INTEGER,
    bsr_main_category     TEXT,
    bsr_sub_rank          INTEGER,
    bsr_sub_category      TEXT,
    variant_option_count  INTEGER,
    other_sellers_count   INTEGER,
    item_weight           TEXT,
    item_dimensions       TEXT,
    date_first_available  TEXT,
    shipping_fee          TEXT,
    shipping_fee_value    REAL,
    fulfillment_type      TEXT,
    country_of_origin     TEXT,
    is_bestseller         INTEGER DEFAULT 0,
    detail_scraped        INTEGER DEFAULT 0
);

-- 兼容已有库：补齐缺失列（CREATE TABLE IF NOT EXISTS 不会改旧表）
ALTER TABLE product_sightings ADD COLUMN IF NOT EXISTS price_raw TEXT;
ALTER TABLE product_sightings ADD COLUMN IF NOT EXISTS original_price REAL;
ALTER TABLE product_sightings ADD COLUMN IF NOT EXISTS discount_pct REAL;
ALTER TABLE product_sightings ADD COLUMN IF NOT EXISTS has_video INTEGER DEFAULT 0;
ALTER TABLE product_sightings ADD COLUMN IF NOT EXISTS is_amazon_choice INTEGER DEFAULT 0;
ALTER TABLE product_sightings ADD COLUMN IF NOT EXISTS list_total INTEGER;
ALTER TABLE product_sightings ADD COLUMN IF NOT EXISTS category_slug TEXT;
ALTER TABLE product_sightings ADD COLUMN IF NOT EXISTS category_depth INTEGER;
ALTER TABLE product_sightings ADD COLUMN IF NOT EXISTS node_id TEXT;
ALTER TABLE product_sightings ADD COLUMN IF NOT EXISTS bsr_main_rank INTEGER;
ALTER TABLE product_sightings ADD COLUMN IF NOT EXISTS bsr_main_category TEXT;
ALTER TABLE product_sightings ADD COLUMN IF NOT EXISTS bsr_sub_rank INTEGER;
ALTER TABLE product_sightings ADD COLUMN IF NOT EXISTS bsr_sub_category TEXT;
ALTER TABLE product_sightings ADD COLUMN IF NOT EXISTS variant_option_count INTEGER;
ALTER TABLE product_sightings ADD COLUMN IF NOT EXISTS other_sellers_count INTEGER;
ALTER TABLE product_sightings ADD COLUMN IF NOT EXISTS item_weight TEXT;
ALTER TABLE product_sightings ADD COLUMN IF NOT EXISTS item_dimensions TEXT;
ALTER TABLE product_sightings ADD COLUMN IF NOT EXISTS date_first_available TEXT;
ALTER TABLE product_sightings ADD COLUMN IF NOT EXISTS shipping_fee TEXT;
ALTER TABLE product_sightings ADD COLUMN IF NOT EXISTS shipping_fee_value REAL;
ALTER TABLE product_sightings ADD COLUMN IF NOT EXISTS fulfillment_type TEXT;
ALTER TABLE product_sightings ADD COLUMN IF NOT EXISTS country_of_origin TEXT;
ALTER TABLE product_sightings ADD COLUMN IF NOT EXISTS is_bestseller INTEGER DEFAULT 0;
ALTER TABLE product_sightings ADD COLUMN IF NOT EXISTS detail_scraped INTEGER DEFAULT 0;

-- 与 SQLite UNIQUE(asin, node_id, list_type) 对齐，并加上 site；NULLS NOT DISTINCT 避免 NULL 绕过唯一性
DO $$ BEGIN
    ALTER TABLE product_sightings
        ADD CONSTRAINT uq_ps_asin_node_list_site
        UNIQUE NULLS NOT DISTINCT (asin, node_id, list_type, site);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;
CREATE INDEX IF NOT EXISTS idx_ps_asin       ON product_sightings(asin);
CREATE INDEX IF NOT EXISTS idx_ps_scraped    ON product_sightings(scraped_at DESC);
CREATE INDEX IF NOT EXISTS idx_ps_list_type  ON product_sightings(list_type);
CREATE INDEX IF NOT EXISTS idx_ps_site       ON product_sightings(site);
CREATE INDEX IF NOT EXISTS idx_ps_asin_site  ON product_sightings(asin, site);

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

-- ── 最新到货（与 fetch_new_arrivals.py / SQLite new_arrivals 对齐）──
CREATE TABLE IF NOT EXISTS new_arrivals (
    id                   SERIAL PRIMARY KEY,
    asin                 TEXT NOT NULL,
    title                TEXT,
    price                TEXT,
    price_value          REAL,
    rating               REAL,
    review_count         INTEGER DEFAULT 0,
    listing_date         TEXT,
    listing_age_days     INTEGER,
    bsr_main_category    TEXT,
    bsr_main_rank        INTEGER,
    bsr_sub              TEXT,
    bsr_sub_rank         INTEGER,
    bsr_sub_category     TEXT,
    image_url            TEXT,
    product_url          TEXT,
    node_id              TEXT,
    category_name        TEXT,
    category_depth       INTEGER,
    site                 TEXT DEFAULT 'US',
    item_weight          TEXT,
    item_dimensions      TEXT,
    weight_lb            REAL,
    dim_l_in             REAL,
    dim_w_in             REAL,
    dim_h_in             REAL,
    variant_option_count INTEGER,
    other_sellers_count  INTEGER,
    fba_fee              REAL,
    placement_fee        REAL,
    fulfillment_type     TEXT,
    country_of_origin    TEXT,
    is_amazon_choice     INTEGER DEFAULT 0,
    is_bestseller        INTEGER DEFAULT 0,
    scraped_at           TIMESTAMPTZ DEFAULT now()
);

DO $$ BEGIN
    ALTER TABLE new_arrivals
        ADD CONSTRAINT uq_na_asin_node_site
        UNIQUE NULLS NOT DISTINCT (asin, node_id, site);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

CREATE INDEX IF NOT EXISTS idx_na_asin      ON new_arrivals(asin);
CREATE INDEX IF NOT EXISTS idx_na_site      ON new_arrivals(site);
CREATE INDEX IF NOT EXISTS idx_na_scraped   ON new_arrivals(scraped_at DESC);
CREATE INDEX IF NOT EXISTS idx_na_asin_site ON new_arrivals(asin, site);
CREATE INDEX IF NOT EXISTS idx_na_price     ON new_arrivals(price_value);
CREATE INDEX IF NOT EXISTS idx_na_bsr_main  ON new_arrivals(bsr_main_rank);
