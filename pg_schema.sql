-- pg_schema.sql — PostgreSQL schema for amz_selection
-- 需要扩展: ltree, pg_trgm

CREATE EXTENSION IF NOT EXISTS ltree;
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- ── 类目表 ──
CREATE TABLE IF NOT EXISTS categories (
    id              SERIAL PRIMARY KEY,
    name            TEXT NOT NULL,
    url             TEXT NOT NULL,
    node_id         TEXT,
    depth           INTEGER DEFAULT 0,
    source          TEXT DEFAULT 'sidebar',
    explored        INTEGER DEFAULT 0,
    created_at      TIMESTAMPTZ DEFAULT now(),
    parent_node_id  TEXT NOT NULL DEFAULT '',
    true_depth      INTEGER,
    nr_valid        INTEGER,
    bs_valid        INTEGER,
    ms_valid        INTEGER,
    mw_valid        INTEGER,
    breadcrumb_checked INTEGER DEFAULT 0,
    slug            TEXT DEFAULT '',
    child_count     INTEGER DEFAULT 0,
    site            TEXT DEFAULT 'US',
    na_valid        INTEGER,
    path            ltree
);

ALTER TABLE categories ADD COLUMN IF NOT EXISTS na_valid INTEGER;

CREATE INDEX IF NOT EXISTS idx_cat_url        ON categories(url);
CREATE INDEX IF NOT EXISTS idx_cat_node_id    ON categories(node_id);
CREATE INDEX IF NOT EXISTS idx_categories_node_site ON categories(node_id, site, parent_node_id);
CREATE INDEX IF NOT EXISTS idx_cat_depth      ON categories(depth);
CREATE INDEX IF NOT EXISTS idx_cat_explored   ON categories(explored);
CREATE INDEX IF NOT EXISTS idx_cat_parent_nid ON categories(parent_node_id);
CREATE INDEX IF NOT EXISTS idx_categories_parent_site ON categories(parent_node_id, site);
CREATE INDEX IF NOT EXISTS idx_cat_site       ON categories(site);
CREATE UNIQUE INDEX IF NOT EXISTS idx_categories_node_parent
    ON categories(site, node_id, parent_node_id);
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
    -- DOUBLE PRECISION：避免 REAL(float4) 把 4.2 存成 4.1999998 导致边界筛选误拒
    rating                DOUBLE PRECISION,
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
    social_proof          TEXT,
    social_proof_count    INTEGER,
    item_weight           TEXT,
    item_dimensions       TEXT,
    date_first_available  TEXT,
    shipping_fee          TEXT,
    shipping_fee_value    REAL,
    fulfillment_type      TEXT,
    country_of_origin     TEXT,
    is_bestseller         INTEGER DEFAULT 0,
    detail_scraped        INTEGER DEFAULT 0,
    run_id                TEXT
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
ALTER TABLE product_sightings ADD COLUMN IF NOT EXISTS social_proof TEXT;
ALTER TABLE product_sightings ADD COLUMN IF NOT EXISTS social_proof_count INTEGER;
ALTER TABLE product_sightings ADD COLUMN IF NOT EXISTS item_weight TEXT;
ALTER TABLE product_sightings ADD COLUMN IF NOT EXISTS item_dimensions TEXT;
ALTER TABLE product_sightings ADD COLUMN IF NOT EXISTS date_first_available TEXT;
ALTER TABLE product_sightings ADD COLUMN IF NOT EXISTS shipping_fee TEXT;
ALTER TABLE product_sightings ADD COLUMN IF NOT EXISTS shipping_fee_value REAL;
ALTER TABLE product_sightings ADD COLUMN IF NOT EXISTS fulfillment_type TEXT;
ALTER TABLE product_sightings ADD COLUMN IF NOT EXISTS country_of_origin TEXT;
ALTER TABLE product_sightings ADD COLUMN IF NOT EXISTS is_bestseller INTEGER DEFAULT 0;
ALTER TABLE product_sightings ADD COLUMN IF NOT EXISTS detail_scraped INTEGER DEFAULT 0;
ALTER TABLE product_sightings ADD COLUMN IF NOT EXISTS run_id TEXT;
-- 已有库：REAL→DOUBLE PRECISION，再按 0.1 精度归一化（修复 4.2→4.1999998 历史漂移）
ALTER TABLE product_sightings
    ALTER COLUMN rating TYPE DOUBLE PRECISION
    USING rating::double precision;
-- 仅更新仍漂移的行，避免每次执行 schema 全表重写
UPDATE product_sightings
SET rating = ROUND(rating::numeric, 1)
WHERE rating IS NOT NULL
  AND rating IS DISTINCT FROM ROUND(rating::numeric, 1);

-- 与 SQLite UNIQUE(asin, node_id, list_type) 对齐，并加上 site；NULLS NOT DISTINCT 避免 NULL 绕过唯一性
DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'uq_ps_asin_node_list_site'
          AND conrelid = 'product_sightings'::regclass
    ) THEN
        ALTER TABLE product_sightings
            ADD CONSTRAINT uq_ps_asin_node_list_site
            UNIQUE NULLS NOT DISTINCT (asin, node_id, list_type, site);
    END IF;
END $$;
CREATE INDEX IF NOT EXISTS idx_ps_asin       ON product_sightings(asin);
CREATE INDEX IF NOT EXISTS idx_ps_scraped    ON product_sightings(scraped_at DESC);
CREATE INDEX IF NOT EXISTS idx_ps_list_type  ON product_sightings(list_type);
CREATE INDEX IF NOT EXISTS idx_ps_site       ON product_sightings(site);
CREATE INDEX IF NOT EXISTS idx_ps_asin_site  ON product_sightings(asin, site);
CREATE INDEX IF NOT EXISTS idx_ps_social_proof ON product_sightings(social_proof_count);
CREATE INDEX IF NOT EXISTS idx_ps_run_id ON product_sightings(run_id);

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
    rating               DOUBLE PRECISION,
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
    social_proof         TEXT,
    social_proof_count   INTEGER,
    fba_fee              REAL,
    placement_fee        REAL,
    fulfillment_type     TEXT,
    country_of_origin    TEXT,
    is_amazon_choice     INTEGER DEFAULT 0,
    is_bestseller        INTEGER DEFAULT 0,
    scraped_at           TIMESTAMPTZ DEFAULT now()
);

ALTER TABLE new_arrivals ADD COLUMN IF NOT EXISTS social_proof TEXT;
ALTER TABLE new_arrivals ADD COLUMN IF NOT EXISTS social_proof_count INTEGER;
ALTER TABLE new_arrivals
    ALTER COLUMN rating TYPE DOUBLE PRECISION
    USING rating::double precision;
UPDATE new_arrivals
SET rating = ROUND(rating::numeric, 1)
WHERE rating IS NOT NULL
  AND rating IS DISTINCT FROM ROUND(rating::numeric, 1);

DO $$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'uq_na_asin_node_site'
          AND conrelid = 'new_arrivals'::regclass
    ) THEN
        ALTER TABLE new_arrivals
            ADD CONSTRAINT uq_na_asin_node_site
            UNIQUE NULLS NOT DISTINCT (asin, node_id, site);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_na_asin      ON new_arrivals(asin);
CREATE INDEX IF NOT EXISTS idx_na_site      ON new_arrivals(site);
CREATE INDEX IF NOT EXISTS idx_na_scraped   ON new_arrivals(scraped_at DESC);
CREATE INDEX IF NOT EXISTS idx_na_asin_site ON new_arrivals(asin, site);
CREATE INDEX IF NOT EXISTS idx_na_price     ON new_arrivals(price_value);
CREATE INDEX IF NOT EXISTS idx_na_bsr_main  ON new_arrivals(bsr_main_rank);
CREATE INDEX IF NOT EXISTS idx_na_social_proof ON new_arrivals(social_proof_count);

-- ── 收藏快照（仅星号收藏写入；普通抓取不得写入）──
CREATE TABLE IF NOT EXISTS favorite_products (
    id SERIAL PRIMARY KEY,
    site TEXT NOT NULL,
    asin TEXT NOT NULL,
    name TEXT,
    title TEXT,
    price REAL,
    price_raw TEXT,
    price_value REAL,
    original_price TEXT,
    discount_pct TEXT,
    rating DOUBLE PRECISION,
    review_count INTEGER,
    rank INTEGER,
    image_url TEXT,
    product_url TEXT,
    has_video INTEGER DEFAULT 0,
    is_amazon_choice INTEGER DEFAULT 0,
    is_bestseller INTEGER DEFAULT 0,
    list_type TEXT,
    list_total INTEGER,
    category_name TEXT,
    category_slug TEXT,
    category_depth INTEGER,
    node_id TEXT,
    bsr_main_rank INTEGER,
    bsr_main_category TEXT,
    bsr_sub_rank INTEGER,
    bsr_sub_category TEXT,
    bsr_sub TEXT,
    variant_option_count INTEGER,
    other_sellers_count INTEGER,
    social_proof TEXT,
    social_proof_count INTEGER,
    item_weight TEXT,
    item_dimensions TEXT,
    weight_lb REAL,
    dim_l_in REAL,
    dim_w_in REAL,
    dim_h_in REAL,
    date_first_available TEXT,
    listing_date TEXT,
    listing_age_days INTEGER,
    shipping_fee TEXT,
    shipping_fee_value REAL,
    fba_fee REAL,
    placement_fee REAL,
    fulfillment_type TEXT,
    country_of_origin TEXT,
    detail_scraped INTEGER DEFAULT 0,
    chart TEXT,
    source_cache_id INTEGER,
    source_run_id TEXT,
    source_list_type TEXT,
    source_node_id TEXT,
    snapshot_json TEXT,
    favorited_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now(),
    UNIQUE(site, asin)
);
CREATE INDEX IF NOT EXISTS idx_fav_site ON favorite_products(site);
CREATE INDEX IF NOT EXISTS idx_fav_time ON favorite_products(favorited_at DESC, id DESC);
