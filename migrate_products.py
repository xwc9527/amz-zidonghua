"""migrate_products.py — 创建 product_sightings 表"""
import sqlite3, os, sys

DB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "categories.db")
conn = sqlite3.connect(DB, timeout=15)
conn.execute("PRAGMA journal_mode=WAL")

conn.executescript("""
CREATE TABLE IF NOT EXISTS product_sightings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    asin TEXT NOT NULL,
    name TEXT,
    price REAL,
    price_raw TEXT,
    original_price TEXT,
    discount_pct TEXT,
    rating REAL,
    review_count INTEGER,
    rank INTEGER,
    image_url TEXT,
    product_url TEXT,
    has_video INTEGER DEFAULT 0,
    is_amazon_choice INTEGER DEFAULT 0,

    -- 来源
    node_id TEXT,
    category_name TEXT,
    category_slug TEXT,
    category_depth INTEGER,
    list_type TEXT,
    list_total INTEGER,

    scraped_at TEXT DEFAULT (datetime('now')),
    UNIQUE(asin, node_id, list_type)
);

CREATE INDEX IF NOT EXISTS idx_ps_asin ON product_sightings(asin);
CREATE INDEX IF NOT EXISTS idx_ps_node ON product_sightings(node_id);
CREATE INDEX IF NOT EXISTS idx_ps_list ON product_sightings(list_type);
CREATE INDEX IF NOT EXISTS idx_ps_review ON product_sightings(review_count);
""")

conn.close()
print("[迁移] product_sightings 表已就绪")
