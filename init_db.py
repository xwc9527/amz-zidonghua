# init_db.py — 建库（创建表结构 + 索引）
import os, sqlite3, sys

sys.stdout.reconfigure(encoding='utf-8', errors='replace')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH  = os.path.join(DATA_DIR, "categories.db")
# 建库
conn = sqlite3.connect(DB_PATH)
conn.execute("PRAGMA journal_mode=WAL")  # 读写不阻塞
cur = conn.cursor()

cur.executescript("""
CREATE TABLE IF NOT EXISTS categories (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    name       TEXT NOT NULL,
    url        TEXT NOT NULL,
    node_id    TEXT,
    depth      INTEGER DEFAULT 0,
    source     TEXT DEFAULT 'sidebar',
    explored   INTEGER DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    parent_node_id     TEXT NOT NULL DEFAULT '',
    true_depth         INTEGER,
    nr_valid           INTEGER,
    bs_valid           INTEGER,
    ms_valid           INTEGER,
    mw_valid           INTEGER,
    breadcrumb_checked INTEGER DEFAULT 0,
    slug               TEXT DEFAULT '',
    child_count        INTEGER DEFAULT 0,
    site               TEXT DEFAULT 'US',
    na_valid           INTEGER
);

CREATE INDEX IF NOT EXISTS idx_url        ON categories(url);
CREATE INDEX IF NOT EXISTS idx_node_id    ON categories(node_id);
CREATE INDEX IF NOT EXISTS idx_categories_node_site ON categories(node_id, site, parent_node_id);
CREATE INDEX IF NOT EXISTS idx_depth      ON categories(depth);
CREATE INDEX IF NOT EXISTS idx_explored   ON categories(explored);
CREATE INDEX IF NOT EXISTS idx_parent_nid ON categories(parent_node_id);
CREATE INDEX IF NOT EXISTS idx_categories_parent_site ON categories(parent_node_id, site);
CREATE INDEX IF NOT EXISTS idx_categories_site ON categories(site);
CREATE UNIQUE INDEX IF NOT EXISTS idx_categories_node_parent
    ON categories(site, node_id, parent_node_id);

CREATE TRIGGER IF NOT EXISTS trg_child_inc AFTER INSERT ON categories
WHEN NEW.parent_node_id != ''
BEGIN
    UPDATE categories SET child_count = child_count + 1
    WHERE site = NEW.site AND node_id = NEW.parent_node_id;
END;

CREATE TRIGGER IF NOT EXISTS trg_child_dec AFTER DELETE ON categories
WHEN OLD.parent_node_id != ''
BEGIN
    UPDATE categories SET child_count = child_count - 1
    WHERE site = OLD.site AND node_id = OLD.parent_node_id;
END;

CREATE TABLE IF NOT EXISTS run_status (
    id         INTEGER PRIMARY KEY CHECK(id=1),
    phase      TEXT,
    paused     INTEGER DEFAULT 0,
    total      INTEGER DEFAULT 0,
    queue      INTEGER DEFAULT 0,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

INSERT OR IGNORE INTO run_status(id, phase, paused) VALUES(1, 'idle', 0);
""")

# 迁移：若 child_count 列不存在则添加
cols = [r[1] for r in cur.execute("PRAGMA table_info(categories)").fetchall()]
if "child_count" not in cols:
    cur.execute("ALTER TABLE categories ADD COLUMN child_count INTEGER DEFAULT 0")

# 一次性填充 child_count
cur.execute("""
    UPDATE categories SET child_count = (
        SELECT COUNT(DISTINCT c2.node_id) FROM categories c2
        WHERE c2.site = categories.site
          AND c2.parent_node_id = categories.node_id
    ) WHERE node_id IS NOT NULL
""")

conn.commit()

total = cur.execute("SELECT COUNT(*) FROM categories").fetchone()[0]
conn.close()

print(f"数据库就绪: {DB_PATH}  当前 {total} 条记录")
