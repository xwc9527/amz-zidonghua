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
    url        TEXT UNIQUE NOT NULL,
    node_id    TEXT,
    depth      INTEGER DEFAULT 0,
    source     TEXT DEFAULT 'sidebar',
    explored   INTEGER DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    parent_node_id     TEXT,
    true_depth         INTEGER,
    nr_valid           INTEGER,
    bs_valid           INTEGER,
    ms_valid           INTEGER,
    mw_valid           INTEGER,
    breadcrumb_checked INTEGER DEFAULT 0,
    slug               TEXT DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_url        ON categories(url);
CREATE INDEX IF NOT EXISTS idx_node_id    ON categories(node_id);
CREATE INDEX IF NOT EXISTS idx_depth      ON categories(depth);
CREATE INDEX IF NOT EXISTS idx_explored   ON categories(explored);
CREATE INDEX IF NOT EXISTS idx_parent_nid ON categories(parent_node_id);

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

conn.commit()

total = cur.execute("SELECT COUNT(*) FROM categories").fetchone()[0]
conn.close()

print(f"数据库就绪: {DB_PATH}  当前 {total} 条记录")
