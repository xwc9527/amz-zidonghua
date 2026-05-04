# init_db.py — 一次性：建库 + 将现有 JSON 数据导入 SQLite
import json, os, sqlite3, sys

sys.stdout.reconfigure(encoding='utf-8', errors='replace')

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH  = os.path.join(DATA_DIR, "categories.db")
JSON_PATH = os.path.join(DATA_DIR, "categories.json")
QUEUE_PATH = os.path.join(DATA_DIR, "categories_queue.json")

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
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_url     ON categories(url);
CREATE INDEX IF NOT EXISTS idx_node_id ON categories(node_id);
CREATE INDEX IF NOT EXISTS idx_depth   ON categories(depth);
CREATE INDEX IF NOT EXISTS idx_explored ON categories(explored);

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

# 导入 JSON
if not os.path.exists(JSON_PATH):
    print("categories.json 不存在，跳过导入")
    conn.close()
    exit()

with open(JSON_PATH, encoding="utf-8") as f:
    nodes = json.load(f)

# 读队列，标记 explored 状态
queue_urls = set()
if os.path.exists(QUEUE_PATH):
    with open(QUEUE_PATH, encoding="utf-8") as f:
        for item in json.load(f):
            queue_urls.add(item["url"].rstrip("/"))

imported = 0
skipped = 0
for n in nodes:
    url = n.get("url", "").rstrip("/")
    if not url:
        skipped += 1
        continue
    explored = 0 if url in queue_urls else 1
    try:
        cur.execute(
            "INSERT OR IGNORE INTO categories(name, url, node_id, depth, source, explored) "
            "VALUES(?, ?, ?, ?, ?, ?)",
            (n.get("name", ""), url, n.get("node_id"),
             n.get("depth", 0), n.get("source", "sidebar"), explored)
        )
        if cur.rowcount > 0:
            imported += 1
        else:
            skipped += 1
    except Exception as e:
        print(f"  跳过: {url} ({e})")
        skipped += 1

# 更新状态
total = cur.execute("SELECT COUNT(*) FROM categories").fetchone()[0]
queue = cur.execute("SELECT COUNT(*) FROM categories WHERE explored=0").fetchone()[0]
cur.execute("UPDATE run_status SET total=?, queue=?, phase='idle', updated_at=CURRENT_TIMESTAMP WHERE id=1",
            (total, queue))

conn.commit()
conn.close()

print(f"导入完成: {imported} 条写入, {skipped} 条跳过")
print(f"总计: {total} 条, 待处理队列: {queue} 条")
print(f"数据库: {DB_PATH}")
