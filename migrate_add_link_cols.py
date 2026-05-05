"""migrate_add_link_cols.py — 热迁移：给 categories 表加 4 个榜单有效性列"""
import sqlite3, os, sys

BASE = os.path.dirname(os.path.abspath(__file__))
DB   = os.path.join(BASE, "data", "categories.db")

conn = sqlite3.connect(DB, timeout=15)
conn.execute("PRAGMA journal_mode=WAL")

cols = {"nr_valid": "新品榜", "bs_valid": "畅销榜", "ms_valid": "飙升榜", "mw_valid": "心愿单"}
existing = {row[1] for row in conn.execute("PRAGMA table_info(categories)")}

added = []
for col, label in cols.items():
    if col not in existing:
        conn.execute(f"ALTER TABLE categories ADD COLUMN {col} INTEGER")
        added.append(f"{col}({label})")

conn.commit()
conn.close()

if added:
    print(f"[迁移] 新增列: {', '.join(added)}")
else:
    print("[迁移] 列已存在，无需操作")
