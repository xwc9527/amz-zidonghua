"""Read-only production integration benchmark using an isolated SQLite DB."""

import io
import json
import sqlite3
import tempfile
import time
from contextlib import redirect_stdout
from pathlib import Path

import category_dedup_migration as migration
import config
import fetch_subtree


SAMPLE_NODES = 1000
SLUG = "automotive"


def load_entries():
    with open(config.PROXY_POOL_FILE, encoding="utf-8") as handle:
        payload = json.load(handle)
    entries = payload.get("entries", []) if isinstance(payload, dict) else payload
    entries = [entry for entry in entries if isinstance(entry, dict) and entry.get("proxy")]
    if len(entries) < 16:
        raise RuntimeError(f"need 16 active proxies, found {len(entries)}")
    return entries[:16]


def load_sample():
    connection = sqlite3.connect(f"file:{config.DB_FILE}?mode=ro", uri=True)
    try:
        rows = connection.execute(
            "SELECT name,url,node_id FROM categories "
            "WHERE site='US' AND slug=? AND node_id IS NOT NULL "
            "GROUP BY node_id LIMIT ?",
            (SLUG, SAMPLE_NODES),
        ).fetchall()
    finally:
        connection.close()
    if len(rows) < SAMPLE_NODES:
        raise RuntimeError(f"need {SAMPLE_NODES} {SLUG} nodes, found {len(rows)}")
    return rows


def create_isolated_db(path, rows):
    connection = sqlite3.connect(path)
    connection.execute(migration.CATEGORIES_TABLE_SQL.format(table_name="categories"))
    migration._create_indexes_and_triggers(connection)
    for index, (name, url, node_id) in enumerate(rows):
        connection.execute(
            "INSERT INTO categories "
            "(name,url,node_id,depth,source,explored,parent_node_id,slug,site) "
            "VALUES(?,?,?,?,?,?,?,?,?)",
            (name, url, node_id, 0, "benchmark", 1, f"bench-parent-{index}", SLUG, "US"),
        )
    connection.commit()
    connection.close()


def main():
    entries = load_entries()
    rows = load_sample()
    with tempfile.TemporaryDirectory(prefix="subtree-integration-") as tmp:
        db_path = Path(tmp) / "categories.db"
        checkpoint = Path(tmp) / "checkpoint.json"
        create_isolated_db(db_path, rows)
        old_db = fetch_subtree.DB_FILE
        old_checkpoint = fetch_subtree.CHECKPOINT_FILE
        fetch_subtree.DB_FILE = str(db_path)
        fetch_subtree.CHECKPOINT_FILE = str(checkpoint)
        output = io.StringIO()
        started = time.monotonic()
        try:
            with redirect_stdout(output):
                fetch_subtree.crawl_slug(SLUG, entries, max_depth=1)
        finally:
            fetch_subtree.DB_FILE = old_db
            fetch_subtree.CHECKPOINT_FILE = old_checkpoint
        elapsed = time.monotonic() - started
        connection = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        try:
            integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
            total_rows = connection.execute("SELECT COUNT(*) FROM categories").fetchone()[0]
        finally:
            connection.close()
        summary = {
            "input_nodes": SAMPLE_NODES,
            "active_ips": len(entries),
            "elapsed_sec": round(elapsed, 2),
            "node_rpm": round(SAMPLE_NODES / elapsed * 60, 2),
            "integrity": integrity,
            "isolated_rows": total_rows,
            "production_db": old_db,
        }
        print(output.getvalue())
        print("INTEGRATION_SUMMARY", summary)
        if integrity != "ok" or summary["node_rpm"] < 1000:
            raise SystemExit(2)


if __name__ == "__main__":
    main()
