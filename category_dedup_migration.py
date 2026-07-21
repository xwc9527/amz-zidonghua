"""One-time category identity migration for SQLite.

The canonical category edge is ``(site, node_id, parent_node_id)``.  Amazon may
place one node below more than one parent, so ``(site, node_id)`` alone is not a
valid identity.  Ranking-page URLs are attributes of an edge, not identities.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
from datetime import datetime
from pathlib import Path


INDEX_NAME = "idx_categories_node_parent"

CATEGORIES_TABLE_SQL = """
CREATE TABLE {table_name} (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    name                 TEXT NOT NULL,
    url                  TEXT NOT NULL,
    node_id              TEXT,
    depth                INTEGER DEFAULT 0,
    source               TEXT DEFAULT 'sidebar',
    explored             INTEGER DEFAULT 0,
    created_at           DATETIME DEFAULT CURRENT_TIMESTAMP,
    parent_node_id       TEXT NOT NULL DEFAULT '',
    true_depth           INTEGER,
    nr_valid             INTEGER,
    bs_valid             INTEGER,
    ms_valid             INTEGER,
    mw_valid             INTEGER,
    breadcrumb_checked   INTEGER DEFAULT 0,
    slug                 TEXT DEFAULT '',
    child_count          INTEGER DEFAULT 0,
    site                 TEXT DEFAULT 'US',
    na_valid             INTEGER
)
"""


def _duplicate_stats(conn: sqlite3.Connection) -> dict[str, int]:
    where = "node_id IS NOT NULL AND node_id != ''"
    groups = conn.execute(
        f"""SELECT COUNT(*) FROM (
                SELECT site, node_id, COALESCE(parent_node_id, '') AS parent_key
                FROM categories WHERE {where}
                GROUP BY site, node_id, parent_key HAVING COUNT(*) > 1
            )"""
    ).fetchone()[0]
    excess = conn.execute(
        f"""SELECT COALESCE(SUM(row_count - 1), 0) FROM (
                SELECT COUNT(*) AS row_count
                FROM categories WHERE {where}
                GROUP BY site, node_id, COALESCE(parent_node_id, '')
                HAVING COUNT(*) > 1
            )"""
    ).fetchone()[0]
    return {
        "rows": conn.execute("SELECT COUNT(*) FROM categories").fetchone()[0],
        "duplicate_groups": int(groups),
        "duplicate_excess_rows": int(excess),
        "null_parents": conn.execute(
            "SELECT COUNT(*) FROM categories WHERE parent_node_id IS NULL"
        ).fetchone()[0],
    }


def _require_columns(conn: sqlite3.Connection) -> None:
    required = {
        "id", "name", "url", "node_id", "depth", "source", "explored",
        "created_at", "parent_node_id", "true_depth", "nr_valid", "bs_valid",
        "ms_valid", "mw_valid", "breadcrumb_checked", "slug", "child_count",
        "site", "na_valid",
    }
    actual = {row[1] for row in conn.execute("PRAGMA table_info(categories)")}
    missing = sorted(required - actual)
    if missing:
        raise RuntimeError(f"categories table is missing required columns: {missing}")


def _create_indexes_and_triggers(conn: sqlite3.Connection) -> None:
    # Do not use executescript here: sqlite3 executescript() commits any active
    # transaction first, which would make --dry-run destructive.
    statements = [
        "CREATE INDEX idx_url ON categories(url)",
        "CREATE INDEX idx_node_id ON categories(node_id)",
        "CREATE INDEX idx_categories_node_site ON categories(node_id, site, parent_node_id)",
        "CREATE INDEX idx_depth ON categories(depth)",
        "CREATE INDEX idx_explored ON categories(explored)",
        "CREATE INDEX idx_parent_nid ON categories(parent_node_id)",
        "CREATE INDEX idx_categories_parent_site ON categories(parent_node_id, site)",
        "CREATE INDEX idx_categories_site ON categories(site)",
        f"CREATE UNIQUE INDEX {INDEX_NAME} ON categories(site, node_id, parent_node_id)",
        """CREATE TRIGGER trg_child_inc AFTER INSERT ON categories
           WHEN NEW.parent_node_id != ''
           BEGIN
               UPDATE categories SET child_count = child_count + 1
               WHERE site = NEW.site AND node_id = NEW.parent_node_id;
           END""",
        """CREATE TRIGGER trg_child_dec AFTER DELETE ON categories
           WHEN OLD.parent_node_id != ''
           BEGIN
               UPDATE categories SET child_count = child_count - 1
               WHERE site = OLD.site AND node_id = OLD.parent_node_id;
           END""",
    ]
    for statement in statements:
        conn.execute(statement)


def migrate_connection(conn: sqlite3.Connection, *, dry_run: bool = False) -> dict:
    """Migrate one open SQLite connection and optionally roll everything back."""
    _require_columns(conn)
    conn.execute("PRAGMA foreign_keys=OFF")
    conn.execute("BEGIN IMMEDIATE")
    try:
        before = _duplicate_stats(conn)
        conn.execute("UPDATE categories SET parent_node_id='' WHERE parent_node_id IS NULL")

        # Keep the oldest row as the stable canonical record, but merge stateful
        # fields so cleanup never discards completed validation/exploration work.
        conn.execute(
            """CREATE TEMP TABLE category_dedup_merged AS
               SELECT MIN(id) AS keep_id,
                      site, node_id, parent_node_id,
                      MAX(COALESCE(explored, 0)) AS explored,
                      CASE WHEN COUNT(true_depth)=0 THEN NULL ELSE MIN(true_depth) END AS true_depth,
                      CASE WHEN COUNT(nr_valid)=0 THEN NULL ELSE MAX(nr_valid) END AS nr_valid,
                      CASE WHEN COUNT(bs_valid)=0 THEN NULL ELSE MAX(bs_valid) END AS bs_valid,
                      CASE WHEN COUNT(ms_valid)=0 THEN NULL ELSE MAX(ms_valid) END AS ms_valid,
                      CASE WHEN COUNT(mw_valid)=0 THEN NULL ELSE MAX(mw_valid) END AS mw_valid,
                      MAX(COALESCE(breadcrumb_checked, 0)) AS breadcrumb_checked,
                      MAX(NULLIF(slug, '')) AS fallback_slug,
                      CASE WHEN COUNT(na_valid)=0 THEN NULL ELSE MAX(na_valid) END AS na_valid
               FROM categories
               GROUP BY site, node_id, parent_node_id"""
        )
        conn.execute(
            "CREATE UNIQUE INDEX category_dedup_merged_keep_idx "
            "ON category_dedup_merged(keep_id)"
        )
        conn.execute(
            """UPDATE categories AS c
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
               WHERE c.id = m.keep_id"""
        )
        conn.execute(
            "DELETE FROM categories WHERE id NOT IN (SELECT keep_id FROM category_dedup_merged)"
        )
        conn.execute("DROP TABLE category_dedup_merged")

        # Rebuild to remove the legacy URL UNIQUE constraint.  URL is mutable
        # ranking-page metadata and may legitimately be shared by two parents.
        conn.execute(CATEGORIES_TABLE_SQL.format(table_name="categories__dedup_new"))
        conn.execute(
            """INSERT INTO categories__dedup_new (
                   id,name,url,node_id,depth,source,explored,created_at,
                   parent_node_id,true_depth,nr_valid,bs_valid,ms_valid,mw_valid,
                   breadcrumb_checked,slug,child_count,site,na_valid
               )
               SELECT id,name,url,node_id,depth,source,explored,created_at,
                      parent_node_id,true_depth,nr_valid,bs_valid,ms_valid,mw_valid,
                      breadcrumb_checked,slug,0,site,na_valid
               FROM categories ORDER BY id"""
        )
        conn.execute("DROP TABLE categories")
        conn.execute("ALTER TABLE categories__dedup_new RENAME TO categories")
        _create_indexes_and_triggers(conn)

        # Old trigger accounting was row-based and not site-scoped. Recompute
        # direct children from canonical edges after cleanup.
        conn.execute(
            """CREATE TEMP TABLE category_child_counts AS
               SELECT site, parent_node_id AS node_id,
                      COUNT(DISTINCT node_id) AS child_count
               FROM categories
               WHERE parent_node_id != ''
               GROUP BY site, parent_node_id"""
        )
        conn.execute("UPDATE categories SET child_count=0")
        conn.execute(
            """UPDATE categories AS c
               SET child_count = counts.child_count
               FROM category_child_counts AS counts
               WHERE c.site = counts.site AND c.node_id = counts.node_id"""
        )
        conn.execute("DROP TABLE category_child_counts")

        after = _duplicate_stats(conn)
        if after["duplicate_groups"] or after["null_parents"]:
            raise RuntimeError(f"category dedup verification failed: {after}")
        index_rows = conn.execute("PRAGMA index_list(categories)").fetchall()
        if not any(row[1] == INDEX_NAME and row[2] == 1 for row in index_rows):
            raise RuntimeError(f"unique index {INDEX_NAME} was not created")
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        if integrity != "ok":
            raise RuntimeError(f"SQLite integrity_check failed: {integrity}")

        report = {"dry_run": dry_run, "before": before, "after": after, "integrity": integrity}
        if dry_run:
            conn.rollback()
        else:
            conn.commit()
        return report
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.execute("PRAGMA foreign_keys=ON")


def backup_database(db_path: Path, backup_dir: Path | None = None) -> Path:
    backup_dir = backup_dir or db_path.parent / "backups"
    backup_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_path = backup_dir / f"{db_path.stem}.before-category-dedup-{stamp}.db"
    source = sqlite3.connect(f"file:{db_path.as_posix()}?mode=ro", uri=True)
    target = sqlite3.connect(backup_path)
    try:
        source.backup(target)
        if target.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise RuntimeError("backup integrity_check failed")
    finally:
        target.close()
        source.close()
    return backup_path


def migrate_database(db_path: Path, *, dry_run: bool = False, create_backup: bool = True) -> dict:
    db_path = db_path.resolve()
    if not db_path.exists():
        raise FileNotFoundError(db_path)
    backup_path = None
    if create_backup and not dry_run:
        backup_path = backup_database(db_path)
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        report = migrate_connection(conn, dry_run=dry_run)
    finally:
        conn.close()
    report["database"] = str(db_path)
    report["backup"] = str(backup_path) if backup_path else None
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Deduplicate category edges and install their identity constraint")
    parser.add_argument("--db", default=os.path.join("data", "categories.db"))
    parser.add_argument("--dry-run", action="store_true", help="run the complete migration and ROLLBACK")
    parser.add_argument("--no-backup", action="store_true", help="skip backup when applying (tests only)")
    args = parser.parse_args()
    report = migrate_database(
        Path(args.db), dry_run=args.dry_run, create_backup=not args.no_backup
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
