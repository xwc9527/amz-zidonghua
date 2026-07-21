import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import category_dedup_migration as migration


LEGACY_SCHEMA = """
CREATE TABLE categories (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    url TEXT UNIQUE NOT NULL,
    node_id TEXT,
    depth INTEGER DEFAULT 0,
    source TEXT DEFAULT 'sidebar',
    explored INTEGER DEFAULT 0,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    parent_node_id TEXT,
    true_depth INTEGER,
    nr_valid INTEGER,
    bs_valid INTEGER,
    ms_valid INTEGER,
    mw_valid INTEGER,
    breadcrumb_checked INTEGER DEFAULT 0,
    slug TEXT DEFAULT '',
    child_count INTEGER DEFAULT 0,
    site TEXT DEFAULT 'US',
    na_valid INTEGER
)
"""


def insert_category(conn, name, url, node_id, depth, parent, *, site="US", **values):
    conn.execute(
        """INSERT INTO categories
           (name,url,node_id,depth,source,explored,parent_node_id,slug,site,
            breadcrumb_checked,child_count,na_valid)
           VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
        (
            name, url, node_id, depth, values.get("source", "subtree"),
            values.get("explored", 1), parent, values.get("slug", "root"), site,
            values.get("breadcrumb_checked", 0), values.get("child_count", 0),
            values.get("na_valid"),
        ),
    )


class TestCategoryDedupMigration(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp.name) / "categories.db"
        conn = sqlite3.connect(self.db_path)
        conn.execute(LEGACY_SCHEMA)
        # Two root aliases and two ranking URLs for the same logical edge.
        insert_category(conn, "Old root", "https://x/root-a/", "root", 0, None, slug="root")
        insert_category(conn, "New root", "https://x/root-b/", "root", 0, None, slug="root")
        insert_category(
            conn, "Child", "https://x/new/child/", "child", 1, "root",
            breadcrumb_checked=0, na_valid=None,
        )
        insert_category(
            conn, "Child", "https://x/gifted/child/", "child", 2, "root",
            breadcrumb_checked=1, na_valid=1,
        )
        # A real second parent must survive cleanup.
        insert_category(conn, "Other", "https://x/other/", "other", 0, None, slug="other")
        insert_category(conn, "Child", "https://x/new/child-other/", "child", 1, "other")
        conn.commit()
        conn.close()

    def tearDown(self):
        self.tmp.cleanup()

    def test_dry_run_rolls_back_schema_and_data(self):
        conn = sqlite3.connect(self.db_path)
        report = migration.migrate_connection(conn, dry_run=True)
        self.assertEqual(report["before"]["duplicate_groups"], 2)
        self.assertEqual(report["after"]["duplicate_groups"], 0)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM categories").fetchone()[0], 6)
        self.assertEqual(
            conn.execute("SELECT COUNT(*) FROM categories WHERE parent_node_id IS NULL").fetchone()[0],
            3,
        )
        indexes = {row[1] for row in conn.execute("PRAGMA index_list(categories)")}
        self.assertNotIn(migration.INDEX_NAME, indexes)
        conn.close()

    def test_apply_merges_aliases_preserves_edges_and_state(self):
        conn = sqlite3.connect(self.db_path)
        report = migration.migrate_connection(conn)
        self.assertEqual(report["before"]["duplicate_excess_rows"], 2)
        self.assertEqual(report["after"]["duplicate_groups"], 0)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM categories").fetchone()[0], 4)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM categories WHERE parent_node_id IS NULL").fetchone()[0], 0)
        child = conn.execute(
            "SELECT breadcrumb_checked,na_valid FROM categories WHERE node_id='child' AND parent_node_id='root'"
        ).fetchone()
        self.assertEqual(child, (1, 1))
        self.assertEqual(
            conn.execute("SELECT COUNT(*) FROM categories WHERE node_id='child'").fetchone()[0], 2
        )
        indexes = {row[1]: row[2] for row in conn.execute("PRAGMA index_list(categories)")}
        self.assertEqual(indexes[migration.INDEX_NAME], 1)

        # URL is no longer an identity: two real parent edges may share it.
        insert_category(conn, "Shared", "https://x/shared/", "shared", 1, "root")
        insert_category(conn, "Shared", "https://x/shared/", "shared", 1, "other")
        conn.commit()
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM categories WHERE node_id='shared'").fetchone()[0], 2)
        conn.close()


class TestCategoryEdgeUpsert(unittest.TestCase):
    def setUp(self):
        import fetch_subtree

        self.fetch_subtree = fetch_subtree
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp.name) / "categories.db"
        conn = sqlite3.connect(self.db_path)
        conn.execute(migration.CATEGORIES_TABLE_SQL.format(table_name="categories"))
        migration._create_indexes_and_triggers(conn)
        conn.commit()
        conn.close()

    def tearDown(self):
        self.tmp.cleanup()

    def test_same_edge_updates_but_multi_parent_survives(self):
        with patch.object(self.fetch_subtree, "DB_FILE", str(self.db_path)), \
             patch.object(self.fetch_subtree, "_SITE", "US"):
            self.fetch_subtree._db_batch_insert([
                {"name": "First", "url": "https://x/new/a", "node_id": "A", "depth": 1,
                 "parent_node_id": "P1", "slug": "root"},
            ])
            self.fetch_subtree._db_batch_insert([
                {"name": "Updated", "url": "https://x/gifted/a", "node_id": "A", "depth": 1,
                 "parent_node_id": "P1", "slug": "root"},
                {"name": "Updated", "url": "https://x/gifted/a", "node_id": "A", "depth": 1,
                 "parent_node_id": "P2", "slug": "root"},
            ])
            self.fetch_subtree._db_batch_insert([
                {"name": "Root old", "url": "https://x/root-old", "node_id": "root", "depth": 0,
                 "parent_node_id": None, "slug": "root"},
                {"name": "Root new", "url": "https://x/root-new", "node_id": "root", "depth": 0,
                 "parent_node_id": "", "slug": "root"},
            ])

        conn = sqlite3.connect(self.db_path)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM categories WHERE node_id='A'").fetchone()[0], 2)
        self.assertEqual(
            conn.execute("SELECT name,url FROM categories WHERE node_id='A' AND parent_node_id='P1'").fetchone(),
            ("Updated", "https://x/gifted/a/"),
        )
        self.assertEqual(
            conn.execute("SELECT COUNT(*) FROM categories WHERE node_id='root' AND parent_node_id='' ").fetchone()[0],
            1,
        )
        self.assertEqual(
            conn.execute("SELECT name FROM categories WHERE node_id='root'").fetchone()[0], "Root new"
        )
        conn.close()

    def test_slug_patterns_match_stored_chart_urls_without_double_gp(self):
        patterns = self.fetch_subtree._slug_url_patterns("home-garden")
        self.assertIn("%/gp/new-releases/home-garden/%", patterns)
        self.assertIn("%/gp/most-gifted/home-garden/%", patterns)
        self.assertTrue(all("/gp/gp/" not in item for item in patterns))

    def test_product_execution_dedupes_real_multi_parent_edges(self):
        import fetch_products

        rows = [
            {"node_id": "A", "parent_node_id": "P1", "url": "https://x/a1"},
            {"node_id": "A", "parent_node_id": "P2", "url": "https://x/a2"},
            {"node_id": "B", "parent_node_id": "P1", "url": "https://x/b"},
        ]
        result = fetch_products._dedupe_category_nodes(rows)
        self.assertEqual([row["node_id"] for row in result], ["A", "B"])


class TestPostgresCategoryIdentitySchema(unittest.TestCase):
    def test_pg_schema_and_migration_share_the_edge_identity(self):
        root = Path(__file__).resolve().parents[1]
        schema = (root / "pg_schema.sql").read_text(encoding="utf-8")
        migration_sql = (root / "pg_category_dedup_migration.sql").read_text(encoding="utf-8")
        identity = "ON categories(site, node_id, parent_node_id)"
        self.assertIn(identity, schema)
        self.assertIn(identity, migration_sql)
        self.assertNotIn("url             TEXT UNIQUE", schema)


if __name__ == "__main__":
    unittest.main()
