"""Regression coverage for category edges discovered from product detail HTML."""
from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import category_dedup_migration as migration
import config
import fetch_products as fp
from detail_parser import parse_detail_fields


DETAIL_HTML = """
<div id="wayfinding-breadcrumbs_feature_div">
  <a href="/s?node=10">Root</a>
  <a href="/s?node=20">Leaf</a>
</div>
<div id="prodDetails">
  #1 in Widgets
  <a href="/gp/bestsellers/widgets/30/ref=zg_bs_nav_0">Widgets</a>
  #2 in Gadget Parts
  <a href="/gp/bestsellers/gadget-parts/40/">Gadget Parts</a>
</div>
"""


def _insert(conn, name, node_id, parent_node_id, depth, slug, **values):
    conn.execute(
        """INSERT INTO categories
           (name, url, node_id, depth, source, explored, parent_node_id, slug, site)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'US')""",
        (
            name, f"https://example.test/gp/bestsellers/{slug}/{node_id}/", node_id,
            depth, values.get("source", "sidebar"), values.get("explored", 1),
            parent_node_id, slug,
        ),
    )


class TestDetailParserReverseFields(unittest.TestCase):
    def test_extracts_breadcrumb_and_all_bsr_node_links(self):
        detail = parse_detail_fields(DETAIL_HTML, "US")

        self.assertEqual(detail["breadcrumb_nodes"], [
            {"name": "Root", "node_id": "10"},
            {"name": "Leaf", "node_id": "20"},
        ])
        self.assertEqual(detail["bsr_node_links"], [
            {"name": "Widgets", "node_id": "30", "slug": "widgets"},
            {"name": "Gadget Parts", "node_id": "40", "slug": "gadget-parts"},
        ])
        # The legacy BSR fields are still parsed from the section's text.
        self.assertEqual(detail["bsr_main_rank"], 1)
        self.assertEqual(detail["bsr_sub_rank"], 2)


class TestDetailReverseCategoryWrites(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp.name) / "categories.db"
        self.cache_path = Path(self.tmp.name) / "product_run_cache.db"
        conn = sqlite3.connect(self.db_path)
        conn.execute(migration.CATEGORIES_TABLE_SQL.format(table_name="categories"))
        migration._create_indexes_and_triggers(conn)
        _insert(conn, "Root", "root", "", 0, "root")
        _insert(conn, "Parent one", "p1", "root", 1, "parent-one")
        _insert(
            conn, "Trusted existing", "shared", "p1", 2, "trusted-slug",
            source="sidebar", explored=1,
        )
        conn.commit()
        conn.close()

    def tearDown(self):
        self.tmp.cleanup()

    def _discover(self, detail):
        # The production safety check must execute under TESTING=1 with both
        # writable paths isolated from data/categories.db.
        with patch.dict(os.environ, {"TESTING": "1"}, clear=False), \
             patch.object(config, "PRODUCT_RUN_CACHE_FILE", str(self.cache_path)), \
             patch.object(fp, "DB_PATH", str(self.db_path)), \
             patch.object(fp, "_DOMAIN", "https://www.amazon.com"):
            return fp._discover_category_nodes_from_detail(detail, "US")

    def test_new_edges_are_unexplored_preserve_existing_and_keep_dag(self):
        # Existing p1 -> shared must remain untouched even though a product
        # reports a different name/slug for the same edge.
        first = {
            "breadcrumb_nodes": [
                {"name": "Root", "node_id": "root"},
                {"name": "Changed p1", "node_id": "p1"},
            ],
            "bsr_node_links": [
                {"name": "Untrusted name", "node_id": "shared", "slug": "dirty-slug"},
                {"name": "New BSR child", "node_id": "new", "slug": "new-slug"},
            ],
        }
        self.assertEqual(self._discover(first), 1)
        # A second parent is a distinct valid DAG edge, and a repeat must not
        # create another row.
        second = {
            "breadcrumb_nodes": [
                {"name": "Root", "node_id": "root"},
                {"name": "Parent two", "node_id": "p2"},
            ],
            "bsr_node_links": [
                {"name": "Shared DAG child", "node_id": "shared", "slug": "shared-slug"},
            ],
        }
        # Both the breadcrumb edge root -> p2 and the p2 -> shared DAG edge
        # are new on this page.
        self.assertEqual(self._discover(second), 2)
        self.assertEqual(self._discover(second), 0)

        conn = sqlite3.connect(self.db_path)
        try:
            self.assertEqual(
                conn.execute(
                    "SELECT name, slug, source, explored FROM categories "
                    "WHERE site='US' AND node_id='shared' AND parent_node_id='p1'"
                ).fetchone(),
                ("Trusted existing", "trusted-slug", "sidebar", 1),
            )
            self.assertEqual(
                conn.execute(
                    "SELECT source, explored, depth, parent_node_id, slug FROM categories "
                    "WHERE site='US' AND node_id='new' AND parent_node_id='p1'"
                ).fetchone(),
                ("asin_reverse", 0, 2, "p1", "new-slug"),
            )
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM categories WHERE site='US' "
                    "AND node_id='shared' AND parent_node_id IN ('p1', 'p2')"
                ).fetchone()[0],
                2,
            )
            self.assertEqual(
                conn.execute(
                    "SELECT source, explored, depth, slug FROM categories "
                    "WHERE site='US' AND node_id='shared' AND parent_node_id='p2'"
                ).fetchone(),
                ("asin_reverse", 0, 2, "shared-slug"),
            )
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
