import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

os.environ.setdefault("DB_BACKEND", "sqlite")

import api_server


class TestCategoryScopeCount(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "categories.db")
        conn = sqlite3.connect(self.db_path)
        conn.execute(
            """CREATE TABLE categories (
                   node_id TEXT, name TEXT, depth INTEGER,
                   parent_node_id TEXT, site TEXT, slug TEXT, na_valid INTEGER
               )"""
        )
        conn.executemany(
            "INSERT INTO categories VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                ("R", "US root", 0, None, "US", "us-root", 0),
                ("A", "Navigation A", 1, "R", "US", "a", 0),
                ("B", "New B", 2, "A", "US", "b", 1),
                ("C", "New C", 3, "B", "US", "c", 1),
                ("D", "Irrelevant D", 1, "R", "US", "d", 0),
                ("DR", "DE root", 0, None, "DE", "de-root", 0),
                ("B", "Other-site B", 1, "DR", "DE", "de-b", 1),
                ("E", "Other-site child", 2, "B", "DE", "de-e", 1),
            ],
        )
        conn.commit()
        conn.close()
        self.patches = (
            patch.object(api_server, "DB_BACKEND", "sqlite"),
            patch.object(api_server, "DB_PATH", self.db_path),
        )
        for item in self.patches:
            item.start()

    def tearDown(self):
        for item in reversed(self.patches):
            item.stop()
        self.tmp.cleanup()

    async def test_descendants_match_crawler_union_and_dedupe_overlapping_roots(self):
        result = await api_server.category_scope_count(
            {"site": "US", "roots": ["A", "B", "A"], "include_descendants": True}
        )
        self.assertEqual(result["selected_count"], 2)
        self.assertEqual(result["count"], 3)

    async def test_selected_only_counts_unique_existing_roots(self):
        result = await api_server.category_scope_count(
            {"site": "US", "roots": ["A", "B", "A", "missing"], "include_descendants": False}
        )
        self.assertEqual(result["selected_count"], 3)
        self.assertEqual(result["count"], 2)

    async def test_site_boundary_is_preserved(self):
        result = await api_server.category_scope_count(
            {"site": "DE", "roots": ["B"], "include_descendants": True}
        )
        self.assertEqual(result["count"], 2)

    async def test_empty_selection(self):
        result = await api_server.category_scope_count({"site": "US", "roots": []})
        self.assertEqual(result["count"], 0)

    async def test_latest_arrivals_tree_keeps_only_new_branches(self):
        roots = await api_server._tree_children_sqlite("root", "", 50, 0, "US", 1)
        self.assertEqual([r["node_id"] for r in roots], ["R"])
        self.assertEqual(roots[0]["na_valid"], 0)
        self.assertEqual(roots[0]["child_count"], 2)

        level1 = await api_server._tree_children_sqlite("R", "", 50, 0, "US", 1)
        self.assertEqual([r["node_id"] for r in level1], ["A"])
        self.assertEqual(level1[0]["na_valid"], 0)
        self.assertEqual(level1[0]["child_count"], 2)

        level2 = await api_server._tree_children_sqlite("A", "", 50, 0, "US", 1)
        self.assertEqual([r["node_id"] for r in level2], ["B"])
        self.assertEqual(level2[0]["na_valid"], 1)
        self.assertEqual(level2[0]["child_count"], 1)

        level3 = await api_server._tree_children_sqlite("B", "", 50, 0, "US", 1)
        self.assertEqual([r["node_id"] for r in level3], ["C"])
        self.assertEqual(level3[0]["na_valid"], 1)
        self.assertEqual(level3[0]["child_count"], 0)

    async def test_latest_arrivals_tree_respects_site_and_level_search(self):
        de_roots = await api_server._tree_children_sqlite("root", "", 50, 0, "DE", 1)
        self.assertEqual([r["node_id"] for r in de_roots], ["DR"])
        self.assertEqual(de_roots[0]["child_count"], 2)

        matched = await api_server._tree_children_sqlite("A", "New B", 50, 0, "US", 1)
        self.assertEqual([r["node_id"] for r in matched], ["B"])
        missing = await api_server._tree_children_sqlite("A", "Irrelevant", 50, 0, "US", 1)
        self.assertEqual(missing, [])


if __name__ == "__main__":
    unittest.main()
