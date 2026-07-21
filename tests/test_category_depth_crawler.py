import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import fetch_products


class TestCategoryDepthCrawler(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "categories.db")
        conn = sqlite3.connect(self.db_path)
        conn.execute(
            """CREATE TABLE categories (
                   node_id TEXT, url TEXT, name TEXT, depth INTEGER, site TEXT
               )"""
        )
        conn.executemany(
            "INSERT INTO categories VALUES (?, ?, ?, ?, ?)",
            [
                ("R", "https://example.test/root", "Root", 0, "US"),
                ("A", "https://example.test/a", "A", 1, "US"),
                ("B", "https://example.test/b", "B", 2, "US"),
                ("B", "https://example.test/b-copy", "B duplicate", 2, "US"),
                ("C", "https://example.test/c", "C leaf", 3, "US"),
                ("D", "https://example.test/de", "DE root", 0, "DE"),
            ],
        )
        conn.commit()
        conn.close()

    def tearDown(self):
        self.tmp.cleanup()

    def _connect(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def test_exact_depth_keeps_root_and_leaf_and_respects_site(self):
        with patch.object(fetch_products, "DB_BACKEND", "sqlite"), \
             patch.object(fetch_products, "db_conn", side_effect=self._connect):
            roots = fetch_products.get_nodes_by_depth([0], site="US")
            leaves = fetch_products.get_nodes_by_depth([3], site="US")
            level2 = fetch_products.get_nodes_by_depth([2], site="US")
            combined = fetch_products.get_nodes_by_depth([0, 3], site="US")
        self.assertEqual([row["node_id"] for row in roots], ["R"])
        self.assertEqual([row["node_id"] for row in leaves], ["C"])
        self.assertEqual([row["node_id"] for row in level2], ["B"])
        self.assertEqual({row["node_id"] for row in combined}, {"R", "C"})

        conn = sqlite3.connect(self.db_path)
        conn.execute(
            "INSERT INTO categories VALUES (?, ?, ?, ?, ?)",
            ("B", "https://example.test/b-deeper", "B deeper duplicate", 3, "US"),
        )
        conn.commit()
        conn.close()
        with patch.object(fetch_products, "DB_BACKEND", "sqlite"), \
             patch.object(fetch_products, "db_conn", side_effect=self._connect):
            descendants = fetch_products.get_nodes_by_depth([2, 3], site="US")
        self.assertEqual({row["node_id"] for row in descendants}, {"B", "C"})
        self.assertEqual(len(descendants), 2)


if __name__ == "__main__":
    unittest.main()
