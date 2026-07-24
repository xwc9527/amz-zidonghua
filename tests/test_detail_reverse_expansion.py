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

    def test_breadcrumb_department_id_aligns_to_existing_root_not_a_new_duplicate(self):
        """2026-07-23 实测 bug：面包屑给的部门级 node_id（Amazon 官方数字 ID）
        跟入库时用的 slug 占位根节点 ID 不是一套编号，直接插入会在 depth=0
        造出"名字相同、ID不同"的假根节点（Electronics/172282 等）。"""
        detail = {
            "breadcrumb_nodes": [
                # "Root" 是 setUp 里已有的根节点（node_id="root"），这里面包屑
                # 给出了一个不同的数字 ID，模拟 Amazon 官方部门 ID 和我们入库时
                # slug 占位 ID 不一致的真实情况。
                {"name": "Root", "node_id": "999888777"},
                {"name": "New child of root", "node_id": "newchild"},
            ],
            "bsr_node_links": [],
        }
        added = self._discover(detail)
        # 唯一应该新增的边是 newchild 挂在已有根节点 "root" 下面；不应该
        # 额外插入一个 node_id=999888777 的竞争性新根。
        self.assertEqual(added, 1)

        conn = sqlite3.connect(self.db_path)
        try:
            self.assertIsNone(
                conn.execute(
                    "SELECT 1 FROM categories WHERE site='US' AND node_id='999888777'"
                ).fetchone(),
                "面包屑给的部门数字ID不应该被当成新根节点插入",
            )
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM categories WHERE site='US' AND depth=0"
                ).fetchone()[0],
                1,
                "depth=0 是固定闭集，反向扩展不应该新增根节点",
            )
            self.assertEqual(
                conn.execute(
                    "SELECT parent_node_id, depth FROM categories "
                    "WHERE site='US' AND node_id='newchild'"
                ).fetchone(),
                ("root", 1),
                "新子节点应该挂在已有根节点 'root' 下面，而不是挂在面包屑给的假根节点上",
            )
        finally:
            conn.close()

    def test_bsr_links_are_skipped_when_breadcrumb_is_empty(self):
        """面包屑解析失败/为空时没有可靠的父节点信息；BSR 榜单节点如果仍被插入，
        parent_node_id 只能是空串，会被当成新根节点——同样违反 depth=0 闭集
        不变量，宁可先不落库。"""
        detail = {
            "breadcrumb_nodes": [],
            "bsr_node_links": [
                {"name": "Orphan BSR node", "node_id": "orphan", "slug": "orphan-slug"},
            ],
        }
        added = self._discover(detail)
        self.assertEqual(added, 0)

        conn = sqlite3.connect(self.db_path)
        try:
            self.assertIsNone(
                conn.execute(
                    "SELECT 1 FROM categories WHERE site='US' AND node_id='orphan'"
                ).fetchone(),
                "面包屑为空时不应该把 BSR 节点当成无父根节点插入",
            )
        finally:
            conn.close()


if __name__ == "__main__":
    unittest.main()
