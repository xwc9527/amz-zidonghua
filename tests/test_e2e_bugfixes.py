"""Regression tests for E2E review bugs BUG-E2E-001 … 008."""
from __future__ import annotations

import ast
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import fetch_products as fp
from proxy_pool_manager import (
    STATUS_IDLE,
    STATUS_PREPARING,
    STATUS_PROXY_FAILED,
    STATUS_RETRY_PENDING,
    get_status,
    set_status,
)
from proxy_worker import FetchOutcome


ROOT = Path(__file__).resolve().parents[1]


class TestFlushRemoved(unittest.TestCase):
    def test_run_batch_log_has_no_flush_kwarg(self):
        src = (ROOT / "fetch_products.py").read_text(encoding="utf-8")
        tree = ast.parse(src)
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                for kw in node.keywords:
                    if kw.arg == "flush":
                        self.fail(f"unexpected flush= at line {node.lineno}")


class TestDbPathRespectsEnv(unittest.TestCase):
    def test_db_path_uses_config_db_file(self):
        """子进程验证 AMZ_DB_FILE/DB_FILE 覆盖真实生效，禁止同对象自比较。"""
        import subprocess
        import sys

        with tempfile.TemporaryDirectory() as tmp:
            isolated = Path(tmp) / "isolated_categories.db"
            isolated.write_bytes(b"")
            env = os.environ.copy()
            env["AMZ_DB_FILE"] = str(isolated)
            env["DB_FILE"] = str(isolated)
            env["TESTING"] = "1"
            env.pop("AMZ_DATA_DIR", None)
            code = (
                "import os, fetch_products as fp\n"
                "expected = os.path.abspath(os.environ['AMZ_DB_FILE'])\n"
                "actual = os.path.abspath(fp.DB_PATH)\n"
                "assert actual == expected, (actual, expected)\n"
                "print('DB_PATH_OK', actual)\n"
            )
            proc = subprocess.run(
                [sys.executable, "-c", code],
                cwd=str(ROOT),
                env=env,
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertEqual(
                proc.returncode,
                0,
                f"stdout={proc.stdout!r} stderr={proc.stderr!r}",
            )
            self.assertIn("DB_PATH_OK", proc.stdout)


class TestDescendantExpansion(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "cat.db")
        conn = sqlite3.connect(self.db)
        conn.execute(
            """CREATE TABLE categories (
                   node_id TEXT, url TEXT, name TEXT, depth INTEGER,
                   site TEXT, parent_node_id TEXT
               )"""
        )
        # URL 故意用不相关前缀，验证不再靠 URL LIKE 猜后代
        conn.executemany(
            "INSERT INTO categories VALUES (?,?,?,?,?,?)",
            [
                ("P", "https://example.test/zgbs/parent/P", "Parent", 2, "US", ""),
                ("C1", "https://example.test/gp/new-releases/c1/C1", "Child1", 3, "US", "P"),
                ("C2", "https://example.test/gp/bestsellers/c2/C2", "Child2", 3, "US", "P"),
                ("G", "https://example.test/other/G", "Grand", 4, "US", "C1"),
                ("X", "https://example.test/zgbs/parent/P/extra", "OutOfBranch", 3, "US", "OTHER"),
            ],
        )
        conn.commit()
        conn.close()

    def tearDown(self):
        self.tmp.cleanup()

    def _connect(self):
        conn = sqlite3.connect(self.db)
        conn.row_factory = sqlite3.Row
        return conn

    def test_include_descendants_uses_parent_node_id(self):
        with mock.patch.object(fp, "DB_BACKEND", "sqlite"), \
             mock.patch.object(fp, "db_conn", side_effect=self._connect):
            nodes = fp.get_descendant_nodes(["P"], ["new-releases"], site="US", include_descendants=True)
        ids = {n["node_id"] for n in nodes}
        self.assertEqual(ids, {"P", "C1", "C2", "G"})
        self.assertNotIn("X", ids)

    def test_exact_roots_no_expand(self):
        with mock.patch.object(fp, "DB_BACKEND", "sqlite"), \
             mock.patch.object(fp, "db_conn", side_effect=self._connect):
            nodes = fp.get_descendant_nodes(["P"], ["new-releases"], site="US", include_descendants=False)
        self.assertEqual([n["node_id"] for n in nodes], ["P"])


class TestRankFallback(unittest.TestCase):
    def test_position_written_when_badge_missing(self):
        html = """
        <div id="gridItemRoot">
          <div class="zg-grid-general-faceout">
            <a href="/dp/B0RANK0001"><span>Item A</span></a>
          </div>
        </div>
        <div id="gridItemRoot">
          <div class="zg-grid-general-faceout">
            <a href="/dp/B0RANK0002"><span>Item B</span></a>
          </div>
        </div>
        """
        products = fp.parse_products(
            html, "n1", "Cat", "slug", 3, "new-releases", 50, review_max=0,
            position_start=31,
        )
        self.assertEqual([p["asin"] for p in products], ["B0RANK0001", "B0RANK0002"])
        self.assertEqual([p["rank"] for p in products], [31, 32])


class _SeqClient:
    def __init__(self, list_html, detail_outcomes):
        self.list_html = list_html
        self.detail_outcomes = list(detail_outcomes)
        self.detail_i = 0

    def get(self, url, *, phase, item_id, referer=""):
        if phase == "LIST":
            return FetchOutcome(ok=True, html=self.list_html, status_code=200, attempts=1)
        outcome = self.detail_outcomes[self.detail_i]
        self.detail_i += 1
        return outcome


class TestLinkValidity(unittest.TestCase):
    def test_pg_validity_update_is_scoped_by_site(self):
        cursor = mock.Mock()
        connection = mock.Mock()
        connection.cursor.return_value = cursor
        with mock.patch.object(fp, "DB_BACKEND", "pg"), \
             mock.patch.object(fp, "_get_pg", return_value=connection):
            fp.save_link_validity("shared", "new-releases", 1, site="UK")
        cursor.execute.assert_called_once_with(
            "UPDATE categories SET nr_valid=%s WHERE node_id=%s AND site=%s",
            (1, "shared", "UK"),
        )

    def test_sqlite_validity_is_scoped_by_site_and_legacy_cache_is_not_written(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = os.path.join(tmp, "validity.db")
            conn = sqlite3.connect(db)
            conn.execute(
                "CREATE TABLE categories("
                "node_id TEXT, site TEXT, nr_valid INTEGER, "
                "PRIMARY KEY(node_id, site))"
            )
            conn.execute(
                "CREATE TABLE link_cache("
                "node_id TEXT PRIMARY KEY, nr_valid INTEGER, checked_at TEXT)"
            )
            conn.executemany(
                "INSERT INTO categories VALUES (?,?,NULL)",
                [("shared", "US"), ("shared", "UK")],
            )
            conn.commit()
            conn.close()

            def connect():
                return sqlite3.connect(db)

            with mock.patch.object(fp, "DB_BACKEND", "sqlite"), \
                 mock.patch.object(fp, "db_conn", side_effect=connect):
                fp.save_link_validity("shared", "new-releases", 1, site="US")

            conn = sqlite3.connect(db)
            rows = conn.execute(
                "SELECT site,nr_valid FROM categories ORDER BY site"
            ).fetchall()
            cache_count = conn.execute("SELECT COUNT(*) FROM link_cache").fetchone()[0]
            conn.close()
            self.assertEqual(rows, [("UK", None), ("US", 1)])
            self.assertEqual(cache_count, 0)

    def test_http_200_empty_page_is_invalid(self):
        node = {
            "node_id": "empty",
            "url": "https://www.amazon.com/gp/new-releases/books/empty/",
            "name": "Empty",
            "depth": 2,
        }
        client = _SeqClient("<html><body>No products</body></html>", [])
        with mock.patch.object(fp, "save_link_validity") as save_validity:
            status, error, found, _ = fp.process_node(
                node,
                ["new-releases"],
                review_max=0,
                min_list_size=0,
                client=client,
                max_pages=1,
                delay=0.0,
            )
        self.assertEqual((status, error, found), ("done", "", 0))
        save_validity.assert_called_once_with(
            "empty", "new-releases", 0, site=fp._SITE,
        )

    def test_page2_does_not_override_page1_validity_and_keeps_canonical_query(self):
        product_html = (
            '<div id="gridItemRoot">'
            '<a href="/dp/B0000000A1"><span>A</span></a></div>'
        )

        class Client:
            def __init__(self):
                self.urls = []

            def get(self, url, *, phase, item_id, referer=""):
                self.urls.append(url)
                html = product_html if len(self.urls) == 1 else "<html>empty</html>"
                return FetchOutcome(ok=True, html=html, status_code=200, attempts=1)

        node = {
            "node_id": "canonical",
            "url": "https://www.amazon.com/gp/bestsellers/books/",
            "name": "Books",
            "depth": 1,
            "canonical_list_url": (
                "https://www.amazon.com/gp/bestsellers/books/"
                "?ref_=zg_bs_tab_bsms"
            ),
            "canonical_list_type": "bestsellers",
        }
        client = Client()
        with mock.patch.object(fp, "save_link_validity") as save_validity, \
             mock.patch.object(fp, "save_products", return_value=1), \
             mock.patch.object(fp, "enrich_with_details", return_value=0):
            fp.process_node(
                node,
                ["bestsellers"],
                review_max=0,
                min_list_size=0,
                client=client,
                max_pages=2,
                delay=0.0,
            )
        save_validity.assert_called_once_with(
            "canonical", "bestsellers", 1, site=fp._SITE,
        )
        self.assertEqual(
            client.urls[1],
            node["canonical_list_url"] + "&pg=2",
        )


class TestPartialDetailFailure(unittest.TestCase):
    _NODE = {
        "node_id": "n1",
        "url": "https://www.amazon.com/gp/new-releases/some-slug/n1/",
        "name": "Cat",
        "depth": 3,
    }
    _LIST_HTML = (
        '<div id="gridItemRoot"><a href="/dp/B0000000A1"><span>A</span></a></div>'
        '<div id="gridItemRoot"><a href="/dp/B0000000B2"><span>B</span></a></div>'
    )

    def setUp(self):
        with fp._seen_lock:
            fp._seen_asins.clear()

    def test_partial_detail_failure_marks_node_error_for_retry(self):
        ok = FetchOutcome(ok=True, html="<html>d</html>", status_code=200, attempts=1)
        bad = FetchOutcome(ok=False, error_code="TLS_ERROR", final_reason="TLS", status_code=0, attempts=3)
        client = _SeqClient(self._LIST_HTML, [ok, bad])

        with mock.patch.object(fp, "save_link_validity"), \
             mock.patch.object(fp, "save_products", return_value=2), \
             mock.patch.object(fp, "_load_cached_detail", return_value=None), \
             mock.patch.object(fp, "parse_detail_fields", return_value={"price": 1.0}), \
             mock.patch.object(fp, "estimate_fba_fees", return_value={}), \
             mock.patch.object(fp, "_check_detail_filters", return_value=True), \
             mock.patch.object(fp, "_update_sighting_detail"), \
             mock.patch.object(fp, "_mark_detail_failed") as mark_failed:
            status, err_code, found, _ = fp.process_node(
                self._NODE, ["new-releases"], review_max=0, min_list_size=0,
                client=client, max_pages=1, delay=0.0,
            )

        self.assertEqual(status, "error")
        self.assertTrue(err_code.startswith("DETAIL_PARTIAL:"))
        self.assertEqual(found, 2)
        mark_failed.assert_called_once()

    def test_empty_detail_parse_counts_as_failure(self):
        list_html = '<div id="gridItemRoot"><a href="/dp/B0000000C3"><span>C</span></a></div>'
        client = _SeqClient(
            list_html,
            [FetchOutcome(ok=True, html="<html></html>", status_code=200, attempts=1)],
        )
        with mock.patch.object(fp, "save_link_validity"), \
             mock.patch.object(fp, "save_products", return_value=1), \
             mock.patch.object(fp, "_load_cached_detail", return_value=None), \
             mock.patch.object(fp, "parse_detail_fields", return_value={}), \
             mock.patch.object(fp, "_mark_detail_failed") as mark_failed:
            status, err_code, found, _ = fp.process_node(
                self._NODE, ["new-releases"], review_max=0, min_list_size=0,
                client=client, max_pages=1, delay=0.0,
            )
        self.assertEqual(status, "error")
        self.assertTrue(err_code.startswith("DETAIL_FETCH_FAILED"))
        mark_failed.assert_called_once()
        self.assertEqual(mark_failed.call_args[0][0], "B0000000C3")

    def test_enrich_reuses_cached_detail_without_network(self):
        products = [
            {
                "asin": "B0000000A1",
                "product_url": "https://www.amazon.com/dp/B0000000A1",
                "node_id": "n1",
                "list_type": "bestsellers",
            },
            {
                "asin": "B0000000B2",
                "product_url": "https://www.amazon.com/dp/B0000000B2",
                "node_id": "n1",
                "list_type": "bestsellers",
            },
        ]
        client = mock.Mock()
        client.get.return_value = FetchOutcome(
            ok=False, error_code="TLS_ERROR", final_reason="TLS", status_code=0, attempts=1,
        )
        client.pool = None
        cached = {"price": 9.9, "rating": 4.5, "detail_scraped": 1}

        def load_cached(asin):
            return cached if asin == "B0000000A1" else None

        with mock.patch.object(fp, "_load_cached_detail", side_effect=load_cached), \
             mock.patch.object(fp, "_finalize_detail", return_value=True) as finalize, \
             mock.patch.object(fp, "_mark_detail_failed") as mark_failed:
            failures = fp.enrich_with_details(products, client, delay=0.0, filters={"rating_min": 4.0})
        self.assertEqual(failures, 1)
        self.assertEqual(client.get.call_count, 1)
        finalize.assert_called_once()
        self.assertEqual(finalize.call_args[0][0]["asin"], "B0000000A1")
        self.assertEqual(finalize.call_args[0][1], cached)
        mark_failed.assert_called_once()
        self.assertEqual(mark_failed.call_args[0][0], "B0000000B2")

    def test_finalize_detail_refilters_and_scoped_delete(self):
        product = {
            "asin": "B0000000A1",
            "node_id": "n2",
            "list_type": "bestsellers",
            "price": 10.0,
        }
        detail = {"price": 10.0, "rating": 3.0}
        with mock.patch.object(fp, "_attach_normalized_dims"), \
             mock.patch.object(fp, "_check_detail_filters", return_value=False), \
             mock.patch.object(fp, "_delete_sighting") as delete_mock, \
             mock.patch.object(fp, "_update_sighting_detail") as update_mock, \
             mock.patch.dict(fp._stats, {"products_saved": 1}, clear=False):
            kept = fp._finalize_detail(product, detail, {"rating_min": 4.0})
        self.assertFalse(kept)
        delete_mock.assert_called_once_with(
            "B0000000A1", node_id="n2", list_type="bestsellers",
        )
        update_mock.assert_not_called()

    def test_finalize_detail_prefers_current_price_and_recomputes_fees(self):
        product = {
            "asin": "B0000000A1",
            "node_id": "n1",
            "list_type": "bestsellers",
            "price": 9.0,
        }
        # 缓存里是旧高价 + 旧费用；当前榜单已降到 Low-Price 区间
        detail = {
            "price": 12.0,
            "fba_fee": 4.5,
            "placement_fee": 1.0,
            "item_weight": "1 lb",
            "item_dimensions": "10 x 8 x 2 inches",
            "rating": 4.5,
        }
        with mock.patch.object(fp, "_attach_normalized_dims"), \
             mock.patch.object(
                 fp, "estimate_fba_fees",
                 return_value={"fba_fee": 3.22, "placement_fee": 0.5},
             ) as fee_mock, \
             mock.patch.object(fp, "_check_detail_filters", return_value=True), \
             mock.patch.object(fp, "_update_sighting_detail") as update_mock:
            kept = fp._finalize_detail(product, detail, {})
        self.assertTrue(kept)
        fee_mock.assert_called_once_with("US", "1 lb", "10 x 8 x 2 inches", 9.0)
        written = update_mock.call_args[0][1]
        self.assertEqual(written["price"], 9.0)
        self.assertEqual(written["fba_fee"], 3.22)
        self.assertEqual(written["placement_fee"], 0.5)
        self.assertNotIn("price", fp._CACHED_DETAIL_KEYS)
        self.assertNotIn("fba_fee", fp._CACHED_DETAIL_KEYS)
        self.assertNotIn("placement_fee", fp._CACHED_DETAIL_KEYS)


class TestStatusDetailReset(unittest.TestCase):
    def test_idle_clears_previous_error_detail(self):
        set_status(
            STATUS_PROXY_FAILED,
            run_id="r1",
            error_code="CRAWLER_EXIT_FAILED",
            return_code=1,
            reason="boom",
        )
        set_status(STATUS_IDLE, run_id="r2", pool_ready=True)
        detail = get_status()["detail"]
        self.assertEqual(detail.get("pool_ready"), True)
        self.assertNotIn("error_code", detail)
        self.assertNotIn("return_code", detail)
        set_status(STATUS_PREPARING)
        self.assertEqual(get_status()["detail"], {})

    def test_retry_pending_replaces_detail(self):
        set_status(STATUS_PROXY_FAILED, run_id="r1", error_code="OLD", return_code=1)
        set_status(
            STATUS_RETRY_PENDING,
            run_id="r2",
            pool_ready=True,
            error_code="CRAWL_RETRY_PENDING",
            return_code=4,
        )
        detail = get_status()["detail"]
        self.assertEqual(get_status()["status"], STATUS_RETRY_PENDING)
        self.assertEqual(detail.get("error_code"), "CRAWL_RETRY_PENDING")
        self.assertNotEqual(detail.get("error_code"), "OLD")
        self.assertTrue(detail.get("pool_ready"))


class TestTreeSearchRoot(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "cat.db")
        conn = sqlite3.connect(self.db)
        conn.execute(
            """CREATE TABLE categories (
                   name TEXT, url TEXT, node_id TEXT, depth INTEGER,
                   parent_node_id TEXT, site TEXT, slug TEXT, na_valid INTEGER
               )"""
        )
        conn.execute(
            "CREATE INDEX idx_categories_parent_site ON categories(parent_node_id, site)"
        )
        conn.execute(
            "CREATE INDEX idx_categories_node_site ON categories(node_id, site, parent_node_id)"
        )
        conn.executemany(
            "INSERT INTO categories VALUES (?,?,?,?,?,?,?,?)",
            [
                ("Home", "u", "home", 0, "", "US", "home", 0),
                ("Blankets & Throws", "u", "bt", 3, "home", "US", "blankets", 0),
                ("Kitchen", "u", "kit", 1, "home", "US", "kitchen", 0),
            ],
        )
        conn.commit()
        conn.close()
        import api_server
        self.api = api_server
        self._patches = [
            mock.patch.object(api_server, "DB_PATH", self.db),
            mock.patch.object(api_server, "DB_BACKEND", "sqlite"),
        ]
        for p in self._patches:
            p.start()

    async def asyncTearDown(self):
        for p in self._patches:
            p.stop()
        self.tmp.cleanup()

    async def test_root_search_finds_nested_name(self):
        rows = await self.api._tree_children_sqlite("root", "Blankets & Throws", 50, 0, "US", 0)
        self.assertEqual([r["node_id"] for r in rows], ["bt"])
        self.assertEqual(rows[0]["name"], "Blankets & Throws")

    async def test_root_without_q_still_lists_roots(self):
        rows = await self.api._tree_children_sqlite("root", "", 50, 0, "US", 0)
        self.assertEqual([r["node_id"] for r in rows], ["home"])


if __name__ == "__main__":
    unittest.main()
