"""隔离 E2E：抓取写入缓存不落正式表；收藏写入正式 favorite_products。"""
from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest


class TestFavoritesIsolationE2E(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "categories.db")
        self.cache = os.path.join(self.tmp.name, "product_run_cache.db")
        os.environ["TESTING"] = "1"
        os.environ["AMZ_DB_FILE"] = self.db
        os.environ["DB_FILE"] = self.db
        os.environ["AMZ_RUN_CACHE_FILE"] = self.cache
        os.environ["PRODUCT_RESULT_MODE"] = "run_cache"
        os.environ["DB_BACKEND"] = "sqlite"
        # 最小正式库 schema
        con = sqlite3.connect(self.db)
        con.executescript(
            """
            CREATE TABLE product_sightings (
              id INTEGER PRIMARY KEY, asin TEXT, name TEXT, site TEXT,
              node_id TEXT, list_type TEXT, detail_scraped INTEGER DEFAULT 0
            );
            CREATE TABLE new_arrivals (
              id INTEGER PRIMARY KEY, asin TEXT, title TEXT, site TEXT, node_id TEXT
            );
            """
        )
        con.close()
        import importlib
        import config
        import product_run_cache as rc
        import favorite_products as fav
        importlib.reload(config)
        importlib.reload(rc)
        importlib.reload(fav)
        self.config = config
        self.rc = rc
        self.fav = fav
        self.rc._INITIALIZED = False
        self.rc.ensure_schema()
        self.fav.ensure_sqlite_schema(self.db)

        # 让 fetch_products 走缓存
        import fetch_products as fp
        importlib.reload(fp)
        self.fp = fp
        self.fp._SITE = "US"
        self.fp._RUN_ID = "E2E-RUN-1"
        self.fp.DB_BACKEND = "sqlite"

    def tearDown(self):
        self.tmp.cleanup()
        for k in (
            "TESTING", "AMZ_DB_FILE", "DB_FILE", "AMZ_RUN_CACHE_FILE",
            "PRODUCT_RESULT_MODE", "DB_BACKEND",
        ):
            os.environ.pop(k, None)

    def _formal_counts(self):
        con = sqlite3.connect(self.db)
        try:
            ps = con.execute("SELECT COUNT(*) FROM product_sightings").fetchone()[0]
            na = con.execute("SELECT COUNT(*) FROM new_arrivals").fetchone()[0]
            try:
                fav = con.execute("SELECT COUNT(*) FROM favorite_products").fetchone()[0]
            except Exception:
                fav = 0
            return ps, na, fav
        finally:
            con.close()

    def test_crawl_writes_cache_not_formal(self):
        self.rc.create_generation("E2E-RUN-1", "products")
        self.rc.activate_generation("E2E-RUN-1", "products")
        n = self.fp.save_products([{
            "asin": "B0E2E00001", "name": "Iso", "price": 12.5,
            "node_id": "node1", "list_type": "bestsellers", "site": "US",
        }])
        self.assertEqual(n, 1)
        self.fp._update_sighting_detail(
            "B0E2E00001",
            {"detail_scraped": 1, "bsr_main_rank": 9, "run_id": "E2E-RUN-1"},
            node_id="node1", list_type="bestsellers",
        )
        ps, na, fav = self._formal_counts()
        self.assertEqual(ps, 0)
        self.assertEqual(na, 0)
        self.assertEqual(fav, 0)
        rows = self.rc.query_products({"site": "US", "detail_only": True})
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["asin"], "B0E2E00001")

        # 收藏
        saved = self.fav.upsert_favorite_from_cache("sqlite", rows[0])
        self.assertEqual(saved["asin"], "B0E2E00001")
        ps, na, fav = self._formal_counts()
        self.assertEqual(ps, 0)
        self.assertEqual(na, 0)
        self.assertEqual(fav, 1)

        # 换代后旧 cache_id 视为过期
        self.rc.create_generation("E2E-RUN-2", "products")
        self.rc.activate_generation("E2E-RUN-2", "products")
        self.assertIsNone(self.rc.get_by_cache_id(rows[0]["cache_id"], run_id="E2E-RUN-1"))


if __name__ == "__main__":
    unittest.main()
