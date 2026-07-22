"""缓存写入 / 收藏 / 换代并发冒烟。"""
from __future__ import annotations

import os
import tempfile
import threading
import unittest


class TestRunCacheConcurrency(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "cats.db")
        self.cache = os.path.join(self.tmp.name, "run_cache.db")
        os.environ["TESTING"] = "1"
        os.environ["AMZ_DB_FILE"] = self.db
        os.environ["DB_FILE"] = self.db
        os.environ["AMZ_RUN_CACHE_FILE"] = self.cache
        os.environ["PRODUCT_RESULT_MODE"] = "run_cache"
        import importlib
        import config
        import product_run_cache as rc
        import favorite_products as fav
        importlib.reload(config)
        importlib.reload(rc)
        importlib.reload(fav)
        self.rc = rc
        self.fav = fav
        self.rc._INITIALIZED = False
        self.rc.ensure_schema()
        self.fav.ensure_sqlite_schema(self.db)
        self.rc.create_generation("CONC-1", "products")
        self.rc.activate_generation("CONC-1", "products")
        self.errors = []

    def tearDown(self):
        self.tmp.cleanup()
        for k in ("TESTING", "AMZ_DB_FILE", "DB_FILE", "AMZ_RUN_CACHE_FILE", "PRODUCT_RESULT_MODE"):
            os.environ.pop(k, None)

    def test_parallel_upsert_and_favorite(self):
        def writer(i):
            try:
                asin = f"B0CONC{i:04d}"
                self.rc.upsert_products(
                    [{
                        "asin": asin, "name": f"P{i}", "node_id": "n1",
                        "list_type": "bestsellers", "site": "US", "detail_scraped": 1,
                    }],
                    run_id="CONC-1",
                )
            except Exception as e:
                self.errors.append(e)

        threads = [threading.Thread(target=writer, args=(i,)) for i in range(30)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(self.errors, [])
        rows = self.rc.query_products({"site": "US", "limit": 200, "detail_only": True})
        self.assertGreaterEqual(len(rows), 30)
        # 收藏其中一条 + 换代竞态：换代后旧 cache 不可收藏
        row = rows[0]
        self.fav.upsert_favorite_from_cache("sqlite", row)
        self.assertEqual(self.fav.count_favorites("sqlite", "US"), 1)
        self.rc.create_generation("CONC-2", "products")
        self.rc.activate_generation("CONC-2", "products")
        self.assertIsNone(self.rc.get_by_cache_id(row["cache_id"], run_id="CONC-1"))
        self.assertEqual(self.fav.count_favorites("sqlite", "US"), 1)


if __name__ == "__main__":
    unittest.main()
