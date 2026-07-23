"""运行缓存隔离与代次切换单测（临时路径，禁止碰正式库）。"""
import os
import tempfile
import unittest


class TestProductRunCache(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "cats.db")
        self.cache = os.path.join(self.tmp.name, "run_cache.db")
        os.environ["TESTING"] = "1"
        os.environ["AMZ_DB_FILE"] = self.db
        os.environ["DB_FILE"] = self.db
        os.environ["AMZ_RUN_CACHE_FILE"] = self.cache
        os.environ["PRODUCT_RESULT_MODE"] = "run_cache"
        # 强制重载配置与缓存模块
        import importlib
        import config
        import product_run_cache as rc
        importlib.reload(config)
        importlib.reload(rc)
        self.config = config
        self.rc = rc
        self.rc._INITIALIZED = False
        self.config.assert_testing_paths_safe()
        self.rc.ensure_schema()

    def tearDown(self):
        self.tmp.cleanup()
        for k in ("TESTING", "AMZ_DB_FILE", "DB_FILE", "AMZ_RUN_CACHE_FILE", "PRODUCT_RESULT_MODE"):
            os.environ.pop(k, None)

    def test_testing_rejects_formal_paths(self):
        os.environ["AMZ_RUN_CACHE_FILE"] = os.path.join(
            self.config.DATA_DIR, "product_run_cache.db"
        )
        import importlib
        import config
        importlib.reload(config)
        with self.assertRaises(RuntimeError):
            config.assert_testing_paths_safe()

    def test_generation_switch_and_purge(self):
        self.rc.create_generation("RUN-A", "products")
        self.rc.activate_generation("RUN-A", "products")
        self.rc.upsert_products(
            [{"asin": "B000000001", "name": "A1", "node_id": "n1", "list_type": "bestsellers", "site": "US", "detail_scraped": 1}],
            run_id="RUN-A", chart="products",
        )
        self.assertEqual(self.rc.stats()["total_asins"], 1)

        self.rc.create_generation("RUN-B", "products")
        # 启动失败：不 activate，旧缓存仍在
        self.assertEqual(self.rc.get_active_run_id(), "RUN-A")
        self.assertEqual(self.rc.stats()["total_asins"], 1)

        self.rc.activate_generation("RUN-B", "products")
        self.assertEqual(self.rc.get_active_run_id(), "RUN-B")
        self.assertEqual(self.rc.stats()["total_asins"], 0)

    def test_same_run_detail_reuse_only(self):
        self.rc.create_generation("RUN-1", "products")
        self.rc.activate_generation("RUN-1", "products")
        self.rc.upsert_products(
            [{"asin": "B000000002", "name": "X", "node_id": "n1", "list_type": "new-releases", "site": "US"}],
            run_id="RUN-1",
        )
        self.rc.update_detail(
            "B000000002",
            {"bsr_main_rank": 12, "detail_scraped": 1, "social_proof_count": 100},
            run_id="RUN-1", site="US", node_id="n1", list_type="new-releases",
        )
        hit = self.rc.load_cached_detail("B000000002", run_id="RUN-1", site="US")
        self.assertIsNotNone(hit)
        self.assertEqual(hit["bsr_main_rank"], 12)
        miss = self.rc.load_cached_detail("B000000002", run_id="RUN-OTHER", site="US")
        self.assertIsNone(miss)

    def test_batch_detail_updates_are_atomic_and_scoped(self):
        self.rc.create_generation("RUN-BATCH", "products")
        self.rc.activate_generation("RUN-BATCH", "products")
        self.rc.upsert_products(
            [
                {"asin": "B000000003", "node_id": "n1", "list_type": "bestsellers", "site": "US"},
                {"asin": "B000000004", "node_id": "n2", "list_type": "new-releases", "site": "US"},
            ],
            run_id="RUN-BATCH",
        )
        written = self.rc.update_details_batch([
            {
                "asin": "B000000003",
                "detail": {"bsr_main_rank": 7, "detail_scraped": 1},
                "run_id": "RUN-BATCH",
                "site": "US",
                "node_id": "n1",
                "list_type": "bestsellers",
            },
            {
                "asin": "B000000004",
                "detail": {"detail_scraped": 2, "detail_status": "failed"},
                "run_id": "RUN-BATCH",
                "site": "US",
                "node_id": "n2",
                "list_type": "new-releases",
            },
        ])
        self.assertEqual(written, 2)
        hit = self.rc.load_cached_detail(
            "B000000003", run_id="RUN-BATCH", site="US",
        )
        self.assertEqual(hit["bsr_main_rank"], 7)
        self.assertIsNone(self.rc.load_cached_detail(
            "B000000004", run_id="RUN-BATCH", site="US",
        ))

    def test_resume_mismatch(self):
        self.rc.create_generation("RUN-1", "products")
        self.rc.activate_generation("RUN-1", "products")
        with self.assertRaises(RuntimeError):
            self.rc.open_existing_generation("RUN-2")


class TestFavoritesSqlite(unittest.TestCase):
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

    def tearDown(self):
        self.tmp.cleanup()
        for k in ("TESTING", "AMZ_DB_FILE", "DB_FILE", "AMZ_RUN_CACHE_FILE", "PRODUCT_RESULT_MODE"):
            os.environ.pop(k, None)

    def test_favorite_upsert_idempotent(self):
        self.rc.create_generation("RUN-F", "products")
        self.rc.activate_generation("RUN-F", "products")
        self.rc.upsert_products(
            [{
                "asin": "B000000099", "name": "FavItem", "price": 19.9,
                "node_id": "n9", "list_type": "bestsellers", "site": "US",
                "detail_scraped": 1, "bsr_main_rank": 3,
            }],
            run_id="RUN-F",
        )
        rows = self.rc.query_products({"site": "US", "detail_only": True, "limit": 10})
        self.assertEqual(len(rows), 1)
        cache_row = rows[0]
        a = self.fav.upsert_favorite_from_cache("sqlite", cache_row)
        b = self.fav.upsert_favorite_from_cache("sqlite", cache_row)
        self.assertEqual(a["asin"], "B000000099")
        self.assertEqual(b["asin"], "B000000099")
        self.assertEqual(self.fav.count_favorites("sqlite", "US"), 1)
        deleted = self.fav.delete_favorite("sqlite", "US", "B000000099")
        self.assertTrue(deleted)
        self.assertEqual(self.fav.count_favorites("sqlite", "US"), 0)


if __name__ == "__main__":
    unittest.main()
