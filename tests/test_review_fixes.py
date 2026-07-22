"""审查修复验收：代次锁、导出分页、TESTING 保护、清表 idle、PG DSN、稳定排序。"""
from __future__ import annotations

import json
import os
import tempfile
import threading
import time
import unittest
from unittest import mock


class _EnvCase(unittest.TestCase):
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
        self.config = config
        self.rc = rc
        self.fav = fav
        self.rc._INITIALIZED = False
        self.rc.ensure_schema()
        self.fav.ensure_sqlite_schema(self.db)

    def tearDown(self):
        self.tmp.cleanup()
        for k in (
            "TESTING", "AMZ_DB_FILE", "DB_FILE", "AMZ_RUN_CACHE_FILE", "PRODUCT_RESULT_MODE",
            "PG_DSN", "PG_TEST_DSN", "PG_TEST_DB_ALLOWLIST",
        ):
            os.environ.pop(k, None)


class TestGenerationFavoriteRace(_EnvCase):
    def test_locked_favorite_blocks_activate_until_done(self):
        self.rc.create_generation("RACE-1", "products")
        self.rc.activate_generation("RACE-1", "products")
        self.rc.upsert_products(
            [{
                "asin": "B0RACE0001", "name": "Race", "node_id": "n1",
                "list_type": "bestsellers", "site": "US", "detail_scraped": 1,
            }],
            run_id="RACE-1",
        )
        row = self.rc.query_products({"site": "US", "detail_only": True})[0]
        events = []
        err = []

        def favoriter():
            try:
                with self.rc.locked_active_cache_item(row["cache_id"], "RACE-1") as cached:
                    events.append("fav_locked")
                    time.sleep(0.35)
                    self.fav.upsert_favorite_from_cache("sqlite", cached)
                    events.append("fav_written")
            except Exception as e:
                err.append(e)

        def activator():
            while "fav_locked" not in events and not err:
                time.sleep(0.01)
            t0 = time.time()
            try:
                self.rc.create_generation("RACE-2", "products")
                self.rc.activate_generation("RACE-2", "products")
                events.append(("activated", time.time() - t0))
            except Exception as e:
                err.append(e)

        t1 = threading.Thread(target=favoriter)
        t2 = threading.Thread(target=activator)
        t1.start()
        t2.start()
        t1.join(5)
        t2.join(5)
        self.assertEqual(err, [])
        self.assertIn("fav_written", events)
        act = [e for e in events if isinstance(e, tuple) and e[0] == "activated"]
        self.assertEqual(len(act), 1)
        self.assertGreaterEqual(act[0][1], 0.25)
        self.assertEqual(self.fav.count_favorites("sqlite", "US"), 1)
        self.assertEqual(self.rc.get_active_run_id(), "RACE-2")
        with self.assertRaises(self.rc.StaleCacheError):
            with self.rc.locked_active_cache_item(row["cache_id"], "RACE-1"):
                pass

    def test_stale_after_activate_returns_conflict(self):
        self.rc.create_generation("STALE-1", "products")
        self.rc.activate_generation("STALE-1", "products")
        self.rc.upsert_products(
            [{
                "asin": "B0STALE001", "name": "S", "node_id": "n1",
                "list_type": "bestsellers", "site": "US", "detail_scraped": 1,
            }],
            run_id="STALE-1",
        )
        row = self.rc.query_products({"site": "US", "detail_only": True})[0]
        self.rc.create_generation("STALE-2", "products")
        self.rc.activate_generation("STALE-2", "products")
        with self.assertRaises(self.rc.StaleCacheError) as cm:
            with self.rc.locked_active_cache_item(row["cache_id"], "STALE-1"):
                pass
        self.assertEqual(cm.exception.error_code, "STALE_CACHE_ITEM")

    def test_cancel_generation_keeps_active(self):
        self.rc.create_generation("KEEP-1", "products")
        self.rc.activate_generation("KEEP-1", "products")
        self.rc.upsert_products(
            [{
                "asin": "B0KEEP0001", "name": "K", "node_id": "n1",
                "list_type": "bestsellers", "site": "US", "detail_scraped": 1,
            }],
            run_id="KEEP-1",
        )
        self.rc.create_generation("KEEP-2", "products")
        self.rc.cancel_generation("KEEP-2")
        self.assertEqual(self.rc.get_active_run_id(), "KEEP-1")
        self.assertEqual(self.rc.stats()["total_asins"], 1)


class TestExportAndTestingGuard(_EnvCase):
    def test_list_all_favorites_beyond_200(self):
        self.rc.create_generation("EXP-1", "products")
        self.rc.activate_generation("EXP-1", "products")
        for i in range(250):
            asin = f"B0E{i:07d}"
            self.rc.upsert_products(
                [{
                    "asin": asin, "name": f"N{i}", "node_id": "n1",
                    "list_type": "bestsellers", "site": "US", "detail_scraped": 1,
                }],
                run_id="EXP-1",
            )
        rows = self.rc.query_products({"site": "US", "limit": 300, "detail_only": True})
        self.assertGreaterEqual(len(rows), 250)
        for r in rows[:250]:
            self.fav.upsert_favorite_from_cache("sqlite", r)
        page = self.fav.list_favorites("sqlite", site="US", limit=5000)
        self.assertEqual(len(page), 200)
        all_rows = self.fav.list_all_favorites("sqlite", site="US")
        self.assertEqual(len(all_rows), 250)

    def test_stable_keyset_export_no_dup_missing(self):
        """同秒 favorited_at 下键集分页不重不漏。"""
        import sqlite3
        path = self.db
        self.fav.ensure_sqlite_schema(path)
        fixed = "2026-07-21 12:00:00"
        con = sqlite3.connect(path)
        try:
            for i in range(450):
                asin = f"B0S{i:07d}"
                con.execute(
                    "INSERT INTO favorite_products(site, asin, name, favorited_at, updated_at) "
                    "VALUES (?,?,?,?,?)",
                    ("US", asin, f"N{i}", fixed, fixed),
                )
            con.commit()
        finally:
            con.close()
        all_rows = self.fav.list_all_favorites("sqlite", site="US", page_size=200)
        self.assertEqual(len(all_rows), 450)
        ids = [r["id"] for r in all_rows]
        asins = [r["asin"] for r in all_rows]
        self.assertEqual(len(ids), len(set(ids)))
        self.assertEqual(len(asins), len(set(asins)))
        # 稳定降序：id 应严格递减（同 favorited_at）
        self.assertEqual(ids, sorted(ids, reverse=True))

    def test_testing_blocks_formal_favorite_path(self):
        formal = os.path.join(self.config.DATA_DIR, "categories.db")
        os.environ["AMZ_DB_FILE"] = formal
        os.environ["DB_FILE"] = formal
        import importlib
        import config
        import favorite_products as fav
        importlib.reload(config)
        importlib.reload(fav)
        with self.assertRaises(RuntimeError):
            fav.ensure_sqlite_schema()
        with self.assertRaises(RuntimeError):
            fav.count_favorites("sqlite")

    def test_actual_db_path_arg_checked_not_just_global(self):
        """全局临时库安全时，传入正式 db_path 仍必须拒绝。"""
        formal = os.path.join(self.config.DATA_DIR, "categories.db")
        with self.assertRaises(RuntimeError) as cm:
            self.fav.ensure_sqlite_schema(formal)
        self.assertIn("正式", str(cm.exception))


class TestMigrateIdleGuard(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.status = os.path.join(self.tmp.name, "proxy_pool_status.json")
        os.environ["TESTING"] = "1"
        os.environ["AMZ_DB_FILE"] = os.path.join(self.tmp.name, "cats.db")
        os.environ["DB_FILE"] = os.environ["AMZ_DB_FILE"]
        os.environ["AMZ_RUN_CACHE_FILE"] = os.path.join(self.tmp.name, "cache.db")

    def tearDown(self):
        self.tmp.cleanup()
        for k in ("TESTING", "AMZ_DB_FILE", "DB_FILE", "AMZ_RUN_CACHE_FILE"):
            os.environ.pop(k, None)

    def _write_status(self, status: str, **extra):
        payload = {"status": status, **extra}
        with open(self.status, "w", encoding="utf-8") as f:
            json.dump(payload, f)

    def test_non_idle_file_rejected_even_if_memory_idle(self):
        import importlib
        import migrate_clear_crawl_results as mig
        importlib.reload(mig)
        self._write_status("running", run_id="X")
        with mock.patch.object(mig, "_crawl_procs", return_value=[]):
            # 即使内存 get_status 假装 idle，也必须以文件为准
            with mock.patch(
                "proxy_pool_manager.get_status",
                return_value={"status": "idle"},
            ):
                with self.assertRaises(RuntimeError) as cm:
                    mig._require_lifecycle_idle(self.status)
                self.assertIn("idle", str(cm.exception).lower())

    def test_missing_status_file_rejected(self):
        import importlib
        import migrate_clear_crawl_results as mig
        importlib.reload(mig)
        missing = os.path.join(self.tmp.name, "nope.json")
        with self.assertRaises(RuntimeError):
            mig._require_lifecycle_idle(missing)

    def test_corrupt_status_file_rejected(self):
        import importlib
        import migrate_clear_crawl_results as mig
        importlib.reload(mig)
        with open(self.status, "w", encoding="utf-8") as f:
            f.write("{not-json")
        with self.assertRaises(RuntimeError):
            mig._require_lifecycle_idle(self.status)

    def test_idle_file_accepted(self):
        import importlib
        import migrate_clear_crawl_results as mig
        importlib.reload(mig)
        self._write_status("idle")
        state = mig._require_lifecycle_idle(self.status)
        self.assertEqual(state["status"], "idle")

    def test_proc_enum_failure_closed(self):
        import migrate_clear_crawl_results as mig
        bad = mock.Mock(returncode=1, stdout="", stderr="wmic boom")
        with mock.patch("subprocess.run", return_value=bad):
            with self.assertRaises(RuntimeError):
                mig._crawl_procs()

    def test_status_file_forbidden_outside_testing(self):
        import importlib
        import config
        import migrate_clear_crawl_results as mig
        os.environ.pop("TESTING", None)
        importlib.reload(config)
        importlib.reload(mig)
        self.assertFalse(config.is_testing())

        class Args:
            confirm = "YES_CLEAR_CRAWL_RESULTS"
            expected_ps = None
            expected_na = None
            backend = "sqlite"
            status_file = self.status

        with mock.patch("argparse.ArgumentParser.parse_args", return_value=Args()):
            code = mig.main()
        self.assertEqual(code, 7)
        # 恢复 TESTING，避免影响同 class 后续用例
        os.environ["TESTING"] = "1"
        importlib.reload(config)
        importlib.reload(mig)

    def test_backup_drift_aborts_and_keeps_rows(self):
        """备份完成后、删除前插入新行 → 拒绝清表且记录保留。"""
        import importlib
        import sqlite3
        import config
        import migrate_clear_crawl_results as mig
        importlib.reload(config)
        importlib.reload(mig)
        self._write_status("idle")
        db = os.environ["DB_FILE"]
        con = sqlite3.connect(db)
        con.executescript("""
            CREATE TABLE IF NOT EXISTS product_sightings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                asin TEXT NOT NULL,
                site TEXT DEFAULT 'US',
                node_id TEXT, list_type TEXT, name TEXT
            );
            CREATE TABLE IF NOT EXISTS new_arrivals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                asin TEXT NOT NULL, site TEXT DEFAULT 'US'
            );
            CREATE TABLE IF NOT EXISTS favorite_products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                site TEXT NOT NULL, asin TEXT NOT NULL, UNIQUE(site, asin)
            );
            INSERT INTO product_sightings(asin, site) VALUES ('B0OLD00001', 'US');
        """)
        con.commit()
        con.close()

        real_verify = mig._verify_backup

        def verify_then_insert(bak, **kwargs):
            h = real_verify(bak, **kwargs)
            c = sqlite3.connect(db)
            c.execute("INSERT INTO product_sightings(asin, site) VALUES ('B0NEW00001', 'US')")
            c.commit()
            c.close()
            return h

        with mock.patch.object(mig, "_crawl_procs", return_value=[]), \
             mock.patch.object(mig, "_verify_backup", side_effect=verify_then_insert), \
             mock.patch.object(mig, "CRAWL_MIGRATION_LOCK_FILE", os.path.join(self.tmp.name, "mig.lock")):
            # 强制锁路径到临时目录
            mig.CRAWL_MIGRATION_LOCK_FILE = os.path.join(self.tmp.name, "mig.lock")
            with self.assertRaises(RuntimeError) as cm:
                mig._sqlite_backup_and_clear(
                    expected_ps=1, expected_na=0, status_file=self.status,
                )
            self.assertIn("外部提交", str(cm.exception))
        con = sqlite3.connect(db)
        n = con.execute("SELECT COUNT(*) FROM product_sightings").fetchone()[0]
        con.close()
        self.assertEqual(n, 2)

    def test_equal_count_replacement_after_backup_is_rejected(self):
        """删除旧行再插入新行即使总数相同，也必须由 data_version 拒绝。"""
        import importlib
        import sqlite3
        import config
        import migrate_clear_crawl_results as mig
        importlib.reload(config)
        importlib.reload(mig)
        self._write_status("idle")
        db = os.environ["DB_FILE"]
        con = sqlite3.connect(db)
        con.executescript("""
            CREATE TABLE product_sightings (
                id INTEGER PRIMARY KEY AUTOINCREMENT, asin TEXT NOT NULL
            );
            CREATE TABLE new_arrivals (
                id INTEGER PRIMARY KEY AUTOINCREMENT, asin TEXT NOT NULL
            );
            CREATE TABLE favorite_products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                site TEXT NOT NULL, asin TEXT NOT NULL
            );
            INSERT INTO product_sightings(asin) VALUES ('B0OLD00001');
        """)
        con.commit()
        con.close()

        real_verify = mig._verify_backup

        def verify_then_replace(bak, **kwargs):
            result = real_verify(bak, **kwargs)
            writer = sqlite3.connect(db)
            writer.execute("DELETE FROM product_sightings")
            writer.execute("INSERT INTO product_sightings(asin) VALUES ('B0NEW00001')")
            writer.commit()
            writer.close()
            return result

        mig.CRAWL_MIGRATION_LOCK_FILE = os.path.join(self.tmp.name, "mig-equal.lock")
        with mock.patch.object(mig, "_crawl_procs", return_value=[]), \
             mock.patch.object(mig, "_verify_backup", side_effect=verify_then_replace):
            with self.assertRaises(RuntimeError) as cm:
                mig._sqlite_backup_and_clear(
                    expected_ps=1, expected_na=0, status_file=self.status,
                )
        self.assertIn("外部提交", str(cm.exception))
        con = sqlite3.connect(db)
        rows = [r[0] for r in con.execute(
            "SELECT asin FROM product_sightings ORDER BY id"
        ).fetchall()]
        con.close()
        self.assertEqual(rows, ["B0NEW00001"])


class TestPgTestDsnIsolation(unittest.TestCase):
    def tearDown(self):
        for k in ("TESTING", "PG_DSN", "PG_TEST_DSN", "PG_TEST_DB_ALLOWLIST"):
            os.environ.pop(k, None)

    def test_testing_requires_pg_test_dsn(self):
        os.environ["TESTING"] = "1"
        os.environ.pop("PG_TEST_DSN", None)
        import importlib
        import pg_config
        importlib.reload(pg_config)
        with self.assertRaises(RuntimeError) as cm:
            pg_config.get_pg_dsn()
        self.assertIn("PG_TEST_DSN", str(cm.exception))

    def test_test_dsn_must_differ_from_formal(self):
        os.environ["TESTING"] = "1"
        os.environ["PG_DSN"] = "postgresql://u:p@localhost:5432/amz_selection"
        os.environ["PG_TEST_DSN"] = "postgresql://u:p@localhost:5432/amz_selection"
        import importlib
        import pg_config
        importlib.reload(pg_config)
        with self.assertRaises(RuntimeError):
            pg_config.get_pg_dsn()

    def test_formal_db_name_rejected(self):
        os.environ["TESTING"] = "1"
        os.environ["PG_TEST_DSN"] = "postgresql://u:p@localhost:5432/amz_selection"
        import importlib
        import pg_config
        importlib.reload(pg_config)
        with self.assertRaises(RuntimeError):
            pg_config.get_pg_dsn()

    def test_test_suffix_ok(self):
        os.environ["TESTING"] = "1"
        os.environ["PG_DSN"] = "postgresql://u:p@localhost:5432/amz_selection"
        os.environ["PG_TEST_DSN"] = "postgresql://u:p@localhost:5432/amz_selection_test"
        import importlib
        import pg_config
        importlib.reload(pg_config)
        self.assertEqual(pg_config.get_pg_dsn(), os.environ["PG_TEST_DSN"])


class TestActivateAbortAlwaysReturns(unittest.IsolatedAsyncioTestCase):
    async def test_abort_returns_cache_activation_failed_despite_cleanup_errors(self):
        import api_server

        class FakeProc:
            def __init__(self):
                self._alive = True

            def poll(self):
                return None if self._alive else 0

            def terminate(self):
                raise RuntimeError("terminate boom")

            def kill(self):
                self._alive = False

            def wait(self, timeout=None):
                self._alive = False
                return 0

        with mock.patch.object(
            api_server.run_cache, "cancel_generation", side_effect=RuntimeError("cancel boom")
        ), mock.patch.object(
            api_server, "stop_proxy_pool", side_effect=RuntimeError("proxy boom")
        ):
            abort = await api_server._abort_crawler_start(FakeProc(), "RUN-X")
        self.assertTrue(abort["cleanup_errors"])
        self.assertIn("cancel_generation", " ".join(abort["cleanup_errors"]))

    async def test_abort_without_proc_still_cancels_generation(self):
        import api_server
        called = []

        def fake_cancel(run_id):
            called.append(run_id)

        with mock.patch.object(api_server.run_cache, "cancel_generation", side_effect=fake_cancel), \
             mock.patch.object(api_server, "stop_proxy_pool", return_value={"ok": True}):
            abort = await api_server._abort_crawler_start(None, "RUN-POPFAIL")
        self.assertEqual(called, ["RUN-POPFAIL"])
        self.assertEqual(abort.get("cleanup_errors") or [], [])

    async def test_attach_favorite_flags_fails_closed(self):
        import api_server
        from fastapi.responses import JSONResponse
        with mock.patch.object(
            api_server.fav_store, "favorite_key_set", side_effect=RuntimeError("pg down")
        ):
            out = await api_server._attach_favorite_flags(
                [{"asin": "B0AAAAAAAA", "site": "US"}], "US"
            )
        self.assertIsInstance(out, JSONResponse)
        self.assertEqual(out.status_code, 503)
        body = json.loads(out.body.decode())
        self.assertEqual(body["error_code"], "FAVORITE_READ_FAILED")

    async def test_lifespan_pg_schema_failure_raises(self):
        import api_server

        class FakeConn:
            async def execute(self, *a, **k):
                raise RuntimeError("schema boom")

            async def __aenter__(self):
                return self

            async def __aexit__(self, *a):
                return False

        class FakePool:
            def acquire(self):
                return FakeConn()

            async def close(self):
                self.closed = True

        pool = FakePool()
        old_backend = api_server.DB_BACKEND
        api_server.DB_BACKEND = "pg"
        api_server._pool = None
        try:
            with mock.patch.object(api_server, "assert_testing_paths_safe"), \
                 mock.patch.object(api_server, "use_run_cache", return_value=False), \
                 mock.patch.object(api_server.asyncpg, "create_pool", new=mock.AsyncMock(return_value=pool)), \
                 mock.patch.object(api_server, "get_pg_dsn", return_value="postgresql://x/y_test"), \
                 mock.patch.object(api_server, "ensure_daemon_running", return_value={"ok": True}):
                cm = api_server.lifespan(mock.Mock())
                with self.assertRaises(RuntimeError):
                    await cm.__aenter__()
            self.assertIsNone(api_server._pool)
        finally:
            api_server.DB_BACKEND = old_backend
            api_server._pool = None


class TestExportAtomicAndPgErrors(_EnvCase):
    def test_export_atomic_replace(self):
        import api_server
        out_rel = os.path.join(self.tmp.name, "fav.xlsx").replace("\\", "/")
        # 使用绝对路径：函数会拼接 BASE_DIR，这里直接测原子写辅助逻辑
        rows = [{"asin": "B0AAAAAAAA", "name": "A"}, {"asin": "B0BBBBBBBB", "name": "B"}]
        # 绕过 BASE_DIR：临时 monkeypatch
        old_base = api_server.BASE_DIR
        api_server.BASE_DIR = self.tmp.name
        try:
            rel = "out.xlsx"
            path = os.path.join(self.tmp.name, rel)
            # 先放一个旧文件
            with open(path, "wb") as f:
                f.write(b"OLD")
            with mock.patch.object(self.fav, "list_all_favorites", side_effect=RuntimeError("boom")):
                pass
            got = api_server._export_rows_xlsx(rows, rel)
            self.assertEqual(got, rel)
            self.assertTrue(os.path.isfile(path))
            self.assertGreater(os.path.getsize(path), 3)
            # 失败时不应留下半成品覆盖：模拟写盘中途异常
            def boom_save(p):
                raise RuntimeError("save failed")

            from openpyxl import Workbook
            real_wb = Workbook

            class BoomWB(real_wb):
                def save(self, filename):
                    raise RuntimeError("save failed")

            with mock.patch("openpyxl.Workbook", BoomWB):
                with self.assertRaises(RuntimeError):
                    api_server._export_rows_xlsx(rows, rel)
            # 旧文件仍在（replace 未发生）
            self.assertTrue(os.path.isfile(path))
        finally:
            api_server.BASE_DIR = old_base

    def test_list_pg_does_not_swallow_errors(self):
        os.environ["PG_DSN"] = "postgresql://u:p@localhost:5432/amz_selection"
        os.environ["PG_TEST_DSN"] = "postgresql://u:p@localhost:5432/amz_selection_test"
        import importlib
        import sys
        import types
        import pg_config
        importlib.reload(pg_config)
        fake = types.ModuleType("psycopg2")
        fake.connect = mock.Mock(side_effect=RuntimeError("down"))
        fake.extras = types.ModuleType("psycopg2.extras")
        fake.extras.RealDictCursor = object
        with mock.patch.dict(sys.modules, {"psycopg2": fake, "psycopg2.extras": fake.extras}):
            with self.assertRaises(RuntimeError):
                self.fav._list_pg("US", "", 10, 0)
            with self.assertRaises(RuntimeError):
                self.fav.count_favorites("pg")
            with self.assertRaises(RuntimeError):
                self.fav.favorite_key_set("pg")

    def test_dashboard_js_guards_favorite_503(self):
        html_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "data", "dashboard.html"
        )
        text = open(html_path, encoding="utf-8").read()
        self.assertIn("收藏读取失败", text)
        self.assertIn("!Array.isArray(products)", text)
        self.assertIn("typeof d.count !== 'number'", text)
        self.assertIn("products.status === 'error'", text)


if __name__ == "__main__":
    unittest.main()
