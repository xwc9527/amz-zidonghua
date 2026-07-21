import json
import os
import tempfile
import unittest
from unittest import mock

from crawl_checkpoint import NewArrivalsCheckpoint, ProductsCheckpoint, canonical_signature
from proxy_session import ForcedProxyPool, ProxyRequiredError
from proxy_worker import http_error_code, raise_if_pool_below_minimum, FetchOutcome


def _entries(count=12):
    return [
        {
            "name": f"node-{i}",
            "port": 31000 + i,
            "proxy": f"http://127.0.0.1:{31000 + i}",
            "exit_ip": f"198.51.100.{i + 1}",
        }
        for i in range(count)
    ]


class _Response:
    def __init__(self, status_code, text):
        self.status_code = status_code
        self.text = text


class _Session:
    def __init__(self, response):
        self.response = response
        self.headers = {}
        self.closed = False

    def get(self, url, timeout=0):
        if isinstance(self.response, Exception):
            raise self.response
        return self.response

    def close(self):
        self.closed = True


class TestRuntimeProxyPool(unittest.TestCase):
    def test_us_currency_cookie_and_mismatch_detection(self):
        import fetch_new_arrivals as crawler

        self.assertTrue(crawler._has_marketplace_currency_mismatch(
            '<span class="a-price"><span class="a-offscreen">S$15.99</span></span>'
        ))
        self.assertTrue(crawler._has_marketplace_currency_mismatch(
            '<span class="a-price-symbol">JPY3,003</span>'
        ))
        self.assertFalse(crawler._has_marketplace_currency_mismatch(
            '<span class="a-price"><span class="a-offscreen">$15.99</span></span>'
        ))
        fake = mock.Mock()
        fake.cookies = mock.Mock()
        with mock.patch.object(crawler, "make_forced_session", return_value=fake), \
             mock.patch.object(crawler, "assert_session_has_proxy"):
            crawler._make_session(0, _entries(1)[0])
        fake.cookies.set.assert_called_once_with("i18n-prefs", "USD")

    def test_currency_mismatch_rotates_to_distinct_exit(self):
        import fetch_new_arrivals as crawler

        pool = ForcedProxyPool(entries=_entries(3), required=True, min_usable=1)
        sessions = [
            _Session(_Response(200, '<span class="a-offscreen">JPY3,003</span>')),
            _Session(_Response(200, '<span class="a-offscreen">$19.99</span>')),
        ]
        with tempfile.TemporaryDirectory() as td, \
             mock.patch.object(crawler, "_AUDIT_PATH", os.path.join(td, "audit.jsonl")), \
             mock.patch.object(crawler, "_make_session", side_effect=sessions):
            client = crawler.WorkerProxyClient(pool, worker_id=4, warmup=False)
            outcome = client.get("https://example.invalid/A1", phase="P2", item_id="A1")
            client.close()
        self.assertTrue(outcome.ok)
        self.assertEqual(outcome.reasons, ["LOCALE_CURRENCY_MISMATCH"])
        self.assertEqual(len(outcome.exit_ips), 2)

    def test_client_tls_internal_error_does_not_penalize_proxy(self):
        import fetch_new_arrivals as crawler

        exc = RuntimeError("curl: (35) TLS connect error: OPENSSL_INTERNAL:invalid library (0)")
        self.assertEqual(crawler._classify_request_exception(exc), "CLIENT_TLS_ERROR")
        pool = ForcedProxyPool(entries=_entries(11), required=True, min_usable=11)
        entry = pool.acquire()
        pool.release(entry, outcome="CLIENT_TLS_ERROR")
        self.assertEqual(pool.usable_count, 11)
        self.assertEqual(pool.health_snapshot()["cooling"], 0)

    def test_cooldown_rotation_and_minimum_gate(self):
        # wait_for_replenish_sec=0：本用例只测冷却/门槛判定本身，不测有界等待
        # 补充节点的独立特性（那部分由 TestLiveReload 专门覆盖），避免默认
        # 300s 的等待预算拖慢这条用例。
        pool = ForcedProxyPool(
            entries=_entries(12), required=True, min_usable=11, wait_for_replenish_sec=0,
        )
        first = pool.acquire()
        pool.release(first, outcome="CAPTCHA")
        self.assertEqual(pool.usable_count, 11)

        second = pool.acquire(exclude_exit_ips={first["exit_ip"]})
        self.assertNotEqual(first["exit_ip"], second["exit_ip"])
        pool.release(second, outcome="HTTP_429")
        self.assertEqual(pool.usable_count, 10)

        with self.assertRaises(ProxyRequiredError) as caught:
            pool.acquire(timeout=0)
        self.assertEqual(caught.exception.code, "POOL_BELOW_MINIMUM")
        health = pool.health_snapshot()
        self.assertEqual(health["cooling"], 2)
        self.assertEqual(health["usable"], 10)

    def test_captcha_immediately_retries_on_distinct_exit(self):
        import fetch_new_arrivals as crawler

        pool = ForcedProxyPool(entries=_entries(3), required=True, min_usable=1)
        sessions = [
            _Session(_Response(200, "<html>captcha /errors/validateCaptcha</html>")),
            _Session(_Response(200, "<html>valid product page</html>")),
        ]
        with tempfile.TemporaryDirectory() as td, \
             mock.patch.object(crawler, "_AUDIT_PATH", os.path.join(td, "audit.jsonl")), \
             mock.patch.object(crawler, "_make_session", side_effect=sessions):
            client = crawler.WorkerProxyClient(pool, worker_id=7, warmup=False)
            outcome = client.get("https://example.invalid/item", phase="P2", item_id="A1")
            client.close()
            self.assertTrue(outcome.ok)
            self.assertEqual(outcome.attempts, 2)
            self.assertEqual(len(outcome.exit_ips), 2)
            self.assertNotEqual(outcome.exit_ips[0], outcome.exit_ips[1])
            with open(os.path.join(td, "audit.jsonl"), encoding="utf-8") as fh:
                events = [json.loads(line) for line in fh]
            self.assertEqual(events[0]["reason"], "CAPTCHA")
            self.assertTrue(events[0]["rotated"])
            self.assertEqual(events[1]["result"], "SUCCESS")
            self.assertTrue(all(event["run_id"] for event in events))

    def test_successful_requests_rotate_across_pool(self):
        import fetch_new_arrivals as crawler

        pool = ForcedProxyPool(entries=_entries(3), required=True, min_usable=1)
        made_entries = []

        def make_session(worker_id, entry):
            made_entries.append(entry["exit_ip"])
            return _Session(_Response(200, "<html>ok</html>"))

        with tempfile.TemporaryDirectory() as td, \
             mock.patch.object(crawler, "_AUDIT_PATH", os.path.join(td, "audit.jsonl")), \
             mock.patch.object(crawler, "_make_session", side_effect=make_session):
            client = crawler.WorkerProxyClient(pool, worker_id=2, warmup=False)
            self.assertTrue(client.get("https://example.invalid/1", phase="P2", item_id="1").ok)
            self.assertTrue(client.get("https://example.invalid/2", phase="P2", item_id="2").ok)
            client.close()
        self.assertEqual(len(made_entries), 2)
        self.assertNotEqual(made_entries[0], made_entries[1])


class TestCheckpoint(unittest.TestCase):
    def test_atomic_p1_restore_and_p2_retry_semantics(self):
        config = {"site": "US", "nodes": ["1"], "max_pages": 1}
        signature = canonical_signature(config)
        with tempfile.TemporaryDirectory() as td:
            cp = NewArrivalsCheckpoint(signature, config, directory=td)
            cp.save_p1_node("1", [{"asin": "A1", "node_id": "1"}])
            cp.save_p2_result("A1", "error", error_code="HTTP_429")
            cp.close()

            resumed = NewArrivalsCheckpoint(signature, config, directory=td)
            self.assertEqual(resumed.p1_done_ids(), {"1"})
            self.assertEqual(set(resumed.load_asins()), {"A1"})
            self.assertEqual(resumed.summary()["p1"], {"done": 1})
            self.assertNotIn("A1", resumed.p2_done_ids())
            resumed.save_p2_result("A1", "matched", attempts=2, exit_ips=["198.51.100.2"])
            self.assertIn("A1", resumed.p2_done_ids())
            resumed.complete()
            resumed.close()

            fresh = NewArrivalsCheckpoint(signature, config, directory=td)
            self.assertEqual(fresh.p1_done_ids(), set())
            self.assertEqual(fresh.load_asins(), {})
            fresh.close()

    def test_failed_p1_node_is_retried(self):
        config = {"site": "US", "nodes": ["1"]}
        signature = canonical_signature(config)
        with tempfile.TemporaryDirectory() as td:
            cp = NewArrivalsCheckpoint(signature, config, directory=td)
            cp.save_p1_node("1", [], status="error", error_code="READ_TIMEOUT", attempts=3)
            self.assertNotIn("1", cp.p1_done_ids())
            self.assertEqual(cp.summary()["p1"], {"error": 1})
            cp.close()


class TestProductsCheckpoint(unittest.TestCase):
    def test_resume_skips_done_and_retries_errors(self):
        config = {"site": "US", "lists": ["new-releases"]}
        signature = canonical_signature(config)
        with tempfile.TemporaryDirectory() as td:
            cp = ProductsCheckpoint(signature, config, directory=td)
            cp.save_node("10", status="done", products_found=3)
            cp.save_node("20", status="error", error_code="CAPTCHA", attempts=2)
            self.assertEqual(cp.done_ids(), {"10"})
            self.assertNotIn("20", cp.done_ids())
            cp.close()

            resumed = ProductsCheckpoint(signature, config, directory=td)
            self.assertEqual(resumed.done_ids(), {"10"})
            resumed.save_node("20", status="done", products_found=1)
            resumed.complete()
            resumed.close()

            fresh = ProductsCheckpoint(signature, config, directory=td)
            self.assertEqual(fresh.done_ids(), set())
            fresh.close()


class TestProxyWorkerHelpers(unittest.TestCase):
    def test_other_http_status_and_pool_gate(self):
        self.assertEqual(http_error_code(500), "OTHER_HTTP_STATUS")
        pool = ForcedProxyPool(entries=_entries(12), required=True, min_usable=11)
        entry = pool.acquire()
        pool.release(entry, outcome="OTHER_HTTP_STATUS")
        self.assertEqual(pool.usable_count, 11)
        self.assertEqual(pool.health_snapshot()["cooling"], 1)
        with self.assertRaises(ProxyRequiredError) as caught:
            raise_if_pool_below_minimum(FetchOutcome(ok=False, error_code="POOL_BELOW_MINIMUM"))
        self.assertEqual(caught.exception.code, "POOL_BELOW_MINIMUM")


class _FakeListDetailClient:
    """最小化伪 client：LIST 阶段返回固定 outcome，DETAIL 阶段返回另一个固定 outcome。"""

    def __init__(self, list_outcome, detail_outcome):
        self.list_outcome = list_outcome
        self.detail_outcome = detail_outcome
        self.calls = []

    def get(self, url, *, phase, item_id, referer=""):
        self.calls.append((phase, item_id))
        return self.list_outcome if phase == "LIST" else self.detail_outcome


class TestFetchProductsDetailFailurePropagation(unittest.TestCase):
    """覆盖 fetch_products：详情页抓取失败必须让节点标记为 error 以便断点重试，
    而不是被 enrich_with_details 静默吞掉（否则失败详情永远不会重试）。"""

    _NODE = {
        "node_id": "n1",
        "url": "https://www.amazon.com/gp/new-releases/some-slug/n1/",
        "name": "Cat",
        "depth": 3,
    }
    _LIST_HTML = (
        '<div id="gridItemRoot1"><a href="/dp/B000000001">'
        '<span>Widget</span></a></div>'
    )

    def setUp(self):
        import fetch_products as fp
        # _seen_asins 是模块级去重集合，跨测试残留会让第二个用例误判为重复 ASIN。
        with fp._seen_lock:
            fp._seen_asins.clear()

    def test_detail_fetch_failure_marks_node_error_for_retry(self):
        import fetch_products as fp

        list_outcome = FetchOutcome(ok=True, html=self._LIST_HTML, status_code=200, attempts=1)
        detail_outcome = FetchOutcome(
            ok=False, error_code="RETRY_EXHAUSTED", final_reason="HTTP_503",
            status_code=503, attempts=3,
        )
        client = _FakeListDetailClient(list_outcome, detail_outcome)

        with mock.patch.object(fp, "save_link_validity"), \
             mock.patch.object(fp, "save_products", return_value=1), \
             mock.patch.object(fp, "_mark_detail_failed") as mark_failed:
            status, err_code, found, attempts = fp.process_node(
                self._NODE, ["new-releases"], review_max=0, min_list_size=0,
                client=client, max_pages=1, delay=0.0,
            )

        self.assertEqual(status, "error")
        self.assertTrue(err_code.startswith("DETAIL_FETCH_FAILED"))
        self.assertEqual(found, 1)
        mark_failed.assert_called_once_with("B000000001")

    def test_detail_fetch_success_marks_node_done(self):
        import fetch_products as fp

        list_outcome = FetchOutcome(ok=True, html=self._LIST_HTML, status_code=200, attempts=1)
        detail_outcome = FetchOutcome(
            ok=True, html="<html>detail</html>", status_code=200, attempts=1,
        )
        client = _FakeListDetailClient(list_outcome, detail_outcome)

        with mock.patch.object(fp, "save_link_validity"), \
             mock.patch.object(fp, "save_products", return_value=1), \
             mock.patch.object(fp, "parse_detail_fields", return_value={"price": 9.99}), \
             mock.patch.object(fp, "estimate_fba_fees", return_value={}), \
             mock.patch.object(fp, "_check_detail_filters", return_value=True), \
             mock.patch.object(fp, "_update_sighting_detail") as update_detail:
            status, err_code, found, attempts = fp.process_node(
                self._NODE, ["new-releases"], review_max=0, min_list_size=0,
                client=client, max_pages=1, delay=0.0,
            )

        self.assertEqual(status, "done")
        self.assertEqual(err_code, "")
        self.assertEqual(found, 1)
        update_detail.assert_called_once()


class TestNewArrivalsDeepToShallow(unittest.TestCase):
    def test_load_nodes_orders_depth_desc(self):
        import sqlite3
        import fetch_new_arrivals as crawler

        with tempfile.TemporaryDirectory() as td:
            db = os.path.join(td, "cat.db")
            con = sqlite3.connect(db)
            con.execute(
                "CREATE TABLE categories("
                "node_id TEXT, name TEXT, depth INTEGER, site TEXT, parent_node_id TEXT, na_valid INTEGER)"
            )
            con.executemany(
                "INSERT INTO categories VALUES (?,?,?,?,?,?)",
                [
                    ("r", "Root", 1, "US", None, 1),
                    ("c", "Child", 2, "US", "r", 1),
                    ("g", "Grand", 3, "US", "c", 1),
                    ("nav", "NavOnly", 2, "US", "r", 0),
                ],
            )
            con.commit()
            con.close()
            with mock.patch.object(crawler, "DB_FILE", db), \
                 mock.patch.object(crawler, "DB_BACKEND", "sqlite"):
                nodes = crawler._load_nodes("US", root_ids=["r"], include_descendants=True)
            depths = [n["depth"] for n in nodes]
            self.assertEqual(depths, sorted(depths, reverse=True))
            # 仅 NEW（na_valid=1），导航节点 nav 被排除
            self.assertEqual([n["node_id"] for n in nodes], ["g", "c", "r"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
