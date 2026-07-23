"""回归测试：fetch_subtree 三处修复。

1. `_safe_get` 最后一次失败不再休眠，并返回失败原因（问题 3）。
2. 节点内 5 路并发时每路使用独立 Session，不跨线程共享（问题 2）。
3. 失败的**单个** chart URL 进补抓队列、换 worker/IP 重试，耗尽后写 errors；
   成功但无子类目的入口正常完成、不进补抓（问题 1）。
"""
import contextlib
import io
import json
import sqlite3
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

import category_dedup_migration as migration
import fetch_subtree


class FakeResp:
    def __init__(self, status_code, text=""):
        self.status_code = status_code
        self.text = text


class FakeSession:
    """按状态码序列返回响应；raise=True 时抛 RequestException。"""

    def __init__(self, statuses=None, raises=False, text="<html></html>"):
        self._statuses = list(statuses or [])
        self._raises = raises
        self._text = text
        self.calls = 0

    def get(self, url, timeout=None, verify=None):
        self.calls += 1
        if self._raises:
            import requests
            raise requests.RequestException("boom")
        code = self._statuses[min(self.calls - 1, len(self._statuses) - 1)]
        return FakeResp(code, self._text)


class ImmediateFuture:
    def __init__(self, value):
        self._value = value

    def result(self, timeout=None):
        return self._value


class InlineParsePipeline:
    def __init__(self, workers, max_pending):
        pass

    def submit(self, markup, page_url, domain):
        return ImmediateFuture(fetch_subtree.parse_sidebar_children(markup, page_url))

    def shutdown(self, **_kwargs):
        pass


class TestSafeGetNoIdleWait(unittest.TestCase):
    def test_rate_limited_last_attempt_does_not_sleep(self):
        session = FakeSession(statuses=[429, 429, 429])
        with patch.object(fetch_subtree.time, "sleep") as slept:
            html, reason = fetch_subtree._safe_get(session, "http://x/429", retries=3)
        self.assertIsNone(html)
        self.assertEqual(reason, "RATE_LIMITED")
        # 3 次尝试全部 429：只在前两次之间休眠，最后一次失败直接返回，不空等。
        self.assertEqual(slept.call_count, 2)
        self.assertEqual(session.calls, 3)

    def test_network_error_last_attempt_does_not_sleep(self):
        session = FakeSession(raises=True)
        with patch.object(fetch_subtree.time, "sleep") as slept:
            html, reason = fetch_subtree._safe_get(session, "http://x/boom", retries=3)
        self.assertIsNone(html)
        self.assertEqual(reason, "NETWORK")
        self.assertEqual(slept.call_count, 2)

    def test_success_returns_empty_reason(self):
        session = FakeSession(statuses=[200], text="<html>zg-browse ok</html>")
        with patch.object(fetch_subtree.time, "sleep"):
            html, reason = fetch_subtree._safe_get(session, "http://x/ok", retries=3)
        self.assertEqual(reason, "")
        self.assertIn("zg-browse", html)
        self.assertEqual(session.calls, 1)

    def test_non_ip_http_error_no_retry(self):
        session = FakeSession(statuses=[404])
        with patch.object(fetch_subtree.time, "sleep") as slept:
            html, reason = fetch_subtree._safe_get(session, "http://x/404", retries=3)
        self.assertIsNone(html)
        self.assertEqual(reason, "HTTP_404")
        self.assertEqual(session.calls, 1)  # 4xx 不重试同一 URL
        self.assertEqual(slept.call_count, 0)


class TestProxyPoolFormatCompatibility(unittest.TestCase):
    def test_current_daemon_envelope_loads_only_entries(self):
        with tempfile.TemporaryDirectory() as tmp:
            pool_path = Path(tmp) / "proxy_pool.json"
            pool_path.write_text(json.dumps({
                "run_id": "test",
                "stats": {"hot_nodes": 2},
                "entries": [
                    {"proxy": "http://127.0.0.1:18001", "exit_ip": "198.51.100.1"},
                    {"proxy": "http://127.0.0.1:18002", "exit_ip": "198.51.100.2"},
                ],
            }), encoding="utf-8")
            with patch.object(fetch_subtree, "PROXY_ENABLED", True), \
                 patch.object(fetch_subtree, "PROXY_POOL_FILE", str(pool_path)):
                pool = fetch_subtree.ProxyPool()
            self.assertEqual(pool.size, 2)
            self.assertEqual(len(pool.all_entries()), 2)
            self.assertTrue(all(isinstance(entry, dict) for entry in pool.all_entries()))


class TestSubtreeUrlGranularRetry(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp.name) / "categories.db"
        conn = sqlite3.connect(self.db_path)
        conn.execute(migration.CATEGORIES_TABLE_SQL.format(table_name="categories"))
        migration._create_indexes_and_triggers(conn)
        conn.commit()
        conn.close()
        self.checkpoint = Path(self.tmp.name) / "checkpoint.json"

    def tearDown(self):
        self.tmp.cleanup()

    def _run_crawl(self, slug, domain, calls, num_workers=1, fail_forever=True):
        root_url = fetch_subtree.normalize_url(f"{domain}/gp/new-releases/{slug}/")
        child_url = fetch_subtree.normalize_url(f"{domain}/gp/new-releases/{slug}/111/")

        failed_once = set()

        def fake_safe_get(session, url, retries=1):
            proxy = session.proxies.get("https", "direct")
            calls.append((url, id(session), proxy))
            # 节点 111 的 movers-and-shakers 入口永久失败 → 单 URL 补抓
            if "movers-and-shakers" in url and "/111/" in url and (
                fail_forever or url not in failed_once
            ):
                failed_once.add(url)
                return None, "RATE_LIMITED"
            return "<html></html>", ""

        def fake_parse(html, page_url, **_):
            if page_url == root_url:
                return [{"name": "Child111", "url": child_url,
                         "node_id": "111", "slug": slug}]
            return []  # 其它页面：成功但无子类目

        entries = [{"proxy": f"http://127.0.0.1:{9000 + i}"} for i in range(num_workers)]
        out = io.StringIO()
        with patch.object(fetch_subtree, "DB_FILE", str(self.db_path)), \
             patch.object(fetch_subtree, "_SITE", "US"), \
             patch.object(fetch_subtree, "_DOMAIN", domain), \
             patch.object(fetch_subtree, "_LANG", "en-US"), \
             patch.object(fetch_subtree, "CHECKPOINT_FILE", str(self.checkpoint)), \
             patch.object(fetch_subtree, "_safe_get", fake_safe_get), \
             patch.object(fetch_subtree, "parse_sidebar_children", fake_parse), \
             patch.object(fetch_subtree, "BoundedParsePipeline", InlineParsePipeline), \
             patch.object(fetch_subtree.time, "sleep"):
            done = threading.Event()
            raised = []

            def runner():
                try:
                    fetch_subtree.crawl_slug(slug, entries, max_depth=99)
                except BaseException as exc:
                    raised.append(exc)
                finally:
                    done.set()

            t = threading.Thread(target=runner, daemon=True)
            with contextlib.redirect_stdout(out):
                t.start()
                t.join(timeout=30)
            self.assertTrue(done.is_set(), "crawl_slug 未在超时内结束（疑似死锁/未按补抓完成判定收尾）")
        return out.getvalue(), root_url, child_url, raised

    def test_single_failed_url_retried_and_exhausted(self):
        domain = "https://www.amazon.com"
        calls = []
        output, root_url, child_url, raised = self._run_crawl("testslug", domain, calls, num_workers=1)
        self.assertEqual(len(raised), 1)
        self.assertIsInstance(raised[0], RuntimeError)

        # 问题 1：只有那个失败的单个 URL 被补抓，且耗尽后计入 errors——
        # 证明补抓粒度是 URL 而非整节点（节点 111 的其它 4 个入口成功、不进补抓）。
        self.assertIn("补抓耗尽 1 个 URL", output)
        self.assertRegex(output, r"\[补抓耗尽\].*movers-and-shakers.*/111/.*attempts=3")

        # 子节点 111 已被展开写入 DB → 说明根节点部分成功入口正常并入。
        conn = sqlite3.connect(self.db_path)
        n = conn.execute("SELECT COUNT(*) FROM categories WHERE node_id='111'").fetchone()[0]
        conn.close()
        self.assertEqual(n, 1)

        # 失败 URL 的真实请求 = 节点内首抓 1 次 + 补抓 MAX_URL_RETRY 次；补抓有界，不会无限重试。
        movers_calls = [c for c in calls if "movers-and-shakers" in c[0] and "/111/" in c[0]]
        self.assertEqual(len(movers_calls), fetch_subtree.MAX_URL_RETRY + 1)

    def test_concurrent_charts_use_distinct_sessions(self):
        domain = "https://www.amazon.com"
        calls = []
        _, _, _, raised = self._run_crawl("laneslug", domain, calls, num_workers=1, fail_forever=False)
        self.assertEqual(raised, [])

        # 问题 2：节点 111 的 5 个榜单入口首次并发抓取时，各自使用了独立 Session。
        first_sid_by_url = {}
        for url, sid, _proxy in calls:
            if "/111/" in url and url not in first_sid_by_url:
                first_sid_by_url[url] = sid
        self.assertEqual(len(first_sid_by_url), fetch_subtree.NUM_LANES)
        self.assertEqual(
            len(set(first_sid_by_url.values())), fetch_subtree.NUM_LANES,
            "5 路并发应使用 5 个不同的 Session 对象，禁止共享单一 Session",
        )

    def test_retry_is_consumed_by_a_different_worker_ip(self):
        domain = "https://www.amazon.com"
        calls = []
        _, _, _, raised = self._run_crawl(
            "switchip", domain, calls, num_workers=2, fail_forever=False
        )
        self.assertEqual(raised, [])
        attempts = [
            proxy for url, _sid, proxy in calls
            if "movers-and-shakers" in url and "/111/" in url
        ]
        self.assertEqual(len(attempts), 2)
        self.assertNotEqual(attempts[0], attempts[1])


if __name__ == "__main__":
    unittest.main()
