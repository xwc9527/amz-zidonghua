"""代理池专项单元/集成测试（不依赖 9097，不污染正式库）。"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import proxy_health
import proxy_node_source
import proxy_pool_manager
import proxy_runtime
import proxy_session
from proxy_node_source import (
    ProfileFingerprint,
    is_iproyal_node,
    is_metadata_node,
    load_candidate_nodes,
)
from proxy_pool_manager import (
    _cache_valid,
    _meets_thresholds,
    _publish_pool,
    read_probe_cache,
    write_probe_cache,
)
from proxy_session import ForcedProxyPool, ProxyRequiredError, load_proxy_pool, make_forced_session


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


class TestNodeSource(unittest.TestCase):
    def test_metadata_and_iproyal_exclusion(self):
        self.assertTrue(is_metadata_node({"name": "剩余流量：100 GB", "type": "ss"}))
        self.assertTrue(is_metadata_node({"name": "ok", "type": "Selector"}))
        self.assertTrue(is_iproyal_node({"name": "IPRoyal-US", "server": "x.com"}))
        self.assertTrue(is_iproyal_node({"name": "a", "remark": "ip royal house"}))
        self.assertFalse(is_iproyal_node({"name": "新加坡1", "server": "sg.example.com", "type": "vless"}))

    def test_load_current_subscription(self):
        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            profiles = td / "profiles"
            profiles.mkdir()
            sub = profiles / "UID1.yaml"
            _write(sub, """
proxies:
  - name: "剩余流量：1GB"
    type: ss
    server: a.com
    port: 1
  - name: "IPRoyal-Test"
    type: http
    server: iproyal.example
    port: 2
  - name: "Good-Node"
    type: vless
    server: good.example
    port: 443
    uuid: fake-uuid
  - name: "Another-Good"
    type: vless
    server: other.example
    port: 443
    uuid: fake-uuid-2
""")
            meta = td / "profiles.yaml"
            _write(meta, """
current: UID1
items:
  - uid: UID1
    name: airport
    file: UID1.yaml
    updated: 12345
""")
            res = load_candidate_nodes(
                profiles_meta=str(meta),
                profiles_dir=str(profiles),
            )
            self.assertTrue(res.ok)
            self.assertEqual(res.stats.subscription_entries, 4)
            self.assertEqual(res.stats.metadata_excluded, 1)
            self.assertEqual(res.stats.iproyal_excluded, 1)
            self.assertEqual(res.stats.candidates, 2)
            self.assertEqual({n["name"] for n in res.nodes}, {"Good-Node", "Another-Good"})
            self.assertEqual(res.fingerprint.uid, "UID1")
            self.assertEqual(res.fingerprint.updated_at, "12345")
            self.assertTrue(res.fingerprint.sha256)


class TestCacheInvalidation(unittest.TestCase):
    def test_uid_and_fingerprint_invalidate(self):
        fp1 = ProfileFingerprint("A", "/p", "sha1", "1")
        fp2 = ProfileFingerprint("B", "/p", "sha1", "1")
        fp3 = ProfileFingerprint("A", "/p", "sha2", "1")
        fp4 = ProfileFingerprint("A", "/p", "sha1", "2")
        cache = {
            "profile_uid": "A",
            "profile_sha256": "sha1",
            "profile_updated_at": "1",
            "probed_at": time.time(),
            "results": [{"name": "x"}],
        }
        self.assertTrue(_cache_valid(cache, fp1))
        self.assertFalse(_cache_valid(cache, fp2))
        self.assertFalse(_cache_valid(cache, fp3))
        self.assertFalse(_cache_valid(cache, fp4))
        empty = dict(cache, results=[])
        self.assertFalse(_cache_valid(empty, fp1))
        aborted = dict(cache, aborted=True)
        self.assertFalse(_cache_valid(aborted, fp1))

    def test_write_read_cache_roundtrip(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "probe_results.json"
            with mock.patch.object(proxy_pool_manager, "PROXY_PROBE_CACHE_FILE", str(path)):
                fp = ProfileFingerprint("U", "/x", "abc", "9")
                write_probe_cache(fp, [{"name": "n1", "ok": True}])
                got = read_probe_cache(fp)
                self.assertIsNotNone(got)
                self.assertEqual(got[0]["name"], "n1")
                fp2 = ProfileFingerprint("U", "/x", "CHANGED", "9")
                self.assertIsNone(read_probe_cache(fp2))


class TestHealthFilters(unittest.TestCase):
    def test_private_ip_rejected(self):
        self.assertFalse(proxy_health.is_public_ip("10.0.0.1"))
        self.assertFalse(proxy_health.is_public_ip("127.0.0.1"))
        self.assertFalse(proxy_health.is_public_ip("192.168.1.1"))
        self.assertTrue(proxy_health.is_public_ip("8.8.8.8"))

    def test_dedupe_by_exit_ip(self):
        entries = [
            {"name": "a", "port": 18001, "proxy": "http://127.0.0.1:18001"},
            {"name": "b", "port": 18002, "proxy": "http://127.0.0.1:18002"},
            {"name": "c", "port": 18003, "proxy": "http://127.0.0.1:18003"},
        ]

        def fake_exit(proxy, timeout=None):
            port = int(proxy.rsplit(":", 1)[1])
            ip_map = {18001: "1.1.1.1", 18002: "1.1.1.1", 18003: "2.2.2.2"}
            return proxy_health.ExitIpResult(ok=True, ip=ip_map[port])

        with mock.patch.object(proxy_health, "fetch_exit_ip", side_effect=fake_exit):
            with mock.patch.object(proxy_health, "lookup_isp_hint", return_value={"ok": True}):
                with mock.patch.object(
                    proxy_health,
                    "check_amazon",
                    return_value=proxy_health.AmazonResult(ok=True, status_code=200),
                ) as amazon_mock:
                    out = proxy_health.verify_pool(entries, concurrency=2)
        self.assertEqual(out["raw_passed"], 3)
        self.assertEqual(out["verified_nodes"], 2)
        self.assertEqual(out["unique_ips"], 2)
        self.assertEqual(out["fail_reasons"].get("DUPLICATE_EXIT_IP"), 1)
        self.assertEqual(amazon_mock.call_count, 2)


class TestAmazonHealthGate(unittest.TestCase):
    def _check(self, status: int, body: str):
        requester = SimpleNamespace(
            get=lambda *args, **kwargs: SimpleNamespace(
                status_code=status,
                text=body,
            )
        )
        with mock.patch.object(proxy_health, "_requests", return_value=(requester, False)):
            return proxy_health.check_amazon("http://127.0.0.1:18001")

    def test_normal_200_passes(self):
        result = self._check(200, "<html><title>Amazon.com</title></html>")
        self.assertTrue(result.ok)
        self.assertFalse(result.captcha)

    def test_200_captcha_fails(self):
        result = self._check(200, "<title>Robot Check</title> /errors/validateCaptcha")
        self.assertFalse(result.ok)
        self.assertTrue(result.captcha)
        self.assertEqual(result.error_code, "CAPTCHA")

    def test_202_captcha_fails(self):
        result = self._check(202, "Type the characters you see in this image")
        self.assertFalse(result.ok)
        self.assertEqual(result.error_code, "CAPTCHA")

    def test_rejected_http_statuses_fail(self):
        for status in (403, 429, 503):
            with self.subTest(status=status):
                result = self._check(status, "access denied")
                self.assertFalse(result.ok)
                self.assertEqual(result.error_code, "AMAZON_HTTP_STATUS")


class TestThresholdAndPublish(unittest.TestCase):
    def test_threshold(self):
        with mock.patch.multiple(
            proxy_pool_manager,
            PROXY_MIN_VERIFIED_NODES=11,
            PROXY_MIN_UNIQUE_IPS=11,
            PROXY_MIN_AMAZON_OK=11,
        ):
            ok, _ = _meets_thresholds(11, 11, 11)
            self.assertTrue(ok)
            for values, field in (
                ((10, 11, 11), "verified_nodes"),
                ((11, 10, 11), "unique_ips"),
                ((11, 11, 10), "amazon_ok"),
            ):
                with self.subTest(values=values):
                    ok, reason = _meets_thresholds(*values)
                    self.assertFalse(ok)
                    self.assertIn(field, reason)

    def test_atomic_publish_and_empty_reject(self):
        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            pool = td / "proxy_pool.json"
            cand = td / "proxy_pool.candidate.json"
            last_good = td / "proxy_pool.last_good.json"
            last_failed = td / "proxy_pool.last_failed.json"
            with mock.patch.multiple(
                proxy_pool_manager,
                PROXY_POOL_FILE=str(pool),
                PROXY_POOL_CANDIDATE_FILE=str(cand),
                PROXY_POOL_LAST_GOOD_FILE=str(last_good),
                PROXY_POOL_LAST_FAILED_FILE=str(last_failed),
            ):
                fp = ProfileFingerprint("U", "/p", "s", "1")
                path = _publish_pool(
                    [{"name": "n", "port": 18001, "proxy": "http://127.0.0.1:18001", "exit_ip": "1.2.3.4"}],
                    fp, "PROXY-TEST", {"verified_nodes": 1},
                )
                self.assertTrue(Path(path).is_file())
                data = json.loads(pool.read_text(encoding="utf-8"))
                self.assertEqual(data["profile_uid"], "U")
                self.assertEqual(len(data["entries"]), 1)

                # 失败路径：正式池不应被写成 []
                before = pool.read_text(encoding="utf-8")
                _record = proxy_pool_manager._record_failure
                _record("PROXY-FAIL", {"reason": "threshold", "error_code": "THRESHOLD_NOT_MET"})
                self.assertEqual(pool.read_text(encoding="utf-8"), before)
                self.assertTrue(last_failed.is_file())


class TestDaemonDelegation(unittest.TestCase):
    """ensure_proxy_ready 新语义：委托常驻守护进程，不再阻塞式冷启动整批验证。"""

    def test_ensure_proxy_ready_force_true_still_cold_starts(self):
        expected = proxy_pool_manager.PrepareResult(
            ok=True, status=proxy_pool_manager.STATUS_PROXY_READY, run_id="R"
        )
        with mock.patch.object(
            proxy_pool_manager, "prepare_proxy_pool", return_value=expected
        ) as prepare_mock:
            result = proxy_pool_manager.ensure_proxy_ready(force=True)
        self.assertIs(result, expected)
        prepare_mock.assert_called_once_with(force=True, stop_owned_after=False)

    def test_ensure_proxy_ready_returns_immediately_when_pool_already_hot(self):
        """守护进程早已常驻、活池已有 >= PROXY_MIN_START_NODES 个节点时，几乎瞬时返回。"""
        with tempfile.TemporaryDirectory() as td:
            pool_path = Path(td) / "proxy_pool.json"
            pool_path.write_text(json.dumps({
                "run_id": "DAEMON-1",
                "entries": [{"name": "n1", "port": 18001, "proxy": "http://127.0.0.1:18001", "exit_ip": "1.1.1.1"}],
            }), encoding="utf-8")
            with mock.patch.object(proxy_pool_manager, "PROXY_POOL_FILE", str(pool_path)):
                with mock.patch.object(proxy_pool_manager, "PROXY_MIN_START_NODES", 1):
                    with mock.patch.object(
                        proxy_pool_manager, "ensure_daemon_running",
                        return_value={"ok": True, "pid": 111, "started": False},
                    ) as spawn_mock:
                        with mock.patch.object(
                            proxy_pool_manager, "daemon_status",
                            return_value={"run_id": "DAEMON-1", "candidates": 3, "checking": 1, "failed": 0},
                        ):
                            with mock.patch.object(proxy_pool_manager, "set_status"):
                                start = time.monotonic()
                                result = proxy_pool_manager.ensure_proxy_ready(force=False, timeout=5)
                                elapsed = time.monotonic() - start
        spawn_mock.assert_called_once()
        self.assertTrue(result.ok)
        self.assertEqual(result.status, proxy_pool_manager.STATUS_PROXY_READY)
        self.assertEqual(result.verified_nodes, 1)
        self.assertEqual(result.candidate_nodes, 3)
        self.assertLess(elapsed, 2.0, "活池已达标时应几乎立即返回，不应等待整个 timeout")

    def test_ensure_proxy_ready_times_out_when_daemon_pool_insufficient(self):
        with tempfile.TemporaryDirectory() as td:
            pool_path = Path(td) / "proxy_pool.json"
            pool_path.write_text(json.dumps({"run_id": "DAEMON-2", "entries": []}), encoding="utf-8")
            with mock.patch.object(proxy_pool_manager, "PROXY_POOL_FILE", str(pool_path)):
                with mock.patch.object(proxy_pool_manager, "PROXY_MIN_START_NODES", 1):
                    with mock.patch.object(
                        proxy_pool_manager, "ensure_daemon_running", return_value={"ok": True, "pid": 111},
                    ):
                        with mock.patch.object(
                            proxy_pool_manager, "daemon_status",
                            return_value={"run_id": "DAEMON-2", "candidates": 0, "checking": 0, "failed": 0},
                        ):
                            with mock.patch.object(proxy_pool_manager, "set_status"):
                                result = proxy_pool_manager.ensure_proxy_ready(force=False, timeout=0.3)
        self.assertFalse(result.ok)
        self.assertEqual(result.error_code, "DAEMON_POOL_NOT_READY")

    def test_ensure_proxy_ready_fails_fast_when_daemon_cannot_start(self):
        with mock.patch.object(
            proxy_pool_manager, "ensure_daemon_running",
            return_value={"ok": False, "error": "spawn boom", "error_code": "DAEMON_SPAWN_FAILED"},
        ):
            with mock.patch.object(proxy_pool_manager, "set_status"):
                result = proxy_pool_manager.ensure_proxy_ready(force=False, timeout=5)
        self.assertFalse(result.ok)
        self.assertEqual(result.error_code, "DAEMON_SPAWN_FAILED")

    def test_prepare_proxy_pool_rejects_when_daemon_alive(self):
        """冷启动体检工具与常驻守护进程共用同一个 Mihomo 归属文件，二者不能同时跑。"""
        with mock.patch.object(proxy_pool_manager, "daemon_alive", return_value=4321):
            result = proxy_pool_manager.prepare_proxy_pool(force=True)
        self.assertFalse(result.ok)
        self.assertEqual(result.error_code, "DAEMON_ACTIVE")

    def test_ensure_daemon_running_spawns_when_not_alive(self):
        calls = {"n": 0}

        def _fake_alive():
            # 前两次调用（拉起前的存活检查 + 拿锁后的二次确认）都返回 None，
            # 第三次起（拉起后轮询 PID 文件）才返回真实 pid。
            calls["n"] += 1
            return None if calls["n"] <= 2 else 777

        with mock.patch.object(proxy_pool_manager, "daemon_alive", side_effect=_fake_alive):
            with mock.patch.object(proxy_pool_manager.subprocess, "Popen") as popen_mock:
                result = proxy_pool_manager.ensure_daemon_running(wait_pid_sec=1)
        popen_mock.assert_called_once()
        self.assertTrue(result["ok"])
        self.assertTrue(result["started"])
        self.assertEqual(result["pid"], 777)

    def test_ensure_daemon_running_noop_when_already_alive(self):
        with mock.patch.object(proxy_pool_manager, "daemon_alive", return_value=555):
            with mock.patch.object(proxy_pool_manager.subprocess, "Popen") as popen_mock:
                result = proxy_pool_manager.ensure_daemon_running()
        popen_mock.assert_not_called()
        self.assertEqual(result, {"ok": True, "pid": 555, "started": False})


class TestReadyPoolReuse(unittest.TestCase):
    def test_hot_reuse_requires_and_publishes_at_least_eleven(self):
        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            fp = ProfileFingerprint("U", "/p", "sha", "1", entry_count=11)
            stats = proxy_node_source.NodeLoadStats(candidates=11, subscription_entries=11)
            loaded = proxy_node_source.NodeLoadResult(
                ok=True,
                fingerprint=fp,
                nodes=[{"name": f"n{i}", "type": "vless", "server": f"s{i}"} for i in range(11)],
                stats=stats,
            )
            entries = [
                {
                    "name": f"n{i}",
                    "port": 18001 + i,
                    "proxy": f"http://127.0.0.1:{18001 + i}",
                    "exit_ip": f"8.8.8.{i + 1}",
                }
                for i in range(11)
            ]
            pool = td / "proxy_pool.json"
            pool.write_text(json.dumps({
                "profile_uid": fp.uid,
                "profile_sha256": fp.sha256,
                "profile_updated_at": fp.updated_at,
                "entries": entries,
            }), encoding="utf-8")
            health = {
                "results": [],
                "verified": [
                    proxy_health.NodeHealthResult(
                        name=e["name"], port=e["port"], proxy=e["proxy"],
                        ok=True, exit_ip=e["exit_ip"], amazon_ok=True,
                    )
                    for e in entries
                ],
                "verified_nodes": 11,
                "unique_ips": 11,
                "amazon_ok": 11,
                "fail_reasons": {},
                "raw_passed": 11,
            }
            with mock.patch.multiple(
                proxy_pool_manager,
                PROXY_POOL_FILE=str(pool),
                PROXY_POOL_CANDIDATE_FILE=str(td / "candidate.json"),
                PROXY_POOL_LAST_GOOD_FILE=str(td / "last_good.json"),
                PROXY_MIN_VERIFIED_NODES=11,
                PROXY_MIN_UNIQUE_IPS=11,
                PROXY_MIN_AMAZON_OK=11,
            ):
                with mock.patch.object(proxy_pool_manager, "owned_mihomo_running", return_value=123):
                    with mock.patch.object(proxy_pool_manager, "start_mihomo") as start_mock:
                        with mock.patch.object(
                            proxy_pool_manager, "get_reference_ips",
                            return_value={"direct_ip": "", "main_proxy_ip": ""},
                        ):
                            with mock.patch.object(proxy_pool_manager, "verify_pool", return_value=health):
                                with mock.patch.object(proxy_pool_manager, "set_status"):
                                    result = proxy_pool_manager._try_reuse_pool(
                                        "PROXY-HOT", loaded, [{}] * 11
                                    )
            self.assertIsNotNone(result)
            self.assertTrue(result.ok)
            self.assertEqual(result.stats["startup_path"], "hot")
            self.assertEqual(result.amazon_ok, 11)
            start_mock.assert_not_called()


class TestForcedProxy(unittest.TestCase):
    def test_empty_pool_rejects(self):
        with tempfile.TemporaryDirectory() as td:
            pool = Path(td) / "proxy_pool.json"
            _write(pool, "[]")
            with mock.patch.object(proxy_session, "PROXY_POOL_FILE", str(pool)):
                with mock.patch.object(proxy_session, "PROXY_REQUIRED", True):
                    with mock.patch.object(proxy_session, "ALLOW_DIRECT_FALLBACK", False):
                        loaded = load_proxy_pool(required=True, allow_direct=False, max_age=0)
                        self.assertFalse(loaded.ok)
                        self.assertEqual(loaded.error_code, "POOL_EMPTY")
                        with self.assertRaises(ProxyRequiredError):
                            ForcedProxyPool(required=True)

    def test_session_without_proxy_errors(self):
        with mock.patch.object(proxy_session, "PROXY_REQUIRED", True):
            with mock.patch.object(proxy_session, "ALLOW_DIRECT_FALLBACK", False):
                with self.assertRaises(ProxyRequiredError):
                    make_forced_session(None, required=True)

    def test_no_silent_direct_in_load(self):
        with tempfile.TemporaryDirectory() as td:
            missing = Path(td) / "nope.json"
            with mock.patch.object(proxy_session, "PROXY_POOL_FILE", str(missing)):
                with mock.patch.object(proxy_session, "PROXY_REQUIRED", True):
                    with mock.patch.object(proxy_session, "ALLOW_DIRECT_FALLBACK", False):
                        loaded = load_proxy_pool(required=True)
                        self.assertFalse(loaded.ok)
                        self.assertFalse(loaded.allow_direct)


class TestLiveReload(unittest.TestCase):
    """ForcedProxyPool 活池热重载 + acquire() 有界等待（取代立即崩溃）。"""

    def _entry(self, name: str, ip: str) -> dict:
        return {"name": name, "port": 0, "proxy": f"http://127.0.0.1:{name}", "exit_ip": ip}

    def test_reload_from_file_adds_new_and_evicts_missing(self):
        pool = ForcedProxyPool(
            entries=[self._entry("a", "1.1.1.1")], required=True, min_usable=1,
        )
        with tempfile.TemporaryDirectory() as td:
            pool_path = Path(td) / "proxy_pool.json"
            pool_path.write_text(json.dumps({
                "entries": [self._entry("b", "2.2.2.2"), self._entry("c", "3.3.3.3")],
            }), encoding="utf-8")
            pool._pool_path = str(pool_path)
            result = pool.reload_from_file()
        self.assertTrue(result["ok"])
        self.assertEqual(result["added"], 2)
        self.assertEqual(result["removed"], 1)  # "a" 消失
        keys = set(pool._states.keys())
        self.assertEqual(keys, {"http://127.0.0.1:b", "http://127.0.0.1:c"})

    def test_reload_defers_removal_of_in_use_entry_until_release(self):
        pool = ForcedProxyPool(entries=[self._entry("a", "1.1.1.1")], required=True, min_usable=1)
        entry = pool.acquire(timeout=1)
        self.assertIsNotNone(entry)
        with tempfile.TemporaryDirectory() as td:
            pool_path = Path(td) / "proxy_pool.json"
            pool_path.write_text(json.dumps({"entries": []}), encoding="utf-8")
            pool._pool_path = str(pool_path)
            pool.reload_from_file()
        # 正被占用：不能立即物理移除，只标记 pending_removal
        self.assertIn("http://127.0.0.1:a", pool._states)
        self.assertTrue(pool._states["http://127.0.0.1:a"]["pending_removal"])
        pool.release(entry, outcome="SUCCESS")
        self.assertNotIn("http://127.0.0.1:a", pool._states)

    def test_acquire_waits_then_raises_when_pool_stays_below_minimum(self):
        pool = ForcedProxyPool(
            entries=[self._entry("a", "1.1.1.1")], required=True, min_usable=2,
            wait_for_replenish_sec=0.3,
        )
        started = time.monotonic()
        with self.assertRaises(ProxyRequiredError) as ctx:
            pool.acquire(timeout=5)
        elapsed = time.monotonic() - started
        self.assertEqual(ctx.exception.code, "POOL_BELOW_MINIMUM")
        # 应该等待了一段时间（而不是立即报错），但不超过 replenish 上限太多
        self.assertGreaterEqual(elapsed, 0.25)
        self.assertLess(elapsed, 3.0)

    def test_acquire_succeeds_once_live_reload_replenishes_pool(self):
        pool = ForcedProxyPool(
            entries=[self._entry("a", "1.1.1.1")], required=True, min_usable=2,
            wait_for_replenish_sec=5,
        )

        def _replenish_soon():
            time.sleep(0.2)
            with pool._cv:
                pool._add_entry_locked(self._entry("b", "2.2.2.2"))
                pool._cv.notify_all()

        t = threading.Thread(target=_replenish_soon, daemon=True)
        t.start()
        started = time.monotonic()
        entry = pool.acquire(timeout=5)
        elapsed = time.monotonic() - started
        t.join(timeout=2)
        self.assertIsNotNone(entry)
        self.assertLess(elapsed, 2.0, "补充节点后应很快被唤醒并成功获取，不应等到超时")


class TestRuntimeLock(unittest.TestCase):
    def test_refresh_lock_single_instance(self):
        with tempfile.TemporaryDirectory() as td:
            lock_path = str(Path(td) / "proxy_pool.lock")
            a = proxy_pool_manager.RefreshLock(lock_path)
            b = proxy_pool_manager.RefreshLock(lock_path)
            self.assertTrue(a.acquire(timeout=0.2))
            self.assertFalse(b.acquire(timeout=0.2))
            a.release()
            self.assertTrue(b.acquire(timeout=0.2))
            b.release()


class TestPrepareFailurePropagation(unittest.TestCase):
    def test_health_all_fail_keeps_formal_pool(self):
        with tempfile.TemporaryDirectory() as td:
            td = Path(td)
            pool = td / "proxy_pool.json"
            pool.write_text(json.dumps({
                "run_id": "OLD", "entries": [{"name": "keep", "port": 1, "proxy": "http://x", "exit_ip": "9.9.9.9"}]
            }), encoding="utf-8")
            before = pool.read_text(encoding="utf-8")
            fake_loaded = proxy_node_source.NodeLoadResult(
                ok=True,
                fingerprint=ProfileFingerprint("U", "/p", "s", "1", entry_count=1),
                nodes=[{"name": "n1", "type": "vless", "server": "a"}],
                stats=proxy_node_source.NodeLoadStats(candidates=1, subscription_entries=1),
            )
            with mock.patch.multiple(
                proxy_pool_manager,
                PROXY_POOL_FILE=str(pool),
                PROXY_POOL_CANDIDATE_FILE=str(td / "c.json"),
                PROXY_POOL_LAST_GOOD_FILE=str(td / "g.json"),
                PROXY_POOL_LAST_FAILED_FILE=str(td / "f.json"),
                PROXY_PROBE_CACHE_FILE=str(td / "probe.json"),
                PROXY_POOL_STATUS_FILE=str(td / "st.json"),
                PROXY_LOCK_FILE=str(td / "lock"),
            ):
                with mock.patch.object(proxy_pool_manager, "load_candidate_nodes", return_value=fake_loaded):
                    with mock.patch.object(
                        proxy_pool_manager, "start_mihomo",
                        return_value=proxy_runtime.RuntimeResult(ok=True, pid=1, ports=[18001]),
                    ):
                        with mock.patch.object(
                            proxy_pool_manager, "check_listener",
                            return_value={"ok": True, "port": 18001, "listening": True, "process_alive": True},
                        ):
                            with mock.patch.object(
                                proxy_pool_manager, "get_reference_ips",
                                return_value={"direct_ip": "1.1.1.1", "main_proxy_ip": ""},
                            ):
                                with mock.patch.object(
                                    proxy_pool_manager, "verify_pool",
                                    return_value={
                                        "results": [], "verified": [], "verified_nodes": 0,
                                        "unique_ips": 0, "amazon_ok": 0, "fail_reasons": {"IP_CHECK_FAILED": 1},
                                        "raw_passed": 0,
                                    },
                                ):
                                    with mock.patch.object(proxy_pool_manager, "stop_owned_mihomo"):
                                        with mock.patch.object(proxy_pool_manager, "daemon_alive", return_value=None):
                                            res = proxy_pool_manager.prepare_proxy_pool(force=True)
            self.assertFalse(res.ok)
            self.assertEqual(res.error_code, "THRESHOLD_NOT_MET")
            self.assertEqual(pool.read_text(encoding="utf-8"), before)

    def test_mihomo_failure_does_not_start_ok(self):
        fake_loaded = proxy_node_source.NodeLoadResult(
            ok=True,
            fingerprint=ProfileFingerprint("U", "/p", "s", "1", entry_count=2),
            nodes=[
                {"name": "n1", "type": "vless", "server": "a"},
                {"name": "n2", "type": "vless", "server": "b"},
            ],
            stats=proxy_node_source.NodeLoadStats(candidates=2, subscription_entries=2),
        )
        with mock.patch.object(proxy_pool_manager, "load_candidate_nodes", return_value=fake_loaded):
            with mock.patch.object(
                proxy_pool_manager, "start_mihomo",
                return_value=proxy_runtime.RuntimeResult(
                    ok=False, error="boom", error_code="PROCESS_EXITED",
                ),
            ):
                with mock.patch.object(proxy_pool_manager, "write_probe_cache"):
                    with mock.patch.object(proxy_pool_manager, "_record_failure"):
                        with mock.patch.object(proxy_pool_manager, "set_status"):
                            with mock.patch.object(proxy_pool_manager, "daemon_alive", return_value=None):
                                res = proxy_pool_manager.prepare_proxy_pool(force=True)
        self.assertFalse(res.ok)
        self.assertEqual(res.status, "proxy_failed")
        self.assertEqual(res.error_code, "PROCESS_EXITED")

    def test_unexpected_exception_stops_owned_mihomo(self):
        fake_loaded = proxy_node_source.NodeLoadResult(
            ok=True,
            fingerprint=ProfileFingerprint("U", "/p", "s", "1", entry_count=1),
            nodes=[{"name": "n1", "type": "vless", "server": "a"}],
            stats=proxy_node_source.NodeLoadStats(candidates=1, subscription_entries=1),
        )
        runtime = proxy_runtime.RuntimeResult(ok=True, pid=123, ports=[18001])
        cleanup = proxy_runtime.RuntimeResult(ok=True, pid=123, ports=[18001])
        with mock.patch.object(proxy_pool_manager, "load_candidate_nodes", return_value=fake_loaded):
            with mock.patch.object(proxy_pool_manager, "start_mihomo", return_value=runtime):
                with mock.patch.object(
                    proxy_pool_manager, "check_listener", side_effect=RuntimeError("listener boom")
                ):
                    with mock.patch.object(
                        proxy_pool_manager, "stop_owned_mihomo", return_value=cleanup
                    ) as stop_mock:
                        with mock.patch.object(proxy_pool_manager, "_record_failure") as record_mock:
                            with mock.patch.object(proxy_pool_manager, "set_status"):
                                with mock.patch.object(proxy_pool_manager, "daemon_alive", return_value=None):
                                    result = proxy_pool_manager.prepare_proxy_pool(force=True)
        self.assertFalse(result.ok)
        self.assertEqual(result.error_code, "PREPARE_EXCEPTION")
        stop_mock.assert_called_once()
        record_mock.assert_called_once()


class TestRealProfileParse(unittest.TestCase):
    """读取真实活动订阅（若存在），验证解析与 IPRoyal 排除统计。"""

    def test_real_subscription_if_present(self):
        from config import CLASH_PROFILES_META, CLASH_PROFILES_DIR
        if not CLASH_PROFILES_META or not os.path.isfile(CLASH_PROFILES_META):
            self.skipTest("本机无 Clash profiles.yaml")
        res = load_candidate_nodes()
        self.assertTrue(res.ok, res.error)
        self.assertGreaterEqual(res.stats.candidates, 1)
        self.assertEqual(res.stats.iproyal_excluded, 0)
        # 元数据至少排除流量/到期类
        self.assertGreaterEqual(res.stats.metadata_excluded, 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
