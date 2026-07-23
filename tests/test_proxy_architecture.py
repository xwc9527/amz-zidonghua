from __future__ import annotations

import json
import os
import tempfile
import time
import sys
import types
import unittest
from unittest.mock import Mock, patch

# 本组用例只验证调度状态机，不解析/生成 Mihomo YAML。仅在受限沙盒
# 确实没有 PyYAML 时注入最小替身；完整环境必须使用真实 yaml，不污染其它用例。
try:
    import yaml as _yaml  # noqa: F401
except ModuleNotFoundError:
    sys.modules["yaml"] = types.SimpleNamespace(
        safe_load=lambda _stream: {}, dump=lambda *_args, **_kwargs: None
    )

import config
import proxy_daemon
from proxy_daemon import NodeState, ProxyDaemon, STATE_FAILED, STATE_NEW, STATE_VERIFIED
from proxy_events import ProxyEvent, drain_proxy_events, read_proxy_quality, report_proxy_event
from proxy_session import ForcedProxyPool, load_proxy_pool


def _node(key: str, idx: int, *, state: str = STATE_VERIFIED, prefix: str | None = None) -> NodeState:
    third = idx // 250 + 1
    fourth = idx % 250 + 1
    exit_ip = prefix or f"10.{third}.{idx % 200}.{fourth}"
    return NodeState(
        key=key,
        node={"name": key},
        port=31000 + idx,
        state=state,
        exit_ip=exit_ip if state == STATE_VERIFIED else "",
        amazon_ok=state == STATE_VERIFIED,
        verified_since=time.time() - 60,
        next_check_at=time.time() + 600,
    )


class ThresholdTests(unittest.TestCase):
    def test_thresholds_are_ordered(self):
        self.assertEqual(config.PROXY_MIN_START_NODES, 8)
        self.assertEqual(config.PROXY_POOL_LOW_WATERMARK, 10)
        self.assertEqual(config.PROXY_POOL_TARGET_NODES, 14)
        self.assertEqual(config.PROXY_POOL_HOT_MAX_NODES, 16)
        self.assertLess(config.PROXY_MIN_START_NODES, config.PROXY_POOL_LOW_WATERMARK)
        self.assertLess(config.PROXY_POOL_LOW_WATERMARK, config.PROXY_POOL_TARGET_NODES)
        self.assertLessEqual(config.PROXY_POOL_TARGET_NODES, config.PROXY_POOL_HOT_MAX_NODES)


class FeedbackStoreTests(unittest.TestCase):
    def test_event_is_drained_once_and_quality_is_aggregated(self):
        with tempfile.TemporaryDirectory() as td:
            db = os.path.join(td, "events.db")
            entry = {"node_key": "n1", "proxy": "http://127.0.0.1:1", "exit_ip": "1.1.1.1"}
            self.assertTrue(report_proxy_event(entry, "SUCCESS", latency_ms=1000, db_path=db))
            self.assertTrue(report_proxy_event(entry, "HTTP_429", db_path=db))
            events = drain_proxy_events(db_path=db)
            self.assertEqual([e.outcome for e in events], ["SUCCESS", "HTTP_429"])
            self.assertEqual(drain_proxy_events(db_path=db), [])
            quality = read_proxy_quality(db_path=db)
            self.assertEqual(len(quality), 1)
            self.assertEqual(quality[0]["successes"], 1)
            self.assertEqual(quality[0]["rate_limited"], 1)
            self.assertEqual(quality[0]["latency_ewma"], 1000)
            # Windows 上若 report/read 遗留 SQLite 连接，此处会稳定触发 WinError 32。
            # 显式删除比依赖 TemporaryDirectory 退出时的隐式清理更便于审计。
            os.remove(db)
            self.assertFalse(os.path.exists(db))


class DaemonArchitectureTests(unittest.TestCase):
    def setUp(self):
        self.daemon = ProxyDaemon()

    def tearDown(self):
        self.daemon._executor.shutdown(wait=False, cancel_futures=True)

    def _install(self, states):
        self.daemon._states = {st.key: st for st in states}
        self.daemon._live_keys = {st.key for st in states if st.state == STATE_VERIFIED}

    def test_tiers_cap_hot_and_keep_warm(self):
        states = [_node(f"n{i}", i) for i in range(18)]
        for i, st in enumerate(states):
            st.feedback_successes = 20 - i
        self._install(states)
        with patch.object(proxy_daemon, "PROXY_POOL_HOT_MAX_NODES", 16):
            hot, warm = self.daemon._select_tiers_locked()
        self.assertEqual(len(hot), 16)
        self.assertEqual(len(warm), 2)
        self.assertFalse(set(hot) & set(warm))

    def test_tier_selection_prefers_network_diversity(self):
        states = [
            _node("same-a", 1, prefix="1.2.3.1"),
            _node("same-b", 2, prefix="1.2.3.2"),
            _node("same-c", 3, prefix="1.2.3.3"),
            _node("other", 4, prefix="8.8.8.8"),
        ]
        for st in states[:3]:
            st.feedback_successes = 100
        self._install(states)
        with (
            patch.object(proxy_daemon, "PROXY_POOL_HOT_MAX_NODES", 3),
            patch.object(proxy_daemon, "PROXY_MAX_ACTIVE_PER_PREFIX", 2),
        ):
            hot, _ = self.daemon._select_tiers_locked()
        self.assertIn("other", hot)
        self.assertEqual(sum(key.startswith("same") for key in hot), 2)

    def test_under_target_fills_slots_but_stable_pool_only_explores_one(self):
        states = [_node(f"v{i}", i) for i in range(7)]
        states.extend(_node(f"new{i}", 100 + i, state=STATE_NEW) for i in range(5))
        self._install(states)
        submit = Mock()
        self.daemon._executor.submit = submit
        with (
            patch.object(proxy_daemon, "PROXY_DAEMON_CONCURRENCY", 3),
            patch.object(self.daemon, "_get_reference_ips_cached", return_value=set()),
        ):
            self.daemon._dispatch_checks()
        self.assertEqual(submit.call_count, 3)

        submit.reset_mock()
        stable = [_node(f"h{i}", i + 200) for i in range(14)]
        stable.extend(_node(f"candidate{i}", 300 + i, state=STATE_NEW) for i in range(5))
        self._install(stable)
        self.daemon._inflight.clear()
        self.daemon._next_exploration_at = 0
        with (
            patch.object(proxy_daemon, "PROXY_DAEMON_CONCURRENCY", 3),
            patch.object(self.daemon, "_get_reference_ips_cached", return_value=set()),
        ):
            self.daemon._dispatch_checks()
        self.assertEqual(submit.call_count, 1)

    def test_feedback_rechecks_single_bad_node_but_not_target_wide_storm(self):
        states = [_node(f"n{i}", i) for i in range(4)]
        self._install(states)
        now = time.time()
        single = [ProxyEvent(1, now, "n0", "p", "1.1.1.1", "US", "CAPTCHA", 0, "")]
        with patch.object(proxy_daemon, "drain_proxy_events", return_value=single):
            self.daemon._consume_feedback()
        self.assertTrue(self.daemon._states["n0"].feedback_recheck)

        for st in states:
            st.feedback_recheck = False
        storm = [
            ProxyEvent(i + 2, now, f"n{i}", "p", f"1.1.1.{i}", "US", "HTTP_429", 0, "")
            for i in range(3)
        ]
        with patch.object(proxy_daemon, "drain_proxy_events", return_value=storm):
            self.daemon._consume_feedback()
        self.assertFalse(any(self.daemon._states[f"n{i}"].feedback_recheck for i in range(3)))
        self.assertGreater(self.daemon._target_pause_until["US"], now)

    def test_captcha_storm_never_pauses_entire_target(self):
        states = [_node(f"n{i}", i) for i in range(3)]
        self._install(states)
        now = time.time()
        storm = [
            ProxyEvent(i + 1, now, f"n{i}", "p", f"1.1.1.{i}", "DE", "CAPTCHA", 0, "")
            for i in range(3)
        ]
        with patch.object(proxy_daemon, "drain_proxy_events", return_value=storm):
            self.daemon._consume_feedback()
        self.assertNotIn("DE", self.daemon._target_pause_until)
        self.assertTrue(all(self.daemon._states[f"n{i}"].feedback_recheck for i in range(3)))

    def test_new_candidate_failure_marks_status_dirty(self):
        st = _node("new-fail", 1, state=STATE_NEW)
        self._install([st])
        self.daemon._dirty = False
        self.daemon._on_check_result(
            st.key,
            ok=False,
            reason="timeout",
            error_code="CONNECT_TIMEOUT",
            exit_ip="",
        )
        self.assertEqual(st.state, STATE_FAILED)
        self.assertTrue(self.daemon._dirty)

    def test_inflight_completion_marks_status_dirty(self):
        st = _node("n1", 1)
        self._install([st])
        self.daemon._inflight.add(st.key)
        self.daemon._dirty = False
        with (
            patch.object(proxy_daemon, "check_listener", return_value={"ok": False}),
            patch.object(self.daemon, "_on_check_result"),
        ):
            self.daemon._check_one(st.key, set())
        self.assertNotIn(st.key, self.daemon._inflight)
        self.assertTrue(self.daemon._dirty)

    def test_real_success_postpones_redundant_probe(self):
        st = _node("n1", 1)
        st.last_checked_at = time.time()
        st.next_check_at = 0
        self._install([st])
        event = ProxyEvent(1, time.time(), "n1", "p", "1.1.1.1", "US", "SUCCESS", 800, "")
        before = time.time()
        with patch.object(proxy_daemon, "drain_proxy_events", return_value=[event]):
            self.daemon._consume_feedback()
        self.assertGreater(st.next_check_at, before + 60)
        self.assertEqual(st.feedback_successes, 1)
        self.assertEqual(st.latency_ewma_ms, 800)
        self.assertLessEqual(
            st.next_check_at,
            st.last_checked_at + proxy_daemon.PROXY_DAEMON_FULL_SCAN_INTERVAL_SEC + 1,
        )

    def test_publish_contains_only_hot_runtime_entries_and_auditable_warm_pool(self):
        states = [_node(f"n{i}", i) for i in range(18)]
        self._install(states)
        with tempfile.TemporaryDirectory() as td:
            pool_file = os.path.join(td, "pool.json")
            with (
                patch.object(proxy_daemon, "PROXY_POOL_FILE", pool_file),
                patch.object(proxy_daemon, "PROXY_POOL_HOT_MAX_NODES", 16),
            ):
                self.daemon._publish_pool()
            with open(pool_file, encoding="utf-8") as fh:
                payload = json.load(fh)
        self.assertEqual(len(payload["entries"]), 16)
        self.assertEqual(len(payload["warm_entries"]), 2)
        self.assertTrue(all(row["tier"] == "hot" for row in payload["entries"]))
        self.assertTrue(all("node_key" in row and "quality_score" in row for row in payload["entries"]))


class RuntimePoolTests(unittest.TestCase):
    @staticmethod
    def _entry(name, ip, quality):
        return {
            "name": name,
            "proxy": f"http://127.0.0.1:{name}",
            "exit_ip": ip,
            "node_key": name,
            "quality_score": quality,
        }

    def test_weighted_selection_and_prefix_cap(self):
        entries = [
            self._entry("1", "1.2.3.1", 1.2),
            self._entry("2", "1.2.3.2", 1.1),
            self._entry("3", "8.8.8.8", 0.8),
        ]
        pool = ForcedProxyPool(entries=entries, required=True, min_usable=1)
        with patch("proxy_session.random.uniform", return_value=0):
            first = pool.acquire()
            second = pool.acquire()
            third = pool.acquire()
        self.assertEqual(first["node_key"], "1")
        self.assertEqual(second["node_key"], "2")
        self.assertEqual(third["node_key"], "3")
        for entry in (first, second, third):
            pool.release(entry)

    def test_eight_nodes_pass_start_gate_and_seven_do_not(self):
        eight = [self._entry(str(i), f"10.0.{i}.1", 1.0) for i in range(8)]
        ready = ForcedProxyPool(entries=eight, required=True, min_usable=8, wait_for_replenish_sec=0)
        entry = ready.acquire(timeout=0)
        self.assertIsNotNone(entry)
        ready.release(entry)

        blocked = ForcedProxyPool(
            entries=eight[:7], required=True, min_usable=8, wait_for_replenish_sec=0,
        )
        with self.assertRaisesRegex(Exception, "7 < 8"):
            blocked.acquire(timeout=0)

    def test_low_score_hot_node_is_not_permanently_starved(self):
        high = self._entry("high", "1.1.1.1", 1.5)
        low = self._entry("low", "2.2.2.2", 0.05)
        pool = ForcedProxyPool(entries=[high, low], required=True, min_usable=1)
        selected = []
        with patch("proxy_session.random.uniform", return_value=0):
            for _ in range(6):
                entry = pool.acquire()
                selected.append(entry["node_key"])
                pool.release(entry, outcome="ROTATE")
        self.assertIn("low", selected)

    def test_runtime_errors_cool_down_instead_of_permanently_freezing_candidate(self):
        pool = ForcedProxyPool(
            entries=[self._entry("1", "1.1.1.1", 1.0)], required=True, min_usable=1,
            feedback_enabled=False,
        )
        for _ in range(2):
            entry = pool.acquire()
            pool.release(entry, outcome="CAPTCHA")
            with pool._cv:
                pool._states[entry["proxy"]]["cooldown_until"] = 0
        snapshot = pool.health_snapshot()
        self.assertEqual(snapshot["disabled"], 0)
        self.assertEqual(snapshot["usable"], 1)

    def test_403_uses_shorter_cooldown_than_429(self):
        pool = ForcedProxyPool(
            entries=[
                self._entry("1", "1.1.1.1", 1.0),
                self._entry("2", "2.2.2.2", 1.0),
            ],
            required=True,
            min_usable=1,
            feedback_enabled=False,
        )
        one = pool.acquire()
        pool.release(one, outcome="HTTP_403")
        two = pool.acquire()
        pool.release(two, outcome="HTTP_429")
        with pool._cv:
            cooldown_403 = pool._states[one["proxy"]]["cooldown_until"]
            cooldown_429 = pool._states[two["proxy"]]["cooldown_until"]
        self.assertLess(cooldown_403, cooldown_429)

    def test_new_daemon_validation_clears_old_local_cooldown(self):
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "pool.json")
            initial = self._entry("1", "1.1.1.1", 1.0)
            initial["last_checked_at"] = 10
            with open(path, "w", encoding="utf-8") as fh:
                json.dump({"entries": [initial]}, fh)
            pool = ForcedProxyPool(required=True, min_usable=1, pool_path=path)
            key = initial["proxy"]
            with pool._cv:
                pool._states[key]["cooldown_until"] = time.time() + 3600
                pool._states[key]["consecutive_errors"] = 2
            updated = dict(initial, last_checked_at=20, quality_score=1.2)
            with open(path, "w", encoding="utf-8") as fh:
                json.dump({"entries": [updated]}, fh)
            result = pool.reload_from_file()
            self.assertTrue(result["ok"])
            self.assertEqual(pool.health_snapshot()["cooling"], 0)
            self.assertEqual(pool._states[key]["consecutive_errors"], 0)

    def test_feedback_write_is_emitted_outside_pool_state_machine(self):
        pool = ForcedProxyPool(
            entries=[self._entry("1", "1.1.1.1", 1.0)], required=True, min_usable=1,
            feedback_enabled=True,
        )
        with patch("proxy_session.report_proxy_event", return_value=True) as report:
            entry = pool.acquire()
            pool.release(entry, outcome="HTTP_429")
        report.assert_called_once()
        self.assertEqual(report.call_args.args[1], "HTTP_429")

    def test_pool_loader_preserves_control_metadata(self):
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "pool.json")
            with open(path, "w", encoding="utf-8") as fh:
                json.dump({
                    "entries": [self._entry("1", "1.1.1.1", 1.0)],
                    "target_pause_until": {"US": 12345},
                }, fh)
            loaded = load_proxy_pool(path, required=True, max_age=0)
        self.assertTrue(loaded.ok)
        self.assertEqual(loaded.metadata["target_pause_until"]["US"], 12345)

    def test_target_pause_also_blocks_an_existing_short_lease(self):
        pool = ForcedProxyPool(
            entries=[self._entry("1", "1.1.1.1", 1.0)], required=True, min_usable=1,
        )
        pool._target_pause_until = time.time() + 0.03
        started = time.monotonic()
        pool.wait_if_target_paused()
        self.assertGreaterEqual(time.monotonic() - started, 0.02)


if __name__ == "__main__":
    unittest.main()
