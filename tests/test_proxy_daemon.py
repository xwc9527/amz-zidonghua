"""proxy_daemon.py 专项测试：节点状态机、增量发布、订阅差量处理（不依赖真实网络/Mihomo）。"""
from __future__ import annotations

import json
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import proxy_daemon
import proxy_node_source
import proxy_runtime
from proxy_daemon import ProxyDaemon, node_key
from proxy_health import NodeHealthResult
from proxy_node_source import ProfileFingerprint, NodeLoadResult, NodeLoadStats


def _node(name="n1", server="s1", port=0, ntype="vless"):
    return {"name": name, "server": server, "port": port, "type": ntype}


def _loaded(nodes, uid="U1", sha="sha1"):
    fp = ProfileFingerprint(uid=uid, path="/p", sha256=sha, updated_at="1", entry_count=len(nodes))
    return NodeLoadResult(
        ok=True, fingerprint=fp, nodes=nodes,
        stats=NodeLoadStats(candidates=len(nodes), subscription_entries=len(nodes)),
    )


class TestNodeKey(unittest.TestCase):
    def test_stable_regardless_of_unrelated_fields(self):
        # 身份只取 name/server/port(远端端口)/type；密码等其它字段变化不影响身份，
        # 使密钥轮换场景下节点状态（已验证/退避计数）仍能延续。
        a = {**_node("n1", "s1", port=443), "password": "old-secret"}
        b = {**_node("n1", "s1", port=443), "password": "new-secret"}
        self.assertEqual(node_key(a), node_key(b))

    def test_differs_by_identity_fields(self):
        a = _node("n1", "s1")
        b = _node("n2", "s1")
        self.assertNotEqual(node_key(a), node_key(b))

    def test_differs_by_remote_port(self):
        a = _node("n1", "s1", port=443)
        b = _node("n1", "s1", port=8443)
        self.assertNotEqual(node_key(a), node_key(b))


class TestSubscriptionDiff(unittest.TestCase):
    def test_new_nodes_are_added_and_marks_runtime_dirty(self):
        daemon = ProxyDaemon()
        with mock.patch.object(proxy_daemon, "load_candidate_nodes", return_value=_loaded([_node("n1", "s1")])):
            daemon._maybe_reload_subscription(force=True)
        self.assertEqual(len(daemon._states), 1)
        self.assertTrue(daemon._runtime_dirty)
        key = node_key(_node("n1", "s1"))
        self.assertEqual(daemon._states[key].state, proxy_daemon.STATE_NEW)

    def test_removed_nodes_are_evicted_and_dropped_from_live(self):
        daemon = ProxyDaemon()
        with mock.patch.object(proxy_daemon, "load_candidate_nodes", return_value=_loaded([_node("n1", "s1")])):
            daemon._maybe_reload_subscription(force=True)
        key = node_key(_node("n1", "s1"))
        daemon._states[key].state = proxy_daemon.STATE_VERIFIED
        daemon._live_keys.add(key)
        daemon._runtime_dirty = False
        # 过滤后无候选节点时 load_candidate_nodes 通常返回 ok=False（NO_CANDIDATES）；
        # 这里改用显式空列表模拟"仍然 ok=True 但节点集合变为空"的场景，专门测试差量剔除。
        empty_ok = NodeLoadResult(
            ok=True,
            fingerprint=ProfileFingerprint(uid="U1", path="/p", sha256="sha2", updated_at="2", entry_count=0),
            nodes=[], stats=NodeLoadStats(),
        )
        with mock.patch.object(proxy_daemon, "load_candidate_nodes", return_value=empty_ok):
            daemon._maybe_reload_subscription(force=True)
        self.assertNotIn(key, daemon._states)
        self.assertNotIn(key, daemon._live_keys)
        self.assertTrue(daemon._runtime_dirty)

    def test_unchanged_nodes_keep_state(self):
        daemon = ProxyDaemon()
        node = _node("n1", "s1")
        with mock.patch.object(proxy_daemon, "load_candidate_nodes", return_value=_loaded([node])):
            daemon._maybe_reload_subscription(force=True)
        key = node_key(node)
        daemon._states[key].state = proxy_daemon.STATE_VERIFIED
        daemon._states[key].consecutive_failures = 0
        daemon._runtime_dirty = False
        with mock.patch.object(proxy_daemon, "load_candidate_nodes", return_value=_loaded([dict(node)])):
            daemon._maybe_reload_subscription(force=True)
        self.assertEqual(daemon._states[key].state, proxy_daemon.STATE_VERIFIED)
        self.assertFalse(daemon._runtime_dirty)


class TestCheckResultStateMachine(unittest.TestCase):
    def test_success_adds_to_live_pool_and_marks_dirty(self):
        daemon = ProxyDaemon()
        key = "k1"
        daemon._states[key] = proxy_daemon.NodeState(key=key, node=_node("n1", "s1"), port=18001)
        daemon._on_check_result(key, ok=True, reason="", error_code="", exit_ip="1.1.1.1", amazon_ok=True)
        self.assertIn(key, daemon._live_keys)
        self.assertEqual(daemon._states[key].state, proxy_daemon.STATE_VERIFIED)
        self.assertEqual(daemon._states[key].consecutive_failures, 0)
        with daemon._publish_lock:
            self.assertTrue(daemon._dirty)

    def test_failure_backs_off_and_evicts_from_live(self):
        daemon = ProxyDaemon()
        key = "k1"
        daemon._states[key] = proxy_daemon.NodeState(key=key, node=_node("n1", "s1"), port=18001)
        daemon._on_check_result(key, ok=True, reason="", error_code="", exit_ip="1.1.1.1", amazon_ok=True)
        self.assertIn(key, daemon._live_keys)
        daemon._on_check_result(key, ok=False, reason="超时", error_code="CONNECT_TIMEOUT", exit_ip="")
        st = daemon._states[key]
        self.assertEqual(st.state, proxy_daemon.STATE_FAILED)
        self.assertEqual(st.consecutive_failures, 1)
        self.assertNotIn(key, daemon._live_keys)
        self.assertGreater(st.next_check_at, time.time())

    def test_repeated_failures_increase_backoff(self):
        daemon = ProxyDaemon()
        key = "k1"
        daemon._states[key] = proxy_daemon.NodeState(key=key, node=_node("n1", "s1"), port=18001)
        deadlines = []
        for _ in range(3):
            daemon._on_check_result(key, ok=False, reason="x", error_code="IP_CHECK_FAILED", exit_ip="")
            deadlines.append(daemon._states[key].next_check_at - time.time())
        # 每次失败退避时长应递增（指数回退）
        self.assertLess(deadlines[0], deadlines[1])
        self.assertLess(deadlines[1], deadlines[2])

    def test_duplicate_exit_ip_is_rejected_without_evicting_owner(self):
        daemon = ProxyDaemon()
        k1, k2 = "k1", "k2"
        daemon._states[k1] = proxy_daemon.NodeState(key=k1, node=_node("n1", "s1"), port=18001)
        daemon._states[k2] = proxy_daemon.NodeState(key=k2, node=_node("n2", "s2"), port=18002)
        daemon._on_check_result(k1, ok=True, reason="", error_code="", exit_ip="9.9.9.9", amazon_ok=True)
        daemon._on_check_result(k2, ok=True, reason="", error_code="", exit_ip="9.9.9.9", amazon_ok=True)
        self.assertIn(k1, daemon._live_keys)
        self.assertNotIn(k2, daemon._live_keys)
        self.assertEqual(daemon._states[k2].last_error_code, "DUPLICATE_EXIT_IP")
        self.assertEqual(daemon._verified_ip_owner.get("9.9.9.9"), k1)

    def test_recheck_failure_evicts_previously_verified_node(self):
        daemon = ProxyDaemon()
        key = "k1"
        daemon._states[key] = proxy_daemon.NodeState(key=key, node=_node("n1", "s1"), port=18001)
        daemon._on_check_result(key, ok=True, reason="", error_code="", exit_ip="1.1.1.1", amazon_ok=True)
        self.assertIn(key, daemon._live_keys)
        daemon._on_check_result(key, ok=False, reason="复核失败", error_code="AMAZON_UNREACHABLE", exit_ip="")
        self.assertNotIn(key, daemon._live_keys)
        self.assertNotIn("1.1.1.1", daemon._verified_ip_owner)


class TestDispatchAndCheckOne(unittest.TestCase):
    def test_check_one_success_path_calls_verify_node(self):
        daemon = ProxyDaemon()
        key = "k1"
        daemon._states[key] = proxy_daemon.NodeState(key=key, node=_node("n1", "s1"), port=18001)
        with mock.patch.object(proxy_daemon, "check_listener", return_value={"ok": True}):
            with mock.patch.object(
                proxy_daemon, "verify_node",
                return_value=NodeHealthResult(
                    name="n1", port=18001, proxy="http://127.0.0.1:18001",
                    ok=True, exit_ip="2.2.2.2", amazon_ok=True,
                ),
            ):
                daemon._check_one(key, banned_ips=set())
        self.assertIn(key, daemon._live_keys)
        self.assertNotIn(key, daemon._inflight)

    def test_check_one_port_not_listening_marks_failed_without_network_call(self):
        daemon = ProxyDaemon()
        key = "k1"
        daemon._states[key] = proxy_daemon.NodeState(key=key, node=_node("n1", "s1"), port=18001)
        with mock.patch.object(proxy_daemon, "check_listener", return_value={"ok": False}):
            with mock.patch.object(proxy_daemon, "verify_node") as verify_mock:
                daemon._check_one(key, banned_ips=set())
        verify_mock.assert_not_called()
        self.assertEqual(daemon._states[key].last_error_code, "PORT_NOT_LISTENING")

    def test_dispatch_prioritizes_new_over_failed_and_respects_concurrency(self):
        daemon = ProxyDaemon()
        for i in range(3):
            key = f"k{i}"
            daemon._states[key] = proxy_daemon.NodeState(key=key, node=_node(f"n{i}", f"s{i}"), port=18001 + i)
        daemon._states["k0"].state = proxy_daemon.STATE_FAILED
        daemon._states["k0"].next_check_at = time.time() - 1
        daemon._runtime_ok = True
        with mock.patch.object(proxy_daemon, "PROXY_DAEMON_CONCURRENCY", 2):
            with mock.patch.object(proxy_daemon, "PROXY_POOL_TARGET_NODES", 11):
                submitted = []

                def _capture(fn, key, refs, light=False):
                    submitted.append(key)

                with mock.patch.object(daemon._executor, "submit", side_effect=_capture):
                    with mock.patch.object(proxy_daemon, "get_reference_ips", return_value={}):
                        daemon._dispatch_checks()
        # NEW 状态（k1/k2）优先于 FAILED（k0），且并发上限=2 时只派发 2 个
        self.assertEqual(len(submitted), 2)
        self.assertNotIn("k0", submitted)

    def test_dispatch_keeps_one_rolling_exploration_when_pool_at_target(self):
        daemon = ProxyDaemon()
        for i in range(3):
            key = f"k{i}"
            daemon._states[key] = proxy_daemon.NodeState(
                key=key, node=_node(f"n{i}", f"s{i}"), port=18001 + i,
                state=proxy_daemon.STATE_NEW,
            )
        # 活池已达目标规模：不再爆发扩容，但仍应派发 1 个滚动探索，
        # 避免“一旦够用就永久冻结候选”。
        for i in range(14):
            key = f"live{i}"
            daemon._states[key] = proxy_daemon.NodeState(
                key=key, node=_node(f"live{i}", f"ls{i}"), port=18100 + i,
                state=proxy_daemon.STATE_VERIFIED, exit_ip=f"10.0.{i}.1",
                next_check_at=time.time() + 600,
            )
            daemon._live_keys.add(key)
        daemon._runtime_ok = True
        with mock.patch.object(proxy_daemon, "PROXY_POOL_TARGET_NODES", 14):
            submitted = []
            with mock.patch.object(
                daemon._executor, "submit",
                side_effect=lambda *a, **k: submitted.append(a[1] if len(a) > 1 else None),
            ):
                with mock.patch.object(proxy_daemon, "get_reference_ips", return_value={}):
                    daemon._dispatch_checks()
        self.assertEqual(len(submitted), 1)
        self.assertIn(submitted[0], {"k0", "k1", "k2"})


class TestPublish(unittest.TestCase):
    def test_publish_pool_writes_only_live_entries(self):
        daemon = ProxyDaemon()
        daemon._states["k1"] = proxy_daemon.NodeState(
            key="k1", node=_node("n1", "s1"), port=18001, state=proxy_daemon.STATE_VERIFIED,
            exit_ip="3.3.3.3", amazon_ok=True,
        )
        daemon._states["k2"] = proxy_daemon.NodeState(
            key="k2", node=_node("n2", "s2"), port=18002, state=proxy_daemon.STATE_FAILED,
        )
        daemon._live_keys.add("k1")
        with tempfile.TemporaryDirectory() as td:
            pool_path = Path(td) / "proxy_pool.json"
            with mock.patch.object(proxy_daemon, "PROXY_POOL_FILE", str(pool_path)):
                daemon._publish_pool()
            data = json.loads(pool_path.read_text(encoding="utf-8"))
        names = [e["name"] for e in data["entries"]]
        self.assertEqual(names, ["n1"])
        self.assertEqual(data["stats"]["verified_nodes"], 1)

    def test_publish_status_reports_counts(self):
        daemon = ProxyDaemon()
        daemon._states["k1"] = proxy_daemon.NodeState(key="k1", node=_node("n1"), state=proxy_daemon.STATE_VERIFIED)
        daemon._states["k2"] = proxy_daemon.NodeState(key="k2", node=_node("n2"), state=proxy_daemon.STATE_FAILED)
        daemon._states["k3"] = proxy_daemon.NodeState(key="k3", node=_node("n3"), state=proxy_daemon.STATE_NEW)
        daemon._live_keys.add("k1")
        with tempfile.TemporaryDirectory() as td:
            state_path = Path(td) / "state.json"
            with mock.patch.object(proxy_daemon, "PROXY_DAEMON_STATE_FILE", str(state_path)):
                daemon._publish_status()
            data = json.loads(state_path.read_text(encoding="utf-8"))
        self.assertEqual(data["candidates"], 3)
        self.assertEqual(data["verified"], 1)
        self.assertEqual(data["failed"], 1)
        self.assertEqual(data["pending"], 1)

    def test_maybe_publish_is_debounced(self):
        daemon = ProxyDaemon()
        with mock.patch.object(daemon, "_publish_pool") as pool_mock:
            with mock.patch.object(daemon, "_publish_status") as status_mock:
                daemon._mark_dirty()
                daemon._maybe_publish()
                daemon._mark_dirty()
                daemon._maybe_publish()  # 去抖窗口内，第二次不应立即再发布
        pool_mock.assert_called_once()
        status_mock.assert_called_once()


class TestRuntimeSync(unittest.TestCase):
    def test_cold_start_assigns_stable_ports(self):
        daemon = ProxyDaemon()
        daemon._port_map = {}
        daemon._free_ports = [20000, 20001]
        for i, name in enumerate(["b", "a"]):
            key = f"k{name}"
            daemon._states[key] = proxy_daemon.NodeState(key=key, node=_node(name, f"s{i}"))
        with mock.patch.object(proxy_daemon, "owned_mihomo_running", return_value=None):
            with mock.patch.object(
                proxy_daemon, "start_mihomo",
                return_value=proxy_runtime.RuntimeResult(ok=True, pid=1, ports=[20000, 20001]),
            ) as start_mock:
                with mock.patch.object(proxy_daemon, "check_listener", return_value={"ok": True}):
                    daemon._sync_runtime()
        start_mock.assert_called_once()
        self.assertTrue(daemon._runtime_ok)
        ports = sorted(st.port for st in daemon._states.values())
        self.assertEqual(ports, [20000, 20001])

    def test_hot_add_uses_reload_without_clearing_live(self):
        daemon = ProxyDaemon()
        daemon._runtime_ok = True
        daemon._pending_runtime_op = "hot_add"
        key = "k1"
        daemon._states[key] = proxy_daemon.NodeState(
            key=key, node=_node("n1", "s1"), port=18001, state=proxy_daemon.STATE_VERIFIED,
            exit_ip="1.1.1.1",
        )
        daemon._live_keys.add(key)
        daemon._port_map[key] = 18001
        with mock.patch.object(proxy_daemon, "owned_mihomo_running", return_value=99):
            with mock.patch.object(
                proxy_daemon, "reload_mihomo_config",
                return_value=proxy_runtime.RuntimeResult(ok=True, pid=99, ports=[18001]),
            ) as reload_mock:
                with mock.patch.object(proxy_daemon, "start_mihomo") as start_mock:
                    daemon._sync_runtime()
        reload_mock.assert_called_once()
        start_mock.assert_not_called()
        self.assertIn(key, daemon._live_keys)
        self.assertEqual(daemon._states[key].state, proxy_daemon.STATE_VERIFIED)

    def test_sync_with_no_nodes_stops_mihomo(self):
        daemon = ProxyDaemon()
        with mock.patch.object(proxy_daemon, "stop_owned_mihomo") as stop_mock:
            daemon._sync_runtime()
        stop_mock.assert_called_once()
        self.assertFalse(daemon._runtime_ok)

    def test_cold_start_failure_sets_retry_backoff(self):
        daemon = ProxyDaemon()
        daemon._states["k1"] = proxy_daemon.NodeState(key="k1", node=_node("n1", "s1"))
        daemon._port_map["k1"] = 18001
        with mock.patch.object(proxy_daemon, "owned_mihomo_running", return_value=None):
            with mock.patch.object(
                proxy_daemon, "start_mihomo",
                return_value=proxy_runtime.RuntimeResult(ok=False, error="boom", error_code="PROCESS_EXITED"),
            ):
                daemon._sync_runtime()
        self.assertFalse(daemon._runtime_ok)
        self.assertGreater(daemon._runtime_retry_at, time.monotonic())

    def test_stable_port_reused_across_assignments(self):
        daemon = ProxyDaemon()
        daemon._port_map = {}
        daemon._free_ports = [18005, 18006]
        with daemon._lock:
            p1 = daemon._ensure_port_locked("kA")
            p2 = daemon._ensure_port_locked("kA")
            p3 = daemon._ensure_port_locked("kB")
        self.assertEqual(p1, p2)
        self.assertNotEqual(p1, p3)


class TestDaemonPidHelpers(unittest.TestCase):
    def test_is_daemon_alive_reads_pid_file(self):
        with tempfile.TemporaryDirectory() as td:
            pid_path = Path(td) / "pid.json"
            pid_path.write_text(json.dumps({"pid": 12345}), encoding="utf-8")
            with mock.patch.object(proxy_daemon, "PROXY_DAEMON_PID_FILE", str(pid_path)):
                with mock.patch.object(proxy_daemon, "pid_alive", return_value=True):
                    self.assertEqual(proxy_daemon.is_daemon_alive(), 12345)
                with mock.patch.object(proxy_daemon, "pid_alive", return_value=False):
                    self.assertIsNone(proxy_daemon.is_daemon_alive())

    def test_is_daemon_alive_missing_file_returns_none(self):
        with tempfile.TemporaryDirectory() as td:
            missing = Path(td) / "nope.json"
            with mock.patch.object(proxy_daemon, "PROXY_DAEMON_PID_FILE", str(missing)):
                self.assertIsNone(proxy_daemon.is_daemon_alive())


if __name__ == "__main__":
    unittest.main(verbosity=2)
