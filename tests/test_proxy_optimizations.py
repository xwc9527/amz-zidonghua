"""风险缓解优化：低可用降速、轻量复核、动态扩容。"""
from __future__ import annotations

import sys
import threading
import time
import unittest
from pathlib import Path
from queue import Queue
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from crawl_autoscale import run_autoscaled_queue
from proxy_health import NodeHealthResult, verify_node
from proxy_worker import pool_aware_delay, pool_aware_rotate_after


class TestPoolAwareDelay(unittest.TestCase):
    def test_full_pool_no_scale(self):
        vals = [pool_aware_delay(1.0, 1.0, usable=11, target=11, scale=2.0) for _ in range(5)]
        self.assertTrue(all(abs(v - 1.0) < 1e-9 for v in vals))

    def test_empty_pool_applies_full_scale(self):
        v = pool_aware_delay(1.0, 1.0, usable=0, target=10, scale=2.0)
        self.assertAlmostEqual(v, 2.0, places=6)

    def test_rotate_after_forces_one_when_low(self):
        self.assertEqual(pool_aware_rotate_after(1, target=11), 1)


class TestLightVerify(unittest.TestCase):
    def test_light_skips_amazon(self):
        entry = {"name": "n1", "port": 18001, "proxy": "http://127.0.0.1:18001"}
        with mock.patch("proxy_health.fetch_exit_ip") as fetch_mock:
            fetch_mock.return_value = mock.Mock(
                ok=True, ip="8.8.8.8", to_dict=lambda: {"ip": "8.8.8.8"},
            )
            with mock.patch("proxy_health.check_amazon") as amz_mock:
                with mock.patch("proxy_health.lookup_isp_hint", return_value={}):
                    res = verify_node(entry, light=True)
        self.assertTrue(res.ok)
        self.assertEqual(res.reason, "ok_light")
        amz_mock.assert_not_called()


class TestAutoscale(unittest.TestCase):
    def test_spawns_extra_workers_when_pool_grows(self):
        q = Queue()
        for i in range(20):
            q.put(i)
        seen = []
        pool = mock.Mock()
        pool.usable_count = 1
        gate = threading.Event()

        def worker(wid, task_q, pool_arg):
            seen.append(wid)
            # 第一个 worker 等扩容发生后再继续消费，给 monitor 留出 spawn 窗口
            if wid == 0:
                gate.wait(timeout=2)
            while True:
                try:
                    task_q.get(timeout=0.3)
                    time.sleep(0.05)
                except Exception:
                    break

        def grow():
            time.sleep(0.15)
            pool.usable_count = 3
            time.sleep(0.4)
            gate.set()

        threading.Thread(target=grow, daemon=True).start()
        run_autoscaled_queue(
            worker, q, pool,
            initial_workers=1, max_workers=3, scale_interval=0.15, log_prefix="t",
        )
        self.assertGreaterEqual(len(set(seen)), 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
