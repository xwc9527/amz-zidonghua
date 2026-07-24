"""按需代理质量体检（check_pool_quality）单测：不发真实网络请求，
mock proxy_health.check_amazon 返回值，只验证聚合/分级逻辑。"""
from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import proxy_pool_manager
from proxy_health import AmazonResult


def _write_pool(path: Path, entries: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"entries": entries}, ensure_ascii=False), encoding="utf-8")


class TestCheckPoolQuality(unittest.TestCase):
    def _entry(self, name: str, tier: str = "hot", port: int = 18000) -> dict:
        return {
            "name": name, "port": port, "proxy": f"http://127.0.0.1:{port}",
            "exit_ip": f"1.2.3.{port % 256}", "tier": tier,
        }

    def test_no_data_when_pool_empty(self):
        with tempfile.TemporaryDirectory() as td:
            pool_file = Path(td) / "proxy_pool.json"
            _write_pool(pool_file, [])
            with mock.patch.object(proxy_pool_manager, "PROXY_POOL_FILE", str(pool_file)):
                report = proxy_pool_manager.check_pool_quality()
        self.assertEqual(report["total"], 0)
        self.assertEqual(report["verdict"], "no_data")

    def test_healthy_when_most_nodes_ok(self):
        entries = [self._entry(f"n{i}", port=18000 + i) for i in range(5)]
        with tempfile.TemporaryDirectory() as td:
            pool_file = Path(td) / "proxy_pool.json"
            _write_pool(pool_file, entries)

            def fake_check_amazon(proxy_url, url=None, timeout=None):
                # 4/5 成功，1/5 超时失败 -> ok_rate=0.8 应判定为 healthy
                if proxy_url.endswith("18004"):
                    return AmazonResult(ok=False, elapsed_ms=12000, error="timeout",
                                         error_code="AMAZON_UNREACHABLE")
                return AmazonResult(ok=True, status_code=200, elapsed_ms=800)

            with mock.patch.object(proxy_pool_manager, "PROXY_POOL_FILE", str(pool_file)), \
                 mock.patch.object(proxy_pool_manager, "check_amazon", side_effect=fake_check_amazon):
                report = proxy_pool_manager.check_pool_quality()

        self.assertEqual(report["total"], 5)
        self.assertEqual(report["ok"], 4)
        self.assertEqual(report["failed"], 1)
        self.assertEqual(report["verdict"], "healthy")
        self.assertGreater(report["avg_latency_ms"], 0)
        self.assertEqual(len(report["nodes"]), 5)

    def test_unhealthy_when_most_nodes_fail(self):
        entries = [self._entry(f"n{i}", port=18010 + i) for i in range(6)]
        with tempfile.TemporaryDirectory() as td:
            pool_file = Path(td) / "proxy_pool.json"
            _write_pool(pool_file, entries)

            def fake_check_amazon(proxy_url, url=None, timeout=None):
                # 只有 1/6 成功 -> ok_rate ~0.167 应判定为 unhealthy
                if proxy_url.endswith("18010"):
                    return AmazonResult(ok=True, status_code=200, elapsed_ms=500)
                return AmazonResult(ok=False, elapsed_ms=12000, error="timeout",
                                     error_code="AMAZON_UNREACHABLE")

            with mock.patch.object(proxy_pool_manager, "PROXY_POOL_FILE", str(pool_file)), \
                 mock.patch.object(proxy_pool_manager, "check_amazon", side_effect=fake_check_amazon):
                report = proxy_pool_manager.check_pool_quality()

        self.assertEqual(report["total"], 6)
        self.assertEqual(report["ok"], 1)
        self.assertEqual(report["verdict"], "unhealthy")

    def test_degraded_band(self):
        entries = [self._entry(f"n{i}", port=18020 + i) for i in range(10)]
        with tempfile.TemporaryDirectory() as td:
            pool_file = Path(td) / "proxy_pool.json"
            _write_pool(pool_file, entries)

            def fake_check_amazon(proxy_url, url=None, timeout=None):
                # 5/10 成功 -> ok_rate=0.5 落在 [0.3, 0.7) -> degraded
                idx = int(proxy_url.rsplit(":", 1)[1]) - 18020
                if idx < 5:
                    return AmazonResult(ok=True, status_code=200, elapsed_ms=600)
                return AmazonResult(ok=False, elapsed_ms=12000, error="timeout",
                                     error_code="AMAZON_UNREACHABLE")

            with mock.patch.object(proxy_pool_manager, "PROXY_POOL_FILE", str(pool_file)), \
                 mock.patch.object(proxy_pool_manager, "check_amazon", side_effect=fake_check_amazon):
                report = proxy_pool_manager.check_pool_quality()

        self.assertEqual(report["verdict"], "degraded")

    def test_captcha_counted_and_treated_as_failure(self):
        entries = [self._entry("n0", port=18030)]
        with tempfile.TemporaryDirectory() as td:
            pool_file = Path(td) / "proxy_pool.json"
            _write_pool(pool_file, entries)

            def fake_check_amazon(proxy_url, url=None, timeout=None):
                return AmazonResult(ok=False, status_code=200, elapsed_ms=900,
                                     captcha=True, error="captcha_page", error_code="CAPTCHA")

            with mock.patch.object(proxy_pool_manager, "PROXY_POOL_FILE", str(pool_file)), \
                 mock.patch.object(proxy_pool_manager, "check_amazon", side_effect=fake_check_amazon):
                report = proxy_pool_manager.check_pool_quality()

        self.assertEqual(report["captcha"], 1)
        self.assertEqual(report["ok"], 0)
        self.assertEqual(report["verdict"], "unhealthy")

    def test_falls_back_to_all_entries_when_tier_missing(self):
        # tier 字段缺失时，按 ("hot","warm") 过滤会全部落空，应兜底探测全部条目
        entries = [{"name": "n0", "port": 18040, "proxy": "http://127.0.0.1:18040",
                    "exit_ip": "9.9.9.9"}]
        with tempfile.TemporaryDirectory() as td:
            pool_file = Path(td) / "proxy_pool.json"
            _write_pool(pool_file, entries)

            with mock.patch.object(proxy_pool_manager, "PROXY_POOL_FILE", str(pool_file)), \
                 mock.patch.object(proxy_pool_manager, "check_amazon",
                                    return_value=AmazonResult(ok=True, status_code=200, elapsed_ms=700)):
                report = proxy_pool_manager.check_pool_quality()

        self.assertEqual(report["total"], 1)
        self.assertEqual(report["ok"], 1)


if __name__ == "__main__":
    unittest.main()
