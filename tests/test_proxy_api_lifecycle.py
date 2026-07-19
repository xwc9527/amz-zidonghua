from __future__ import annotations

import threading
import unittest
from types import SimpleNamespace
from unittest import mock

import api_server


class _AcquireTimeoutLock:
    def acquire(self, *args, **kwargs):
        return False

    def release(self):
        raise AssertionError("未取得锁时不得释放")


class TestProxyApiLifecycle(unittest.IsolatedAsyncioTestCase):
    def test_idle_timeout_stops_only_current_generation(self):
        with mock.patch.object(api_server, "_crawl_generation", 7):
            with mock.patch.object(api_server, "_product_proc", None):
                with mock.patch.object(api_server, "stop_proxy_pool", return_value={"ok": True}) as stop_mock:
                    with mock.patch.object(api_server, "set_proxy_status") as status_mock:
                        api_server._stop_proxy_after_idle(7, "PROXY-IDLE", 0)
        stop_mock.assert_called_once()
        status_mock.assert_called_once_with(
            api_server.STATUS_IDLE,
            run_id="PROXY-IDLE",
            pool_ready=False,
        )

    def test_idle_timeout_is_cancelled_by_new_generation(self):
        with mock.patch.object(api_server, "_crawl_generation", 8):
            with mock.patch.object(api_server, "stop_proxy_pool") as stop_mock:
                api_server._stop_proxy_after_idle(7, "PROXY-OLD", 0)
        stop_mock.assert_not_called()

    def test_lifecycle_reports_proxy_preparation_as_active(self):
        with mock.patch.object(api_server, "_product_proc", None):
            with mock.patch.object(
                api_server,
                "get_proxy_status",
                return_value={
                    "status": api_server.STATUS_PREPARING,
                    "run_id": "PROXY-PREPARING",
                    "detail": {},
                },
            ):
                running, lifecycle, run_id = api_server._crawl_lifecycle_state()
        self.assertTrue(running)
        self.assertEqual(lifecycle, api_server.STATUS_PREPARING)
        self.assertEqual(run_id, "PROXY-PREPARING")

    def test_lifecycle_reports_idle_as_inactive(self):
        with mock.patch.object(api_server, "_product_proc", None):
            with mock.patch.object(
                api_server,
                "get_proxy_status",
                return_value={"status": api_server.STATUS_IDLE, "run_id": "", "detail": {}},
            ):
                running, lifecycle, run_id = api_server._crawl_lifecycle_state()
        self.assertFalse(running)
        self.assertEqual(lifecycle, api_server.STATUS_IDLE)
        self.assertEqual(run_id, "")

    async def test_stop_timeout_does_not_touch_process_or_pool(self):
        sentinel_proc = SimpleNamespace(poll=lambda: None)
        with mock.patch.object(api_server, "_proxy_prepare_lock", _AcquireTimeoutLock()):
            with mock.patch.object(api_server, "_product_proc", sentinel_proc):
                with mock.patch.object(api_server, "stop_proxy_pool") as stop_mock:
                    with mock.patch.object(api_server, "set_proxy_status"):
                        result = await api_server.stop_products()
        self.assertEqual(result["status"], "stop_timeout")
        self.assertEqual(result["error_code"], "STOP_PREPARE_TIMEOUT")
        stop_mock.assert_not_called()

    async def test_crawler_start_failure_cleans_proxy_pool(self):
        prep = SimpleNamespace(
            ok=True,
            run_id="PROXY-TEST",
            candidate_nodes=5,
            verified_nodes=4,
            unique_ips=4,
            reason="ok",
            error_code="",
            fail_reasons={},
        )
        cleanup = {"ok": True, "pid": 123}
        prepare_lock = threading.Lock()
        with mock.patch.object(api_server, "_proxy_prepare_lock", prepare_lock):
            with mock.patch.object(api_server, "_product_proc", None):
                with mock.patch.object(api_server, "_validate_start_filters", return_value=None):
                    with mock.patch.object(api_server, "ensure_proxy_ready", return_value=prep):
                        with mock.patch.object(api_server.subprocess, "Popen", side_effect=OSError("spawn boom")):
                            with mock.patch.object(
                                api_server, "stop_proxy_pool", return_value=cleanup
                            ) as stop_mock:
                                with mock.patch.object(api_server, "set_proxy_status"):
                                    result = await api_server.start_products({
                                        "chart": "la",
                                        "roots": ["11965981"],
                                        "site": "US",
                                        "max_pages": 1,
                                        "include_descendants": False,
                                    })
        self.assertEqual(result["status"], api_server.STATUS_PROXY_FAILED)
        self.assertEqual(result["error_code"], "CRAWLER_START_FAILED")
        stop_mock.assert_called_once()
        self.assertFalse(prepare_lock.locked())


if __name__ == "__main__":
    unittest.main(verbosity=2)
