from __future__ import annotations

import os
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
    def test_runtime_pool_drop_rebuilds_and_resumes_checkpoint(self):
        old_proc = SimpleNamespace(wait=lambda: 3, pid=101)
        resumed_proc = SimpleNamespace(wait=lambda: 0, poll=lambda: None, pid=202)
        prep = SimpleNamespace(ok=True, run_id="PROXY-RECOVERED", reason="", error_code="")
        prepare_lock = threading.Lock()
        with mock.patch.object(api_server, "_crawl_generation", 5):
            with mock.patch.object(api_server, "_proxy_prepare_lock", prepare_lock):
                with mock.patch.object(api_server, "_product_proc", old_proc):
                    with mock.patch.object(api_server, "ensure_proxy_ready", return_value=prep) as ensure_mock:
                        with mock.patch.object(api_server.subprocess, "Popen", return_value=resumed_proc) as popen_mock:
                            with mock.patch.object(api_server.threading, "Thread") as thread_mock:
                                with mock.patch.object(api_server, "set_proxy_status") as status_mock:
                                    api_server._watch_and_sleep_proxy(
                                        old_proc, 5, "START-1", "PROXY-OLD", 0.0,
                                        ["python", "fetch_new_arrivals.py"], {"AMZ_RUN_ID": "START-1"}, 0,
                                    )
        # 运行时恢复委托常驻守护进程（force=False），不再与它抢占 Mihomo 做完整冷启动
        ensure_mock.assert_called_once_with(force=False)
        popen_mock.assert_called_once()
        self.assertTrue(any(
            call.args and call.args[0] == api_server.STATUS_RUNNING
            and call.kwargs.get("resumed_from_checkpoint") is True
            for call in status_mock.call_args_list
        ))
        thread_mock.assert_called_once()
        self.assertFalse(prepare_lock.locked())

    def test_runtime_pool_recovery_is_bounded(self):
        with mock.patch.object(api_server, "PROXY_RUNTIME_RECOVERY_ATTEMPTS", 1):
            with mock.patch.object(api_server, "_proxy_sleep") as sleep_mock:
                with mock.patch.object(api_server, "ensure_proxy_ready") as ensure_mock:
                    with mock.patch.object(api_server, "set_proxy_status") as status_mock:
                        api_server._recover_proxy_and_resume(
                            generation=1, request_id="START-1", previous_run_id="PROXY-OLD",
                            cmd=["python"], env={}, recovery_attempt=1,
                        )
        ensure_mock.assert_not_called()
        sleep_mock.assert_called_once_with("runtime_pool_recovery_exhausted")
        self.assertEqual(status_mock.call_args.args[0], api_server.STATUS_PROXY_FAILED)
        self.assertEqual(status_mock.call_args.kwargs["error_code"], "POOL_RECOVERY_EXHAUSTED")

    def test_non_proxy_crawler_crash_is_not_reported_as_idle(self):
        proc = SimpleNamespace(wait=lambda: 7, pid=303)
        with mock.patch.object(api_server, "_crawl_generation", 9):
            with mock.patch.object(api_server, "_product_proc", proc):
                with mock.patch.object(api_server, "_proxy_sleep"):
                    with mock.patch.object(api_server, "set_proxy_status") as status_mock:
                        api_server._watch_and_sleep_proxy(proc, 9, "START-2", "PROXY-2", 0.0)
        self.assertEqual(status_mock.call_args.args[0], api_server.STATUS_PROXY_FAILED)
        self.assertEqual(status_mock.call_args.kwargs["error_code"], "CRAWLER_EXIT_FAILED")

    def test_retry_pending_exit_preserves_explicit_status(self):
        proc = SimpleNamespace(wait=lambda: 4, pid=304)
        with mock.patch.object(api_server, "_crawl_generation", 10):
            with mock.patch.object(api_server, "_product_proc", proc):
                with mock.patch.object(api_server, "_proxy_sleep"):
                    with mock.patch.object(api_server, "set_proxy_status") as status_mock:
                        api_server._watch_and_sleep_proxy(proc, 10, "START-3", "PROXY-3", 0.0)
        self.assertEqual(status_mock.call_args.args[0], api_server.STATUS_PROXY_FAILED)
        self.assertEqual(status_mock.call_args.kwargs["error_code"], "CRAWL_RETRY_PENDING")

    def test_natural_end_sets_idle_without_idle_timer(self):
        """常驻守护进程/独立 Mihomo 一直运行，抓取自然结束后不再需要延迟关闭
        Mihomo 的空闲计时器线程——直接把生命周期状态复位为 idle 即可。"""
        proc = SimpleNamespace(wait=lambda: 0, pid=305)
        with mock.patch.object(api_server, "_crawl_generation", 11):
            with mock.patch.object(api_server, "_product_proc", proc):
                with mock.patch.object(api_server.threading, "Thread") as thread_mock:
                    with mock.patch.object(api_server, "set_proxy_status") as status_mock:
                        api_server._watch_and_sleep_proxy(proc, 11, "START-4", "PROXY-4", 0.0)
        status_mock.assert_called_once_with(
            api_server.STATUS_IDLE, run_id="PROXY-4", pool_ready=True,
        )
        thread_mock.assert_not_called()

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

    async def test_lifespan_startup_ensures_daemon_running(self):
        """只要 API 服务在运行，常驻验证守护进程也应在运行；lifespan 启动时
        应确保守护进程存活，且不等待它完成任何验证（fire-and-forget）。"""
        with mock.patch.object(api_server, "DB_BACKEND", "sqlite"):
            with mock.patch.object(
                api_server, "ensure_daemon_running", return_value={"ok": True, "pid": 999, "started": True},
            ) as ensure_daemon_mock:
                with mock.patch.object(
                    api_server, "stop_daemon", return_value={"ok": True, "forced": False},
                ) as stop_daemon_mock:
                    with mock.patch.dict(os.environ, {"PROXY_DAEMON_PERSIST": "0"}):
                        async with api_server.lifespan(api_server.app):
                            pass
        ensure_daemon_mock.assert_called_once_with()
        stop_daemon_mock.assert_called_once_with()

    async def test_lifespan_persistent_daemon_requires_explicit_opt_in(self):
        with mock.patch.object(api_server, "DB_BACKEND", "sqlite"):
            with mock.patch.object(
                api_server, "ensure_daemon_running", return_value={"ok": True, "pid": 999, "started": False},
            ):
                with mock.patch.object(api_server, "stop_daemon") as stop_daemon_mock:
                    with mock.patch.dict(os.environ, {"PROXY_DAEMON_PERSIST": "1"}):
                        async with api_server.lifespan(api_server.app):
                            pass
        stop_daemon_mock.assert_not_called()

    async def test_lifespan_exception_still_cleans_daemon(self):
        with mock.patch.object(api_server, "DB_BACKEND", "sqlite"), \
             mock.patch.object(
                 api_server, "ensure_daemon_running",
                 return_value={"ok": True, "pid": 999, "started": True},
             ), \
             mock.patch.object(
                 api_server, "stop_daemon", return_value={"ok": True, "forced": False},
             ) as stop_daemon_mock, \
             mock.patch.dict(os.environ, {"PROXY_DAEMON_PERSIST": "0"}):
            with self.assertRaisesRegex(RuntimeError, "boom"):
                async with api_server.lifespan(api_server.app):
                    raise RuntimeError("boom")
        stop_daemon_mock.assert_called_once_with()

    async def test_proxy_status_merges_daemon_state(self):
        with mock.patch.object(api_server, "get_proxy_status", return_value={"status": "idle", "run_id": ""}):
            with mock.patch.object(api_server, "daemon_alive", return_value=4321):
                with mock.patch.object(
                    api_server, "daemon_status",
                    return_value={"candidates": 5, "verified": 3, "checking": 1, "failed": 1},
                ):
                    result = await api_server.proxy_status()
        self.assertEqual(result["status"], "idle")
        self.assertEqual(result["daemon"]["alive"], True)
        self.assertEqual(result["daemon"]["pid"], 4321)
        self.assertEqual(result["daemon"]["verified"], 3)
        self.assertEqual(result["daemon"]["candidates"], 5)

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
