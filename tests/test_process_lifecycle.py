"""Web/代理子进程生命周期回归：单实例、优雅退出和 Windows 无弹窗。"""
from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import proxy_daemon
import proxy_pool_manager
import proxy_runtime


class TestDaemonSingleInstance(unittest.TestCase):
    def test_os_lock_rejects_second_daemon_and_releases(self):
        with tempfile.TemporaryDirectory() as td:
            path = str(Path(td) / "daemon.lock")
            first = proxy_daemon.DaemonInstanceLock(path)
            second = proxy_daemon.DaemonInstanceLock(path)
            third = proxy_daemon.DaemonInstanceLock(path)
            self.assertTrue(first.acquire())
            self.assertFalse(second.acquire())
            first.release()
            self.assertTrue(third.acquire())
            third.release()

    def test_file_stop_request_targets_only_current_pid(self):
        daemon = proxy_daemon.ProxyDaemon()
        with tempfile.TemporaryDirectory() as td:
            stop_path = Path(td) / "stop.json"
            with mock.patch.object(proxy_daemon, "_DAEMON_STOP_FILE", str(stop_path)):
                proxy_daemon.request_daemon_stop(os.getpid())
                self.assertTrue(daemon._file_stop_requested())
                proxy_daemon.request_daemon_stop(os.getpid() + 1000)
                self.assertFalse(daemon._file_stop_requested())
        daemon._executor.shutdown(wait=True, cancel_futures=True)


class TestHiddenChildProcesses(unittest.TestCase):
    def test_pid_probe_does_not_spawn_tasklist(self):
        with mock.patch.object(proxy_runtime.subprocess, "run") as run_mock:
            self.assertTrue(proxy_runtime._pid_alive(os.getpid()))
        run_mock.assert_not_called()

    def test_daemon_spawn_redirects_all_standard_handles(self):
        calls = {"n": 0}

        def fake_alive():
            calls["n"] += 1
            return None if calls["n"] <= 2 else 777

        with mock.patch.object(proxy_pool_manager, "daemon_alive", side_effect=fake_alive):
            with mock.patch.object(proxy_pool_manager.subprocess, "Popen") as popen_mock:
                result = proxy_pool_manager.ensure_daemon_running(wait_pid_sec=1)
        self.assertTrue(result["ok"])
        kwargs = popen_mock.call_args.kwargs
        self.assertIs(kwargs["stdin"], subprocess.DEVNULL)
        self.assertIs(kwargs["stdout"], subprocess.DEVNULL)
        self.assertIs(kwargs["stderr"], subprocess.DEVNULL)
        if os.name == "nt":
            self.assertTrue(kwargs["creationflags"] & subprocess.CREATE_NO_WINDOW)

    def test_mihomo_cannot_break_away_and_has_no_stdin(self):
        proc = mock.Mock(pid=4321)
        proc.poll.return_value = None
        ready = proxy_runtime.RuntimeResult(ok=True, pid=4321, ports=[18001])
        fake_log = mock.MagicMock()
        with mock.patch.object(proxy_runtime.os.path, "isfile", return_value=True), \
             mock.patch.object(proxy_runtime, "_write_mihomo_config", return_value="config.yaml"), \
             mock.patch("builtins.open", return_value=fake_log), \
             mock.patch.object(proxy_runtime.subprocess, "Popen", return_value=proc) as popen_mock, \
             mock.patch.object(proxy_runtime, "_assign_kill_on_close_job"), \
             mock.patch.object(proxy_runtime, "_atomic_write_json"), \
             mock.patch.object(proxy_runtime.time, "sleep"), \
             mock.patch.object(proxy_runtime, "wait_ports_ready", return_value=ready), \
             mock.patch.object(proxy_runtime, "_port_listening", return_value=True):
            result = proxy_runtime.start_mihomo([{"name": "n1"}], ports=[18001])
        self.assertTrue(result.ok)
        kwargs = popen_mock.call_args.kwargs
        self.assertIs(kwargs["stdin"], subprocess.DEVNULL)
        if os.name == "nt":
            self.assertTrue(kwargs["creationflags"] & subprocess.CREATE_NO_WINDOW)
            self.assertFalse(kwargs["creationflags"] & 0x01000000)


class TestGracefulStop(unittest.TestCase):
    def test_stop_daemon_requests_cleanup_instead_of_force_kill(self):
        with mock.patch.object(proxy_pool_manager, "daemon_alive", side_effect=[321, None, None]), \
             mock.patch.object(proxy_pool_manager, "request_daemon_stop") as request_mock, \
             mock.patch.object(proxy_pool_manager, "stop_owned_mihomo") as stop_mihomo_mock:
            result = proxy_pool_manager.stop_daemon(wait_sec=0.1)
        self.assertTrue(result["ok"])
        self.assertFalse(result["forced"])
        request_mock.assert_called_once_with(321)
        stop_mihomo_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main(verbosity=2)
