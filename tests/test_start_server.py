from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest import mock

import start_server


class _FakeLock:
    def __init__(self, acquired=True):
        self.acquired = acquired
        self.error = "busy" if not acquired else ""
        self.released = False

    def acquire(self):
        return self.acquired

    def release(self):
        self.released = True


class _FakeProcess:
    def __init__(self, code=0, pid=1234, interrupt=False):
        self.returncode = code
        self.pid = pid
        self.interrupt = interrupt
        self.wait_calls = 0
        self.terminated = False
        self.killed = False

    def wait(self, timeout=None):
        self.wait_calls += 1
        if self.interrupt and self.wait_calls == 1:
            raise KeyboardInterrupt
        return self.returncode

    def poll(self):
        return None if self.interrupt and not self.terminated else self.returncode

    def terminate(self):
        self.terminated = True

    def kill(self):
        self.killed = True


class TestSingleInstanceLock(unittest.TestCase):
    def test_second_lock_is_rejected_and_release_allows_next(self):
        with tempfile.TemporaryDirectory() as td:
            path = Path(td) / "server.lock"
            first = start_server.SingleInstanceLock(path, host="127.0.0.1", port=8081)
            second = start_server.SingleInstanceLock(path, host="127.0.0.1", port=8081)
            try:
                self.assertTrue(first.acquire())
                self.assertFalse(second.acquire())
                owner = start_server._read_lock_owner(path)
                self.assertEqual(owner["port"], 8081)
            finally:
                first.release()
                second.release()
            third = start_server.SingleInstanceLock(path, host="127.0.0.1", port=8081)
            try:
                self.assertTrue(third.acquire())
            finally:
                third.release()


class TestStartDecision(unittest.TestCase):
    def _run_main(self, lock, state):
        with mock.patch.object(start_server, "SingleInstanceLock", return_value=lock), \
             mock.patch.object(start_server, "probe_port", return_value=state), \
             mock.patch.object(start_server, "supervise") as supervise_mock, \
             mock.patch.object(start_server, "_emit"):
            result = start_server.main(["8081"])
        return result, supervise_mock

    def test_duplicate_supervisor_exits_without_spawning(self):
        lock = _FakeLock(acquired=False)
        with mock.patch.object(start_server, "SingleInstanceLock", return_value=lock), \
             mock.patch.object(start_server, "_read_lock_owner", return_value={"pid": 99}), \
             mock.patch.object(start_server, "supervise") as supervise_mock, \
             mock.patch.object(start_server, "_emit"):
            result = start_server.main(["8081"])
        self.assertEqual(result, 0)
        supervise_mock.assert_not_called()

    def test_existing_project_api_exits_cleanly(self):
        lock = _FakeLock()
        result, supervise_mock = self._run_main(lock, {
            "occupied": True, "project_api": True, "pid": 2396,
        })
        self.assertEqual(result, 0)
        supervise_mock.assert_not_called()
        self.assertTrue(lock.released)

    def test_foreign_port_owner_is_fatal_without_retry(self):
        lock = _FakeLock()
        result, supervise_mock = self._run_main(lock, {
            "occupied": True, "project_api": False, "pid": 888,
        })
        self.assertEqual(result, 2)
        supervise_mock.assert_not_called()
        self.assertTrue(lock.released)

    def test_invalid_port_is_rejected(self):
        self.assertEqual(start_server.main(["bad"]), 2)
        self.assertEqual(start_server.main(["70000"]), 2)


class TestRestartPolicy(unittest.TestCase):
    def test_clean_exit_never_restarts(self):
        proc = _FakeProcess(code=0)
        with mock.patch.object(start_server.subprocess, "Popen", return_value=proc) as popen_mock, \
             mock.patch.object(start_server.time, "monotonic", side_effect=[10.0, 13.0]), \
             mock.patch.object(start_server.time, "sleep") as sleep_mock, \
             mock.patch.object(start_server, "_emit"):
            result = start_server.supervise("127.0.0.1", 8081, dev_reload=False, env={})
        self.assertEqual(result, 0)
        popen_mock.assert_called_once()
        sleep_mock.assert_not_called()

    def test_crashes_have_hard_restart_limit(self):
        processes = [_FakeProcess(code=1, pid=2000 + i) for i in range(3)]
        times = [float(i) for i in range(6)]
        with mock.patch.object(start_server, "MAX_RESTARTS", 2), \
             mock.patch.object(start_server.subprocess, "Popen", side_effect=processes) as popen_mock, \
             mock.patch.object(start_server, "probe_port", return_value={"occupied": False}), \
             mock.patch.object(start_server.time, "monotonic", side_effect=times), \
             mock.patch.object(start_server.time, "sleep") as sleep_mock, \
             mock.patch.object(start_server, "_emit"):
            result = start_server.supervise("127.0.0.1", 8081, dev_reload=False, env={})
        self.assertEqual(result, 1)
        self.assertEqual(popen_mock.call_count, 3)
        self.assertEqual(sleep_mock.call_count, 2)

    def test_port_conflict_after_crash_stops_immediately(self):
        proc = _FakeProcess(code=1)
        with mock.patch.object(start_server.subprocess, "Popen", return_value=proc) as popen_mock, \
             mock.patch.object(start_server, "probe_port", return_value={
                 "occupied": True, "project_api": False, "pid": 777,
             }), \
             mock.patch.object(start_server.time, "monotonic", side_effect=[1.0, 2.0]), \
             mock.patch.object(start_server.time, "sleep") as sleep_mock, \
             mock.patch.object(start_server, "_emit"):
            result = start_server.supervise("127.0.0.1", 8081, dev_reload=False, env={})
        self.assertEqual(result, 2)
        popen_mock.assert_called_once()
        sleep_mock.assert_not_called()

    def test_keyboard_interrupt_stops_child_without_restart(self):
        proc = _FakeProcess(code=0, interrupt=True)
        with mock.patch.object(start_server.subprocess, "Popen", return_value=proc), \
             mock.patch.object(start_server.time, "monotonic", return_value=1.0), \
             mock.patch.object(start_server.time, "sleep") as sleep_mock, \
             mock.patch.object(start_server, "_emit"):
            result = start_server.supervise("127.0.0.1", 8081, dev_reload=False, env={})
        self.assertEqual(result, 0)
        self.assertTrue(proc.terminated)
        sleep_mock.assert_not_called()

    def test_keyboard_interrupt_during_backoff_stops_restart(self):
        proc = _FakeProcess(code=1)
        with mock.patch.object(start_server.subprocess, "Popen", return_value=proc) as popen_mock, \
             mock.patch.object(start_server, "probe_port", return_value={"occupied": False}), \
             mock.patch.object(start_server.time, "monotonic", side_effect=[1.0, 2.0]), \
             mock.patch.object(start_server.time, "sleep", side_effect=KeyboardInterrupt), \
             mock.patch.object(start_server, "_emit"):
            result = start_server.supervise("127.0.0.1", 8081, dev_reload=False, env={})
        self.assertEqual(result, 0)
        popen_mock.assert_called_once()


if __name__ == "__main__":
    unittest.main(verbosity=2)
