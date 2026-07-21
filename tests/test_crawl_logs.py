import os
import tempfile
import unittest
from unittest.mock import patch

os.environ.setdefault("DB_BACKEND", "sqlite")

import api_server


class TestCrawlLogs(unittest.IsolatedAsyncioTestCase):
    def test_read_log_tail_and_incremental(self):
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "fetch_new_arrivals.log")
            with open(path, "w", encoding="utf-8") as fh:
                fh.write("line-1 INFO start\n")
                fh.write("line-2 WARNING slow\n")
                fh.write("line-3 ERROR boom\n")
            first = api_server._read_log_tail(path, max_lines=50, since_pos=0)
            self.assertTrue(first["exists"])
            self.assertFalse(first["incremental"])
            self.assertEqual(first["lines"][-1], "line-3 ERROR boom")
            pos = first["pos"]

            with open(path, "a", encoding="utf-8") as fh:
                fh.write("line-4 INFO resumed\n")
            second = api_server._read_log_tail(path, max_lines=50, since_pos=pos)
            self.assertTrue(second["incremental"])
            self.assertEqual(second["lines"], ["line-4 INFO resumed"])

    async def test_crawl_logs_endpoint_uses_la_file(self):
        with tempfile.TemporaryDirectory() as td:
            path = os.path.join(td, "fetch_new_arrivals.log")
            with open(path, "w", encoding="utf-8") as fh:
                fh.write("2026-07-19 00:00:00 INFO [P1 1/10] ok\n")
            with patch.dict(api_server._CRAWL_LOG_FILES, {"la": path}, clear=False), \
                 patch.object(api_server, "_crawl_lifecycle_state", return_value=(True, "running", "RUN-1")), \
                 patch.object(api_server, "get_proxy_status", return_value={"status": "running", "detail": {}}):
                result = await api_server.crawl_logs(chart="la", lines=100, since_pos=0)
            self.assertEqual(result["chart"], "la")
            self.assertEqual(result["source"], "fetch_new_arrivals.log")
            self.assertTrue(result["running"])
            self.assertIn("[P1 1/10]", result["lines"][-1])


if __name__ == "__main__":
    unittest.main(verbosity=2)
