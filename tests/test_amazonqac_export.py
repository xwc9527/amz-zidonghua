"""CSV lifecycle checks plus real-data checks on the isolated test database."""
from contextlib import closing, contextmanager
import csv
import io
from itertools import zip_longest
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import uuid

from fastapi.testclient import TestClient
import psycopg2

from amazonqac_search import CSVFileResponse, app
from pg_config import get_pg_dsn


@contextmanager
def test_directory():
    # Windows restricted tokens cannot reopen mode-0700 mkdtemp directories.
    path = Path(tempfile.gettempdir()) / ("amazonqac-test-" + uuid.uuid4().hex)
    path.mkdir()
    try:
        yield str(path)
    finally:
        for child in path.iterdir():
            child.unlink()
        path.rmdir()


class ExportValidationTests(unittest.TestCase):
    def test_invalid_filters_and_pagination_do_not_generate_files(self):
        with patch("amazonqac_search.tempfile.mkstemp") as create:
            for body in ({"query": " "}, {"page": 2}, {"page_size": 100000},
                         {"popularity_min": 5, "popularity_max": 1}):
                self.assertEqual(TestClient(app).post("/api/v2/amazonqac/export", json=body).status_code, 422)
            create.assert_not_called()

    def test_database_failure_removes_partial_file(self):
        with test_directory() as directory:
            with patch("amazonqac_search.tempfile.tempdir", directory), patch(
                "amazonqac_search.write_csv_export", side_effect=psycopg2.OperationalError("private DSN")
            ):
                response = TestClient(app).post("/api/v2/amazonqac/export", json={"query": "couch"})
            self.assertEqual(response.status_code, 503)
            self.assertNotIn("private", response.text)
            self.assertEqual(list(Path(directory).iterdir()), [])

    def test_temporary_storage_failure_returns_safe_error(self):
        with patch("amazonqac_search.tempfile.mkstemp", side_effect=OSError("private disk path")):
            response = TestClient(app).post("/api/v2/amazonqac/export", json={})
        self.assertEqual(response.status_code, 507)
        self.assertNotIn("private", response.text)


class ResponseLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_failed_transfer_still_removes_file(self):
        with test_directory() as directory:
            path = Path(directory) / "download.csv"
            path.write_bytes(b"header\n")
            response = CSVFileResponse(path)
            async def send(message):
                raise ConnectionError("Client disconnected")
            async def receive():
                return {"type": "http.disconnect"}
            with self.assertRaises(ConnectionError):
                await response({"type": "http", "method": "POST", "headers": []}, receive, send)
            self.assertFalse(path.exists())


@unittest.skipUnless(os.getenv("PG_TEST_DSN"), "Requires isolated real-data PG_TEST_DSN")
class RealDatabaseExportTests(unittest.TestCase):
    def test_all_rows_and_sorted_filter_preserve_original_values_and_source(self):
        with patch.dict(os.environ, {"TESTING": "1"}), test_directory() as directory:
            with patch("amazonqac_search.tempfile.tempdir", directory):
                for body,where,params,direction in (
                    ({}, "TRUE", [], "DESC"),
                    ({"query": "storage", "sort": "popularity_asc"},
                     "final_search_term ILIKE %s", ["%storage%"], "ASC"),
                    ({"query": "__qac_absent_20261030__"},
                     "final_search_term ILIKE %s", ["%__qac_absent_20261030__%"], "DESC"),
                ):
                    response = TestClient(app).post("/api/v2/amazonqac/export", json=body)
                    self.assertEqual(response.status_code, 200)
                    self.assertTrue(response.content.startswith(b"\xef\xbb\xbf"))
                    self.assertEqual(int(response.headers["content-length"]), len(response.content))
                    reader = csv.DictReader(io.StringIO(response.content.decode("utf-8-sig"), newline=""))
                    count = 0
                    with closing(psycopg2.connect(get_pg_dsn())) as connection:
                        connection.set_session(readonly=True)
                        with connection, connection.cursor(name="export_test_oracle") as cursor:
                            cursor.execute(
                                "SELECT final_search_term,popularity,k.source_id,d.dataset_name,d.revision,d.source_url,"
                                "d.split,d.market,d.period_start,d.period_end,d.popularity_definition "
                                "FROM amazonqac.keywords k JOIN amazonqac.dataset d USING(source_id) WHERE " + where +
                                " ORDER BY popularity " + direction + ',final_search_term COLLATE "C" ASC', params,
                            )
                            for row, expected in zip_longest(reader, cursor):
                                self.assertIsNotNone(row)
                                self.assertIsNotNone(expected)
                                self.assertEqual(list(row.values()), [str(v) for v in expected])
                                count += 1
                    self.assertEqual(count, int(response.headers["X-Export-Row-Count"]))
                    if not body:
                        self.assertGreater(count, 1000)
                    self.assertEqual(list(Path(directory).iterdir()), [])


if __name__ == "__main__":
    unittest.main()
