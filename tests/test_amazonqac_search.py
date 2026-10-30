"""Validation plus optional read-only checks on the existing real-data test DB."""
import os
from contextlib import closing
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient
import psycopg2

from amazonqac_search import SearchRequest, app, search_keywords
from pg_config import get_pg_dsn


class RequestTests(unittest.TestCase):
    def test_invalid_requests_never_open_database(self):
        client = TestClient(app)
        with patch("amazonqac_search.psycopg2.connect") as connect:
            for body in (
                {"query": " "}, {"query": "x\x00y"}, {"query": 42},
                {"include_terms": "couch"}, {"exclude_terms": [False]},
                {"popularity_min": 10, "popularity_max": 9},
                {"word_count_min": 4, "word_count_max": 2},
                {"page": 0}, {"page": True}, {"page_size": 1001},
                {"sort": "popularity; DROP TABLE keywords"}, {"sql": "SELECT 1"},
            ):
                with self.subTest(body=body):
                    self.assertEqual(client.post("/api/v2/amazonqac/search", json=body).status_code, 422)
            connect.assert_not_called()

    def test_unavailable_database_has_safe_error(self):
        with patch("amazonqac_search.psycopg2.connect", side_effect=psycopg2.OperationalError("private DSN")):
            response = TestClient(app).post("/api/v2/amazonqac/search", json={"query": "couch"})
        self.assertEqual(response.status_code, 503)
        self.assertNotIn("private", response.text)


@unittest.skipUnless(os.getenv("PG_TEST_DSN"), "Requires the existing isolated real-data test database")
class RealTestDatabaseTests(unittest.TestCase):
    def setUp(self):
        self.environment = patch.dict(os.environ, {"TESTING": "1"})
        self.environment.start()
        self.addCleanup(self.environment.stop)

    def test_filters_match_independent_python_oracle_and_preserve_values(self):
        with closing(psycopg2.connect(get_pg_dsn())) as connection:
            connection.set_session(readonly=True)
            with connection, connection.cursor() as cursor:
                cursor.execute("SELECT final_search_term,popularity FROM amazonqac.keywords")
                original = cursor.fetchall()
        self.assertGreater(len(original), 0)
        for query, excludes, low, high, min_words, max_words in (
            ("storage", ["bag"], 1, 10000, 2, 8),
            ("couch", [], 1, 179976, 1, 10),
            ("%", [], 0, 179976, 0, 50),
            ("_", [], 0, 179976, 0, 50),
            ("' OR TRUE --", [], 0, 179976, 0, 50),
        ):
            expected = [(term, pop) for term, pop in original
                        if query in term.lower() and not any(e in term.lower() for e in excludes)
                        and low <= pop <= high and min_words <= len(term.split()) <= max_words]
            expected.sort(key=lambda row: (-row[1], row[0].encode("utf-8")))
            result = search_keywords(SearchRequest(query=query, exclude_terms=excludes,
                                     popularity_min=low, popularity_max=high,
                                     word_count_min=min_words, word_count_max=max_words, page_size=10))
            self.assertEqual(result["total"], len(expected))
            self.assertEqual([(r["final_search_term"], r["popularity"]) for r in result["items"]], expected[:10])

    def test_exact_word_boundary_and_stable_pages_on_real_terms(self):
        exact = search_keywords(SearchRequest(query="couch", match_mode="exact"))
        self.assertTrue(all(r["final_search_term"].lower() == "couch" for r in exact["items"]))
        import re
        whole = search_keywords(SearchRequest(query="couch", match_mode="words", page_size=1000))
        self.assertTrue(all(re.search(r"(?<![a-zA-Z0-9_])couch(?![a-zA-Z0-9_])", r["final_search_term"], re.I)
                            for r in whole["items"]))
        first = search_keywords(SearchRequest(query="storage", sort="popularity_asc", page_size=5))
        second = search_keywords(SearchRequest(query="storage", sort="popularity_asc", page_size=5, page=2))
        combined = search_keywords(SearchRequest(query="storage", sort="popularity_asc", page_size=10))
        self.assertEqual(first["items"] + second["items"], combined["items"])
        self.assertEqual(first["total"], second["total"])


if __name__ == "__main__":
    unittest.main()
