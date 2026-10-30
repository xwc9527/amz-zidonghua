"""Offline route contracts; live AMZ HTTP acceptance is recorded separately."""
import os
import unittest
from unittest.mock import patch

os.environ.setdefault("DB_BACKEND", "sqlite")
from fastapi.testclient import TestClient
import api_server
import keyword_search as search_api
from oxylabs_amazon_probe import ProbeError


def envelope(keyword="behind couch table", page=1, price=29.99, asin="B012345678"):
    from urllib.parse import urlencode
    return {
        "state": "done", "metadata": {"request_id": f"request-{keyword}-{page}"},
        "results": [{"metadata": {"statuses": {"http_request": {"code": 200, "tag": "HTTP_OK"},
                                                "json_parse": {"code": 12000, "tag": "PARSE_SUCCESS"}}},
                     "json": {"query": keyword, "page": page,
                              "url": "https://www.amazon.com/s?" + urlencode({"k": keyword, "page": page}),
                              "results": {"organic": [{"asin": asin, "title": "Table", "price": price,
                                                        "url_image": "https://m.media-amazon.com/images/I/table.jpg",
                                                        "rating": 4.5, "reviews_count": 100}]}}}],
    }


class KeywordSearchTests(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(api_server.app)
        self.key = patch.dict(os.environ, {"OXYLABS_WEB_API_KEY": "dummy-not-a-real-key"})
        self.key.start()
        self.addCleanup(self.key.stop)

    def post(self, **values):
        return self.client.post("/api/v2/keyword_search", json={"site": "US", "keywords": ["behind couch table"], **values})

    def test_complete_workload_validation_prevents_paid_calls(self):
        cases = [
            {"site": "UNKNOWN"}, {"keywords": []}, {"keywords": "table"},
            {"keywords": ["table", " "]}, {"keywords": [4]}, {"keywords": ["x" * 2049]},
            {"start_page": 0}, {"start_page": True}, {"pages": "2"}, {"pages": 0},
            {"pages": 19}, {"keywords": ["a", "b"], "pages": 10}, {"api_key": "not-accepted"},
        ]
        with patch.object(search_api.oxylabs, "fetch_once") as send:
            for case in cases:
                with self.subTest(case=case):
                    self.assertEqual(self.post(**case).status_code, 400)
            send.assert_not_called()

    def test_missing_server_key_does_not_send(self):
        with patch.dict(os.environ, {}, clear=True), patch.object(search_api.oxylabs, "fetch_once") as send:
            response = self.post()
            self.assertEqual(response.status_code, 503)
            self.assertEqual(response.json()["error_code"], "OXYLABS_NOT_CONFIGURED")
            send.assert_not_called()

    def test_registered_in_full_amz_app_and_field_mapping(self):
        self.assertIn("/api/v2/keyword_search", api_server.app.openapi()["paths"])
        with patch.object(search_api.oxylabs, "fetch_once", return_value=envelope()) as send:
            response = self.post(site=" us ")
        self.assertEqual(response.status_code, 200)
        result = response.json()
        self.assertEqual(result["status"], "ok")
        p = result["products"][0]
        self.assertEqual(p["name"], p["title"])
        self.assertEqual(p["price"], p["price_value"])
        self.assertEqual(p["currency"], "USD")
        self.assertEqual(p["review_count"], 100)
        self.assertEqual(p["product_url"], "https://www.amazon.com/dp/B012345678")
        self.assertEqual(p["detail_status"], "pending")
        self.assertEqual(p["detail_scraped"], 0)
        self.assertTrue(p["core_fields_complete"])
        self.assertEqual(p["sources"][0]["page"], 1)
        self.assertEqual(send.call_args.args[0], {"query": "behind couch table", "domain": "com", "start_page": 1, "pages": 1, "output": ["json"]})

    def test_multi_keyword_pages_dedup_sources_and_complete_observation_preference(self):
        def fetch(payload, key):
            return envelope(payload["query"], payload["start_page"], price=0 if payload["query"] == "behind couch table" else 39.99)
        with patch.object(search_api.oxylabs, "fetch_once", side_effect=fetch) as send:
            result = self.post(keywords=[" behind couch table ", "behind couch table", "narrow console table"], start_page=2, pages=2).json()
        self.assertEqual(send.call_count, 4)
        self.assertEqual([c.args[0]["start_page"] for c in send.call_args_list], [2, 3, 2, 3])
        self.assertTrue(all(c.args[0]["pages"] == 1 for c in send.call_args_list))
        self.assertEqual(result["summary"]["duplicate_asin_observations"], 3)
        self.assertEqual(result["summary"]["unique_asins"], 1)
        product = result["products"][0]
        self.assertEqual(product["price"], 39.99)
        self.assertEqual(len(product["sources"]), 4)
        self.assertEqual(product["sources"][0]["price_status"], "zero")
        self.assertEqual(product["sources"][2]["keyword"], "narrow console table")

    def test_zero_missing_and_invalid_prices_are_not_usable(self):
        for raw, status in [(0, "zero"), (None, "missing"), (-1, "invalid"), (False, "invalid"), ("19.99", "invalid")]:
            with self.subTest(raw=raw), patch.object(search_api.oxylabs, "fetch_once", return_value=envelope(price=raw)):
                result = self.post().json()
                product = result["products"][0]
                self.assertIsNone(product["price"])
                self.assertIsNone(product["price_value"])
                self.assertEqual(product["price_raw"], raw)
                self.assertEqual(product["price_status"], status)
                self.assertFalse(product["core_fields_complete"])
                self.assertEqual(result["summary"]["usable_products"], 0)

    def test_partial_failure_is_visible_without_paid_retry(self):
        with patch.object(search_api.oxylabs, "fetch_once", side_effect=[envelope(), ProbeError("transport error")]) as send:
            response = self.post(pages=2)
        self.assertEqual(response.status_code, 207)
        self.assertEqual(send.call_count, 2)
        self.assertEqual(response.json()["status"], "partial")
        self.assertEqual(response.json()["summary"]["failed_page_tasks"], 1)
        self.assertEqual(response.json()["products"][0]["asin"], "B012345678")

    def test_nonfinite_price_is_reported_without_breaking_json_response(self):
        with patch.object(search_api.oxylabs, "fetch_once", return_value=envelope(price=float('nan'))):
            response = self.post()
        self.assertEqual(response.status_code, 200)
        product = response.json()["products"][0]
        self.assertEqual(product["price_status"], "invalid")
        self.assertIsNone(product["price"])
        self.assertEqual(product["sources"][0]["price_raw"], "nan")

    def test_http_200_with_failed_parse_or_wrong_page_is_not_success(self):
        failed = envelope()
        failed["results"][0]["metadata"]["statuses"]["json_parse"] = {"code": 12001, "tag": "PARSE_FAILED"}
        wrong_page = envelope(page=2)
        for data in [failed, wrong_page, {"state": "done", "results": []}]:
            with self.subTest(data=data), patch.object(search_api.oxylabs, "fetch_once", return_value=data):
                response = self.post()
                self.assertEqual(response.status_code, 502)
                self.assertEqual(response.json()["summary"]["unique_asins"], 0)

    def test_empty_page_is_explicit_not_product_success(self):
        data = envelope()
        data["results"][0]["json"]["results"]["organic"] = []
        with patch.object(search_api.oxylabs, "fetch_once", return_value=data):
            result = self.post().json()
        self.assertEqual(result["status"], "empty")
        self.assertEqual(result["summary"]["successful_page_tasks"], 0)

    def test_upstream_error_cannot_echo_runtime_key(self):
        with patch.object(search_api.oxylabs, "fetch_once", side_effect=ProbeError("failed dummy-not-a-real-key")):
            response = self.post()
        self.assertNotIn("dummy-not-a-real-key", response.text)
        self.assertIn("[REDACTED]", response.text)

    def test_marketplace_reuses_existing_configuration(self):
        search = search_api.validate_search({"site": "UK", "keywords": ["table"]})
        self.assertEqual(search["domain"], "co.uk")
        self.assertEqual(search["market"]["currency_code"], "GBP")


if __name__ == "__main__":
    unittest.main()
