"""Offline contract tests for one-page Oxylabs probe."""
import json
import unittest
from unittest.mock import patch
from oxylabs_amazon_probe import build_payload, fetch_once, parse_products, ProbeError, ENDPOINT

class MockResponse:
    status = 200
    def __enter__(self): return self
    def __exit__(self, *args): return False
    def read(self): return json.dumps({"results":[{"json":{"results":{"organic":[{"asin":"B012345678","title":"Test Table","price":29.99,"url_image":"https://example.com/x.jpg"}]}}}]}).encode()

class ProbeTests(unittest.TestCase):
    def test_one_page_payload(self):
        self.assertEqual(build_payload("behind couch table")["pages"], 1)
    def test_request_and_valid_product(self):
        with patch("oxylabs_amazon_probe.urlopen", return_value=MockResponse()) as send:
            data = fetch_once(build_payload("behind couch table"), "dummy")
            self.assertEqual(send.call_args.args[0].full_url, ENDPOINT)
        products, report = parse_products(data)
        self.assertEqual(products[0]["asin"], "B012345678")
        self.assertTrue(report["pass"])
    def test_no_key_no_request(self):
        with patch("oxylabs_amazon_probe.urlopen") as send:
            with self.assertRaises(ProbeError):
                fetch_once(build_payload("x"), "")
            send.assert_not_called()
    def test_empty_not_success(self):
        items, report = parse_products({"results":[{"json":{"results":{"organic":[]}}}]})
        self.assertFalse(report["pass"])
    def test_missing_fields_not_success(self):
        items, report = parse_products({"results":[{"json":{"results":{"organic":[{"asin":"B012345678"}]}}}]})
        self.assertFalse(report["pass"])
    def test_unknown_schema_fails(self):
        with self.assertRaises(ProbeError):
            parse_products({"results":[{"json":{"unknown":"data"}}]})

if __name__ == "__main__":
    unittest.main()
