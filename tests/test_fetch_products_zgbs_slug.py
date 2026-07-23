"""Regression: Best-Sellers /zgbs/ URLs must not produce empty-slug LIST 404s."""
from __future__ import annotations

import unittest
from unittest import mock

import fetch_products as fp
from proxy_worker import FetchOutcome


class TestExtractSlugZgbsCompat(unittest.TestCase):
    def test_gp_bestsellers_slug(self):
        self.assertEqual(
            fp.extract_slug(
                "https://www.amazon.com/gp/bestsellers/sporting-goods/3225970011/"
            ),
            "sporting-goods",
        )

    def test_zgbs_seo_slug(self):
        self.assertEqual(
            fp.extract_slug(
                "https://www.amazon.com/Best-Sellers-Sports-Outdoors-Gun-Jags/"
                "zgbs/sporting-goods/3225970011/"
            ),
            "sporting-goods",
        )

    def test_zgns_seo_slug(self):
        self.assertEqual(
            fp.extract_slug(
                "https://www.amazon.com/gp/new-releases/toys-and-games/165793011/"
            ),
            "toys-and-games",
        )
        self.assertEqual(
            fp.extract_slug(
                "https://www.amazon.com/New-Releases-Toys/zgns/toys-and-games/165793011/"
            ),
            "toys-and-games",
        )

    def test_most_gifted_still_works(self):
        self.assertEqual(
            fp.extract_slug(
                "https://www.amazon.com/gp/most-gifted/sporting-goods/13364373011/"
            ),
            "sporting-goods",
        )

    def test_non_chart_url_returns_empty(self):
        self.assertEqual(
            fp.extract_slug("https://www.amazon.com/s?node=123"),
            "",
        )


class TestProcessNodeReusesZgbsUrl(unittest.TestCase):
    def test_bestsellers_reuses_stored_zgbs_url(self):
        stored = (
            "https://www.amazon.com/Best-Sellers-Sports-Outdoors-Gun-Jags/"
            "zgbs/sporting-goods/3225970011/"
        )
        node = {
            "node_id": "3225970011",
            "url": stored,
            "name": "Gun Jags",
            "depth": 5,
        }
        seen = []

        class Client:
            def get(self, url, *, phase, item_id, referer=""):
                seen.append(url)
                return FetchOutcome(
                    ok=True,
                    html="<html><body>No products</body></html>",
                    status_code=200,
                    attempts=1,
                )

        with mock.patch.object(fp, "save_link_validity"):
            status, error, found, _ = fp.process_node(
                node,
                ["bestsellers"],
                review_max=0,
                min_list_size=0,
                client=Client(),
                max_pages=1,
                delay=0.0,
            )
        self.assertEqual((status, error, found), ("done", "", 0))
        self.assertEqual(seen, [stored])
        self.assertNotIn("//", seen[0].replace("https://", ""))

    def test_cross_list_reconstructs_with_zgbs_slug_not_empty(self):
        """类目 URL 是 bestsellers/zgbs 时，抓 new-releases 应拼出带 slug 的 gp 链。"""
        node = {
            "node_id": "3225970011",
            "url": (
                "https://www.amazon.com/Best-Sellers-Sports-Outdoors-Gun-Jags/"
                "zgbs/sporting-goods/3225970011/"
            ),
            "name": "Gun Jags",
            "depth": 5,
        }
        seen = []

        class Client:
            def get(self, url, *, phase, item_id, referer=""):
                seen.append(url)
                return FetchOutcome(
                    ok=False,
                    html="",
                    status_code=404,
                    attempts=1,
                    error_code="HTTP_404",
                    final_reason="HTTP_404",
                )

        with mock.patch.object(fp, "_DOMAIN", "https://www.amazon.com"):
            status, error, found, _ = fp.process_node(
                node,
                ["new-releases"],
                review_max=0,
                min_list_size=0,
                client=Client(),
                max_pages=1,
                delay=0.0,
            )
        self.assertEqual(status, "error")
        self.assertEqual(error, "HTTP_404")
        self.assertEqual(
            seen,
            ["https://www.amazon.com/gp/new-releases/sporting-goods/3225970011/"],
        )

    def test_empty_slug_does_not_emit_double_slash_url(self):
        node = {
            "node_id": "999",
            "url": "https://www.amazon.com/s?k=no-chart",
            "name": "Bad",
            "depth": 2,
        }
        seen = []

        class Client:
            def get(self, url, *, phase, item_id, referer=""):
                seen.append(url)
                return FetchOutcome(ok=True, html="", status_code=200, attempts=1)

        status, error, found, _ = fp.process_node(
            node,
            ["bestsellers"],
            review_max=0,
            min_list_size=0,
            client=Client(),
            max_pages=1,
            delay=0.0,
        )
        self.assertEqual((status, error, found), ("error", "EMPTY_CHART_SLUG", 0))
        self.assertEqual(seen, [])


if __name__ == "__main__":
    unittest.main()
