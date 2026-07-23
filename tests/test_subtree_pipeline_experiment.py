import unittest

import fetch_subtree
from subtree_pipeline_experiment import BoundedParsePipeline, parse_sidebar_payload


SAMPLE = """
<ul class="zg-browse-root">
  <li><span class="zg-selected">Current</span></li>
  <li><ul class="zg-browse-group">
    <li><a href="/gp/bestsellers/toys/123456/"> Alpha </a></li>
    <li class="browse-up"><a href="/gp/bestsellers/toys/999999/">Up</a></li>
    <li><a href="/gp/new-releases/toys/234567/?ref_=x">Beta</a></li>
  </ul></li>
</ul>
"""


class TestIndependentParser(unittest.TestCase):
    def test_matches_production_parser(self):
        url = "https://www.amazon.com/gp/bestsellers/toys/111111/"
        expected = fetch_subtree.parse_sidebar_children(SAMPLE, url)
        actual = parse_sidebar_payload(SAMPLE, url, "https://www.amazon.com")
        self.assertEqual(actual, expected)

    def test_process_pipeline_roundtrip_and_close_gate(self):
        url = "https://www.amazon.com/gp/bestsellers/toys/111111/"
        pipeline = BoundedParsePipeline(workers=2, max_pending=4)
        future = pipeline.submit(SAMPLE, url, "https://www.amazon.com")
        self.assertEqual(future.result(timeout=10), fetch_subtree.parse_sidebar_children(SAMPLE, url))
        pipeline.shutdown()
        with self.assertRaisesRegex(RuntimeError, "closed"):
            pipeline.submit(SAMPLE, url, "https://www.amazon.com")

    def test_worker_exception_reaches_caller(self):
        with BoundedParsePipeline(workers=1, max_pending=1) as pipeline:
            future = pipeline.submit(None, "https://www.amazon.com/x", "https://www.amazon.com")
            with self.assertRaises(Exception):
                future.result(timeout=10)


if __name__ == "__main__":
    unittest.main()
