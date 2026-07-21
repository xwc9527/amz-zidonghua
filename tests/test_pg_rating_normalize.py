"""Unit-level check: ROUND(rating, 1) recovers float4 drift for Amazon 0.1 ratings."""
from __future__ import annotations

import sqlite3
import unittest


class TestRatingNormalize(unittest.TestCase):
    def test_round_recovers_float4_drift(self):
        # Simulate the float4 value observed for 4.2
        drifted = 4.199999809265137
        # SQLite ROUND matches PostgreSQL ROUND(numeric, 1) for this case
        conn = sqlite3.connect(":memory:")
        got = conn.execute("SELECT ROUND(?, 1)", (drifted,)).fetchone()[0]
        conn.close()
        self.assertEqual(got, 4.2)
        self.assertTrue(got >= 4.2)

    def test_schema_contains_normalize_update(self):
        from pathlib import Path
        schema = (Path(__file__).resolve().parents[1] / "pg_schema.sql").read_text(encoding="utf-8")
        self.assertIn("ROUND(rating::numeric, 1)", schema)
        self.assertIn("rating IS DISTINCT FROM ROUND(rating::numeric, 1)", schema)
        self.assertGreaterEqual(schema.count("ROUND(rating::numeric, 1)"), 2)
        self.assertIn("ADD COLUMN IF NOT EXISTS run_id TEXT", schema)


if __name__ == "__main__":
    unittest.main()
