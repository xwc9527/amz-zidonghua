from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

import fetch_new_arrivals
from api_server import (
    _append_filter_flags,
    _build_new_arrivals_where,
    _build_product_where,
    _validate_start_filters,
)
from detail_parser import check_detail_filters, parse_detail_fields, parse_social_proof_count


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("50+ bought in past month", 50),
        ("500+ bought in past month", 500),
        ("1K+ bought in past month", 1_000),
        ("2.5K+ bought in past month", 2_500),
        ("1,5K+ gekauft im letzten Monat", 1_500),
        ("20K+ bought in past month", 20_000),
        ("1,000+ bought in past month", 1_000),
        ("1000+ gekauft Mal im letzten Monat", 1_000),
        ("過去1か月で500点以上購入されました", 500),
        (None, None),
        ("bought recently", None),
    ],
)
def test_parse_social_proof_count(raw, expected):
    assert parse_social_proof_count(raw) == expected


def test_detail_parser_keeps_raw_and_normalized_value():
    html = """
    <html><body>
      <div id="socialProofingAsinFaceout_feature_div">
        <span>2K+ bought in past month</span>
      </div>
    </body></html>
    """
    detail = parse_detail_fields(html, "US")
    assert detail["social_proof"] == "2K+ bought in past month"
    assert detail["social_proof_count"] == 2_000


def test_social_proof_filter_is_minimum_and_missing_is_strictly_rejected():
    detail = {"social_proof_count": 500}
    assert check_detail_filters(detail, {"social_proof_min": 500}) is True
    assert check_detail_filters(detail, {"social_proof_min": 501}) is False
    assert check_detail_filters({}, {"social_proof_min": 1}) is False
    assert check_detail_filters({}, {}) is True


def test_validation_cli_and_query_layers_accept_social_proof_min():
    assert _validate_start_filters({"social_proof_min": 500}) is None
    assert "不能为负" in _validate_start_filters({"social_proof_min": -1})
    assert "整数" in _validate_start_filters({"social_proof_min": 1.5})

    cmd = ["crawler"]
    _append_filter_flags(cmd, {"social_proof_min": 500}, for_la=True)
    assert cmd == ["crawler", "--social-proof-min", "500"]

    product_where, product_params = _build_product_where(
        {"site": "US", "detail_only": True, "social_proof_min": 500}, "sqlite"
    )
    assert "social_proof_count IS NOT NULL" in product_where
    assert "social_proof_count >= ?" in product_where
    assert product_params[-1] == 500

    arrival_where, arrival_params = _build_new_arrivals_where(
        {"site": "US", "social_proof_min": 500}, "sqlite"
    )
    assert "social_proof_count IS NOT NULL" in arrival_where
    assert "social_proof_count >= ?" in arrival_where
    assert arrival_params[-1] == 500


def test_new_arrivals_sqlite_schema_and_save_round_trip(tmp_path, monkeypatch):
    db_path = tmp_path / "social_proof.db"
    monkeypatch.setattr(fetch_new_arrivals, "DB_BACKEND", "sqlite")
    monkeypatch.setattr(fetch_new_arrivals, "DB_FILE", str(db_path))
    monkeypatch.setattr(fetch_new_arrivals, "_LAST_PURGE_CHECK", 0.0)

    fetch_new_arrivals._init_db()
    saved = fetch_new_arrivals._save_products_sqlite(
        [
            {
                "asin": "B0SOCIAL001",
                "title": "Social proof fixture",
                "node_id": "NODE1",
                "site": "US",
                "social_proof": "500+ bought in past month",
                "social_proof_count": 500,
            }
        ]
    )
    assert saved == 1

    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT social_proof, social_proof_count FROM new_arrivals WHERE asin=?",
            ("B0SOCIAL001",),
        ).fetchone()
        indexes = {r[1] for r in conn.execute("PRAGMA index_list(new_arrivals)")}
    assert row == ("500+ bought in past month", 500)
    assert "idx_na_social_proof" in indexes


def test_dashboard_uses_single_minimum_input_and_content_width_start_button():
    dashboard = (Path(__file__).parents[1] / "data" / "dashboard.html").read_text(encoding="utf-8")
    assert 'id="f_social_proof_min"' in dashboard
    assert "社交证明(月销量)" in dashboard
    assert ".run-filter-inputs input { flex: 1 1 auto; }" in dashboard
    assert ".run-filter-inputs #btnToggleRun { flex: 0 0 auto; width: auto;" in dashboard
    assert 'id="p_progress_text"' not in dashboard
    assert "social_proof_min: +document.getElementById('f_social_proof_min').value || 0" in dashboard


def test_postgresql_schema_contains_raw_and_normalized_columns():
    schema = (Path(__file__).parents[1] / "pg_schema.sql").read_text(encoding="utf-8")
    assert "social_proof          TEXT" in schema
    assert "social_proof_count    INTEGER" in schema
    assert "idx_na_social_proof" in schema
    assert "idx_ps_social_proof" in schema
