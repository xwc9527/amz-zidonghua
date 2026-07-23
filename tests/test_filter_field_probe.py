"""覆盖 fetch_products.py 的"字段解析坍缩"探针：
_finalize_detail 按维度累计 None 率，_warn_if_filter_field_collapsed 在跑完后
自动识别"选择器过期导致字段恒为 None"这类静默失效，而不是等人工去猜测。
"""
from __future__ import annotations

import logging

import fetch_products as fp


def _reset_probe():
    with fp._stats_lock:
        fp._filter_field_probe.clear()
        for key in fp._stats:
            if key not in ("pool_usable", "pool_cooling", "pool_disabled"):
                fp._stats[key] = 0


def test_probe_warns_when_active_field_collapses_to_none(caplog):
    _reset_probe()
    with fp._stats_lock:
        fp._filter_field_probe["variant_option_count"] = {"none": 18, "total": 20}
        fp._stats["products_found"] = 20
        fp._stats["products_saved"] = 0
    with caplog.at_level(logging.WARNING, logger="fetch_products"):
        fp._warn_if_filter_field_collapsed()
    msgs = [r.message for r in caplog.records]
    assert any("疑似字段解析失效" in m and "variant_option_count" in m for m in msgs)


def test_probe_silent_when_none_rate_is_low(caplog):
    _reset_probe()
    with fp._stats_lock:
        fp._filter_field_probe["variant_option_count"] = {"none": 2, "total": 20}
        fp._stats["products_found"] = 20
        fp._stats["products_saved"] = 15
    with caplog.at_level(logging.WARNING, logger="fetch_products"):
        fp._warn_if_filter_field_collapsed()
    assert not any("疑似字段解析失效" in r.message for r in caplog.records)


def test_probe_silent_when_sample_too_small(caplog):
    _reset_probe()
    with fp._stats_lock:
        fp._filter_field_probe["variant_option_count"] = {"none": 3, "total": 3}
        fp._stats["products_found"] = 3
        fp._stats["products_saved"] = 0
    with caplog.at_level(logging.WARNING, logger="fetch_products"):
        fp._warn_if_filter_field_collapsed()
    # 样本量太小（<15），不足以下结论，不应该按字段坍缩告警
    assert not any("疑似字段解析失效" in r.message for r in caplog.records)
    # 但 found>0 saved=0 这个宏观异常仍然应该被兜底提示一次
    assert any("found=" in r.message and "saved=0" in r.message for r in caplog.records)


def test_probe_falls_back_to_found_saved_zero_warning_without_suspect_field(caplog):
    _reset_probe()
    with fp._stats_lock:
        fp._stats["products_found"] = 50
        fp._stats["products_saved"] = 0
    with caplog.at_level(logging.WARNING, logger="fetch_products"):
        fp._warn_if_filter_field_collapsed()
    msgs = [r.message for r in caplog.records]
    assert any("found=50" in m and "saved=0" in m for m in msgs)


def test_probe_fully_silent_on_healthy_run(caplog):
    _reset_probe()
    with fp._stats_lock:
        fp._stats["products_found"] = 20
        fp._stats["products_saved"] = 12
    with caplog.at_level(logging.WARNING, logger="fetch_products"):
        fp._warn_if_filter_field_collapsed()
    assert len(caplog.records) == 0


def test_finalize_detail_updates_probe_counts_for_active_dimension(monkeypatch):
    _reset_probe()
    monkeypatch.setattr(fp, "_attach_normalized_dims", lambda payload: payload)
    monkeypatch.setattr(fp, "_check_detail_filters", lambda payload, filters: True)
    monkeypatch.setattr(fp, "_update_sighting_detail", lambda *a, **k: None)

    product = {"asin": "B0TESTTEST", "node_id": "1", "list_type": "bestsellers", "price": 9.99}
    detail_missing = {"variant_option_count": None}
    detail_present = {"variant_option_count": 4}
    filters = {"variant_max": 3}

    fp._finalize_detail(dict(product), detail_missing, filters)
    fp._finalize_detail(dict(product), detail_present, filters)

    with fp._stats_lock:
        slot = fp._filter_field_probe["variant_option_count"]
    assert slot == {"none": 1, "total": 2}
