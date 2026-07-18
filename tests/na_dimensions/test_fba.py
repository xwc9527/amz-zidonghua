"""FBA 费用档位与不支持站点。"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from detail_parser import check_detail_filters
from fba_fees_us import estimate_fba_fees, fba_support_info
from tests.na_dimensions.helpers import CaseResult, SuiteCollector


def _ok(col, cid, dim, layer, passed, expected, actual, detail="", severity="P2"):
    col.add(CaseResult(
        cid, dim, layer, "PASS" if passed else "FAIL",
        expected=expected, actual=actual, detail=detail,
        severity="" if passed else severity,
    ))


def run_fba_tests(col: SuiteCollector):
    # 实重主导 vs 体积重主导
    light_bulky = estimate_fba_fees("US", "0.5 pounds", "18 x 14 x 8 inches", 25.0)
    heavy_small = estimate_fba_fees("US", "10 pounds", "8 x 6 x 2 inches", 25.0)
    _ok(col, "FBA-US-LIGHT-BULKY", "FBA费用", "单位换算",
        light_bulky.get("fba_fee") is not None, "有估算", light_bulky.get("fba_fee"))
    _ok(col, "FBA-US-HEAVY", "FBA费用", "单位换算",
        heavy_small.get("fba_fee") is not None, "有估算", heavy_small.get("fba_fee"))

    # 低价商品
    low = estimate_fba_fees("US", "0.5 pounds", "10 x 5 x 0.5 inches", 8.0)
    mid = estimate_fba_fees("US", "0.5 pounds", "10 x 5 x 0.5 inches", 25.0)
    _ok(col, "FBA-US-LOWPRICE", "FBA费用", "单维度",
        low.get("fba_fee") is not None and mid.get("fba_fee") is not None
        and low["fba_fee"] <= mid["fba_fee"],
        "低价费率 <= 普通", (low.get("fba_fee"), mid.get("fba_fee")))

    # 不支持站点
    info = fba_support_info("IE")
    _ok(col, "FBA-UNSUP-IE", "FBA费用", "单维度",
        info.get("supported") is False, False, info.get("supported"))

    # 无法估算 → 筛选拒绝
    detail = {"fba_fee": None, "item_weight": None, "item_dimensions": None}
    _ok(col, "FBA-MISSING-FILTER", "FBA费用", "缺失值",
        check_detail_filters(detail, {"fba_fee_max": 10}) is False, False, False, severity="P1")

    # 未设置条件时缺失可通过
    _ok(col, "FBA-MISSING-NOFILTER", "FBA费用", "缺失值",
        check_detail_filters(detail, {}) is True, True, True)
