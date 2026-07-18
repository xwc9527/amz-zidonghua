"""参数/输入校验层测试。"""
from __future__ import annotations

import math
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from api_server import _parse_filter_number, _validate_start_filters
from tests.na_dimensions.helpers import CaseResult, SuiteCollector


def _expect(col: SuiteCollector, case_id, dim, layer, ok: bool, expected, actual, detail="", severity="P2"):
    col.add(CaseResult(
        case_id, dim, layer,
        "PASS" if ok else "FAIL",
        expected=expected, actual=actual, detail=detail,
        severity="" if ok else severity,
    ))


def run_validation_tests(col: SuiteCollector):
    # 合法整数 / 整数字符串 / 小数拒绝 / 布尔拒绝
    cases = [
        ("VAL-INT-01", "评论数", True, _parse_filter_number(10, as_int=True, label="评论数"), (None, 10)),
        ("VAL-INT-02", "评论数", True, _parse_filter_number("10", as_int=True, label="评论数"), (None, 10)),
        ("VAL-INT-03", "评论数", False, _parse_filter_number(10.5, as_int=True, label="评论数")[0] is None, False),
        ("VAL-INT-04", "评论数", False, _parse_filter_number(True, as_int=True, label="评论数")[0] is None, False),
        ("VAL-FLT-01", "现价", True, _parse_filter_number(10.5, as_int=False, label="现价"), (None, 10.5)),
        ("VAL-FLT-02", "现价", True, _parse_filter_number("10.5", as_int=False, label="现价"), (None, 10.5)),
        ("VAL-FLT-03", "现价", False, _parse_filter_number("abc", as_int=False, label="现价")[0] is None, False),
    ]
    for cid, dim, expect_ok, result, expected in cases:
        if isinstance(result, tuple):
            ok = result == expected
            actual = result
        else:
            # result 是 bool：是否解析成功
            ok = (result is True) == expect_ok if False else (result == expect_ok)
            # 上面逻辑乱了，重写：
            pass

    # 清晰重写
    err, v = _parse_filter_number(10, as_int=True, label="评论数")
    _expect(col, "VAL-INT-01", "评论数", "参数校验", err is None and v == 10, (None, 10), (err, v))

    err, v = _parse_filter_number("10", as_int=True, label="评论数")
    _expect(col, "VAL-INT-02", "评论数", "参数校验", err is None and v == 10, (None, 10), (err, v))

    err, v = _parse_filter_number(10.5, as_int=True, label="评论数")
    _expect(col, "VAL-INT-03", "评论数", "参数校验", err is not None, "拒绝小数", (err, v),
            severity="P2")

    err, v = _parse_filter_number(True, as_int=True, label="评论数")
    _expect(col, "VAL-INT-04", "评论数", "参数校验", err is not None, "拒绝布尔", (err, v),
            severity="P2")

    err, v = _parse_filter_number(False, as_int=True, label="评论数")
    _expect(col, "VAL-INT-05", "评论数", "参数校验", err is not None, "拒绝布尔False", (err, v),
            severity="P2")

    # 负数
    msg = _validate_start_filters({"price_min": -1})
    _expect(col, "VAL-NEG-01", "价格", "参数校验", msg is not None and "负" in msg,
            "拒绝负数", msg, severity="P1")

    msg = _validate_start_filters({"weight_max": -0.1})
    _expect(col, "VAL-NEG-02", "重量", "参数校验", msg is not None, "拒绝负数", msg, severity="P1")

    # min > max
    msg = _validate_start_filters({"price_min": 50, "price_max": 10})
    _expect(col, "VAL-RANGE-01", "价格", "参数校验", msg is not None and "大于" in msg,
            "拒绝 min>max", msg, severity="P1")

    msg = _validate_start_filters({"rating_min": 4, "rating_max": 3})
    _expect(col, "VAL-RANGE-02", "评分", "参数校验", msg is not None, "拒绝 min>max", msg)

    # 评分 > 5
    msg = _validate_start_filters({"rating_max": 5.5})
    _expect(col, "VAL-RATE-01", "评分", "参数校验", msg is not None and "0~5" in msg,
            "拒绝评分>5", msg, severity="P1")

    msg = _validate_start_filters({"rating_min": 0, "rating_max": 5})
    _expect(col, "VAL-RATE-02", "评分", "参数校验", msg is None, None, msg)

    # 日期起止颠倒
    msg = _validate_start_filters({
        "date_range": "custom", "date_from": "2026-07-01", "date_to": "2026-06-01",
    })
    _expect(col, "VAL-DATE-01", "上架日期", "参数校验", msg is not None and "晚于" in msg,
            "拒绝起晚于止", msg, severity="P1")

    msg = _validate_start_filters({
        "date_range": "custom", "date_from": "2026-06-01", "date_to": "2026-07-01",
    })
    _expect(col, "VAL-DATE-02", "上架日期", "参数校验", msg is None, None, msg)

    # 非数字
    msg = _validate_start_filters({"price_min": "abc"})
    _expect(col, "VAL-NANSTR-01", "价格", "参数校验", msg is not None, "拒绝非数字", msg, severity="P1")

    # NaN / Infinity — 规范要求启动前拒绝
    for cid, val, label in [
        ("VAL-NAN-01", float("nan"), "price_min"),
        ("VAL-INF-01", float("inf"), "price_max"),
        ("VAL-NINF-01", float("-inf"), "weight_min"),
    ]:
        msg = _validate_start_filters({label: val})
        ok = msg is not None
        _expect(
            col, cid, "价格" if "price" in label else "重量", "参数校验",
            ok, "拒绝 NaN/Infinity", msg,
            detail=f"输入 {val!r} → {msg!r}",
            severity="P1",
        )

    # 合法组合应通过
    msg = _validate_start_filters({
        "price_min": 10, "price_max": 50,
        "rating_min": 4, "rating_max": 5,
        "review_min": 10, "review_max": 1000,
    })
    _expect(col, "VAL-OK-01", "参数校验", "参数校验", msg is None, None, msg)
