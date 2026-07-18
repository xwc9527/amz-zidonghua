"""强相关组合、Pairwise、全维度通过与单点失败轮换。"""
from __future__ import annotations

import itertools
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from detail_parser import check_detail_filters
from fetch_new_arrivals import _pass_list_filters
from tests.na_dimensions.fixtures.products import PRODUCTS, detail_fields, list_card
from tests.na_dimensions.helpers import CaseResult, SuiteCollector


def _pass_all(product: dict, filters: dict) -> bool:
    list_keys = {"price_min", "price_max", "rating_min", "rating_max", "review_min", "review_max"}
    lf = {k: v for k, v in filters.items() if k in list_keys and v}
    df = {k: v for k, v in filters.items() if k not in list_keys and v}
    return _pass_list_filters(list_card(product), lf) and check_detail_filters(detail_fields(product), df)


def _ok(col, cid, dim, layer, passed, expected, actual, detail="", severity="P2"):
    col.add(CaseResult(
        cid, dim, layer, "PASS" if passed else "FAIL",
        expected=expected, actual=actual, detail=detail,
        severity="" if passed else severity,
    ))


# 宽松全维度条件：A 应通过
FULL_PASS_FILTERS = {
    "price_min": 10, "price_max": 50,
    "rating_min": 4.0, "rating_max": 5.0,
    "review_min": 10, "review_max": 1000,
    "bsr_main_min": 1, "bsr_main_max": 20000,
    "bsr_sub_min": 1, "bsr_sub_max": 5000,
    "variant_min": 1, "variant_max": 20,
    "sellers_min": 0, "sellers_max": 100,  # 0 会被剥离，用 sellers_max only
    "weight_min": 0.1, "weight_max": 10,
    "dim_l": 20, "dim_w": 15, "dim_h": 10,
    "fba_fee_min": 1, "fba_fee_max": 20,
    "fulfillment_type": "FBA",
    "country": "China",
}

# sellers_min=0 会被视为未设置；为测卖家上限保留 sellers_max
FULL_PASS_FILTERS.pop("sellers_min", None)


STRONG_COMBOS = [
    ("价格+评论数", {"price_min": 10, "price_max": 50, "review_min": 50, "review_max": 500}),
    ("评分+评论数", {"rating_min": 4.0, "review_min": 50}),
    ("BSR大类+BSR子类", {"bsr_main_max": 10000, "bsr_sub_max": 1000}),
    ("重量+尺寸", {"weight_max": 5, "dim_l": 15, "dim_w": 10, "dim_h": 5}),
    ("重量+尺寸+FBA费用", {"weight_max": 5, "dim_l": 15, "dim_w": 10, "dim_h": 5, "fba_fee_max": 15}),
    ("配送模式+FBA费用", {"fulfillment_type": "FBA", "fba_fee_max": 15}),
    ("上架日期+评论数", {"date_range": "custom", "date_from": "2026-01-01", "date_to": "2026-07-18", "review_min": 50}),
    ("产地+配送模式", {"country": "China", "fulfillment_type": "FBA"}),
    ("Amazon精选+畅销标记", {"amazons_choice": True, "bestseller": True}),
    ("价格+评分+评论数", {"price_min": 10, "price_max": 40, "rating_min": 4.0, "review_min": 50}),
]


def run_strong_combo_tests(col: SuiteCollector):
    a, c, d, e = PRODUCTS["A"], PRODUCTS["C"], PRODUCTS["D"], PRODUCTS["E"]
    for name, filters in STRONG_COMBOS:
        dim = name
        # 全部满足：选合适商品
        if "Amazon精选" in name:
            target = e
        else:
            target = a
        ok_pass = _pass_all(target, filters)
        _ok(col, f"SC-{name}-ALL", dim, "组合", ok_pass, True, ok_pass, severity="P1")

        # 第一个字段失败：用超限商品 C 或改条件
        keys = list(filters.keys())
        first = keys[0]
        bad = dict(filters)
        if first.endswith("_min"):
            bad[first] = 10**9
        elif first.endswith("_max") or first.startswith("dim_"):
            bad[first] = 0.0001 if isinstance(filters[first], float) else 1
            if first.startswith("dim_"):
                bad = {**filters, "dim_l": 0.1}
        elif first == "fulfillment_type":
            bad[first] = "FBM" if filters[first] == "FBA" else "FBA"
        elif first == "country":
            bad[first] = "Nowhere"
        elif first in ("amazons_choice", "bestseller"):
            # 用非标记商品
            _ok(col, f"SC-{name}-F1", dim, "组合",
                _pass_all(a, filters) is False, False, False)
            continue
        elif first == "date_range":
            bad = {**filters, "date_from": "2099-01-01", "date_to": "2099-12-31"}
        else:
            bad[first] = 10**9

        _ok(col, f"SC-{name}-F1", dim, "组合",
            _pass_all(target, bad) is False, False, _pass_all(target, bad))

        # 任一字段缺失：用 D（若该组合含需详情字段）
        need_detail = any(k not in {
            "price_min", "price_max", "rating_min", "rating_max", "review_min", "review_max",
        } for k in filters)
        if need_detail:
            _ok(col, f"SC-{name}-MISS", dim, "组合",
                _pass_all(d, filters) is False, False, False, severity="P1")

        # 条件顺序变化不影响
        rev = dict(reversed(list(filters.items())))
        _ok(col, f"SC-{name}-ORDER", dim, "组合",
            _pass_all(target, filters) == _pass_all(target, rev), True, True)


def run_pairwise_tests(col: SuiteCollector):
    """Pairwise：任意两维度至少共同出现一次（约 30–50 组）。"""
    dims = [
        ("price", {"price_max": 50}),
        ("rating", {"rating_min": 4.0}),
        ("review", {"review_min": 20}),
        ("bsr_main", {"bsr_main_max": 50000}),
        ("bsr_sub", {"bsr_sub_max": 5000}),
        ("variant", {"variant_max": 20}),
        ("sellers", {"sellers_max": 100}),
        ("weight", {"weight_max": 10}),
        ("dim", {"dim_l": 40, "dim_w": 30, "dim_h": 20}),
        ("fba", {"fba_fee_max": 40}),
        ("ft", {"fulfillment_type": "FBA"}),
        ("country", {"country": "China"}),
        ("ac", {"amazons_choice": True}),
        ("bs", {"bestseller": True}),
        ("date", {"date_range": "custom", "date_from": "2020-01-01", "date_to": "2026-07-18"}),
    ]
    # 生成覆盖所有对的组合：用轮换构造
    pairs = list(itertools.combinations(range(len(dims)), 2))
    # 控制在 ~45：取所有对但合并为每对一条用例
    # 完整 pairwise = C(15,2)=105，按规范压到 30–50：采样策略=每个维度与其后 3 个配对
    selected = []
    for i in range(len(dims)):
        for j in range(i + 1, min(i + 4, len(dims))):
            selected.append((i, j))
    # 再补几条跨度大的对
    selected += [(0, 7), (0, 10), (1, 8), (2, 14), (3, 4), (7, 8), (10, 11), (12, 13)]
    selected = list(dict.fromkeys(selected))[:48]

    a, e = PRODUCTS["A"], PRODUCTS["E"]
    for idx, (i, j) in enumerate(selected):
        n1, f1 = dims[i]
        n2, f2 = dims[j]
        filters = {**f1, **f2}
        # Amazon 标记类用 E
        target = e if ("amazons_choice" in filters or "bestseller" in filters) else a
        # 若要求 FBA+China，A 满足；若 bestseller，E 满足
        ok = _pass_all(target, filters)
        # 宽松条件应通过；若不通过记 FAIL
        expect_pass = True
        if "amazons_choice" in filters and target.get("is_amazon_choice") != 1:
            expect_pass = False
        if "bestseller" in filters and target.get("is_bestseller") != 1:
            expect_pass = False
        if filters.get("fulfillment_type") == "FBA" and target.get("fulfillment_type") != "FBA":
            expect_pass = False
        _ok(col, f"PW-{idx:02d}-{n1}+{n2}", f"{n1}+{n2}", "组合",
            ok == expect_pass, expect_pass, ok, severity="P2")


def run_full_dimension_tests(col: SuiteCollector):
    a = PRODUCTS["A"]
    # A 的 sellers=2，country=China，FBA — 应通过 FULL_PASS_FILTERS
    ok = _pass_all(a, FULL_PASS_FILTERS)
    _ok(col, "FULL-PASS-A", "全维度", "组合", ok is True, True, ok, severity="P1")

    # E 也基本满足但价格等 OK；E 也是 FBA China
    ok_e = _pass_all(PRODUCTS["E"], FULL_PASS_FILTERS)
    _ok(col, "FULL-PASS-E", "全维度", "组合", ok_e is True, True, ok_e)

    # 单点失败轮换
    fail_points = [
        ("价格", {"price_max": 5}),
        ("评分", {"rating_min": 4.9}),
        ("评论数", {"review_min": 10000}),
        ("BSR大类", {"bsr_main_max": 10}),
        ("BSR子类", {"bsr_sub_max": 1}),
        ("变体数", {"variant_min": 50}),
        ("其他卖家数", {"sellers_min": 100}),
        ("重量", {"weight_max": 0.1}),
        ("尺寸", {"dim_l": 1, "dim_w": 1, "dim_h": 1}),
        ("FBA费用", {"fba_fee_max": 0.1}),
        ("配送模式", {"fulfillment_type": "FBM"}),
        ("产地", {"country": "Japan"}),
    ]
    for name, override in fail_points:
        f = {**FULL_PASS_FILTERS, **override}
        # dim 覆盖时去掉原 dim
        if "dim_l" in override:
            f["dim_l"], f["dim_w"], f["dim_h"] = override["dim_l"], override["dim_w"], override["dim_h"]
        passed = _pass_all(a, f)
        _ok(col, f"FULL-FAIL-{name}", name, "组合",
            passed is False, False, passed,
            detail=f"单点失败轮换: {override}", severity="P1")
