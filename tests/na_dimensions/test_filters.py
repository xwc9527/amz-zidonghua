"""单维度筛选 + 缺失规则 + 抓取/查询一致性。"""
from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from detail_parser import check_detail_filters
from fetch_new_arrivals import _pass_list_filters
from tests.na_dimensions.fixtures.products import PRODUCTS, FROZEN_NOW, detail_fields, list_card
from tests.na_dimensions.helpers import (
    CaseResult,
    SuiteCollector,
    insert_products,
    query_asins_sqlite,
    scrape_pass_asins,
    temp_sqlite_db,
)


def _ok(col, cid, dim, layer, passed: bool, expected, actual, detail="", severity="P2"):
    col.add(CaseResult(
        cid, dim, layer, "PASS" if passed else "FAIL",
        expected=expected, actual=actual, detail=detail,
        severity="" if passed else severity,
    ))


def _range_matrix(col: SuiteCollector, *, dim: str, field: str, product_key: str,
                  value, list_level: bool, min_key: str, max_key: str):
    """对数值区间执行标准用例矩阵。"""
    p = PRODUCTS[product_key]
    item = list_card(p) if list_level else detail_fields(p)
    check = _pass_list_filters if list_level else check_detail_filters
    prefix = f"SD-{dim}"

    # 不设置条件
    _ok(col, f"{prefix}-NONE", dim, "单维度",
        check(item, {}) is True, True, True)

    # 只设 min 命中
    f = {min_key: value if not isinstance(value, float) else value * 0.5}
    if isinstance(value, (int, float)):
        f = {min_key: value - (1 if isinstance(value, int) else 0.1) if value > 1 else value}
        # 简化：min = value（等于最小值应通过）
        f = {min_key: value}
    _ok(col, f"{prefix}-MIN-HIT", dim, "单维度",
        check(item, {min_key: value}) is True, True, check(item, {min_key: value}))

    # 只设 min 不命中
    hi_min = value + (10 if isinstance(value, int) else 10.0)
    _ok(col, f"{prefix}-MIN-MISS", dim, "单维度",
        check(item, {min_key: hi_min}) is False, False, check(item, {min_key: hi_min}))

    # 只设 max 命中
    _ok(col, f"{prefix}-MAX-HIT", dim, "单维度",
        check(item, {max_key: value}) is True, True, check(item, {max_key: value}))

    # 只设 max 不命中
    lo_max = max(0, value - (10 if isinstance(value, int) else 10.0))
    if lo_max == 0 and value > 0:
        lo_max = value * 0.1 if isinstance(value, float) else max(1, value // 10)
    _ok(col, f"{prefix}-MAX-MISS", dim, "单维度",
        check(item, {max_key: lo_max}) is False, False, check(item, {max_key: lo_max}))

    # 等于边界
    _ok(col, f"{prefix}-EQ-MIN", dim, "边界",
        check(item, {min_key: value, max_key: value}) is True, True, True)

    # 字段缺失（用 D）
    missing = list_card(PRODUCTS["D"]) if list_level else detail_fields(PRODUCTS["D"])
    # 对 D，部分列表字段仍有值；仅当 field 在 D 上为 None 时测缺失
    miss_val = PRODUCTS["D"].get(field)
    if miss_val is None:
        _ok(col, f"{prefix}-MISSING", dim, "缺失值",
            check(missing, {max_key: value if value else 999999}) is False,
            False, check(missing, {max_key: value if value else 999999}),
            severity="P1")
    else:
        # 构造缺失样本
        miss_item = dict(item)
        miss_item[field] = None
        if field == "weight_lb":
            miss_item["item_weight"] = None
        if field.startswith("dim_"):
            miss_item["dim_l_in"] = miss_item["dim_w_in"] = miss_item["dim_h_in"] = None
            miss_item["item_dimensions"] = None
        _ok(col, f"{prefix}-MISSING", dim, "缺失值",
            check(miss_item, {max_key: 999999}) is False, False,
            check(miss_item, {max_key: 999999}), severity="P1")


def run_single_dimension_tests(col: SuiteCollector):
    # 列表维度
    _range_matrix(col, dim="价格", field="price_value", product_key="A",
                  value=24.99, list_level=True, min_key="price_min", max_key="price_max")
    _range_matrix(col, dim="评分", field="rating", product_key="A",
                  value=4.5, list_level=True, min_key="rating_min", max_key="rating_max")
    _range_matrix(col, dim="评论数", field="review_count", product_key="A",
                  value=120, list_level=True, min_key="review_min", max_key="review_max")

    # 详情维度
    _range_matrix(col, dim="BSR大类", field="bsr_main_rank", product_key="A",
                  value=5000, list_level=False, min_key="bsr_main_min", max_key="bsr_main_max")
    _range_matrix(col, dim="BSR子类", field="bsr_sub_rank", product_key="A",
                  value=200, list_level=False, min_key="bsr_sub_min", max_key="bsr_sub_max")
    _range_matrix(col, dim="变体数", field="variant_option_count", product_key="A",
                  value=3, list_level=False, min_key="variant_min", max_key="variant_max")
    _range_matrix(col, dim="其他卖家数", field="other_sellers_count", product_key="A",
                  value=2, list_level=False, min_key="sellers_min", max_key="sellers_max")
    social = detail_fields(PRODUCTS["A"])
    _ok(col, "SD-SOCIAL-MIN-HIT", "月销量", "单维度",
        check_detail_filters(social, {"social_proof_min": 1000}) is True, True, True)
    _ok(col, "SD-SOCIAL-MIN-MISS", "月销量", "单维度",
        check_detail_filters(social, {"social_proof_min": 1001}) is False, False, False)
    _ok(col, "SD-SOCIAL-MISSING", "月销量", "缺失值",
        check_detail_filters(detail_fields(PRODUCTS["D"]), {"social_proof_min": 1}) is False,
        False, False, severity="P1")
    _range_matrix(col, dim="重量", field="weight_lb", product_key="A",
                  value=1.5, list_level=False, min_key="weight_min", max_key="weight_max")
    _range_matrix(col, dim="FBA费用", field="fba_fee", product_key="A",
                  value=4.5, list_level=False, min_key="fba_fee_min", max_key="fba_fee_max")

    # 尺寸：上限 + 缺维
    a = detail_fields(PRODUCTS["A"])
    _ok(col, "SD-尺寸-PASS", "尺寸", "单维度",
        check_detail_filters(a, {"dim_l": 10, "dim_w": 5, "dim_h": 2}) is True, True, True)
    _ok(col, "SD-尺寸-FAIL-L", "尺寸", "单维度",
        check_detail_filters(a, {"dim_l": 9}) is False, False, False)
    miss = dict(a)
    miss["dim_h_in"] = None
    _ok(col, "SD-尺寸-MISS-AXIS", "尺寸", "缺失值",
        check_detail_filters(miss, {"dim_l": 100}) is False, False, False, severity="P1")
    d = detail_fields(PRODUCTS["D"])
    _ok(col, "SD-尺寸-MISS-ALL", "尺寸", "缺失值",
        check_detail_filters(d, {"dim_l": 100}) is False, False, False, severity="P1")

    # 配送模式
    e = detail_fields(PRODUCTS["E"])
    f = detail_fields(PRODUCTS["F"])
    _ok(col, "SD-FT-FBA-HIT", "配送模式", "单维度",
        check_detail_filters(e, {"fulfillment_type": "FBA"}) is True, True, True)
    _ok(col, "SD-FT-FBA-MISS", "配送模式", "单维度",
        check_detail_filters(f, {"fulfillment_type": "FBA"}) is False, False, False)
    _ok(col, "SD-FT-FBM-HIT", "配送模式", "单维度",
        check_detail_filters(f, {"fulfillment_type": "FBM"}) is True, True, True)
    _ok(col, "SD-FT-MISSING", "配送模式", "缺失值",
        check_detail_filters(detail_fields(PRODUCTS["D"]), {"fulfillment_type": "FBA"}) is False,
        False, False, severity="P1")

    # 产地
    _ok(col, "SD-COO-EXACT", "产地", "单维度",
        check_detail_filters(e, {"country": "China"}) is True, True, True)
    _ok(col, "SD-COO-CASE", "产地", "单维度",
        check_detail_filters(e, {"country": "china"}) is True, True, True)
    _ok(col, "SD-COO-SUB", "产地", "单维度",
        check_detail_filters(e, {"country": "Chi"}) is True, True, True)
    _ok(col, "SD-COO-MISS-VAL", "产地", "单维度",
        check_detail_filters(e, {"country": "Japan"}) is False, False, False)
    _ok(col, "SD-COO-MISSING", "产地", "缺失值",
        check_detail_filters(detail_fields(PRODUCTS["D"]), {"country": "China"}) is False,
        False, False, severity="P1")

    # 标记
    _ok(col, "SD-AC-ON", "Amazon精选", "单维度",
        check_detail_filters(e, {"amazons_choice": True}) is True, True, True)
    _ok(col, "SD-AC-OFF", "Amazon精选", "单维度",
        check_detail_filters(f, {"amazons_choice": True}) is False, False, False)
    _ok(col, "SD-BS-ON", "畅销标记", "单维度",
        check_detail_filters(e, {"bestseller": True}) is True, True, True)
    _ok(col, "SD-BS-OFF", "畅销标记", "单维度",
        check_detail_filters(f, {"bestseller": True}) is False, False, False)
    _ok(col, "SD-AC-BS-BOTH", "Amazon精选", "组合",
        check_detail_filters(e, {"amazons_choice": True, "bestseller": True}) is True, True, True)
    _ok(col, "SD-AC-BS-HALF", "畅销标记", "组合",
        check_detail_filters(
            {**detail_fields(PRODUCTS["A"]), "is_amazon_choice": 1, "is_bestseller": 0},
            {"amazons_choice": True, "bestseller": True},
        ) is False, False, False)

    # 上架日期（冻结时间）
    fixed = datetime.fromisoformat(FROZEN_NOW)
    with patch("detail_parser.datetime") as mock_dt:
        mock_dt.now.return_value = fixed
        mock_dt.strptime = datetime.strptime
        a = detail_fields(PRODUCTS["A"])  # 2026-06-20 → 28 days
        b = detail_fields(PRODUCTS["B"])  # 2026-06-18 → 30 days
        c = detail_fields(PRODUCTS["C"])  # 2025-01-01 → 老
        _ok(col, "SD-DATE-30-A", "上架日期", "单维度",
            check_detail_filters(a, {"date_range": "30"}) is True, True, True)
        _ok(col, "SD-DATE-30-B", "上架日期", "边界",
            check_detail_filters(b, {"date_range": "30"}) is True, True, True,
            detail="正好30天应通过（days > 30 才拒）")
        # 31 天边界：构造
        day31 = dict(a)
        day31["date_first_available"] = "2026-06-17"  # 31 days
        _ok(col, "SD-DATE-31", "上架日期", "边界",
            check_detail_filters(day31, {"date_range": "30"}) is False, False, False)
        _ok(col, "SD-DATE-90-C", "上架日期", "单维度",
            check_detail_filters(c, {"date_range": "90"}) is False, False, False)
        _ok(col, "SD-DATE-CUSTOM", "上架日期", "单维度",
            check_detail_filters(a, {
                "date_range": "custom", "date_from": "2026-06-01", "date_to": "2026-06-30",
            }) is True, True, True)
        _ok(col, "SD-DATE-CUSTOM-OUT", "上架日期", "单维度",
            check_detail_filters(a, {
                "date_range": "custom", "date_from": "2026-07-01", "date_to": "2026-07-18",
            }) is False, False, False)
        _ok(col, "SD-DATE-MISSING", "上架日期", "缺失值",
            check_detail_filters(detail_fields(PRODUCTS["D"]), {"date_range": "30"}) is False,
            False, False, severity="P1")

    # BSR 大类/子类互不误用
    only_main = {"bsr_main_rank": 100, "bsr_sub_rank": None}
    _ok(col, "SD-BSR-NO-CROSS", "BSR大类", "单维度",
        check_detail_filters(only_main, {"bsr_sub_max": 50}) is False, False, False,
        detail="只有大类时设子类条件应拒绝")
    _ok(col, "SD-BSR-MAIN-ONLY", "BSR大类", "单维度",
        check_detail_filters(only_main, {"bsr_main_max": 100}) is True, True, True)


def run_scrape_query_consistency(col: SuiteCollector):
    """抓取筛选 vs 查询筛选：同一条件 ASIN 集合一致。"""
    scenarios = [
        ("CQ-PRICE", "价格", {"price_min": 10, "price_max": 30, "site": "US"}),
        ("CQ-RATING", "评分", {"rating_min": 4.0, "site": "US"}),
        ("CQ-REVIEW", "评论数", {"review_min": 50, "review_max": 200, "site": "US"}),
        ("CQ-BSR", "BSR大类", {"bsr_main_max": 10000, "site": "US"}),
        ("CQ-WEIGHT", "重量", {"weight_max": 2.0, "site": "US"}),
        ("CQ-DIM", "尺寸", {"dim_l": 12, "dim_w": 8, "dim_h": 4, "site": "US"}),
        ("CQ-FBA", "FBA费用", {"fba_fee_max": 10, "site": "US"}),
        ("CQ-SOCIAL", "月销量", {"social_proof_min": 1000, "site": "US"}),
        ("CQ-FT", "配送模式", {"fulfillment_type": "FBA", "site": "US"}),
        ("CQ-COO", "产地", {"country": "China", "site": "US"}),
        ("CQ-AC", "Amazon精选", {"amazons_choice": True, "site": "US"}),
        ("CQ-BS", "畅销标记", {"bestseller": True, "site": "US"}),
        ("CQ-COMBO", "组合", {
            "price_min": 10, "price_max": 50, "rating_min": 4.0,
            "review_min": 20, "weight_max": 5, "fulfillment_type": "FBA", "site": "US",
        }),
    ]

    with temp_sqlite_db() as db:
        insert_products(db, test_run_id=col.run_id)
        for cid, dim, filters in scenarios:
            # 查询层日期相对时间也需冻结
            filt = dict(filters)
            scrape_filters = {k: v for k, v in filt.items() if k != "site"}
            # 查询用 listing_date；抓取层用 date_first_available —— 对日期单独处理
            if "date_range" in scrape_filters:
                fixed = datetime.fromisoformat(FROZEN_NOW)
                with patch("detail_parser.datetime") as mock_dt, \
                     patch("api_server.datetime", create=True):
                    mock_dt.now.return_value = fixed
                    mock_dt.strptime = datetime.strptime
                    scrape_set = scrape_pass_asins(scrape_filters)
            else:
                scrape_set = scrape_pass_asins(scrape_filters)

            # 日期查询：patch api_server 内 datetime
            if filt.get("date_range") and filt["date_range"] != "custom":
                from datetime import timedelta
                days = int(filt["date_range"])
                cutoff = (datetime.fromisoformat(FROZEN_NOW) - timedelta(days=days)).strftime("%Y-%m-%d")
                # 直接用 custom 等价，避免 now() 漂移
                qfilt = {k: v for k, v in filt.items() if k != "date_range"}
                qfilt["date_range"] = "custom"
                qfilt["date_from"] = cutoff
                query_set = query_asins_sqlite(db, qfilt)
            else:
                query_set = query_asins_sqlite(db, filt)

            # 查询结果需与抓取层比较时去掉 site 影响外的差异：
            # 抓取层不过滤 site；两边都用 US 商品即可
            ok = scrape_set == query_set
            _ok(col, cid, dim, "抓取/查询一致性", ok, scrape_set, query_set,
                detail=f"scrape={sorted(scrape_set)} query={sorted(query_set)}",
                severity="P1")

    # 日期一致性单独测
    with temp_sqlite_db() as db:
        insert_products(db, test_run_id=col.run_id)
        fixed = datetime.fromisoformat(FROZEN_NOW)
        with patch("detail_parser.datetime") as mock_dt:
            mock_dt.now.return_value = fixed
            mock_dt.strptime = datetime.strptime
            scrape_set = scrape_pass_asins({"date_range": "30"})
        from datetime import timedelta
        cutoff = (fixed - timedelta(days=30)).strftime("%Y-%m-%d")
        query_set = query_asins_sqlite(db, {
            "site": "US", "date_range": "custom", "date_from": cutoff,
        })
        _ok(col, "CQ-DATE", "上架日期", "抓取/查询一致性",
            scrape_set == query_set, scrape_set, query_set, severity="P1")
