"""固定 HTML 解析测试：列表字段、详情字段、单位换算、多站点格式。"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from detail_parser import parse_detail_fields
from fba_fees_us import parse_dims_inches, parse_weight_lb
from fetch_new_arrivals import _parse_listing_page, _parse_price_value
import fetch_new_arrivals as na

from tests.na_dimensions.fixtures.html_builders import (
    build_de_detail_html,
    build_jp_detail_html,
    build_list_card_html,
    build_list_page,
    build_us_detail_html,
)
from tests.na_dimensions.helpers import CaseResult, SuiteCollector


def _expect(col, case_id, dim, layer, ok, expected, actual, detail="", severity="P2"):
    col.add(CaseResult(
        case_id, dim, layer,
        "PASS" if ok else "FAIL",
        expected=expected, actual=actual, detail=detail,
        severity="" if ok else severity,
    ))


def run_parsing_tests(col: SuiteCollector):
    # ── 价格解析 ──
    # US 小数点
    old_sep = na._DECIMAL_SEP
    try:
        na._DECIMAL_SEP = "."
        v = _parse_price_value("$1,234.56")
        _expect(col, "PRS-PRICE-01", "价格", "列表解析", v == 1234.56, 1234.56, v)

        na._DECIMAL_SEP = ","
        v = _parse_price_value("1.234,56 €")
        _expect(col, "PRS-PRICE-02", "价格", "列表解析", abs((v or 0) - 1234.56) < 0.01, 1234.56, v)

        na._DECIMAL_SEP = "."
        v = _parse_price_value("")
        _expect(col, "PRS-PRICE-03", "价格", "列表解析", v is None, None, v)
    finally:
        na._DECIMAL_SEP = old_sep

    # ── 列表页卡片 ──
    card = build_list_card_html(
        "B0TESTAAAAA", "Std Product", "$24.99",
        "4.5 out of 5 stars", "1,234",
    )
    page = build_list_page([card])
    na._DECIMAL_SEP = "."
    na._RATING_PAT = r"([\d,\.]+)\s+out of\s+5"
    parsed = _parse_listing_page(page, {})
    items = parsed.get("items") or []
    ok = (
        len(items) == 1
        and items[0]["asin"] == "B0TESTAAAAA"
        and items[0]["price_value"] == 24.99
        and items[0]["rating"] == 4.5
        and items[0]["review_count"] == 1234
    )
    _expect(col, "PRS-LIST-01", "价格", "列表解析", ok, "完整卡片字段", {
        "n": len(items),
        "item": items[0] if items else None,
    }, severity="P1")

    # 评论数千位分隔
    _expect(
        col, "PRS-REV-01", "评论数", "列表解析",
        items and items[0].get("review_count") == 1234,
        1234, items[0].get("review_count") if items else None,
    )

    # ── 重量换算 ──
    weight_cases = [
        ("PRS-W-LB", "1.5 pounds", 1.5),
        ("PRS-W-LBS", "2 lbs", 2.0),
        ("PRS-W-OZ", "16 ounces", 1.0),
        ("PRS-W-OZ2", "8 oz", 0.5),
        ("PRS-W-KG", "1 kg", 2.20462),
        ("PRS-W-KG2", "1.2 kilograms", 1.2 * 2.20462),
        ("PRS-W-G", "500 grams", 500 * 0.00220462),
        ("PRS-W-G2", "800 g", 800 * 0.00220462),
        ("PRS-W-DE", "500 Gramm", 500 * 0.00220462),
        ("PRS-W-NONE", None, None),
        ("PRS-W-EMPTY", "", None),
    ]
    for cid, text, expected in weight_cases:
        actual = parse_weight_lb(text)
        if expected is None:
            ok = actual is None
        else:
            ok = actual is not None and abs(actual - expected) < 1e-5
        _expect(col, cid, "重量", "单位换算", ok, expected, actual)

    # ── 尺寸换算 + 排序 ──
    dim_cases = [
        ("PRS-D-IN", "10 x 5 x 2 inches", (10.0, 5.0, 2.0)),
        ("PRS-D-SORT", "2 x 10 x 5 inches", (10.0, 5.0, 2.0)),
        ("PRS-D-CM", "20 x 10 x 5 cm", (20 / 2.54, 10 / 2.54, 5 / 2.54)),
        ("PRS-D-DWH", '23.62"D x 11.61"W x 32.28"H', (32.28, 23.62, 11.61)),
        ("PRS-D-MISS", "10 x 5 inches", None),
        ("PRS-D-NONE", None, None),
    ]
    for cid, text, expected in dim_cases:
        actual = parse_dims_inches(text)
        if expected is None:
            ok = actual is None
        else:
            ok = (
                actual is not None
                and abs(actual[0] - expected[0]) < 1e-4
                and abs(actual[1] - expected[1]) < 1e-4
                and abs(actual[2] - expected[2]) < 1e-4
            )
        _expect(col, cid, "尺寸", "单位换算", ok, expected, actual, severity="P1")

    # ── US 详情解析 ──
    html = build_us_detail_html(
        variant_asins=["A1", "A2", "A3"],
        sellers_text="5 new from $12.00",
        amazon_choice=True,
        bestseller=True,
    )
    d = parse_detail_fields(html, "US")
    checks = [
        ("PRS-US-BSRM", "BSR大类", d.get("bsr_main_rank") == 5000, 5000, d.get("bsr_main_rank")),
        ("PRS-US-BSRS", "BSR子类", d.get("bsr_sub_rank") == 200, 200, d.get("bsr_sub_rank")),
        ("PRS-US-DATE", "上架日期", d.get("date_first_available") == "2026-06-20", "2026-06-20", d.get("date_first_available")),
        ("PRS-US-W", "重量", d.get("weight_lb") is not None and abs(d["weight_lb"] - 1.5) < 1e-6, 1.5, d.get("weight_lb")),
        ("PRS-US-DIM", "尺寸", d.get("dim_l_in") == 10.0 and d.get("dim_w_in") == 5.0 and d.get("dim_h_in") == 2.0,
         (10, 5, 2), (d.get("dim_l_in"), d.get("dim_w_in"), d.get("dim_h_in"))),
        ("PRS-US-VAR", "变体数", d.get("variant_option_count") == 3, 3, d.get("variant_option_count")),
        ("PRS-US-SEL", "其他卖家数", d.get("other_sellers_count") == 5, 5, d.get("other_sellers_count")),
        ("PRS-US-COO", "产地", d.get("country_of_origin") == "China", "China", d.get("country_of_origin")),
        ("PRS-US-FT", "配送模式", d.get("fulfillment_type") == "FBA", "FBA", d.get("fulfillment_type")),
        ("PRS-US-AC", "Amazon精选", d.get("is_amazon_choice") == 1, 1, d.get("is_amazon_choice")),
        ("PRS-US-BS", "畅销标记", d.get("is_bestseller") == 1, 1, d.get("is_bestseller")),
    ]
    for cid, dim, ok, exp, act in checks:
        _expect(col, cid, dim, "详情解析", ok, exp, act, severity="P1")

    # 单格「标签: 值」形态 — 应剥离前缀只留 China
    html_co = """<!doctype html><html><body><div id="prodDetails"><table>
      <tr><td>Country of Origin : China</td></tr></table></div></body></html>"""
    d_co = parse_detail_fields(html_co, "US")
    _expect(col, "PRS-US-COO-COMBINED", "产地", "详情解析",
            d_co.get("country_of_origin") == "China", "China", d_co.get("country_of_origin"),
            detail="单格文本含标签时是否剥离前缀", severity="P2")

    # BSR 顺序：只有大类
    html2 = build_us_detail_html(bsr_main=(100, "Toys"), bsr_sub=None)
    d2 = parse_detail_fields(html2, "US")
    _expect(col, "PRS-BSR-ONLY-MAIN", "BSR大类", "详情解析",
            d2.get("bsr_main_rank") == 100 and d2.get("bsr_sub_rank") is None,
            "main=100 sub=None", (d2.get("bsr_main_rank"), d2.get("bsr_sub_rank")))

    # 无变体组件
    html3 = build_us_detail_html(variant_asins=[], sellers_text=None)
    d3 = parse_detail_fields(html3, "US")
    _expect(col, "PRS-VAR-NONE", "变体数", "详情解析",
            d3.get("variant_option_count") is None, None, d3.get("variant_option_count"))

    # FBM
    html4 = build_us_detail_html(merchant_text="Sold by ThirdParty and Fulfilled by Merchant")
    d4 = parse_detail_fields(html4, "US")
    _expect(col, "PRS-FBM", "配送模式", "详情解析",
            d4.get("fulfillment_type") == "FBM", "FBM", d4.get("fulfillment_type"))

    # DE
    de = parse_detail_fields(build_de_detail_html(), "DE")
    _expect(col, "PRS-DE-BSR", "BSR大类", "详情解析",
            de.get("bsr_main_rank") == 1234, 1234, de.get("bsr_main_rank"), severity="P1")
    _expect(col, "PRS-DE-DATE", "上架日期", "详情解析",
            de.get("date_first_available") == "2026-03-15", "2026-03-15", de.get("date_first_available"))
    _expect(col, "PRS-DE-W", "重量", "详情解析",
            de.get("weight_lb") is not None and abs(de["weight_lb"] - 500 * 0.00220462) < 1e-5,
            500 * 0.00220462, de.get("weight_lb"))
    _expect(col, "PRS-DE-FT", "配送模式", "详情解析",
            de.get("fulfillment_type") == "FBA", "FBA", de.get("fulfillment_type"))

    # JP
    jp = parse_detail_fields(build_jp_detail_html(), "JP")
    _expect(col, "PRS-JP-DATE", "上架日期", "详情解析",
            jp.get("date_first_available") == "2026-04-10", "2026-04-10", jp.get("date_first_available"))
    _expect(col, "PRS-JP-W", "重量", "详情解析",
            jp.get("weight_lb") is not None and abs(jp["weight_lb"] - 1.2 * 2.20462) < 1e-4,
            1.2 * 2.20462, jp.get("weight_lb"))
    _expect(col, "PRS-JP-AC", "Amazon精选", "详情解析",
            jp.get("is_amazon_choice") == 1, 1, jp.get("is_amazon_choice"))
    _expect(col, "PRS-JP-FT", "配送模式", "详情解析",
            jp.get("fulfillment_type") == "FBA", "FBA", jp.get("fulfillment_type"))

    # FBA 费用估算（US 有重量尺寸应有值）
    html5 = build_us_detail_html()
    d5 = parse_detail_fields(html5, "US")
    _expect(col, "PRS-FBA-US", "FBA费用", "详情解析",
            d5.get("fba_fee") is not None and d5["fba_fee"] > 0,
            ">0", d5.get("fba_fee"), severity="P1")
