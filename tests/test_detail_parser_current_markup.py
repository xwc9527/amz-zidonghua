"""覆盖 variant_option_count / other_sellers_count 在 Amazon 当前真实标记下的解析。

背景：Amazon 把 twister 变体标记从 `li[data-defaultasin]` 换成了
`li[data-asin]`（部分页面容器也从 #twister_feature_div 换成了
#twister-plus-inline-twister），把"其他卖家"文案从 "N new from $X"
换成了 "New (N) from $X"。旧选择器/正则在这两种新标记下会全部解析为
None，一旦筛选条件里配置了 variant_max/sellers_max 等阈值，
_range_check 会把 None 一律判定为不通过——导致组合条件抓取误杀
几乎所有真实商品（详见 2026-07-23 的现场核实）。

本测试锁定新标记下的正确解析结果，同时保留旧标记兜底覆盖，防止再退化。
"""
from __future__ import annotations

from detail_parser import active_filter_none_flags, parse_detail_fields


def _wrap(inner: str) -> str:
    return f"<!doctype html><html><body>{inner}</body></html>"


def test_variant_option_count_new_data_asin_markup():
    html = _wrap(
        """
        <div id="twister_feature_div">
          <ul>
            <li data-asin="B0AAAAAAAA" data-initiallyselected="true"></li>
            <li data-asin="B0BBBBBBBB" data-initiallyselected="false"></li>
            <li data-asin="B0CCCCCCCC" data-initiallyselected="false"></li>
          </ul>
        </div>
        """
    )
    d = parse_detail_fields(html, "US")
    assert d.get("variant_option_count") == 3


def test_variant_option_count_new_twister_plus_container():
    html = _wrap(
        """
        <div id="twister-plus-inline-twister">
          <ul>
            <li data-asin="B0AAAAAAAA"></li>
            <li data-asin="B0BBBBBBBB"></li>
          </ul>
        </div>
        """
    )
    d = parse_detail_fields(html, "US")
    assert d.get("variant_option_count") == 2


def test_variant_option_count_dedupes_repeated_asin():
    # 同一 ASIN 在多个尺寸/颜色维度的列表里重复出现时按去重后的变体数计数
    html = _wrap(
        """
        <div id="twister_feature_div">
          <ul>
            <li data-asin="B0AAAAAAAA"></li>
            <li data-asin="B0AAAAAAAA"></li>
            <li data-asin="B0BBBBBBBB"></li>
          </ul>
        </div>
        """
    )
    d = parse_detail_fields(html, "US")
    assert d.get("variant_option_count") == 2


def test_variant_option_count_legacy_data_defaultasin_still_supported():
    html = _wrap(
        """
        <div id="twister_feature_div">
          <ul>
            <li data-defaultasin="B0AAAAAAAA"></li>
            <li data-defaultasin="B0BBBBBBBB"></li>
          </ul>
        </div>
        """
    )
    d = parse_detail_fields(html, "US")
    assert d.get("variant_option_count") == 2


def test_variant_option_count_none_when_no_twister():
    html = _wrap("<div id='prodDetails'>no variants here</div>")
    d = parse_detail_fields(html, "US")
    assert d.get("variant_option_count") is None


def test_other_sellers_count_new_new_paren_n_format():
    html = _wrap(
        '<div id="olp_feature_div">Other sellers on Amazon New (22) from $39.00 '
        '$ 39 . 00 &amp; FREE Shipping.</div>'
    )
    d = parse_detail_fields(html, "US")
    assert d.get("other_sellers_count") == 22


def test_other_sellers_count_aod_offer_list_container():
    html = _wrap('<div id="aod-offer-list">New (5) from $12.34</div>')
    d = parse_detail_fields(html, "US")
    assert d.get("other_sellers_count") == 5


def test_other_sellers_count_legacy_n_new_format_still_supported():
    html = _wrap('<div id="olp_feature_div">3 new from $19.99</div>')
    d = parse_detail_fields(html, "US")
    assert d.get("other_sellers_count") == 3


def test_other_sellers_count_none_when_no_offers_widget():
    html = _wrap("<div id='prodDetails'>single seller listing</div>")
    d = parse_detail_fields(html, "US")
    assert d.get("other_sellers_count") is None


def test_item_weight_parsed_from_top_highlight_product_overview_table():
    # 新版"Product overview"紧凑表格（class 前缀 po-），出现在买盒附近，
    # 旧的 detailBullets/techSpec 选择器覆盖不到。
    html = _wrap(
        """
        <div id="topHighlight">
          <table>
            <tr class="a-spacing-small po-brand"><td>Brand</td><td>David</td></tr>
            <tr class="a-spacing-small po-item_weight"><td>Item Weight</td><td>16 ounces</td></tr>
          </table>
        </div>
        """
    )
    d = parse_detail_fields(html, "US")
    assert d.get("item_weight") == "16 ounces"


def test_item_weight_parsed_from_voyager_ns_desktop_table():
    # Northstar 改版详情表（table.voyager-ns-desktop-table），容器 div 的 id
    # 会因子部件不同而变化（item_details / measurements / ...），只有表格自身
    # 的 class 是稳定的，因此选择器按 class 而非容器 id 匹配。
    html = _wrap(
        """
        <div id="measurements" class="a-section voyager-ns-desktop-data">
          <table class="a-keyvalue voyager-ns-desktop-table">
            <tr><th class="voyager-ns-desktop-table-label">Item Weight</th>
                <td class="voyager-ns-desktop-table-value">8 ounces</td></tr>
          </table>
        </div>
        """
    )
    d = parse_detail_fields(html, "US")
    assert d.get("item_weight") == "8 ounces"


# ── active_filter_none_flags：运行期探针（区分"字段解析坍缩"与"真实不达标"）──

def test_active_filter_none_flags_no_filters_returns_empty():
    assert active_filter_none_flags({"variant_option_count": None}, {}) == {}


def test_active_filter_none_flags_only_flags_enabled_dimensions():
    detail = {"variant_option_count": None, "bsr_main_rank": 5000}
    filters = {"variant_max": 3}
    flags = active_filter_none_flags(detail, filters)
    assert flags == {"variant_option_count": True}
    # bsr_main 维度没启用阈值，不应该出现在返回值里
    assert "bsr_main_rank" not in flags


def test_active_filter_none_flags_false_when_value_present():
    detail = {"variant_option_count": 5}
    filters = {"variant_max": 3}
    assert active_filter_none_flags(detail, filters) == {"variant_option_count": False}


def test_active_filter_none_flags_dims_group_flags_together():
    filters = {"dim_l": 10}
    assert active_filter_none_flags({"dim_l_in": None, "dim_w_in": 5, "dim_h_in": 2}, filters) == {
        "dim_l_in/dim_w_in/dim_h_in": True
    }
    assert active_filter_none_flags(
        {"dim_l_in": 8, "dim_w_in": 5, "dim_h_in": 2}, filters
    ) == {"dim_l_in/dim_w_in/dim_h_in": False}


def test_active_filter_none_flags_fulfillment_and_country():
    filters = {"fulfillment_type": "FBA", "country": "China"}
    flags = active_filter_none_flags({}, filters)
    assert flags == {"fulfillment_type": True, "country_of_origin": True}
    flags2 = active_filter_none_flags(
        {"fulfillment_type": "FBA", "country_of_origin": "China"}, filters
    )
    assert flags2 == {"fulfillment_type": False, "country_of_origin": False}
