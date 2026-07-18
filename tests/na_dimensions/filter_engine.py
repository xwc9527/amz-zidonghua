"""抓取层筛选统一入口（不改业务代码，仅调用现有函数）。"""
from __future__ import annotations

from detail_parser import check_detail_filters
from fetch_new_arrivals import _pass_list_filters
from tests.na_dimensions.fixtures.products import detail_fields, list_card

LIST_KEYS = {"price_min", "price_max", "rating_min", "rating_max", "review_min", "review_max"}


def split_filters(filters: dict) -> tuple[dict, dict]:
    lf = {k: v for k, v in filters.items() if k in LIST_KEYS and v}
    df = {k: v for k, v in filters.items() if k not in LIST_KEYS and k != "site" and v}
    return lf, df


def pass_product(product: dict, filters: dict) -> bool:
    lf, df = split_filters(filters)
    return _pass_list_filters(list_card(product), lf) and check_detail_filters(detail_fields(product), df)


def with_missing(product: dict, fields: list[str], miss_value=None) -> dict:
    p = dict(product)
    for f in fields:
        p[f] = miss_value
    return p
