"""强相关组合：每字段独立失败/缺失 + 顺序反转 + 抓取/查询一致性。"""
from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from tests.na_dimensions.filter_engine import pass_product, with_missing
from tests.na_dimensions.fixtures.products import DIM_SPEC, PRODUCTS
from tests.na_dimensions.helpers import (
    CaseResult,
    SuiteCollector,
    insert_products,
    query_asins_sqlite,
    scrape_pass_asins,
    temp_sqlite_db,
)

# (名称, 因子列表)
STRONG = [
    ("价格+评论数", ["price", "review"]),
    ("月销量+评论数", ["social", "review"]),
    ("评分+评论数", ["rating", "review"]),
    ("BSR大类+BSR子类", ["bsr_main", "bsr_sub"]),
    ("重量+尺寸", ["weight", "dim"]),
    ("重量+尺寸+FBA费用", ["weight", "dim", "fba"]),
    ("配送模式+FBA费用", ["ft", "fba"]),
    ("上架日期+评论数", ["date", "review"]),
    ("产地+配送模式", ["country", "ft"]),
    ("Amazon精选+畅销标记", ["ac", "bs"]),
    ("价格+评分+评论数", ["price", "rating", "review"]),
]


def _merge_pass(factors: list[str]) -> dict:
    out = {}
    for f in factors:
        out.update(DIM_SPEC[f]["pass"])
    return out


def run_strong_complete(col: SuiteCollector):
    base = PRODUCTS["G"]
    with temp_sqlite_db() as db:
        insert_products(db, list(PRODUCTS.values()), test_run_id=col.run_id)

        for name, factors in STRONG:
            filters = _merge_pass(factors)
            target = base

            # 全部满足
            ok = pass_product(target, filters)
            col.add(CaseResult(
                f"SC2-{name}-ALL", name, "组合",
                "PASS" if ok else "FAIL", True, ok, severity="" if ok else "P1",
            ))

            # 每字段单独失败（其余字段仍满足）
            for f in factors:
                spec = DIM_SPEC[f]
                if f == "ac":
                    ok_f = pass_product({**target, "is_amazon_choice": 0}, filters) is False
                elif f == "bs":
                    ok_f = pass_product({**target, "is_bestseller": 0}, filters) is False
                else:
                    ff = {**_merge_pass([x for x in factors if x != f]), **spec["fail"]}
                    ok_f = pass_product(target, ff) is False
                col.add(CaseResult(
                    f"SC2-{name}-FAIL-{f}", name, "组合",
                    "PASS" if ok_f else "FAIL", False, ok_f,
                    detail=f"{f} 单独失败", severity="" if ok_f else "P1",
                ))

            # 每字段单独缺失
            for f in factors:
                spec = DIM_SPEC[f]
                mv = 0 if f in ("ac", "bs") else None
                mp = with_missing(target, spec["miss_fields"], mv)
                ok_m = pass_product(mp, filters) is False
                col.add(CaseResult(
                    f"SC2-{name}-MISS-{f}", name, "组合",
                    "PASS" if ok_m else "FAIL", False, ok_m,
                    detail=f"{f} 单独缺失", severity="" if ok_m else "P1",
                ))

            # 条件顺序反转
            rev = dict(reversed(list(filters.items())))
            ok_ord = pass_product(target, filters) == pass_product(target, rev)
            col.add(CaseResult(
                f"SC2-{name}-ORDER", name, "组合",
                "PASS" if ok_ord else "FAIL", True, ok_ord,
            ))

            # 抓取/查询一致性
            scrape = scrape_pass_asins(filters)
            qf = {**filters, "site": "US"}
            query = query_asins_sqlite(db, qf)
            ok_cq = scrape == query
            col.add(CaseResult(
                f"SC2-{name}-CQ", name, "抓取/查询一致性",
                "PASS" if ok_cq else "FAIL", scrape, query,
                detail=f"scrape={sorted(scrape)} query={sorted(query)}",
                severity="" if ok_cq else "P1",
            ))
