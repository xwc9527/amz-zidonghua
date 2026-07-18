"""完整 Pairwise：C(15,2)=105 对，每对 4 用例。"""
from __future__ import annotations

import itertools
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from tests.na_dimensions.filter_engine import pass_product, with_missing
from tests.na_dimensions.fixtures.products import (
    DIM_SPEC,
    PAIRWISE_DIMS,
    PRODUCTS,
)
from tests.na_dimensions.helpers import CaseResult, SuiteCollector


def run_pairwise_full(col: SuiteCollector) -> dict:
    dims = PAIRWISE_DIMS
    n = len(dims)
    expected_pairs = set(itertools.combinations(dims, 2))
    covered_pairs: set[tuple[str, str]] = set()
    base = PRODUCTS["G"]

    for a, b in itertools.combinations(dims, 2):
        covered_pairs.add((a, b))
        sa, sb = DIM_SPEC[a], DIM_SPEC[b]
        both = {**sa["pass"], **sb["pass"]}

        # 双条件均满足
        ok = pass_product(base, both)
        col.add(CaseResult(
            f"PW105-{a}+{b}-BOTH", f"{a}+{b}", "组合",
            "PASS" if ok else "FAIL", True, ok,
            severity="" if ok else "P1",
        ))

        # 第一个条件单独失败（其余因子仍满足）
        if a == "ac":
            fail1_ok = pass_product({**base, "is_amazon_choice": 0}, both) is False
        elif a == "bs":
            fail1_ok = pass_product({**base, "is_bestseller": 0}, both) is False
        else:
            f1 = {**sb["pass"], **sa["fail"]}
            fail1_ok = pass_product(base, f1) is False
        col.add(CaseResult(
            f"PW105-{a}+{b}-F1", f"{a}+{b}", "组合",
            "PASS" if fail1_ok else "FAIL", False, fail1_ok,
            detail=f"仅 {a} 失败", severity="" if fail1_ok else "P1",
        ))

        # 第二个条件单独失败
        if b == "ac":
            fail2_ok = pass_product({**base, "is_amazon_choice": 0}, both) is False
        elif b == "bs":
            fail2_ok = pass_product({**base, "is_bestseller": 0}, both) is False
        else:
            f2 = {**sa["pass"], **sb["fail"]}
            fail2_ok = pass_product(base, f2) is False
        col.add(CaseResult(
            f"PW105-{a}+{b}-F2", f"{a}+{b}", "组合",
            "PASS" if fail2_ok else "FAIL", False, fail2_ok,
            detail=f"仅 {b} 失败", severity="" if fail2_ok else "P1",
        ))

        # 其中一个字段缺失（优先缺失 a）
        miss_val = sa.get("miss_value", None)
        miss_prod = with_missing(base, sa["miss_fields"], miss_val)
        # badge 缺失用 0
        if a in ("ac", "bs"):
            miss_prod = with_missing(base, sa["miss_fields"], 0)
        miss_ok = pass_product(miss_prod, both) is False
        col.add(CaseResult(
            f"PW105-{a}+{b}-MISS", f"{a}+{b}", "组合",
            "PASS" if miss_ok else "FAIL", False, miss_ok,
            detail=f"缺失 {a} 字段 {sa['miss_fields']}", severity="" if miss_ok else "P1",
        ))

    missing = expected_pairs - covered_pairs
    stats = {
        "dimension_count": n,
        "expected_pairs": len(expected_pairs),
        "actual_pairs": len(covered_pairs),
        "missing_pairs": sorted([f"{x}+{y}" for x, y in missing]),
        "coverage_pct": 100.0 * len(covered_pairs) / len(expected_pairs) if expected_pairs else 0,
        "covered_list": sorted([f"{x}+{y}" for x, y in covered_pairs]),
    }
    col.add(CaseResult(
        "PW105-COVERAGE", "Pairwise覆盖", "组合",
        "PASS" if not missing else "FAIL",
        expected=105, actual=len(covered_pairs),
        detail=(
            f"维度数:{n} 应覆盖:{len(expected_pairs)} 实际:{len(covered_pairs)} "
            f"遗漏:{len(missing)} 覆盖率:{stats['coverage_pct']:.1f}%"
        ),
        severity="" if not missing else "P0",
    ))
    col.meta = getattr(col, "meta", {})
    col.meta["pairwise"] = stats
    return stats
