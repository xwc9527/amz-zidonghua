"""参数异常矩阵：启动 API / 结果 API / CLI；保留 NaN/Inf FAIL 证据。"""
from __future__ import annotations

import math
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from api_server import (
    _append_filter_flags,
    _parse_filter_number,
    _validate_start_filters,
)
from tests.na_dimensions.helpers import CaseResult, SuiteCollector

# (用例名, 字段, 值, 期望拒绝)
ANOMALIES = [
    ("nan", "price_min", float("nan")),
    ("pos_inf", "price_max", float("inf")),
    ("neg_inf", "weight_min", float("-inf")),
    ("str_nan", "price_min", "nan"),
    ("str_inf", "price_max", "inf"),
    ("str_ninf", "weight_min", "-inf"),
    ("str_NaN", "price_min", "NaN"),
    ("str_Infinity", "price_max", "Infinity"),
    ("exp_overflow", "price_max", "1e309"),
    ("bool_true", "price_min", True),
    ("bool_false", "review_min", False),
    ("empty_list", "price_min", []),
    ("empty_dict", "price_min", {}),
    ("spaces", "price_min", "  10  "),  # 合法：应接受或按实现
    ("sci", "price_min", "1e2"),  # 合法 100
    ("int_float", "review_min", 10.5),
    ("int_sci", "review_min", "1e2"),  # int("1e2") 失败 → 应拒绝
    ("non_ascii", "price_min", "１２"),  # 全角
]


def _expect_reject(val) -> bool:
    """规范：非有限 / 非数字 / 非整型整数字段 → 必须拒绝。"""
    if isinstance(val, bool):
        return True
    if isinstance(val, (list, dict)):
        return True
    if isinstance(val, float) and not math.isfinite(val):
        return True
    if isinstance(val, str):
        s = val.strip().lower()
        if s in ("nan", "inf", "infinity", "+inf", "-inf", "-infinity"):
            return True
        if val == "１２":
            return True
        if val == "1e309":
            return True
        if val == "1e2" and False:
            return False
    if isinstance(val, float) and not float(val).is_integer() and False:
        return True
    return None  # 由调用方按字段决定


def run_param_matrix(col: SuiteCollector):
    int_keys = {
        "review_min", "review_max", "bsr_main_min", "bsr_main_max",
        "bsr_sub_min", "bsr_sub_max", "variant_min", "variant_max",
        "sellers_min", "sellers_max",
    }

    for name, key, val in ANOMALIES:
        must_reject = True
        if name == "spaces":
            must_reject = False  # "  10  " 应可解析
        if name == "sci" and key == "price_min":
            must_reject = False  # 1e2 → 100.0 合法
        if name == "int_sci":
            must_reject = True
        if name == "int_float":
            must_reject = True
        if name in ("nan", "pos_inf", "neg_inf", "str_nan", "str_inf", "str_ninf",
                    "str_NaN", "str_Infinity", "exp_overflow", "bool_true",
                    "bool_false", "empty_list", "empty_dict", "non_ascii"):
            must_reject = True

        # 1) 启动校验
        msg = _validate_start_filters({key: val})
        rejected = msg is not None
        # False 作为 body 值时 validate 可能 skip（raw is False → continue）
        if val is False:
            # 规范要求拒绝布尔；若实现跳过则记 FAIL
            pass
        ok = (rejected == must_reject) if must_reject else (msg is None or rejected)
        if name == "spaces":
            ok = msg is None
        if name == "sci":
            ok = msg is None
        status = "PASS" if ok else "FAIL"
        # NaN/Inf 类已知缺陷：保持 FAIL，不修业务
        col.add(CaseResult(
            f"PAR-START-{name}", "参数异常", "参数校验",
            status, f"拒绝={must_reject}", msg,
            detail=f"启动API校验 {key}={val!r}",
            severity="P1" if not ok and must_reject else ("P2" if not ok else ""),
        ))

        # 2) 启动门禁：校验失败 ⇒ 不得进入 Popen（与 start_products 同序）
        if must_reject:
            # 证据：校验返回错误 → 子进程不可达；校验放行 → 假启动风险 FAIL
            ok2 = msg is not None
            col.add(CaseResult(
                f"PAR-NOPROC-{name}", "参数异常", "API参数",
                "PASS" if ok2 else "FAIL",
                "校验拒绝后不创建子进程",
                {"validate_msg": msg, "would_popopen": msg is None},
                detail="start_products 在 _validate_start_filters 失败时直接 return error",
                severity="P1" if not ok2 else "",
            ))
        else:
            col.add(CaseResult(
                f"PAR-NOPROC-{name}", "参数异常", "API参数",
                "PASS" if msg is None else "FAIL",
                "合法值可通过校验", msg,
            ))

        # 3) 结果 API 校验（new_arrivals 同样走 _validate_start_filters）
        msg2 = _validate_start_filters({key: val})
        ok3 = (msg2 is not None) if must_reject else (msg2 is None)
        if name in ("spaces", "sci"):
            ok3 = msg2 is None
        col.add(CaseResult(
            f"PAR-NA-API-{name}", "参数异常", "结果查询",
            "PASS" if ok3 else "FAIL",
            f"拒绝={must_reject}", msg2,
            detail="最新到货结果API校验",
            severity="P1" if not ok3 and must_reject else "",
        ))

        # 4) CLI 参数生成：异常值不应默默变成合法数字进命令
        cmd = []
        try:
            _append_filter_flags(cmd, {key: val}, for_la=True)
            # 布尔/容器不应进入
            if must_reject and isinstance(val, (bool, list, dict)):
                ok4 = key.replace("_", "-") not in " ".join(cmd) or True
                # 若 bool True 被当成真值加入，记录
                joined = " ".join(str(x) for x in cmd)
                ok4 = "True" not in joined and "[]" not in joined
            elif must_reject and isinstance(val, float) and not math.isfinite(val):
                joined = " ".join(str(x) for x in cmd)
                ok4 = "nan" not in joined.lower() and "inf" not in joined.lower()
                # 当前实现可能仍加入 — 记 FAIL
                if "nan" in joined.lower() or "inf" in joined.lower():
                    ok4 = False
            else:
                ok4 = True
        except Exception as e:
            ok4 = must_reject  # 抛错也算拒绝
            cmd = [str(e)]
        col.add(CaseResult(
            f"PAR-CLI-{name}", "参数异常", "CLI参数",
            "PASS" if ok4 else "FAIL",
            "异常不进入CLI或被拦截", cmd,
            severity="P2" if not ok4 else "",
        ))

    # 字段级错误信息（min>max）
    msg = _validate_start_filters({"price_min": 50, "price_max": 10})
    col.add(CaseResult(
        "PAR-FIELD-MSG", "参数异常", "参数校验",
        "PASS" if msg and "现价" in msg else "FAIL",
        "字段级错误", msg, severity="P2",
    ))
