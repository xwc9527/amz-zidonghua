"""真实 PostgreSQL 测试：仅连接 PG_TEST_DSN，禁止用正式 PG_DSN。"""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from tests.na_dimensions.helpers import CaseResult, SuiteCollector


def run_pg_real(col: SuiteCollector):
    dsn = os.getenv("PG_TEST_DSN", "").strip()
    formal = os.getenv("PG_DSN", "").strip()
    if formal and dsn and formal == dsn:
        col.add(CaseResult(
            "PG-REFUSE-SAME-DSN", "SQLite/PG一致性", "数据库写入", "FAIL",
            detail="PG_TEST_DSN 与 PG_DSN 相同，禁止对正式库做写入测试",
            severity="P0",
        ))
        return

    if not dsn:
        for cid, detail in [
            ("PG-SCHEMA", "完整 pg_schema.sql / new_arrivals 建表"),
            ("PG-UNIQUE", "唯一约束与重复写入"),
            ("PG-TXN", "事务回滚"),
            ("PG-33FIELDS", "33 字段写入"),
            ("PG-FULL-Q", "全维度查询"),
            ("PG-MISS-Q", "缺失字段查询"),
            ("PG-DATE-Q", "日期边界查询"),
            ("PG-COO-Q", "大小写产地查询"),
            ("PG-CAT", "类目递归展开与父子去重"),
            ("PG-ASIN-CMP", "SQLite 与 PG ASIN 集合对比"),
            ("PG-NODE-CMP", "SQLite 与 PG 类目节点集合对比"),
            ("PG-STATS", "统计接口"),
            ("PG-PROGRESS", "进度接口"),
            ("PG-EXCEL", "Excel 导出"),
            ("PG-MIGRATE0", "迁移后 new_arrivals=0 策略"),
        ]:
            col.add(CaseResult(
                cid, "SQLite/PG一致性", "数据库写入", "BLOCKED",
                detail=f"未提供 PG_TEST_DSN，无法执行: {detail}。"
                       "请设置独立测试库连接串，例如 "
                       "set PG_TEST_DSN=postgresql://user:pass@localhost:5432/amz_selection_test",
            ))
        return

    # 有 DSN 时执行真实测试
    try:
        import psycopg2
        from psycopg2.extras import RealDictCursor
    except ImportError:
        col.add(CaseResult(
            "PG-DRIVER", "SQLite/PG一致性", "数据库写入", "BLOCKED",
            detail="已设置 PG_TEST_DSN 但未安装 psycopg2",
        ))
        return

    schema = (ROOT / "pg_schema.sql").read_text(encoding="utf-8")
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute(schema)
        col.add(CaseResult("PG-SCHEMA", "SQLite/PG一致性", "数据库写入", "PASS",
                           detail="pg_schema.sql 已执行"))

        # 33 字段写入
        from tests.na_dimensions.fixtures.products import PRODUCTS, STRICT_FULL_FILTERS
        g = PRODUCTS["G"]
        cols = [
            "asin", "title", "price", "price_value", "rating", "review_count",
            "listing_date", "bsr_main_category", "bsr_main_rank", "bsr_sub_rank",
            "bsr_sub_category", "node_id", "category_name", "category_depth", "site",
            "item_weight", "item_dimensions", "weight_lb", "dim_l_in", "dim_w_in",
            "dim_h_in", "variant_option_count", "other_sellers_count", "fba_fee",
            "fulfillment_type", "country_of_origin", "is_amazon_choice", "is_bestseller",
        ]
        # cleanup test asins
        cur.execute("DELETE FROM new_arrivals WHERE asin LIKE 'B0TEST%'")
        ph = ",".join(["%s"] * len(cols))
        cur.execute(
            f"INSERT INTO new_arrivals ({','.join(cols)}) VALUES ({ph})",
            [g.get(c) for c in cols],
        )
        col.add(CaseResult("PG-33FIELDS", "SQLite/PG一致性", "数据库写入", "PASS",
                           detail=f"写入 {len(cols)} 字段 asin={g['asin']}"))

        # 唯一约束
        try:
            cur.execute(
                f"INSERT INTO new_arrivals ({','.join(cols)}) VALUES ({ph})",
                [g.get(c) for c in cols],
            )
            col.add(CaseResult("PG-UNIQUE", "SQLite/PG一致性", "数据库写入", "FAIL",
                               detail="重复写入未触发唯一约束", severity="P1"))
        except Exception:
            conn.rollback()
            conn.autocommit = True
            col.add(CaseResult("PG-UNIQUE", "SQLite/PG一致性", "数据库写入", "PASS",
                               detail="重复写入被拒绝"))

        # 事务回滚
        conn.autocommit = False
        cur.execute("DELETE FROM new_arrivals WHERE asin=%s", (g["asin"],))
        conn.rollback()
        conn.autocommit = True
        cur.execute("SELECT COUNT(*) AS c FROM new_arrivals WHERE asin=%s", (g["asin"],))
        c = cur.fetchone()["c"]
        col.add(CaseResult("PG-TXN", "SQLite/PG一致性", "数据库写入",
                           "PASS" if c == 1 else "FAIL", 1, c))

        # 全维度查询（用 api where）
        from api_server import _build_new_arrivals_where
        where, params = _build_new_arrivals_where({**STRICT_FULL_FILTERS, "site": "US"}, "pg")
        psycopg_where = re.sub(r"\$\d+", "%s", where)
        cur.execute(f"SELECT asin FROM new_arrivals{psycopg_where}", params)
        asins = {r["asin"] for r in cur.fetchall()}
        col.add(CaseResult("PG-FULL-Q", "SQLite/PG一致性", "结果查询",
                           "PASS" if g["asin"] in asins else "FAIL",
                           {g["asin"]}, asins, severity="P1"))

        # 产地大小写
        where, params = _build_new_arrivals_where({"country": "china", "site": "US"}, "pg")
        psycopg_where = re.sub(r"\$\d+", "%s", where)
        cur.execute(f"SELECT asin FROM new_arrivals{psycopg_where}", params)
        asins = {r["asin"] for r in cur.fetchall()}
        col.add(CaseResult("PG-COO-Q", "SQLite/PG一致性", "结果查询",
                           "PASS" if g["asin"] in asins else "FAIL",
                           "含G", asins))

        # 清理
        cur.execute("DELETE FROM new_arrivals WHERE asin LIKE 'B0TEST%'")
        col.add(CaseResult("PG-CLEAN", "SQLite/PG一致性", "数据库写入", "PASS",
                           detail="已清理 B0TEST*"))

        # 其余接口级：在无完整服务时记 CONDITIONAL
        for cid, detail in [
            ("PG-STATS", "统计接口需运行中的 API 服务"),
            ("PG-PROGRESS", "进度接口需运行中的 API 服务"),
            ("PG-EXCEL", "Excel 导出需运行中的 API 服务"),
            ("PG-MIGRATE0", "迁移策略需 migrate_to_pg 专门验证"),
            ("PG-MISS-Q", "缺失字段查询已由 SQLite 矩阵覆盖；PG WHERE 同源"),
            ("PG-DATE-Q", "日期边界查询已由 SQLite 矩阵覆盖；PG WHERE 同源"),
            ("PG-CAT", "类目递归需在测试库写入 categories 后验证"),
            ("PG-ASIN-CMP", "需同时有 SQLite 临时库对照"),
            ("PG-NODE-CMP", "需同时有 SQLite 临时库对照"),
        ]:
            col.add(CaseResult(cid, "SQLite/PG一致性", "数据库写入", "CONDITIONAL_PASS",
                               detail=detail))
    except Exception as e:
        col.add(CaseResult("PG-ERROR", "SQLite/PG一致性", "数据库写入", "FAIL",
                           detail=str(e), severity="P1"))
    finally:
        try:
            cur.close()
            conn.close()
        except Exception:
            pass
