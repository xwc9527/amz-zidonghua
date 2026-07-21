"""测试辅助：临时库、冻结时间、抓取/查询一致性、结果记录。"""
from __future__ import annotations

import os
import sqlite3
import sys
import tempfile
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tests.na_dimensions.fixtures.products import FROZEN_NOW, PRODUCTS


def frozen_datetime(now_iso: str = FROZEN_NOW):
    """冻结 datetime.now / datetime.utcnow 到固定时刻。"""
    fixed = datetime.fromisoformat(now_iso)

    class _FrozenDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            if tz is not None:
                return fixed.replace(tzinfo=tz)
            return fixed

        @classmethod
        def utcnow(cls):
            return fixed

    return patch("datetime.datetime", _FrozenDateTime)


@contextmanager
def temp_sqlite_db():
    fd, path = tempfile.mkstemp(suffix="_na_test.db")
    os.close(fd)
    try:
        yield path
    finally:
        try:
            os.remove(path)
        except OSError:
            pass


def init_categories_tree(db_path: str, site: str = "US"):
    """创建隔离类目树：L1 → L2 → L3 → L4，另加跨站干扰节点。"""
    conn = sqlite3.connect(db_path)
    conn.execute(
        """CREATE TABLE categories (
            node_id TEXT, name TEXT, depth INTEGER,
            parent_node_id TEXT, site TEXT, na_valid INTEGER DEFAULT 1,
            PRIMARY KEY (node_id, site)
        )"""
    )
    rows = [
        ("ROOT", "Root", 0, None, site, 1),
        ("L2A", "L2 Alpha", 2, "ROOT", site, 1),
        ("L3A1", "L3 Alpha-1", 3, "L2A", site, 1),
        ("L3A2", "L3 Alpha-2", 3, "L2A", site, 1),
        ("L4A11", "L4 Alpha-1-1", 4, "L3A1", site, 1),
        ("L2B", "L2 Beta", 2, "ROOT", site, 1),
        ("L3B1", "L3 Beta-1", 3, "L2B", site, 1),
        # 跨站：同 node_id 不应混入
        ("L2A", "DE L2", 2, None, "DE", 1),
        ("L3A1", "DE L3", 3, "L2A", "DE", 1),
    ]
    conn.executemany(
        "INSERT INTO categories(node_id, name, depth, parent_node_id, site, na_valid) VALUES (?,?,?,?,?,?)",
        rows,
    )
    conn.commit()
    conn.close()


def init_new_arrivals_table(db_path: str):
    conn = sqlite3.connect(db_path)
    conn.execute(
        """CREATE TABLE IF NOT EXISTS new_arrivals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            asin TEXT NOT NULL,
            title TEXT,
            price TEXT,
            price_value REAL,
            rating REAL,
            review_count INTEGER DEFAULT 0,
            listing_date TEXT,
            listing_age_days INTEGER,
            bsr_main_category TEXT,
            bsr_main_rank INTEGER,
            bsr_sub TEXT,
            bsr_sub_rank INTEGER,
            bsr_sub_category TEXT,
            image_url TEXT,
            product_url TEXT,
            node_id TEXT,
            category_name TEXT,
            category_depth INTEGER,
            site TEXT DEFAULT 'US',
            item_weight TEXT,
            item_dimensions TEXT,
            weight_lb REAL,
            dim_l_in REAL,
            dim_w_in REAL,
            dim_h_in REAL,
            variant_option_count INTEGER,
            other_sellers_count INTEGER,
            fba_fee REAL,
            placement_fee REAL,
            fulfillment_type TEXT,
            country_of_origin TEXT,
            is_amazon_choice INTEGER DEFAULT 0,
            is_bestseller INTEGER DEFAULT 0,
            scraped_at TEXT DEFAULT (datetime('now')),
            UNIQUE(asin, node_id, site)
        )"""
    )
    conn.commit()
    conn.close()


def insert_products(db_path: str, products: list[dict] | None = None, *, test_run_id: str = ""):
    products = products or list(PRODUCTS.values())
    init_new_arrivals_table(db_path)
    conn = sqlite3.connect(db_path)
    cols = [
        "asin", "title", "price", "price_value", "rating", "review_count",
        "listing_date", "bsr_main_category", "bsr_main_rank", "bsr_sub_rank",
        "bsr_sub_category", "node_id", "category_name", "category_depth", "site",
        "item_weight", "item_dimensions", "weight_lb", "dim_l_in", "dim_w_in",
        "dim_h_in", "variant_option_count", "other_sellers_count", "fba_fee",
        "fulfillment_type", "country_of_origin", "is_amazon_choice", "is_bestseller",
    ]
    for p in products:
        row = dict(p)
        if test_run_id:
            # 用 title 前缀标记测试批次，避免污染正式库时不可识别
            row["title"] = f"[TEST:{test_run_id}] {row.get('title') or ''}"
        vals = [row.get(c) for c in cols]
        ph = ",".join("?" * len(cols))
        conn.execute(
            f"INSERT OR IGNORE INTO new_arrivals ({','.join(cols)}) VALUES ({ph})",
            vals,
        )
    conn.commit()
    conn.close()


def query_asins_sqlite(db_path: str, filters: dict) -> set[str]:
    from api_server import _build_new_arrivals_where

    where, params = _build_new_arrivals_where(filters, style="sqlite")
    conn = sqlite3.connect(db_path)
    rows = conn.execute(f"SELECT asin FROM new_arrivals{where}", params).fetchall()
    conn.close()
    return {r[0] for r in rows}


def scrape_pass_asins(filters: dict) -> set[str]:
    """用抓取层筛选函数判断 A–F 哪些通过（列表+详情）。"""
    from fetch_new_arrivals import _pass_list_filters
    from detail_parser import check_detail_filters
    from tests.na_dimensions.fixtures.products import list_card, detail_fields

    list_keys = {"price_min", "price_max", "rating_min", "rating_max", "review_min", "review_max"}
    list_f = {k: v for k, v in filters.items() if k in list_keys and v}
    detail_f = {k: v for k, v in filters.items() if k not in list_keys and v}

    passed = set()
    for p in PRODUCTS.values():
        if not _pass_list_filters(list_card(p), list_f):
            continue
        if not check_detail_filters(detail_fields(p), detail_f):
            continue
        passed.add(p["asin"])
    return passed


def cutoff_days(days: int, now_iso: str = FROZEN_NOW) -> str:
    fixed = datetime.fromisoformat(now_iso)
    return (fixed - timedelta(days=days)).strftime("%Y-%m-%d")


# ── 结果收集 ─────────────────────────────────────────────────────

class CaseResult:
    __slots__ = ("case_id", "dimension", "layer", "status", "expected", "actual", "detail", "severity")

    def __init__(
        self,
        case_id: str,
        dimension: str,
        layer: str,
        status: str,
        expected: Any = None,
        actual: Any = None,
        detail: str = "",
        severity: str = "",
    ):
        self.case_id = case_id
        self.dimension = dimension
        self.layer = layer
        self.status = status  # PASS / FAIL / BLOCKED / NOT_RUN / CONDITIONAL_PASS
        self.expected = expected
        self.actual = actual
        self.detail = detail
        self.severity = severity


class SuiteCollector:
    def __init__(self, run_id: str):
        self.run_id = run_id
        self.results: list[CaseResult] = []
        self.issues: list[dict] = []
        self.meta: dict = {}

    def add(self, r: CaseResult):
        self.results.append(r)
        if r.status == "FAIL":
            self.issues.append({
                "id": f"BUG-{len(self.issues)+1:03d}",
                "case_id": r.case_id,
                "dimension": r.dimension,
                "layer": r.layer,
                "expected": r.expected,
                "actual": r.actual,
                "detail": r.detail,
                "severity": r.severity or "P2",
            })

    def counts(self) -> dict[str, int]:
        out = {"PASS": 0, "FAIL": 0, "BLOCKED": 0, "NOT_RUN": 0, "CONDITIONAL_PASS": 0}
        for r in self.results:
            out[r.status] = out.get(r.status, 0) + 1
        return out

    def by_dimension(self) -> dict[str, dict[str, str]]:
        dims: dict[str, dict[str, list[str]]] = {}
        for r in self.results:
            d = dims.setdefault(r.dimension, {})
            d.setdefault(r.layer, []).append(r.status)
        summary = {}
        for dim, layers in dims.items():
            row = {}
            for layer, sts in layers.items():
                if "FAIL" in sts:
                    row[layer] = "FAIL"
                elif "BLOCKED" in sts and "PASS" not in sts:
                    row[layer] = "BLOCKED"
                elif all(s in ("PASS", "CONDITIONAL_PASS") for s in sts):
                    row[layer] = "PASS"
                else:
                    row[layer] = "/".join(sorted(set(sts)))
            row["结论"] = "不通过" if "FAIL" in row.values() else (
                "阻塞" if "BLOCKED" in row.values() and "PASS" not in [v for k, v in row.items() if k != "结论"]
                else "通过"
            )
            summary[dim] = row
        return summary
