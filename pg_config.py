import os
from urllib.parse import urlparse, unquote


_FORMAL_PG_DB_NAMES = frozenset({
    "postgres",
    "template0",
    "template1",
    "amz_selection",
})


def _is_testing() -> bool:
    flag = (os.environ.get("TESTING") or "").strip().lower()
    return flag in ("1", "true", "yes", "on")


def _parse_db_name(dsn: str) -> str:
    """从 DSN 解析数据库名；支持 URI 与 key=value 形式。"""
    raw = (dsn or "").strip()
    if not raw:
        return ""
    if "://" in raw:
        parsed = urlparse(raw)
        name = (parsed.path or "").lstrip("/")
        if name:
            return unquote(name.split("/")[0])
    # key=value 形式：dbname=foo
    for part in raw.replace(";", " ").split():
        if "=" not in part:
            continue
        k, v = part.split("=", 1)
        if k.strip().lower() in ("dbname", "database"):
            return v.strip().strip("'\"")
    return ""


def assert_test_database_name(dsn: str) -> str:
    name = _parse_db_name(dsn)
    if not name:
        raise RuntimeError("无法从 DSN 解析数据库名")
    allow = {
        x.strip().lower()
        for x in (os.environ.get("PG_TEST_DB_ALLOWLIST") or "").split(",")
        if x.strip()
    }
    lower = name.lower()
    if lower in _FORMAL_PG_DB_NAMES:
        raise RuntimeError(f"TESTING=1 拒绝正式/系统库名: {name}")
    if lower.endswith("_test") or lower in allow:
        return name
    raise RuntimeError(
        f"TESTING=1 仅允许 *_test 数据库或 PG_TEST_DB_ALLOWLIST；当前={name}"
    )


def get_pg_dsn() -> str:
    """读取 PG 连接串。

    TESTING=1：必须使用 PG_TEST_DSN，且不得等于正式 PG_DSN，库名须为测试库。
    正式环境：读取 PG_DSN。
    """
    if _is_testing():
        dsn = os.getenv("PG_TEST_DSN", "").strip()
        if not dsn:
            raise RuntimeError(
                "TESTING=1 必须设置 PG_TEST_DSN。"
                "示例: set PG_TEST_DSN=postgresql://user:pass@localhost:5432/amz_selection_test"
            )
        formal = os.getenv("PG_DSN", "").strip()
        if formal and dsn == formal:
            raise RuntimeError("PG_TEST_DSN 不得等于正式 PG_DSN")
        assert_test_database_name(dsn)
        return dsn

    dsn = os.getenv("PG_DSN", "").strip()
    if not dsn:
        raise RuntimeError(
            "未设置 PG_DSN 环境变量。示例:\n"
            "  set PG_DSN=postgresql://user:password@localhost:5432/amz_selection"
        )
    return dsn


# 兼容旧 import；值为 None 表示未配置（SQLite 模式可忽略）
PG_DSN = os.getenv("PG_DSN") or None
