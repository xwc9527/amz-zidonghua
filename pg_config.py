import os

def get_pg_dsn() -> str:
    """读取 PG 连接串；必须由环境变量提供，不再内置默认密码。"""
    dsn = os.getenv("PG_DSN", "").strip()
    if not dsn:
        raise RuntimeError(
            "未设置 PG_DSN 环境变量。示例:\n"
            "  set PG_DSN=postgresql://user:password@localhost:5432/amz_selection"
        )
    return dsn


# 兼容旧 import；值为 None 表示未配置（SQLite 模式可忽略）
PG_DSN = os.getenv("PG_DSN") or None
