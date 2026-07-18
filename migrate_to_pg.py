# migrate_to_pg.py — SQLite → PostgreSQL 一键迁移
import os, sys, sqlite3, re

sys.stdout.reconfigure(encoding='utf-8', errors='replace')

try:
    import psycopg2
except ImportError:
    print("正在安装 psycopg2-binary ...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "psycopg2-binary"])
    import psycopg2

from pg_config import get_pg_dsn

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "data", "categories.db")
SCHEMA_PATH = os.path.join(BASE_DIR, "pg_schema.sql")


def sanitize_ltree_label(s):
    if not s:
        return "_"
    s = re.sub(r'[^a-zA-Z0-9_]', '_', s)
    s = re.sub(r'_+', '_', s).strip('_')
    return s or "_"


def build_ltree_paths(sqlite_rows):
    by_node = {}
    for r in sqlite_rows:
        nid = r['node_id']
        if nid:
            by_node[nid] = r

    paths = {}
    def get_path(nid):
        if nid in paths:
            return paths[nid]
        row = by_node.get(nid)
        if not row:
            paths[nid] = sanitize_ltree_label(nid)
            return paths[nid]
        parent = row['parent_node_id']
        if not parent or parent not in by_node:
            paths[nid] = sanitize_ltree_label(nid)
        else:
            paths[nid] = get_path(parent) + '.' + sanitize_ltree_label(nid)
        return paths[nid]

    for nid in by_node:
        get_path(nid)
    return paths


def migrate():
    print(f"读取 SQLite: {DB_PATH}")
    sconn = sqlite3.connect(DB_PATH)
    sconn.row_factory = sqlite3.Row

    # 读类目
    cats = [dict(r) for r in sconn.execute("SELECT * FROM categories").fetchall()]
    print(f"  类目: {len(cats)} 条")

    # 读商品
    try:
        prods = [dict(r) for r in sconn.execute("SELECT * FROM product_sightings").fetchall()]
    except sqlite3.OperationalError:
        prods = []
    print(f"  商品: {len(prods)} 条")

    sconn.close()

    # 构建 ltree path
    ltree_paths = build_ltree_paths(cats)
    print(f"  ltree paths: {len(ltree_paths)} 条")

    # 连接 PG
    print(f"\n连接 PostgreSQL ...")
    conn = psycopg2.connect(get_pg_dsn())
    conn.autocommit = True
    cur = conn.cursor()

    # 建库（如果 DSN 指向的库不存在需要先创建）
    # 执行 schema
    print("执行 schema ...")
    with open(SCHEMA_PATH, 'r', encoding='utf-8') as f:
        cur.execute(f.read())

    # 清空旧数据（幂等迁移）
    # new_arrivals：按产品决策不迁 SQLite 历史，统一清空后由后续抓取重建
    cur.execute(
        "TRUNCATE categories, product_sightings, new_arrivals, run_status RESTART IDENTITY CASCADE"
    )
    cur.execute("INSERT INTO run_status(id, phase, paused) VALUES(1, 'idle', 0) ON CONFLICT(id) DO NOTHING")
    print("  已清空 new_arrivals（不迁移最新到货历史，需重新抓取）")

    # 批量插入类目（禁用触发器，手动维护 child_count）
    print("写入类目 ...")
    cur.execute("ALTER TABLE categories DISABLE TRIGGER trg_child_inc")
    cur.execute("ALTER TABLE categories DISABLE TRIGGER trg_child_dec")

    cols = ['name', 'url', 'node_id', 'depth', 'source', 'explored', 'created_at',
            'parent_node_id', 'true_depth', 'nr_valid', 'bs_valid', 'ms_valid', 'mw_valid',
            'breadcrumb_checked', 'slug', 'child_count', 'site', 'path']

    batch = []
    for c in cats:
        nid = c.get('node_id')
        lpath = ltree_paths.get(nid) if nid else None
        vals = [
            c['name'], c['url'], nid, c.get('depth', 0),
            c.get('source', 'sidebar'), c.get('explored', 0), c.get('created_at'),
            c.get('parent_node_id'), c.get('true_depth'), c.get('nr_valid'),
            c.get('bs_valid'), c.get('ms_valid'), c.get('mw_valid'),
            c.get('breadcrumb_checked', 0), c.get('slug', ''),
            c.get('child_count', 0), c.get('site') or 'US', lpath
        ]
        batch.append(vals)

    placeholders = ','.join(['%s'] * len(cols))
    col_str = ','.join(cols)
    insert_sql = f"INSERT INTO categories ({col_str}) VALUES ({placeholders})"

    CHUNK = 500
    for i in range(0, len(batch), CHUNK):
        chunk = batch[i:i+CHUNK]
        cur.executemany(insert_sql, chunk)
        print(f"  {min(i+CHUNK, len(batch))}/{len(batch)}")

    cur.execute("ALTER TABLE categories ENABLE TRIGGER trg_child_inc")
    cur.execute("ALTER TABLE categories ENABLE TRIGGER trg_child_dec")

    # 写入商品（字段与 pg_schema product_sightings 对齐，保留详情/站点/节点上下文）
    if prods:
        print("写入商品 ...")
        prod_cols = [
            'name', 'asin', 'price', 'price_raw', 'original_price', 'discount_pct',
            'review_count', 'rank', 'rating', 'image_url', 'product_url',
            'has_video', 'is_amazon_choice', 'list_type', 'list_total',
            'category_name', 'category_slug', 'category_depth', 'node_id', 'site',
            'scraped_at',
            'bsr_main_rank', 'bsr_main_category', 'bsr_sub_rank', 'bsr_sub_category',
            'variant_option_count', 'other_sellers_count', 'item_weight', 'item_dimensions',
            'date_first_available', 'shipping_fee', 'shipping_fee_value',
            'fulfillment_type', 'country_of_origin', 'is_bestseller', 'detail_scraped',
        ]
        # 仅写入 SQLite 行里实际存在的列，避免旧库缺列时报错
        available = set(prods[0].keys()) if prods else set()
        use_cols = [c for c in prod_cols if c in available]
        # site 若 SQLite 无该列，回退默认 US
        if 'site' not in use_cols:
            use_cols.append('site')
        prod_ph = ','.join(['%s'] * len(use_cols))
        prod_sql = f"INSERT INTO product_sightings ({','.join(use_cols)}) VALUES ({prod_ph})"

        prod_batch = []
        for p in prods:
            row = []
            for c in use_cols:
                if c == 'site':
                    row.append(p.get('site') or 'US')
                else:
                    row.append(p.get(c))
            prod_batch.append(row)

        for i in range(0, len(prod_batch), CHUNK):
            chunk = prod_batch[i:i+CHUNK]
            cur.executemany(prod_sql, chunk)
            print(f"  {min(i+CHUNK, len(prod_batch))}/{len(prod_batch)}")

    conn.commit()

    # 验证
    print("\n── 验证 ──")
    cur.execute("SELECT COUNT(*) FROM categories")
    pg_cats = cur.fetchone()[0]
    print(f"  PG 类目: {pg_cats} (SQLite: {len(cats)})")

    cur.execute("SELECT COUNT(*) FROM categories WHERE path IS NULL AND node_id IS NOT NULL")
    null_paths = cur.fetchone()[0]
    print(f"  path 为 NULL 但有 node_id: {null_paths}")

    cur.execute("SELECT COUNT(*) FROM product_sightings")
    pg_prods = cur.fetchone()[0]
    print(f"  PG 商品: {pg_prods} (SQLite: {len(prods)})")

    cur.execute("SELECT COUNT(*) FROM new_arrivals")
    pg_na = cur.fetchone()[0]
    print(f"  PG 最新到货: {pg_na}（策略：不迁移历史，期望 0）")
    if pg_na != 0:
        print("  警告: new_arrivals 未清空干净，请检查 TRUNCATE")

    # ltree 查询测试
    cur.execute("SELECT COUNT(*) FROM categories WHERE path IS NOT NULL")
    with_path = cur.fetchone()[0]
    print(f"  有 ltree path: {with_path}")

    # trgm 搜索测试
    # psycopg2 将 %% 转义为单个 %，即 pg_trgm 相似度运算符
    cur.execute("SELECT name FROM categories WHERE name %% %s LIMIT 3", ("kitchn",))
    trgm_hits = [r[0] for r in cur.fetchall()]
    print(f"  trgm 搜索 'kitchn': {trgm_hits}")

    cur.close()
    conn.close()

    ok = pg_cats == len(cats) and null_paths == 0
    print(f"\n{'✓ 迁移成功!' if ok else '✗ 数据不一致，请检查'}")
    return ok


if __name__ == '__main__':
    migrate()
