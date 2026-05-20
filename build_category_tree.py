"""
build_category_tree.py — 全量重建类目层级树（parent_node_id）
BFS 从 root 开始，逐页解析 #zg-left-col 侧边栏导航树，
提取完整 parent-child 关系并写入 categories.db。

运行：python -u build_category_tree.py
"""

import re
import sys
import time
import random
import sqlite3
from collections import deque

import requests
from bs4 import BeautifulSoup

# Windows 控制台默认 GBK，强制 UTF-8 避免中文/特殊字符崩溃
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

from config import HEADERS, DB_FILE

# ── 常量 ──────────────────────────────────────────────────────────
ROOT_URL = "https://www.amazon.com/gp/new-releases/"
DELAY_MIN = 1.5   # 请求间隔下限（秒）
DELAY_MAX = 2.5   # 请求间隔上限（秒）
PROGRESS_INTERVAL = 20  # 每 N 页打印一次进度

# ── 全局 Session：复用 TCP+TLS 连接 ──────────────────────────────
session = requests.Session()
session.headers.update(HEADERS)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 工具函数
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def norm(url):
    """URL 标准化：去查询串、ref=、尾斜杠，统一小写（仅用于去重）"""
    url = url.split('?')[0].split('/ref=')[0]
    url = url.rstrip('/')
    return url.lower()


def norm_for_fetch(url):
    """实际发请求用的 URL：保留大小写，确保尾斜杠"""
    url = url.split('?')[0].split('/ref=')[0]
    if not url.endswith('/'):
        url += '/'
    return url


def delay():
    """请求后随机等待 1.5-2.5 秒"""
    time.sleep(random.uniform(DELAY_MIN, DELAY_MAX))


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 解析侧边栏导航树
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def find_direct_sub_ul(li):
    """
    找 li 下的直接子 ul，允许穿透一层 span.a-list-item，不再深入。
    结构: <li> -> <span.a-list-item> -> <ul>
          或: <li> -> <ul>
    """
    for child in li.children:
        if hasattr(child, 'name') and child.name == 'ul':
            return child
    for child in li.children:
        if hasattr(child, 'name') and child.name == 'span':
            for grandchild in child.children:
                if hasattr(grandchild, 'name') and grandchild.name == 'ul':
                    return grandchild
    return None


def parse_nav(html):
    """
    解析 #zg-left-col 导航树，提取 (key, name, parent_key, depth, url) 记录
    以及需要继续 BFS 探索的子 URL 列表。
    """
    soup = BeautifulSoup(html, 'html.parser')

    # 优先 #zg-left-col，回退到 ul[class*='zg-browse-root']
    left = soup.select_one('#zg-left-col')
    if not left:
        root_ul = soup.select_one("ul[class*='zg-browse-root']")
        if not root_ul:
            return [], []
        left = root_ul

    results = []       # (key, name, parent_key, depth, url)
    child_urls = []    # 需要继续 BFS 的 URL
    seen_keys = set()  # 去重：同一页面同一 key 只记录一次

    def walk(el, parent_key, depth):
        for li in el.find_all('li', recursive=False):
            # 跳过 zg-browse-up（回退导航链接）
            li_classes = ' '.join(li.get('class', []))
            if 'browse-up' in li_classes:
                continue

            a = li.find('a')
            sub = find_direct_sub_ul(li)

            if not a:
                # 没有 <a>（可能是选中的当前节点，用 span 显示）-> 递归子 ul
                if sub:
                    walk(sub, parent_key, depth)
                continue

            name = a.get_text(strip=True)
            href = a.get('href', '')

            # 跳过无效名称
            if not name or name.isdigit() or name in ('Any Department', 'See More'):
                if sub:
                    walk(sub, parent_key, depth)
                continue

            # 提取 key：优先数字 node_id，否则 slug
            m_nid = re.search(r'/(\d+)', href)
            m_slug = re.search(
                r'/gp/new-releases/([a-z][a-z0-9-]+)/?$',
                href.split('?')[0].split('/ref=')[0]
            )
            nid = m_nid.group(1) if m_nid else None
            slug = m_slug.group(1) if m_slug else None
            key = nid or slug

            # 构造完整 URL
            full = href if href.startswith('http') else 'https://www.amazon.com' + href
            full = norm_for_fetch(full)

            # 去重
            if key and key not in seen_keys:
                seen_keys.add(key)
                results.append((key, name, parent_key, depth, full))

            if sub:
                # 递归解析子树
                walk(sub, key or parent_key, depth + 1)
                # 把直接子页面加入 BFS 队列
                for child_li in sub.find_all('li', recursive=False):
                    ca = child_li.find('a')
                    if ca:
                        ch = ca.get('href', '')
                        if ch and '/gp/new-releases/' in ch:
                            cf = ch if ch.startswith('http') else 'https://www.amazon.com' + ch
                            cu = norm_for_fetch(cf)
                            if cu not in child_urls:
                                child_urls.append(cu)

    top = left.find('ul')
    if top:
        walk(top, None, 1)

    return results, child_urls


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 数据库操作
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def get_conn():
    """获取 SQLite 连接，开启 WAL 模式"""
    conn = sqlite3.connect(DB_FILE, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.row_factory = sqlite3.Row
    return conn


def clear_all_parent_ids():
    """全量清空 parent_node_id，为完整重建做准备"""
    conn = get_conn()
    try:
        affected = conn.execute(
            "UPDATE categories SET parent_node_id = NULL WHERE parent_node_id IS NOT NULL"
        ).rowcount
        conn.commit()
        print(f"[清空] 已将 {affected} 条记录的 parent_node_id 设为 NULL", flush=True)
    finally:
        conn.close()


def update_parent_ids(records):
    """
    批量更新 parent_node_id（仅更新当前为 NULL 的行，first-come-first-served 去重）。
    records: [(key, name, parent_key, depth, url), ...]
    返回本次实际更新的行数。
    """
    if not records:
        return 0

    conn = get_conn()
    updated = 0
    try:
        for (key, name, parent_key, depth, url) in records:
            if not key or not parent_key:
                # 没有 key 或没有 parent_key（顶层节点），跳过
                continue
            try:
                # 按 node_id 精确匹配，仅更新 parent_node_id 为 NULL 的行
                c = conn.execute(
                    "UPDATE categories SET parent_node_id = ? "
                    "WHERE node_id = ? AND parent_node_id IS NULL",
                    (parent_key, key)
                )
                if c.rowcount > 0:
                    updated += c.rowcount
                    continue

                # 回退：按 URL 匹配（兼容有/无尾斜杠）
                url_bare = url.rstrip('/')
                url_slash = url_bare + '/'
                c = conn.execute(
                    "UPDATE categories SET parent_node_id = ? "
                    "WHERE url IN (?, ?) AND parent_node_id IS NULL",
                    (parent_key, url_bare, url_slash)
                )
                if c.rowcount > 0:
                    updated += c.rowcount
            except Exception as e:
                print(f"  [DB异常] key={key} err={e}", flush=True)

        conn.commit()
    finally:
        conn.close()

    return updated


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 主流程：BFS 重建层级树
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def run():
    print("=" * 60)
    print("  build_category_tree.py — 全量重建类目层级")
    print("=" * 60)
    print(f"[配置] 数据库: {DB_FILE}")
    print(f"[配置] 根 URL: {ROOT_URL}")
    print(f"[配置] 请求延迟: {DELAY_MIN}-{DELAY_MAX}s")
    print()

    # ── 第0步：全量清空 parent_node_id ──
    clear_all_parent_ids()

    # ── 第1步：访问根页面 ──
    queue = deque()
    visited = set()
    total_updated = 0
    pages = 0
    errors = 0

    print(f"\n[开始] 访问根页面 ...", flush=True)
    try:
        r = session.get(ROOT_URL, timeout=15)
        r.raise_for_status()
    except Exception as e:
        print(f"[致命] 无法访问根页面: {e}", flush=True)
        return

    records, child_urls = parse_nav(r.text)
    updated = update_parent_ids(records)
    total_updated += updated
    pages += 1
    visited.add(norm(ROOT_URL))

    print(f"  发现 L1 类目: {len(records)} 个, 更新 parent_node_id: {updated} 条", flush=True)

    # 把所有 L1 页面和发现的子 URL 加入队列
    all_seed_urls = [rec[4] for rec in records] + child_urls
    for url in all_seed_urls:
        nk = norm(url)
        if nk not in visited:
            queue.append(url)

    # ── 第2步：BFS 遍历 ──
    print(f"\n[BFS] 开始层级扫描，初始队列: {len(queue)} ...", flush=True)
    t0 = time.time()

    while queue:
        url = queue.popleft()
        nk = norm(url)
        if nk in visited:
            continue
        visited.add(nk)

        # 发请求
        try:
            r = session.get(url, timeout=15)
        except Exception as e:
            errors += 1
            print(f"  [网络异常] {e}", flush=True)
            delay()
            continue

        if r.status_code != 200:
            errors += 1
            if r.status_code in (429, 503):
                wait = 30 + random.uniform(0, 15)
                print(f"  [HTTP {r.status_code}] 等待 {wait:.0f}s ...", flush=True)
                time.sleep(wait)
                # 把 URL 放回队列重试
                visited.discard(nk)
                queue.append(url)
            delay()
            continue

        # 解析侧边栏
        try:
            records, child_urls = parse_nav(r.text)
        except Exception as e:
            errors += 1
            print(f"  [解析异常] {url}: {e}", flush=True)
            delay()
            continue

        # 更新 DB
        if records:
            updated = update_parent_ids(records)
            total_updated += updated

        # 把新发现的子 URL 加入队列
        for cu in child_urls:
            cnk = norm(cu)
            if cnk not in visited:
                queue.append(cu)

        pages += 1

        # 定期打印进度
        if pages % PROGRESS_INTERVAL == 0:
            elapsed = time.time() - t0
            print(
                f"  [{pages}页 | {elapsed:.0f}s] "
                f"队列:{len(queue)} 已访问:{len(visited)} "
                f"已更新:{total_updated} 异常:{errors}",
                flush=True
            )

        delay()

    # ── 第3步：统计报告 ──
    elapsed = time.time() - t0
    print()
    print("=" * 60)
    print(f"[完成] 共访问 {pages} 页, 耗时 {elapsed:.0f}s")
    print(f"[完成] 已更新 parent_node_id: {total_updated} 条")
    print(f"[完成] 网络/解析异常: {errors} 次")
    print("=" * 60)

    # 覆盖率统计
    conn = get_conn()
    try:
        total_rows = conn.execute("SELECT COUNT(*) FROM categories").fetchone()[0]
        has_parent = conn.execute(
            "SELECT COUNT(*) FROM categories WHERE parent_node_id IS NOT NULL"
        ).fetchone()[0]
        no_parent = total_rows - has_parent

        print(f"\n  总节点数: {total_rows}")
        print(f"  已设 parent_node_id: {has_parent} ({100*has_parent/total_rows:.1f}%)" if total_rows else "")
        print(f"  未设 parent_node_id: {no_parent}")

        # 按 depth 分布
        print("\n  parent_node_id 覆盖分布:")
        for row in conn.execute(
            "SELECT depth, "
            "COUNT(*) as total, "
            "SUM(CASE WHEN parent_node_id IS NOT NULL THEN 1 ELSE 0 END) as filled "
            "FROM categories GROUP BY depth ORDER BY depth"
        ):
            d, t, f = row['depth'], row['total'], row['filled']
            pct = 100 * f / t if t else 0
            print(f"    depth={d}: {f}/{t} ({pct:.1f}%)")

        # 未覆盖样本
        if no_parent > 0:
            print(f"\n  未覆盖样本（前10条）:")
            for row in conn.execute(
                "SELECT name, url, node_id, depth FROM categories "
                "WHERE parent_node_id IS NULL LIMIT 10"
            ):
                print(f"    [{row['depth']}] {row['name']} | node_id={row['node_id']}")
    finally:
        conn.close()

    print(f"\n[结束] 数据库: {DB_FILE}")


if __name__ == '__main__':
    run()
