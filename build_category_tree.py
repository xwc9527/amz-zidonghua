"""
build_category_tree.py — 全量重建类目层级树（parent_node_id）
BFS 从 root 开始，逐页解析 #zg-left-col 侧边栏导航树，
提取完整 parent-child 关系并写入 categories.db。

运行：python -u build_category_tree.py
"""

import re
import os
import sys
import json
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
PROGRESS_INTERVAL = 20   # 每 N 页打印一次进度
CHECKPOINT_FILE = os.path.join(os.path.dirname(DB_FILE), "tree_checkpoint.json")  # 断点文件
CHECKPOINT_INTERVAL = 50  # 每 N 页保存一次断点

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
                # 只把"本身也有子列表"的子节点加入 BFS 队列（非叶子节点）
                # 叶子节点的 parent_node_id 在当前页面解析时已经提取，无需单独访问
                for child_li in sub.find_all('li', recursive=False):
                    if find_direct_sub_ul(child_li):  # 有子列表 → 非叶子，才需要继续探索
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
    返回 (本次实际更新行数, 本次成功更新的子节点 key 列表)。
    """
    if not records:
        return 0, []

    conn = get_conn()
    updated = 0
    updated_keys = []
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
                    updated_keys.append(key)
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
                    updated_keys.append(key)
            except Exception as e:
                print(f"  [DB异常] key={key} err={e}", flush=True)

        conn.commit()
    finally:
        conn.close()

    return updated, updated_keys


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 断点保存 / 加载
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def save_checkpoint(pass_num, remaining_queue, next_keys, visited,
                    total_updated, total_pages, errors):
    """将当前运行状态写入断点文件，供中断后续跑使用。"""
    state = {
        "pass_num": pass_num,
        "remaining_queue": remaining_queue,
        "next_keys": next_keys,
        "visited": list(visited),
        "total_updated": total_updated,
        "total_pages": total_pages,
        "errors": errors,
    }
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False)


def load_checkpoint():
    """尝试从断点文件恢复运行状态，文件不存在则返回 None。"""
    if not os.path.exists(CHECKPOINT_FILE):
        return None
    try:
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 多轮次 BFS 辅助函数
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def _get_l1_urls():
    """从数据库取所有 L1 根类目（depth=1 且 node_id IS NULL）的 URL 列表。"""
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT url FROM categories WHERE depth=1 AND node_id IS NULL"
        ).fetchall()
        return [norm_for_fetch(r["url"]) for r in rows]
    finally:
        conn.close()


def _get_urls_for_keys(keys):
    """
    根据子节点 key（node_id 数字串）从数据库查询对应 URL。
    过滤掉 depth >= 4 的叶子节点（无需继续探索）。
    返回去重后的 URL 列表。
    """
    if not keys:
        return []
    conn = get_conn()
    result = []
    seen = set()
    try:
        for key in set(keys):
            if not key or not key.isdigit():
                continue  # slug 是 L1 根，不需要再入队
            rows = conn.execute(
                "SELECT url, depth FROM categories WHERE node_id = ?", (key,)
            ).fetchall()
            for row in rows:
                url = norm_for_fetch(row["url"])
                if url in seen:
                    continue
                if row["depth"] is not None and row["depth"] >= 4:
                    continue  # 叶子节点，跳过
                seen.add(url)
                result.append(url)
    finally:
        conn.close()
    return result


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 主流程：BFS 重建层级树
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def run():
    print("=" * 60)
    print("  build_category_tree.py — 多轮次数据库驱动 BFS")
    print("=" * 60)
    print(f"[配置] 数据库: {DB_FILE}")
    print(f"[配置] 根 URL: {ROOT_URL}")
    print(f"[配置] 请求延迟: {DELAY_MIN}-{DELAY_MAX}s")
    print()

    # ── 断点感知启动：检测断点文件决定是续跑还是重头开始 ──
    checkpoint = load_checkpoint()
    if checkpoint:
        print(f"[断点] 发现断点文件: Pass{checkpoint['pass_num']} "
              f"剩余{len(checkpoint['remaining_queue'])}页未访问", flush=True)
        resume = "--resume" in sys.argv
        if not resume:
            ans = input("是否从断点续跑？[y/N] ").strip().lower()
            resume = (ans == "y")
        if resume:
            print("[断点] 从断点续跑，跳过清空 DB", flush=True)
            pass_num      = checkpoint["pass_num"]
            current_queue = checkpoint["remaining_queue"]
            next_keys_carry = checkpoint["next_keys"]
            visited       = set(checkpoint["visited"])
            total_updated = checkpoint["total_updated"]
            total_pages   = checkpoint["total_pages"]
            errors        = checkpoint["errors"]
        else:
            print("[断点] 用户选择重头开始，清空 DB 及旧断点", flush=True)
            clear_all_parent_ids()
            os.remove(CHECKPOINT_FILE)
            pass_num = 0
            current_queue = _get_l1_urls()
            next_keys_carry = []
            visited = set()
            total_updated = total_pages = errors = 0
    else:
        clear_all_parent_ids()
        pass_num = 0
        current_queue = _get_l1_urls()
        next_keys_carry = []
        visited = set()
        total_updated = total_pages = errors = 0

    print(f"\n[启动] Pass{pass_num+1} 种子: {len(current_queue)} 个 URL", flush=True)
    t0 = time.time()

    while current_queue:
        pass_num += 1
        pass_updated = 0
        pass_pages = 0
        # 续跑时第一轮保留断点中已收集的 key，之后各轮从空列表开始
        next_keys = next_keys_carry if pass_num == checkpoint.get("pass_num") else [] if checkpoint else []
        next_keys_carry = []  # 后续轮不再携带

        print(f"\n[Pass {pass_num}] 开始，本轮队列: {len(current_queue)} 个 URL", flush=True)

        for url in current_queue:
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
                    visited.discard(nk)  # 放回重试
                delay()
                continue

            # 解析侧边栏
            try:
                records, _ = parse_nav(r.text)
            except Exception as e:
                errors += 1
                print(f"  [解析异常] {url}: {e}", flush=True)
                delay()
                continue

            # 更新 DB，收集本次新建立关系的子节点 key
            if records:
                cnt, keys = update_parent_ids(records)
                pass_updated += cnt
                total_updated += cnt
                next_keys.extend(keys)

            pass_pages += 1
            total_pages += 1

            if pass_pages % PROGRESS_INTERVAL == 0:
                elapsed = time.time() - t0
                print(
                    f"  [Pass {pass_num} | {pass_pages}/{len(current_queue)}页 | {elapsed:.0f}s] "
                    f"本轮新增:{pass_updated} 累计:{total_updated} 异常:{errors}",
                    flush=True
                )

            # 每 CHECKPOINT_INTERVAL 页保存一次断点
            if total_pages % CHECKPOINT_INTERVAL == 0:
                remaining = current_queue[pass_pages:]  # 本轮尚未访问的 URL
                save_checkpoint(pass_num, remaining, next_keys,
                                visited, total_updated, total_pages, errors)
                print(f"  [断点] 已保存 (总页:{total_pages})", flush=True)

            delay()

        elapsed_pass = time.time() - t0
        print(
            f"\n[Pass {pass_num} 完成] 访问:{pass_pages}页 本轮新增关系:{pass_updated} "
            f"累计:{total_updated} 异常:{errors} 总耗时:{elapsed_pass:.0f}s",
            flush=True
        )

        if pass_updated == 0:
            print("[终止] 本轮无新增关系，BFS 结束。", flush=True)
            break

        # 构建下一轮队列：从本轮新建关系的子节点 key 查 DB 取 URL，过滤叶子节点
        next_urls = _get_urls_for_keys(next_keys)
        current_queue = [u for u in next_urls if norm(u) not in visited]
        print(f"[Pass {pass_num+1} 种子] 下一轮待访问 URL: {len(current_queue)} 个", flush=True)

    # ── 统计报告 ──
    elapsed = time.time() - t0
    print()
    print("=" * 60)
    print(f"[完成] 共访问 {total_pages} 页, 共 {pass_num} 轮次, 耗时 {elapsed:.0f}s")
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

        print("\n  parent_node_id 覆盖分布:")
        for row in conn.execute(
            "SELECT depth, "
            "COUNT(*) as total, "
            "SUM(CASE WHEN parent_node_id IS NOT NULL THEN 1 ELSE 0 END) as filled "
            "FROM categories GROUP BY depth ORDER BY depth"
        ):
            d, t, f = row["depth"], row["total"], row["filled"]
            pct = 100 * f / t if t else 0
            print(f"    depth={d}: {f}/{t} ({pct:.1f}%)")

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

    # 正常完成：删除断点文件
    if os.path.exists(CHECKPOINT_FILE):
        os.remove(CHECKPOINT_FILE)
        print("[断点] 任务完成，断点文件已清除。", flush=True)


if __name__ == "__main__":
    run()
