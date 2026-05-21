"""
fix_l1_ids.py - 修复 L1 节点的 node_id 和 parent_node_id
问题：L1 节点 node_id=NULL，parent_node_id 存的是自身 slug
"""
import sqlite3, sys, re
sys.stdout.reconfigure(encoding='utf-8')

DB = 'data/categories.db'
conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row

# ── 1. 给 node_id 为 NULL 的行提取 slug 作为 node_id ──
rows = conn.execute("SELECT id, url FROM categories WHERE node_id IS NULL").fetchall()
print(f"node_id IS NULL: {len(rows)} 条")
fixed_nid = 0
for r in rows:
    url = r['url'] or ''
    # 从 URL 提取 slug: /gp/new-releases/automotive/ → automotive
    m = re.search(r'/gp/(?:new-releases|bestsellers|most-wished-for)/([a-z][a-z0-9-]+?)/?$', url.rstrip('/').lower())
    if m:
        slug = m.group(1)
        conn.execute("UPDATE categories SET node_id=? WHERE id=?", (slug, r['id']))
        fixed_nid += 1
print(f"  补充 node_id(slug): {fixed_nid}")
conn.commit()

# ── 2. 去重：同一 node_id 只保留一条 ──
dupes = conn.execute("""
    SELECT node_id, COUNT(*) as cnt FROM categories 
    WHERE node_id IS NOT NULL 
    GROUP BY node_id HAVING cnt > 1
""").fetchall()
print(f"\n重复 node_id: {len(dupes)} 组")
deleted = 0
for d in dupes:
    nid = d['node_id']
    # 保留 true_depth 最小的那条（最高层级），其余删除
    keep = conn.execute(
        "SELECT id FROM categories WHERE node_id=? ORDER BY COALESCE(true_depth,999), id LIMIT 1",
        (nid,)
    ).fetchone()
    if keep:
        c = conn.execute("DELETE FROM categories WHERE node_id=? AND id!=?", (nid, keep['id']))
        deleted += c.rowcount
print(f"  删除重复: {deleted} 条")
conn.commit()

# ── 3. 修复 L1 的 parent_node_id ──
# L1 (true_depth=2) 的 parent 应该是 NULL（它们是顶级）
c = conn.execute("UPDATE categories SET parent_node_id=NULL WHERE true_depth=2")
print(f"\nL1 parent_node_id 置 NULL: {c.rowcount} 条")

# ── 4. 修复 L2 的 parent_node_id ──
# L2 (true_depth=3) 的 parent 应该指向 L1 的 node_id(slug)
# 通过 URL 前缀匹配：L2 URL = /gp/new-releases/{slug}/{node_id}/
# 其中 {slug} 就是 L1 的 node_id
l2_rows = conn.execute("SELECT id, url, parent_node_id FROM categories WHERE true_depth=3").fetchall()
fixed_l2 = 0
for r in l2_rows:
    url = r['url'] or ''
    m = re.search(r'/gp/(?:new-releases|bestsellers|most-wished-for)/([a-z][a-z0-9-]+?)/', url)
    if m:
        slug = m.group(1)
        # 确认 slug 是一个有效的 L1 node_id
        exists = conn.execute("SELECT 1 FROM categories WHERE node_id=? AND true_depth=2", (slug,)).fetchone()
        if exists and r['parent_node_id'] != slug:
            conn.execute("UPDATE categories SET parent_node_id=? WHERE id=?", (slug, r['id']))
            fixed_l2 += 1
print(f"L2 parent 修复: {fixed_l2} 条")
conn.commit()

# ── 5. 最终统计 ──
total = conn.execute("SELECT COUNT(*) FROM categories").fetchone()[0]
print(f"\n=== 最终 ===")
print(f"总节点: {total}")

print("\ntrue_depth 分布:")
for r in conn.execute("SELECT true_depth, COUNT(*) FROM categories GROUP BY true_depth ORDER BY true_depth"):
    print(f"  L{r[0]}: {r[1]}")

print("\nL1 (true_depth=2) 样本:")
for r in conn.execute("SELECT name, node_id, parent_node_id FROM categories WHERE true_depth=2 LIMIT 5"):
    print(f"  {r['name']} | nid={r['node_id']} | parent={r['parent_node_id']}")

print("\nL2 (true_depth=3) parent 统计:")
l2_with_parent = conn.execute("SELECT COUNT(*) FROM categories WHERE true_depth=3 AND parent_node_id IS NOT NULL").fetchone()[0]
l2_total = conn.execute("SELECT COUNT(*) FROM categories WHERE true_depth=3").fetchone()[0]
print(f"  {l2_with_parent}/{l2_total} 有 parent")

# 验证子节点数
print("\nL1 子节点数:")
for r in conn.execute("""
    SELECT c.name, c.node_id, 
    (SELECT COUNT(*) FROM categories c2 WHERE c2.parent_node_id=c.node_id) as cc
    FROM categories c WHERE c.true_depth=2 ORDER BY c.name LIMIT 10
"""):
    print(f"  {r[0]} ({r[1]}): {r[2]} 子节点")

conn.close()
