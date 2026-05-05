import sqlite3, sys
sys.stdout.reconfigure(encoding='utf-8')
conn = sqlite3.connect('data/categories.db')

# L4 (true_depth=4) 的 parent_node_id 都指向谁？
print("=== L4 节点的 parent_node_id 指向哪里 ===\n")

# 取 10 个 L4 样本
print("L4 样本:")
for r in conn.execute("""
    SELECT c.name, c.node_id, c.parent_node_id, c.true_depth, c.url,
    (SELECT name FROM categories WHERE node_id=c.parent_node_id LIMIT 1) as parent_name
    FROM categories c WHERE c.true_depth=4 LIMIT 10
"""):
    print(f"  {r[0]} | nid={r[1]} | parent_nid={r[2]} | parent_name={r[5]}")

# L4 的 parent_node_id 是否在 L3(true_depth=3) 的 node_id 里
print("\n=== L4 parent 是否指向 L3 ===")
l4_total = conn.execute("SELECT COUNT(*) FROM categories WHERE true_depth=4").fetchone()[0]
l4_to_l3 = conn.execute("""
    SELECT COUNT(*) FROM categories c4
    WHERE c4.true_depth=4 AND c4.parent_node_id IN 
    (SELECT node_id FROM categories WHERE true_depth=3)
""").fetchone()[0]
print(f"  L4 总数: {l4_total}")
print(f"  parent 指向 L3: {l4_to_l3}")
print(f"  parent 不指向 L3: {l4_total - l4_to_l3}")

# L3 有子节点的数量
l3_with_kids = conn.execute("""
    SELECT COUNT(DISTINCT c3.node_id) FROM categories c3
    WHERE c3.true_depth=3 AND EXISTS 
    (SELECT 1 FROM categories c4 WHERE c4.parent_node_id=c3.node_id)
""").fetchone()[0]
l3_total = conn.execute("SELECT COUNT(*) FROM categories WHERE true_depth=3").fetchone()[0]
print(f"\n  L3 总数: {l3_total}")
print(f"  L3 有子节点的: {l3_with_kids}")

# 每层 parent 指向上一层的比例
print("\n=== 各层 parent 指向正确性 ===")
for depth in range(3, 12):
    total = conn.execute("SELECT COUNT(*) FROM categories WHERE true_depth=?", (depth,)).fetchone()[0]
    if total == 0:
        continue
    linked = conn.execute("""
        SELECT COUNT(*) FROM categories c 
        WHERE c.true_depth=? AND c.parent_node_id IN 
        (SELECT node_id FROM categories WHERE true_depth=?)
    """, (depth, depth-1)).fetchone()[0]
    print(f"  L{depth} → L{depth-1}: {linked}/{total} ({100*linked/total:.0f}%)")

conn.close()
