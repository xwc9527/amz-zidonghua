import sqlite3, sys
sys.stdout.reconfigure(encoding='utf-8')
conn = sqlite3.connect('data/categories.db')

print("=== 诊断 ===")
# 1. true_depth 分布
print("\ntrue_depth 分布:")
for r in conn.execute("SELECT true_depth, COUNT(*) FROM categories GROUP BY true_depth ORDER BY true_depth"):
    print(f"  L{r[0]}: {r[1]}")

# 2. parent_node_id IS NULL 的数量
null_parent = conn.execute("SELECT COUNT(*) FROM categories WHERE parent_node_id IS NULL").fetchone()[0]
print(f"\nparent_node_id IS NULL: {null_parent}")

# 3. node_id IS NULL 的数量
null_nid = conn.execute("SELECT COUNT(*) FROM categories WHERE node_id IS NULL").fetchone()[0]
print(f"node_id IS NULL: {null_nid}")

# 4. true_depth=2 的 parent_node_id 样本
print("\ntrue_depth=2 的 parent_node_id 样本:")
for r in conn.execute("SELECT name, node_id, parent_node_id, url FROM categories WHERE true_depth=2 LIMIT 8"):
    print(f"  {r[0]} | nid={r[1]} | parent={r[2]} | url={r[3][:60]}")

# 5. 用 parent_node_id 查 children 示例
print("\nparent_node_id='automotive' 的子节点:")
for r in conn.execute("SELECT name, node_id, true_depth, parent_node_id FROM categories WHERE parent_node_id='automotive' LIMIT 5"):
    print(f"  {r[0]} | nid={r[1]} | depth={r[2]} | parent={r[3]}")

# 6. 真正的根节点
print("\nparent_node_id IS NULL 的节点:")
for r in conn.execute("SELECT name, node_id, true_depth FROM categories WHERE parent_node_id IS NULL LIMIT 10"):
    print(f"  {r[0]} | nid={r[1]} | depth={r[2]}")

conn.close()
