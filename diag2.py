import sqlite3, sys
sys.stdout.reconfigure(encoding='utf-8')
conn = sqlite3.connect('data/categories.db')

print("=== 类目树断链诊断 ===\n")

# 1. 各层级节点数
print("1. true_depth 分布:")
for r in conn.execute("SELECT true_depth, COUNT(*) FROM categories GROUP BY true_depth ORDER BY true_depth"):
    print(f"   L{r[0]}: {r[1]}")

# 2. 以 Automotive 为例，追踪完整链路
print("\n2. Automotive 完整链路:")
print("   L1 (root):")
for r in conn.execute("SELECT id,name,node_id,parent_node_id,true_depth FROM categories WHERE name='Automotive' AND true_depth=2"):
    print(f"     id={r[0]} name={r[1]} node_id={r[2]} parent={r[3]} depth={r[4]}")
    # 查它的子节点
    nid = r[2]
    print(f"\n   L2 (parent_node_id='{nid}'):")
    for c in conn.execute("SELECT id,name,node_id,parent_node_id,true_depth FROM categories WHERE parent_node_id=?", (nid,)):
        print(f"     id={c[0]} name={c[1]} node_id={c[2]} parent={c[3]} depth={c[4]}")
        # 查 L3
        if c[2]:
            l3 = conn.execute("SELECT COUNT(*) FROM categories WHERE parent_node_id=?", (c[2],)).fetchone()[0]
            print(f"       → L3 子节点数: {l3}")

# 3. 有多少节点的 parent_node_id 指向了不存在的 node_id
print("\n3. 断链统计（parent_node_id 指向不存在的 node_id）:")
orphan = conn.execute("""
    SELECT COUNT(*) FROM categories c1 
    WHERE c1.parent_node_id IS NOT NULL 
    AND c1.parent_node_id NOT IN (SELECT node_id FROM categories WHERE node_id IS NOT NULL)
""").fetchone()[0]
total_with_parent = conn.execute("SELECT COUNT(*) FROM categories WHERE parent_node_id IS NOT NULL").fetchone()[0]
print(f"   有 parent 的: {total_with_parent}")
print(f"   parent 指向不存在的 node_id: {orphan}")
print(f"   正常链接: {total_with_parent - orphan}")

# 4. 断链样本
print("\n4. 断链样本 (parent 指向不存在的 node_id):")
for r in conn.execute("""
    SELECT c1.name, c1.node_id, c1.parent_node_id, c1.true_depth, c1.url 
    FROM categories c1 
    WHERE c1.parent_node_id IS NOT NULL 
    AND c1.parent_node_id NOT IN (SELECT node_id FROM categories WHERE node_id IS NOT NULL)
    LIMIT 10
"""):
    print(f"   {r[0]} | nid={r[1]} | parent={r[2]} | depth={r[3]}")
    print(f"     url={r[4][:80]}")

# 5. L3 节点的 parent 都指向谁
print("\n5. L3 (true_depth=3) 的 parent_node_id 类型分布:")
print("   指向 slug:")
slug_cnt = conn.execute("""
    SELECT COUNT(*) FROM categories WHERE true_depth=3 
    AND parent_node_id IS NOT NULL AND parent_node_id NOT GLOB '*[0-9]*'
""").fetchone()[0]
print(f"     {slug_cnt}")
print("   指向数字 node_id:")
num_cnt = conn.execute("""
    SELECT COUNT(*) FROM categories WHERE true_depth=3 
    AND parent_node_id IS NOT NULL AND parent_node_id GLOB '*[0-9]*'
""").fetchone()[0]
print(f"     {num_cnt}")
print("   parent IS NULL:")
null_cnt = conn.execute("SELECT COUNT(*) FROM categories WHERE true_depth=3 AND parent_node_id IS NULL").fetchone()[0]
print(f"     {null_cnt}")

conn.close()
