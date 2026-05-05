import sqlite3
conn = sqlite3.connect('data/categories.db')
conn.row_factory = sqlite3.Row

# 1. depth=1 样本：看 parent_url 是否指向 root
print("=== L1 样本（前10个）===")
for r in conn.execute("SELECT name, url, parent_url, depth FROM categories WHERE depth=1 ORDER BY name LIMIT 10"):
    print(f"  [{r['depth']}] {r['name']}")
    print(f"       url: {r['url']}")
    print(f"       parent: {r['parent_url']}")
    print()

# 2. 选一个 L1，看它的子节点
print("=== 'Home & Kitchen' 的直接子节点（parent_url 匹配）===")
hk = conn.execute("SELECT url FROM categories WHERE name='Home & Kitchen' AND depth=1").fetchone()
if hk:
    hk_url = hk['url'].rstrip('/')
    print(f"  Home & Kitchen URL: {hk_url}")
    kids = conn.execute(
        "SELECT name, url, depth FROM categories WHERE RTRIM(parent_url,'/')=? ORDER BY name LIMIT 15",
        (hk_url,)
    ).fetchall()
    print(f"  子节点数: {len(kids)}")
    for k in kids:
        print(f"    [{k['depth']}] {k['name']} => {k['url']}")
else:
    print("  未找到 Home & Kitchen")

# 3. 选 CDs & Vinyl，看子节点是否真是音乐类
print()
print("=== 'CDs & Vinyl' 的直接子节点 ===")
cd = conn.execute("SELECT url FROM categories WHERE name='CDs & Vinyl' AND depth=1").fetchone()
if cd:
    cd_url = cd['url'].rstrip('/')
    kids2 = conn.execute(
        "SELECT name, url, depth FROM categories WHERE RTRIM(parent_url,'/')=? ORDER BY name LIMIT 10",
        (cd_url,)
    ).fetchall()
    print(f"  CDs URL: {cd_url}")
    print(f"  子节点数: {len(kids2)}")
    for k in kids2:
        print(f"    [{k['depth']}] {k['name']}")
else:
    print("  未找到")

# 4. 统计：有多少节点的 parent_url 在 categories.url 中找不到
print()
print("=== 孤立节点（parent_url 无匹配）===")
orphans = conn.execute("""
    SELECT COUNT(*) FROM categories c1
    WHERE c1.parent_url IS NOT NULL AND c1.parent_url != ''
      AND NOT EXISTS (SELECT 1 FROM categories c2 WHERE RTRIM(c2.url,'/')=RTRIM(c1.parent_url,'/'))
""").fetchone()[0]
total = conn.execute("SELECT COUNT(*) FROM categories").fetchone()[0]
print(f"  总节点: {total}")
print(f"  孤立节点: {orphans} ({100*orphans/total:.1f}%)")

# 5. 随机检查一个 depth=2 节点
print()
print("=== 随机 depth=2 节点 ===")
for r in conn.execute("SELECT name, url, parent_url, depth FROM categories WHERE depth=2 LIMIT 5"):
    print(f"  [{r['depth']}] {r['name']}")
    print(f"       parent: {r['parent_url']}")
    # 查 parent 名字
    p = conn.execute("SELECT name FROM categories WHERE RTRIM(url,'/')=?", (r['parent_url'].rstrip('/'),)).fetchone()
    print(f"       parent name: {p['name'] if p else '!!! NOT FOUND'}")
    print()

conn.close()
