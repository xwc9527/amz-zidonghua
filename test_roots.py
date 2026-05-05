import sqlite3, sys
sys.stdout.reconfigure(encoding='utf-8')
conn = sqlite3.connect('data/categories.db')
rows = conn.execute("""
    SELECT name, COALESCE(node_id, parent_node_id) as nid,
    (SELECT COUNT(*) FROM categories c2 
     WHERE c2.parent_node_id=COALESCE(c.node_id, c.parent_node_id)) AS cc
    FROM categories c WHERE true_depth=2
    GROUP BY name ORDER BY name
""").fetchall()
print(f'根节点: {len(rows)}')
for r in rows:
    print(f'  {r[0]} | id={r[1]} | children={r[2]}')
conn.close()
