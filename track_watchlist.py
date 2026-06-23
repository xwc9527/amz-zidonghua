# track_watchlist.py — 每日快照 watchlist 中 ASIN 的价格/排名
# 用法: python track_watchlist.py (建议 cron/任务计划每天运行一次)
import sys, os
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

import psycopg2
from pg_config import PG_DSN


def snapshot():
    conn = psycopg2.connect(PG_DSN)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("SELECT asin FROM watchlist")
    asins = [r[0] for r in cur.fetchall()]
    if not asins:
        print("[track] watchlist 为空，跳过")
        return

    inserted = 0
    for asin in asins:
        cur.execute("""
            SELECT price, rank, rating, review_count
            FROM product_sightings
            WHERE asin = %s
            ORDER BY scraped_at DESC
            LIMIT 1
        """, (asin,))
        row = cur.fetchone()
        if not row:
            continue
        price, rank, rating, review_count = row
        try:
            cur.execute("""
                INSERT INTO tracking (asin, price, rank, rating, review_count)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (asin, snapshot_date) DO UPDATE
                SET price=EXCLUDED.price, rank=EXCLUDED.rank,
                    rating=EXCLUDED.rating, review_count=EXCLUDED.review_count
            """, (asin, price, rank, rating, review_count))
            inserted += 1
        except Exception as e:
            print(f"[track] {asin} 写入失败: {e}")

    cur.close()
    conn.close()
    print(f"[track] 完成: {inserted}/{len(asins)} 条快照已写入")


if __name__ == '__main__':
    snapshot()
