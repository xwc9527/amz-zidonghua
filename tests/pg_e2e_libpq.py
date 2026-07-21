"""Real PostgreSQL E2E checks through the bundled official libpq client.

This test deliberately avoids the application database and only connects to the
ephemeral local cluster created for E2E verification.
"""
from __future__ import annotations

import ctypes
import json
import sqlite3
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
LIBPQ = Path(r"C:\tmp\pg-runtime\package\native\bin\libpq.dll")
HOST = "127.0.0.1"
PORT = 55432
DB = "amz_selection_test"


class PgError(RuntimeError):
    pass


class Libpq:
    CONNECTION_OK = 0
    PGRES_COMMAND_OK = 1
    PGRES_TUPLES_OK = 2

    def __init__(self) -> None:
        self.lib = ctypes.WinDLL(str(LIBPQ))
        self.lib.PQconnectdb.argtypes = [ctypes.c_char_p]
        self.lib.PQconnectdb.restype = ctypes.c_void_p
        self.lib.PQstatus.argtypes = [ctypes.c_void_p]
        self.lib.PQstatus.restype = ctypes.c_int
        self.lib.PQerrorMessage.argtypes = [ctypes.c_void_p]
        self.lib.PQerrorMessage.restype = ctypes.c_char_p
        self.lib.PQfinish.argtypes = [ctypes.c_void_p]
        self.lib.PQexec.argtypes = [ctypes.c_void_p, ctypes.c_char_p]
        self.lib.PQexec.restype = ctypes.c_void_p
        self.lib.PQresultStatus.argtypes = [ctypes.c_void_p]
        self.lib.PQresultStatus.restype = ctypes.c_int
        self.lib.PQresultErrorMessage.argtypes = [ctypes.c_void_p]
        self.lib.PQresultErrorMessage.restype = ctypes.c_char_p
        self.lib.PQntuples.argtypes = [ctypes.c_void_p]
        self.lib.PQntuples.restype = ctypes.c_int
        self.lib.PQnfields.argtypes = [ctypes.c_void_p]
        self.lib.PQnfields.restype = ctypes.c_int
        self.lib.PQgetvalue.argtypes = [ctypes.c_void_p, ctypes.c_int, ctypes.c_int]
        self.lib.PQgetvalue.restype = ctypes.c_char_p
        self.lib.PQclear.argtypes = [ctypes.c_void_p]

    @staticmethod
    def _text(value: bytes | None) -> str:
        return (value or b"").decode("utf-8", "replace").strip()

    def connect(self, database: str) -> ctypes.c_void_p:
        dsn = f"host={HOST} port={PORT} user=postgres dbname={database} connect_timeout=5"
        conn = self.lib.PQconnectdb(dsn.encode())
        if not conn or self.lib.PQstatus(conn) != self.CONNECTION_OK:
            msg = self._text(self.lib.PQerrorMessage(conn)) if conn else "null connection"
            if conn:
                self.lib.PQfinish(conn)
            raise PgError(msg)
        return conn

    def execute(self, conn: ctypes.c_void_p, sql: str, *, allow_error: bool = False):
        result = self.lib.PQexec(conn, sql.encode("utf-8"))
        if not result:
            raise PgError(self._text(self.lib.PQerrorMessage(conn)))
        try:
            status = self.lib.PQresultStatus(result)
            if status not in (self.PGRES_COMMAND_OK, self.PGRES_TUPLES_OK):
                msg = self._text(self.lib.PQresultErrorMessage(result))
                if allow_error:
                    return None, msg
                raise PgError(msg)
            rows = []
            if status == self.PGRES_TUPLES_OK:
                for row_i in range(self.lib.PQntuples(result)):
                    rows.append(
                        tuple(
                            self._text(self.lib.PQgetvalue(result, row_i, col_i))
                            for col_i in range(self.lib.PQnfields(result))
                        )
                    )
            return rows, ""
        finally:
            self.lib.PQclear(result)


def scalar(pg: Libpq, conn, sql: str) -> str:
    rows, _ = pg.execute(conn, sql)
    return rows[0][0]


def main() -> int:
    pg = Libpq()
    admin = pg.connect("postgres")
    pg.execute(admin, f"DROP DATABASE IF EXISTS {DB} WITH (FORCE)")
    pg.execute(admin, f"CREATE DATABASE {DB}")
    pg.lib.PQfinish(admin)

    conn = pg.connect(DB)
    results: list[dict] = []

    def check(case: str, ok: bool, detail: str) -> None:
        results.append({"case": case, "status": "PASS" if ok else "FAIL", "detail": detail})

    try:
        version = scalar(pg, conn, "SHOW server_version")
        check("PG-SERVER", version.startswith("17."), f"PostgreSQL {version}")

        schema = (ROOT / "pg_schema.sql").read_text(encoding="utf-8-sig")
        pg.execute(conn, schema)
        tables = scalar(
            pg,
            conn,
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_schema='public' AND table_name IN "
            "('categories','product_sightings','new_arrivals','run_status')",
        )
        check("PG-SCHEMA", tables == "4", f"core_tables={tables}/4")
        extensions = scalar(
            pg, conn, "SELECT count(*) FROM pg_extension WHERE extname IN ('ltree','pg_trgm')"
        )
        check("PG-EXTENSIONS", extensions == "2", f"extensions={extensions}/2")

        social_cols = scalar(
            pg,
            conn,
            "SELECT count(*) FROM information_schema.columns WHERE table_schema='public' "
            "AND table_name IN ('product_sightings','new_arrivals') "
            "AND column_name IN ('social_proof','social_proof_count')",
        )
        check("PG-SOCIAL-PROOF", social_cols == "4", f"columns={social_cols}/4")

        pg.execute(
            conn,
            "INSERT INTO categories(name,url,node_id,depth,parent_node_id,site,path) VALUES "
            "('Root','/root','R',1,'','US','R'),"
            "('Parent A','/a','A',2,'R','US','R.A'),"
            "('Parent B','/b','B',2,'R','US','R.B'),"
            "('Child','/a/c','C',3,'A','US','R.A.C'),"
            "('Child','/b/c','C',3,'B','US','R.B.C')",
        )
        _, duplicate_error = pg.execute(
            conn,
            "INSERT INTO categories(name,url,node_id,depth,parent_node_id,site,path) "
            "VALUES ('Duplicate','/dup','C',3,'A','US','R.A.C')",
            allow_error=True,
        )
        same_parent = scalar(
            pg, conn, "SELECT count(*) FROM categories WHERE site='US' AND node_id='C' AND parent_node_id='A'"
        )
        multi_parent = scalar(pg, conn, "SELECT count(*) FROM categories WHERE site='US' AND node_id='C'")
        check(
            "PG-CATEGORY-UNIQUE",
            same_parent == "1" and "duplicate key" in duplicate_error.lower(),
            f"same_parent={same_parent}, rejected={bool(duplicate_error)}",
        )
        check("PG-CATEGORY-MULTIPARENT", multi_parent == "2", f"real_parent_branches={multi_parent}")

        descendants = scalar(
            pg,
            conn,
            "WITH RECURSIVE tree AS ("
            "SELECT node_id,parent_node_id FROM categories WHERE site='US' AND node_id='R' "
            "UNION SELECT c.node_id,c.parent_node_id FROM categories c JOIN tree t "
            "ON c.parent_node_id=t.node_id WHERE c.site='US') "
            "SELECT count(DISTINCT node_id) FROM tree",
        )
        check("PG-CATEGORY-RECURSIVE", descendants == "4", f"distinct_nodes={descendants}")

        pg.execute(conn, "BEGIN")
        pg.execute(
            conn,
            "INSERT INTO new_arrivals(asin,title,node_id,site,price_value) "
            "VALUES ('B0ROLLBACK','rollback','N','US',9.9)",
        )
        pg.execute(conn, "ROLLBACK")
        rolled_back = scalar(pg, conn, "SELECT count(*) FROM new_arrivals WHERE asin='B0ROLLBACK'")
        check("PG-TXN-ROLLBACK", rolled_back == "0", f"rows_after_rollback={rolled_back}")

        pg.execute(
            conn,
            "INSERT INTO product_sightings(asin,name,node_id,list_type,site,social_proof,social_proof_count,detail_scraped) "
            "VALUES ('B0PS','product','N','new-releases','US','1K+ bought',1000,1)",
        )
        _, ps_error = pg.execute(
            conn,
            "INSERT INTO product_sightings(asin,name,node_id,list_type,site) "
            "VALUES ('B0PS','duplicate','N','new-releases','US')",
            allow_error=True,
        )
        check("PG-PRODUCT-UNIQUE", "duplicate key" in ps_error.lower(), f"rejected={bool(ps_error)}")

        fixtures = [
            ("B0A", 10.0, 4.8, 1200, 50, 4, 1000, 0, "2026-07-01", "China"),
            ("B0B", 25.0, 4.2, 300, 500, 8, 500, 1, "2026-01-01", "china"),
            ("B0C", 55.0, 3.9, 10, 5000, 1, None, 0, None, None),
            ("B0D", 99.0, 5.0, 9000, 5, 20, 30000, 1, "2025-01-01", "USA"),
            ("B0E", None, None, None, None, None, None, 0, None, None),
            ("B0F", 35.0, 4.7, 700, 80, 3, 7000, 1, "2026-06-15", "China"),
        ]
        for asin, price, rating, reviews, bsr, variants, social, choice, date, country in fixtures:
            def lit(value):
                if value is None:
                    return "NULL"
                if isinstance(value, str):
                    return "'" + value.replace("'", "''") + "'"
                return str(value)

            pg.execute(
                conn,
                "INSERT INTO new_arrivals(asin,title,node_id,site,price_value,rating,review_count,"
                "bsr_main_rank,variant_option_count,social_proof_count,is_amazon_choice,listing_date,"
                "country_of_origin) VALUES ("
                + ",".join(
                    map(
                        lit,
                        (asin, asin, "N", "US", price, rating, reviews, bsr, variants, social, choice, date, country),
                    )
                )
                + ")",
            )

        sqlite = sqlite3.connect(":memory:")
        sqlite.execute(
            "CREATE TABLE x(asin TEXT, price REAL, rating REAL, reviews INTEGER, bsr INTEGER, "
            "variants INTEGER, social INTEGER, choice INTEGER, listing_date TEXT, country TEXT)"
        )
        sqlite.executemany("INSERT INTO x VALUES(?,?,?,?,?,?,?,?,?,?)", fixtures)
        sqlite_set = {
            row[0]
            for row in sqlite.execute(
                "SELECT asin FROM x WHERE price BETWEEN 9 AND 60 AND rating>=4.2 AND reviews>=100 "
                "AND bsr<=500 AND variants<=8 AND social>=500 AND choice=1 "
                "AND lower(country)=lower('China')"
            )
        }
        pg_rows, _ = pg.execute(
            conn,
            "SELECT asin FROM new_arrivals WHERE price_value BETWEEN 9 AND 60 AND rating>=4.2 "
            "AND review_count>=100 AND bsr_main_rank<=500 AND variant_option_count<=8 "
            "AND social_proof_count>=500 AND is_amazon_choice=1 "
            "AND lower(country_of_origin)=lower('China')",
        )
        pg_set = {row[0] for row in pg_rows}
        check("PG-SQLITE-ASIN-CMP", pg_set == sqlite_set, f"sqlite={sorted(sqlite_set)}, pg={sorted(pg_set)}")
        rating_type = scalar(
            pg,
            conn,
            "SELECT data_type FROM information_schema.columns "
            "WHERE table_name='new_arrivals' AND column_name='rating'",
        )
        rating_rows, _ = pg.execute(
            conn,
            "SELECT rating::text, (rating >= 4.2)::text "
            "FROM new_arrivals WHERE asin='B0B'",
        )
        rating_text, boundary_ok = rating_rows[0]
        check(
            "PG-DOUBLE-BOUNDARY",
            rating_type == "double precision" and boundary_ok == "true" and "B0B" in pg_set,
            f"type={rating_type}, rating_text={rating_text}, rating>=4.2={boundary_ok}, pg_set={sorted(pg_set)}",
        )

        # 模拟旧 REAL 漂移值写入后执行与 schema 相同的 ROUND 归一化
        pg.execute(conn, "DELETE FROM new_arrivals WHERE asin='B0LEGACY'")
        pg.execute(
            conn,
            "INSERT INTO new_arrivals(asin,title,node_id,site,rating) "
            "VALUES ('B0LEGACY','legacy','N','US',4.199999809265137::double precision)",
        )
        pg.execute(
            conn,
            "UPDATE new_arrivals SET rating = ROUND(rating::numeric, 1) WHERE asin='B0LEGACY'",
        )
        legacy_text = scalar(pg, conn, "SELECT rating::text FROM new_arrivals WHERE asin='B0LEGACY'")
        legacy_ok = scalar(
            pg, conn, "SELECT (rating >= 4.2)::text FROM new_arrivals WHERE asin='B0LEGACY'"
        )
        check(
            "PG-RATING-NORMALIZE",
            legacy_text == "4.2" and legacy_ok == "true",
            f"after_round={legacy_text}, rating>=4.2={legacy_ok}",
        )

        missing = scalar(
            pg,
            conn,
            "SELECT count(*) FROM new_arrivals WHERE asin LIKE 'B0_' AND social_proof_count IS NULL",
        )
        check("PG-MISSING-VALUE", missing == "2", f"social_proof_missing={missing}")
        date_boundary = scalar(
            pg,
            conn,
            "SELECT count(*) FROM new_arrivals WHERE listing_date IS NOT NULL "
            "AND listing_date::date BETWEEN DATE '2026-06-01' AND DATE '2026-07-31'",
        )
        check("PG-DATE-BOUNDARY", date_boundary == "2", f"matching_rows={date_boundary}")

        pg.execute(conn, "DELETE FROM product_sightings; DELETE FROM new_arrivals; DELETE FROM categories")
        clean = scalar(
            pg,
            conn,
            "SELECT (SELECT count(*) FROM product_sightings) + "
            "(SELECT count(*) FROM new_arrivals) + (SELECT count(*) FROM categories)",
        )
        check("PG-CLEAN", clean == "0", f"remaining_test_rows={clean}")
    finally:
        pg.lib.PQfinish(conn)

    failed = [item for item in results if item["status"] != "PASS"]
    print(json.dumps({"total": len(results), "passed": len(results) - len(failed), "failed": failed, "results": results}, ensure_ascii=False, indent=2))
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
