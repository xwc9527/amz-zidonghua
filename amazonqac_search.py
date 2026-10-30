"""Independent, read-only search over the imported AmazonQAC keywords."""
from __future__ import annotations

from contextlib import closing
import re
import time
from typing import Annotated, Literal

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
import psycopg2
from psycopg2.extras import RealDictCursor

from pg_config import get_pg_dsn


Term = Annotated[str, Field(min_length=1, max_length=2048)]
WORD_COUNT_SQL = "regexp_count(final_search_term, '[^[:space:]]+')"
QUERY_TIMEOUT_MS = 60000


class SearchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    query: Term | None = None
    match_mode: Literal["substring", "words", "exact"] = "substring"
    include_terms: list[Term] = Field(default_factory=list, max_length=32)
    exclude_terms: list[Term] = Field(default_factory=list, max_length=32)
    popularity_min: int | None = Field(default=None, ge=0, le=9223372036854775807)
    popularity_max: int | None = Field(default=None, ge=0, le=9223372036854775807)
    word_count_min: int | None = Field(default=None, ge=0, le=2147483647)
    word_count_max: int | None = Field(default=None, ge=0, le=2147483647)
    sort: Literal["popularity_asc", "popularity_desc"] = "popularity_desc"
    page: int = Field(default=1, ge=1, le=2147483647)
    page_size: int = Field(default=50, ge=1, le=1000)

    @field_validator("query", "include_terms", "exclude_terms")
    @classmethod
    def literal_terms(cls, value):
        def clean(term):
            if "\x00" in term or not term.strip():
                raise ValueError("Search terms must be nonblank and cannot contain NUL")
            return term.strip()
        if isinstance(value, list):
            return list(dict.fromkeys(clean(term) for term in value))
        return clean(value) if value is not None else None

    @model_validator(mode="after")
    def ordered_ranges(self):
        for prefix in ("popularity", "word_count"):
            low, high = getattr(self, prefix + "_min"), getattr(self, prefix + "_max")
            if low is not None and high is not None and low > high:
                raise ValueError(prefix + "_min must be <= " + prefix + "_max")
        return self


def build_filter(request: SearchRequest) -> tuple[str, list]:
    """Only fixed SQL and bound literal values; no user SQL or regex syntax."""
    clauses, params = [], []

    def match(term, exclude=False):
        if request.match_mode == "words":
            # PostgreSQL word boundaries are alphanumeric/underscore boundaries.
            literal = re.sub(r"([\\.^$|?*+(){}\[\]])", r"\\\1", term)
            pattern = r"(?<![[:alnum:]_])" + literal + r"(?![[:alnum:]_])"
            predicate = "final_search_term ~* %s"
        else:
            pattern = term.replace("\\", "\\\\").replace("%", r"\%").replace("_", r"\_")
            if request.match_mode == "substring":
                pattern = "%" + pattern + "%"
            predicate = "final_search_term ILIKE %s"
        clauses.append("NOT (" + predicate + ")" if exclude else predicate)
        params.append(pattern)

    for term in ([request.query] if request.query else []) + request.include_terms:
        match(term)
    for term in request.exclude_terms:
        match(term, exclude=True)
    for field, expression in (("popularity", "popularity"), ("word_count", WORD_COUNT_SQL)):
        for suffix, operator in (("min", ">="), ("max", "<=")):
            value = getattr(request, field + "_" + suffix)
            if value is not None:
                clauses.append(expression + " " + operator + " %s")
                params.append(value)
    return " AND ".join(clauses) or "TRUE", params


def search_keywords(request: SearchRequest) -> dict:
    started = time.perf_counter()
    where, params = build_filter(request)
    direction = "ASC" if request.sort == "popularity_asc" else "DESC"
    with closing(psycopg2.connect(get_pg_dsn(), connect_timeout=5)) as connection:
        # The exact count, page and provenance see the same committed snapshot.
        connection.set_session(readonly=True, isolation_level="REPEATABLE READ")
        with connection, connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute("SET LOCAL statement_timeout = %s", (QUERY_TIMEOUT_MS,))
            count_started = time.perf_counter()
            cursor.execute("SELECT count(*) AS total FROM amazonqac.keywords WHERE " + where, params)
            total = cursor.fetchone()["total"]
            count_ms = (time.perf_counter() - count_started) * 1000
            page_started = time.perf_counter()
            cursor.execute(
                "SELECT final_search_term, popularity, " + WORD_COUNT_SQL + " AS word_count, source_id "
                "FROM amazonqac.keywords WHERE " + where +
                " ORDER BY popularity " + direction + ', final_search_term COLLATE "C" ASC LIMIT %s OFFSET %s',
                params + [request.page_size, (request.page - 1) * request.page_size],
            )
            items = [dict(row) for row in cursor.fetchall()]
            page_ms = (time.perf_counter() - page_started) * 1000
            cursor.execute(
                "SELECT source_id, dataset_name, revision, source_url, split, market, "
                "period_start, period_end, popularity_definition, import_status, unique_keywords "
                "FROM amazonqac.dataset ORDER BY source_id"
            )
            sources = [dict(row) for row in cursor.fetchall()]
    return {
        "total": total, "page": request.page, "page_size": request.page_size,
        "total_pages": (total + request.page_size - 1) // request.page_size,
        "items": items, "filters": request.model_dump(), "sources": sources,
        "timing_ms": {"count": round(count_ms, 3), "page": round(page_ms, 3),
                      "total": round((time.perf_counter() - started) * 1000, 3)},
    }


app = FastAPI(title="AmazonQAC historical keyword search", version="1.0.0")


@app.post("/api/v2/amazonqac/search")
def search(request: SearchRequest):
    try:
        return search_keywords(request)
    except psycopg2.errors.QueryCanceled:
        raise HTTPException(status_code=504, detail="Query exceeded the 60 second statement timeout") from None
    except (psycopg2.OperationalError, RuntimeError):
        raise HTTPException(status_code=503, detail="AmazonQAC database unavailable or not configured") from None
    except psycopg2.DatabaseError:
        raise HTTPException(status_code=500, detail="AmazonQAC database query failed") from None
