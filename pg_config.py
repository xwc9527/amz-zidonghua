import os

PG_DSN = os.getenv(
    "PG_DSN",
    "postgresql://postgres:amz2026@localhost:5432/amz_selection"
)
