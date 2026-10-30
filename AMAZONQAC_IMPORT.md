# AmazonQAC Train historical keyword library

This independent importer writes only the `amazonqac` PostgreSQL schema. It does
not change product discovery, Oxylabs, category crawlers, or application routes.
It provides no search/filter/export API.

## Source and meaning

- Dataset: <https://huggingface.co/datasets/amazon/AmazonQAC>
- Pinned revision: `975cbfac4623ffb054c59631e06e39d0831e47fb`.
- All 20 original `train/*.parquet` files, 395,550,004 rows (verified from all
  original Parquet footers). Test split is not imported.
- US search logs, 2023-09-01 through 2023-09-30, according to the official card.
- `final_search_term` is stored exactly: no trimming, lowercasing, Unicode
  normalization, semantic rewriting, or subjective filtering.
- `popularity` is the source's occurrences-before-filtering metadata. It is
  **never summed across duplicate observations**.

## Run and resume

Use the existing PostgreSQL, Python 3.12+, and the isolated import dependencies:

```sh
python -m pip install -r requirements-amazonqac.txt
python amazonqac_import.py --data-dir /persistent/path/amazonqac --workers 3
```

`PG_DSN` comes from the existing `pg_config.get_pg_dsn()` mechanism. For testing,
set `TESTING=1` and a separate `PG_TEST_DSN`; existing database-name and DSN
isolation checks apply. Do not pass database passwords on the command line.

For the provisioned Windows environment, load
`C:\Users\Administrator\Documents\Codex\services\amazonqac-postgresql\load-env.ps1`
in the same PowerShell that starts the importer. The persistent production data
directory is `C:\Users\Administrator\Documents\Codex\data\amazonqac`.
Keep data directories and credential files outside Git.

Repeat exactly the same command after interruption. A PostgreSQL session
advisory lock prevents concurrent importers against the same database.
`--project-only` stops after all source projections have been persisted.

## Processing and durability

1. Read original Parquet columns `final_search_term` and `popularity` with
   PyArrow/fsspec HTTP ranges. Irrelevant source columns are not downloaded as
   full files. Pin source commit and record official LFS hashes and file sizes;
   LFS hashes are provenance, not a claim of hashing the whole remote file.
2. Persist each original row group as compressed Parquet, with original source
   file index and zero-based original file row. Atomically publish its checkpoint
   and SHA-256. Completed groups survive process interruption. Read errors caused
   by observed TLS resets are retried with bounded backoff and recorded.
3. Validate hashes, then use mature DuckDB aggregation with bounded memory/spill.
   It is an import processing tool, not a second keyword-serving database.
   Group exact `(final_search_term, popularity)` variants and retain occurrence
   counts and first original location.
4. For each exact original term, select the popularity from its **first physical
   source occurrence** (file-name order, then original row). This deterministic
   choice never combines frequencies. `observations` counts Train records and is
   explicitly separate from popularity.
5. If a term has different popularity values, set `popularity_conflict=true` and
   preserve every distinct observed value, record count, and first source row in
   `popularity_conflicts`. The durable projections retain every original record
   for further tracing. No conflicting value is silently added or overwritten.
6. Null terms, null/negative popularity and NUL-containing terms are preserved in
   `anomalies` with source location and UTF-8 hex term, because PostgreSQL text
   cannot store NUL. Empty/whitespace terms and zero popularity are not
   subjectively removed. Invalid UTF-8/schema errors stop the import rather than
   silently replacing source text.
7. PostgreSQL `COPY` and a batch checkpoint commit in one transaction. Resume
   skips committed batches and verifies the aggregate artifact SHA-256. A
   changed artifact cannot be appended to an existing import. No upsert adds
   popularity. Keyword primary key is built after the initial bulk COPY; only
   then are final counts verified and source status marked `complete`.

Reserve at least 25 GiB free on the data volume. Every projection, aggregation
stage and COPY batch checks disk reserve. The validated environment's source
cache, spill and database are on C:. DuckDB has a 5 GB memory limit, one
aggregation worker, and a 35 GiB spill cap. The full-data run exhausted its
initial 3 GB/four-worker allocation; the revised limit follows the measured
9.35 GiB available RAM and the engine's OOM guidance. This does not automatically limit other applications or a remote
database's disk use; check the actual PostgreSQL volume before import.

## Tables and audit artifacts

See `amazonqac_schema.sql` for the exact DDL:

- `dataset`: source/revision, market, period, import status and totals.
- `source_files`: original file order/path/LFS hash/size/record count.
- `keywords`: exact term, preserved popularity, source FK, first original row,
  observation count and conflict flag; primary key uses PostgreSQL `C` collation.
- `popularity_conflicts`: different source frequency variants and provenance.
- `anomalies`: unrepresentable/invalid values and exact source evidence.
- `import_batches`: durable COPY transaction checkpoints.

The persistent data directory contains the pinned official card/manifest,
projection hashes, aggregate hashes, aggregation totals and completion evidence.
The library's source URL and period are retained through `source_id` rather than
duplicated 40 million times. Normal SQL clients can read it directly; no existing
product API is required. `dataset.import_status='complete'` is the acceptance
state, not merely a successful connection or the existence of a table.

## Verification performed

The independent real-data smoke validation uses original Train file 0 rows
0–29,999 in `amazonqac_test`, compares every term/popularity/source row against
the actual source, checks duplicate counts, executes PostgreSQL COPY and repeats
it to verify checkpoint idempotence. Full-import counts, anomalies, source
accuracy samples, persistence and disk measurements are reported separately in
the actual run evidence; a sample run is not full-import acceptance.
