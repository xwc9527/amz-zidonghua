"""Import the official AmazonQAC Train split into the existing PostgreSQL.

Only the two required source columns are fetched. Durable Parquet projections,
content hashes and PostgreSQL transaction checkpoints make restart idempotent.
No keyword normalization or addition of popularity values is performed.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import time

import duckdb
import fsspec
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
import psycopg2
import requests

from pg_config import get_pg_dsn

REVISION = '975cbfac4623ffb054c59631e06e39d0831e47fb'
BASE = 'https://huggingface.co/datasets/amazon/AmazonQAC'
GIB = 1024 ** 3
POSITION_BASE = 1_000_000_000


def timestamp():
    return datetime.now(timezone.utc).isoformat()


def emit(event, **fields):
    print(json.dumps({'time': timestamp(), 'event': event, **fields}, ensure_ascii=True), flush=True)


def sha256(path):
    digest = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(8 * 1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def save_json(path, value):
    partial = path.with_suffix(path.suffix + '.partial')
    partial.write_text(json.dumps(value, indent=2, ensure_ascii=True), encoding='utf-8')
    os.replace(partial, path)


def space_check(root, reserve_gib=25):
    free = shutil.disk_usage(root).free
    if free < reserve_gib * GIB:
        raise RuntimeError(f'Disk reserve reached: {free / GIB:.2f} GiB free; {reserve_gib} GiB required. Resume after adding space.')
    return round(free / GIB, 2)


def sql_path(path):
    return "'" + path.as_posix().replace("'", "''") + "'"


def manifest(root):
    path = root / 'source-manifest.json'
    if path.exists():
        result = json.loads(path.read_text(encoding='utf-8'))
    else:
        response = requests.get(f'https://huggingface.co/api/datasets/amazon/AmazonQAC/revision/{REVISION}?blobs=true', timeout=60)
        response.raise_for_status()
        result = response.json()
        save_json(path, result)
        response = requests.get(f'{BASE}/raw/{REVISION}/README.md', timeout=60)
        response.raise_for_status()
        (root / 'OFFICIAL_DATASET_CARD.md').write_bytes(response.content)
    if result['sha'] != REVISION:
        raise RuntimeError('Source revision mismatch; existing library is never silently replaced.')
    files = sorted((f for f in result['siblings'] if f['rfilename'].startswith('train/') and f['rfilename'].endswith('.parquet')), key=lambda f: f['rfilename'])
    return files


def project_file(root, file_index, item):
    """One durable artifact per original row group, in original row order."""
    meta_path = root / 'metadata' / f's{file_index:02d}.json'
    meta = json.loads(meta_path.read_text(encoding='utf-8')) if meta_path.exists() else None
    if meta and all((root / 'projected' / f's{file_index:02d}-r{g:03d}.json').exists() for g in range(len(meta['row_group_rows']))):
        emit('source_projection_reused', source_file=file_index, rows=meta['rows'])
        return meta
    fs = fsspec.filesystem('http', client_kwargs={'trust_env': True})
    url = f"{BASE}/resolve/{REVISION}/{item['rfilename']}?download=true"
    remote = parquet = None
    try:
        # TLS resets were observed during real metadata reads. Retry only these
        # read operations; completed row groups are never fetched again.
        for attempt in range(6):
            try:
                remote = fs.open(url, 'rb', size=item['size'], block_size=1024 * 1024)
                parquet = pq.ParquetFile(remote)
                break
            except OSError as exc:
                emit('metadata_read_retry', source_file=file_index, attempt=attempt+1, error=type(exc).__name__)
                if remote:
                    remote.close()
                if attempt == 5:
                    raise
                time.sleep(min(2 ** attempt, 8))
        schema = parquet.schema_arrow
        if schema.field('final_search_term').type != pa.string() or schema.field('popularity').type != pa.int64():
            raise RuntimeError(f'Unexpected official schema in source file {file_index}')
        meta = {'source_file': file_index, 'source_path': item['rfilename'], 'rows': parquet.metadata.num_rows,
                'row_group_rows': [parquet.metadata.row_group(g).num_rows for g in range(parquet.num_row_groups)],
                'source_bytes': item['size'], 'source_lfs_sha256': item['lfs']['sha256'], 'revision': REVISION}
        if meta['rows'] >= POSITION_BASE:
            raise RuntimeError('Source file exceeds provenance position encoding.')
        save_json(meta_path, meta)
        offset = 0
        for group, count in enumerate(meta['row_group_rows']):
            path = root / 'projected' / f's{file_index:02d}-r{group:03d}.parquet'
            checkpoint = path.with_suffix('.json')
            if checkpoint.exists():
                info = json.loads(checkpoint.read_text(encoding='utf-8'))
                if not path.exists() or path.stat().st_size != info['bytes']:
                    raise RuntimeError(f'Projection artifact missing or truncated: {path.name}')
                offset += count
                continue
            free = space_check(root)
            started = time.monotonic()
            for attempt in range(6):
                try:
                    table = parquet.read_row_group(group, columns=['final_search_term', 'popularity'])
                    break
                except OSError as exc:
                    emit('row_group_read_retry', source_file=file_index, row_group=group, attempt=attempt+1, error=type(exc).__name__)
                    if attempt == 5:
                        raise
                    remote.close()
                    time.sleep(min(2 ** attempt, 8))
                    remote = fs.open(url, 'rb', size=item['size'], block_size=1024 * 1024)
                    parquet = pq.ParquetFile(remote)
            assert table.num_rows == count
            table = table.append_column('source_file', pa.array([file_index] * count, type=pa.int16()))
            table = table.append_column('source_row', pa.array(range(offset, offset + count), type=pa.int64()))
            partial = path.with_suffix('.parquet.partial')
            pq.write_table(table, partial, compression='zstd', row_group_size=131072)
            os.replace(partial, path)
            info = {'source_file': file_index, 'row_group': group, 'source_row_start': offset, 'rows': count,
                    'bytes': path.stat().st_size, 'sha256': sha256(path), 'completed_at': timestamp()}
            save_json(checkpoint, info)
            emit('row_group_projected', **info, elapsed_seconds=round(time.monotonic()-started, 2), free_gib=free)
            offset += count
            del table
        return meta
    finally:
        if remote:
            remote.close()


def aggregate(root, metadata):
    for meta in metadata:
        for group in range(len(meta['row_group_rows'])):
            path = root / 'projected' / f"s{meta['source_file']:02d}-r{group:03d}.parquet"
            checkpoint = json.loads(path.with_suffix('.json').read_text(encoding='utf-8'))
            if sha256(path) != checkpoint['sha256']:
                raise RuntimeError(f'Projection hash mismatch: {path.name}')
    space_check(root)
    con = duckdb.connect()
    # The full 395.55M-row GROUP BY exhausted 3GB with four workers on this
    # machine. One worker and 5GB fit its measured 9.35GiB available RAM.
    con.execute("SET memory_limit='5GB'")
    con.execute('SET threads=1')
    con.execute('SET preserve_insertion_order=false')
    con.execute('SET temp_directory=' + sql_path(root / 'spill'))
    con.execute("SET max_temp_directory_size='35GiB'")
    con.execute(f'CREATE VIEW raw AS SELECT *, source_file::BIGINT * {POSITION_BASE} + source_row AS position FROM read_parquet({sql_path(root / "projected" / "*.parquet")})')
    invalid = 'final_search_term IS NULL OR popularity IS NULL OR popularity < 0 OR contains(final_search_term, chr(0))'
    con.execute(f'CREATE VIEW valid AS SELECT * FROM raw WHERE NOT ({invalid})')
    def artifact(name, query):
        path = root / 'aggregated' / (name + '.parquet')
        checkpoint = path.with_suffix('.json')
        if checkpoint.exists():
            info = json.loads(checkpoint.read_text(encoding='utf-8'))
            if sha256(path) != info['sha256']:
                raise RuntimeError(f'Aggregate hash mismatch: {name}')
            emit('aggregate_reused', stage=name, rows=info['rows'])
            return path
        started = time.monotonic()
        space_check(root)
        partial = path.with_suffix('.parquet.partial')
        emit('aggregate_started', stage=name)
        con.execute(f'COPY ({query}) TO {sql_path(partial)} (FORMAT parquet, COMPRESSION zstd, ROW_GROUP_SIZE 250000)')
        os.replace(partial, path)
        info = {'rows': pq.ParquetFile(path).metadata.num_rows, 'bytes': path.stat().st_size, 'sha256': sha256(path), 'completed_at': timestamp()}
        save_json(checkpoint, info)
        emit('aggregate_completed', stage=name, **info, elapsed_seconds=round(time.monotonic()-started, 2))
        return path
    variants = artifact('variants', 'SELECT final_search_term,popularity,count(*)::BIGINT AS observations,min(position)::BIGINT AS first_position FROM valid GROUP BY final_search_term,popularity')
    con.execute('CREATE VIEW variants AS SELECT * FROM read_parquet(' + sql_path(variants) + ')')
    keywords = artifact('keywords', f'''SELECT final_search_term,arg_min(popularity,first_position)::BIGINT AS popularity,
        1::SMALLINT AS source_id,(min(first_position)//{POSITION_BASE})::SMALLINT AS source_file,
        (min(first_position)%{POSITION_BASE})::BIGINT AS source_row,sum(observations)::BIGINT AS observations,
        count(*)>1 AS popularity_conflict FROM variants GROUP BY final_search_term ORDER BY final_search_term''')
    con.execute('CREATE VIEW keywords AS SELECT * FROM read_parquet(' + sql_path(keywords) + ')')
    conflicts = artifact('popularity_conflicts', f'''SELECT v.final_search_term,v.popularity,1::SMALLINT AS source_id,
        (first_position//{POSITION_BASE})::SMALLINT AS source_file,(first_position%{POSITION_BASE})::BIGINT AS source_row,v.observations
        FROM variants v JOIN keywords k USING(final_search_term) WHERE k.popularity_conflict''')
    anomalies = artifact('anomalies', f'''SELECT hex(encode(final_search_term)) AS final_search_term_utf8_hex,popularity,
        1::SMALLINT AS source_id,source_file,source_row,
        CASE WHEN final_search_term IS NULL THEN 'null_search_term' WHEN popularity IS NULL THEN 'null_popularity'
             WHEN popularity<0 THEN 'negative_popularity' ELSE 'nul_in_search_term' END AS reason FROM raw WHERE {invalid}''')
    raw_count = sum(m['rows'] for m in metadata)
    valid_count = con.execute('SELECT sum(observations) FROM variants').fetchone()[0]
    unique, conflict_count = con.execute('SELECT count(*), count(*) FILTER(WHERE popularity_conflict) FROM keywords').fetchone()
    anomaly_count = pq.ParquetFile(anomalies).metadata.num_rows
    assert raw_count == valid_count + anomaly_count
    stats = {'raw_records':raw_count,'valid_records':valid_count,'unique_keywords':unique,'anomalous_records':anomaly_count,
             'conflict_keywords':conflict_count,'duplicate_valid_records':valid_count-unique,'revision':REVISION}
    save_json(root / 'aggregation-summary.json', stats)
    con.close()
    return {'keywords':keywords,'popularity_conflicts':conflicts,'anomalies':anomalies},stats


def copy_artifacts(pg, root, artifacts):
    for stage, path in artifacts.items():
        fingerprint = sha256(path)
        with pg.cursor() as cur:
            cur.execute('SELECT batch_index,artifact_sha256 FROM amazonqac.import_batches WHERE stage=%s', (stage,))
            done = dict(cur.fetchall())
            if any(value != fingerprint for value in done.values()):
                raise RuntimeError('Committed import artifact changed; refusing to duplicate or replace data.')
        for index, batch in enumerate(pq.ParquetFile(path).iter_batches(batch_size=250000)):
            if index in done:
                continue
            space_check(root)
            started = time.monotonic()
            sink = pa.BufferOutputStream()
            pacsv.write_csv(batch, sink, write_options=pacsv.WriteOptions(include_header=False))
            buffer = io.BytesIO(sink.getvalue().to_pybytes())
            # COPY and its checkpoint are committed together. A crash rolls
            # back both; a resumed batch never adds existing popularity.
            with pg:
                with pg.cursor() as cur:
                    cur.copy_expert(f'COPY amazonqac.{stage} FROM STDIN WITH (FORMAT csv)', buffer)
                    cur.execute('INSERT INTO amazonqac.import_batches(stage,batch_index,artifact_sha256,rows_copied) VALUES (%s,%s,%s,%s)',(stage,index,fingerprint,batch.num_rows))
            emit('postgres_batch_committed',stage=stage,batch=index,rows=batch.num_rows,elapsed_seconds=round(time.monotonic()-started,2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--data-dir', type=Path, required=True, help='Persistent source projections, checkpoints and audit evidence; outside Git.')
    parser.add_argument('--workers', type=int, default=3, choices=range(1,5))
    parser.add_argument('--project-only', action='store_true', help='Stop after durable projections; normal execution resumes without fetching completed groups.')
    args = parser.parse_args()
    root = args.data_dir.resolve()
    for folder in ('metadata','projected','aggregated','spill'):
        (root / folder).mkdir(parents=True, exist_ok=True)
    space_check(root)
    pg = psycopg2.connect(get_pg_dsn(), connect_timeout=10)
    try:
        with pg.cursor() as cur:
            cur.execute('SELECT pg_try_advisory_lock(638510300)')
            if not cur.fetchone()[0]:
                raise RuntimeError('Another AmazonQAC importer is already running in this database.')
            cur.execute(Path(__file__).with_name('amazonqac_schema.sql').read_text(encoding='utf-8'))
            cur.execute('SELECT revision FROM amazonqac.dataset WHERE source_id=1')
            prior = cur.fetchone()
            if prior and prior[0] != REVISION:
                raise RuntimeError('Existing source revision differs; no overwrite allowed.')
            cur.execute('''INSERT INTO amazonqac.dataset(source_id,dataset_name,revision,source_url,split,market,period_start,period_end,popularity_definition)
                VALUES (1,'amazon/AmazonQAC',%s,%s,'train','US','2023-09-01','2023-09-30','Occurrences of the search term before filtering; preserved, never summed') ON CONFLICT(source_id) DO NOTHING''',(REVISION,BASE))
        pg.commit()
        files = manifest(root)
        started = time.monotonic()
        metadata = []
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            pending = [pool.submit(project_file,root,index,item) for index,item in enumerate(files)]
            for future in as_completed(pending):
                meta = future.result()
                metadata.append(meta)
                with pg:
                    with pg.cursor() as cur:
                        cur.execute('''INSERT INTO amazonqac.source_files VALUES (1,%s,%s,%s,%s,%s,%s) ON CONFLICT(source_id,file_index) DO NOTHING''',
                                    (meta['source_file'],meta['source_path'],meta['source_bytes'],meta['source_lfs_sha256'],meta['rows'],len(meta['row_group_rows'])))
                emit('source_file_completed',source_file=meta['source_file'],rows=meta['rows'],files_completed=len(metadata),files_total=len(files))
        metadata.sort(key=lambda row:row['source_file'])
        if args.project_only:
            emit('projection_phase_completed',files=len(metadata),rows=sum(m['rows'] for m in metadata))
            return
        artifacts,stats = aggregate(root,metadata)
        copy_artifacts(pg,root,artifacts)
        with pg:
            with pg.cursor() as cur:
                cur.execute("SELECT 1 FROM pg_constraint WHERE conrelid='amazonqac.keywords'::regclass AND contype='p'")
                if not cur.fetchone():
                    emit('primary_key_build_started')
                    cur.execute('ALTER TABLE amazonqac.keywords ADD CONSTRAINT amazonqac_keywords_pkey PRIMARY KEY(final_search_term)')
                for table,key in (('keywords','unique_keywords'),('anomalies','anomalous_records')):
                    cur.execute(f'SELECT count(*) FROM amazonqac.{table}')
                    assert cur.fetchone()[0] == stats[key]
                cur.execute('SELECT sum(observations),count(*) FILTER(WHERE popularity_conflict) FROM amazonqac.keywords')
                observations,conflicts=cur.fetchone()
                assert observations == stats['valid_records'] and conflicts == stats['conflict_keywords']
                cur.execute('ANALYZE amazonqac.keywords')
                cur.execute('''UPDATE amazonqac.dataset SET import_status='complete',raw_records=%s,unique_keywords=%s,anomalous_records=%s,conflict_keywords=%s,completed_at=now() WHERE source_id=1''',
                            (stats['raw_records'],stats['unique_keywords'],stats['anomalous_records'],stats['conflict_keywords']))
        stats.update({'completed_at':timestamp(),'total_runtime_seconds':round(time.monotonic()-started,2),'free_gib':space_check(root)})
        save_json(root/'import-completed.json',stats)
        emit('import_completed',**stats)
    finally:
        pg.close()


if __name__ == '__main__':
    main()
