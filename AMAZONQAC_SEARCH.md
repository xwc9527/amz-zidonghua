# AmazonQAC 独立关键词检索

直接只读查询现有 `amazonqac.keywords`，不运行导入器，不启动商品采集服务。
使用项目已有 `requirements.txt` 中的 FastAPI、uvicorn、psycopg2 和现有
`pg_config.get_pg_dsn()`；请求模型使用 Pydantic 2（当前运行环境已有）。
数据库必须已完成 AmazonQAC Train 导入。

## 启动与调用

在仓库目录、已有 Python 依赖环境中运行：

```powershell
. 'C:\Users\Administrator\Documents\Codex\services\amazonqac-postgresql\load-env.ps1'
$env:TESTING = '0'
python -m uvicorn amazonqac_search:app --host 127.0.0.1 --port 8091
```

其他部署环境安全设置 `PG_DSN` 即可；不把凭证放进命令参数或请求体。
本机 PostgreSQL 仍使用原有持久化实例；若未运行，先在普通 PowerShell 中运行其
`start.ps1`。启动本接口不会自动部署或修复数据库。

独立入口：`POST http://127.0.0.1:8091/api/v2/amazonqac/search`。
OpenAPI 和交互式调用说明：`http://127.0.0.1:8091/docs`。

全部匹配结果的 CSV 下载见 [AMAZONQAC_EXPORT.md](AMAZONQAC_EXPORT.md)，
使用 `POST /api/v2/amazonqac/export`；复用下列筛选和排序条件，不使用分页参数。

```powershell
$body = @{
    query = 'couch'
    match_mode = 'words'
    include_terms = @('table')
    exclude_terms = @('cover', 'outdoor')
    popularity_min = 10
    popularity_max = 10000
    word_count_min = 3
    word_count_max = 6
    sort = 'popularity_desc'
    page = 1
    page_size = 20
} | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:8091/api/v2/amazonqac/search' `
    -ContentType 'application/json' -Body $body
```

也可直接调用 Python：

```python
from amazonqac_search import SearchRequest, search_keywords
result = search_keywords(SearchRequest(query="sofa table", match_mode="words"))
print(result["total"], result["items"])
```

## 匹配和筛选语义

| 参数 | 语义 |
| --- | --- |
| `query` | 可选主搜索条件；与所有包含词共同满足 |
| `match_mode=substring` | 默认，字面子串包含；`stor` 可匹配 `storage` |
| `match_mode=words` | 完整词或连续词组；两端不能紧邻字母、数字或下划线；`couch` 不匹配 `couches` |
| `match_mode=exact` | 整条关键词匹配 |
| `include_terms` | 多个包含条件，全部满足；最多 32 项 |
| `exclude_terms` | 任一排除条件匹配即剔除；最多 32 项 |
| `popularity_min/max` | 原始历史频次的闭区间，可单独指定一端；不设置默认频次门槛 |
| `word_count_min/max` | 空白分隔的非空片段数量，闭区间；连字符内部不拆分 |
| `sort` | `popularity_desc`（默认）或 `popularity_asc` |
| `page/page_size` | 页码从 1 开始；默认 50 条，单页最多 1000 条 |

所有包含和排除条件使用同一匹配模式。大小写匹配遵循现有列的 PostgreSQL `C`
排序规则：英文字母大小写不敏感，不额外承诺 Unicode 大小写折叠。词组内部空格按
输入字面匹配；只去除输入条件两端的空白，不改写数据库原始词条。
`%`、`_`、反斜杠、正则元字符均视为字面字符，不允许用户注入 SQL 或正则语法。
词根或概念词以其文本在历史搜索词中检索；例如 `organiz` 包含检索。
不自动推断同义词或改写概念。

单词数量使用 PostgreSQL `regexp_count(final_search_term, '[^[:space:]]+')`，
可在返回值 `word_count` 中审查。未提供文本条件时，可以仅用频次或词数筛选；
空请求表示整个词库。空白条件、逆序区间、非整数页码及未知字段返回 422。

## 返回值和一致性

- `total` 为实际匹配的精确 `count(*)`，不是优化器估算或数据集元数据。
- `items` 包含 `final_search_term`、原始 `popularity`、派生 `word_count`、`source_id`。
- `sources` 返回来源、版本、US 市场、2023 年 9 月统计时期和原有频次定义。
- `page`、`page_size`、`total_pages`、`filters` 和 `timing_ms` 提供调用和耗时信息。
- 页码超出结果范围返回空 `items`，仍保留正确 `total`。
- 排序固定为 `popularity ASC/DESC, final_search_term COLLATE "C" ASC`。
  原始关键词主键是唯一的第二排序键，频次相同也能稳定分页。
- 单次请求的计数、结果和来源信息在同一个只读 REPEATABLE READ 快照中查询。
  跨请求分页在词库未改变时稳定；不提供跨请求的长期快照。
- 原始词条和频次不做大小写转换、覆盖、累加或标准化写入。
  `popularity` 是历史数据集字段，不代表当前 Amazon 实时搜索量。

## 索引和运行边界

现有原始词条 B-tree 主键无法有效加速 `%storage%` 检索，真实基线执行计划出现
39,588,974 行表上的并行顺序扫描。为此新增 PostgreSQL `pg_trgm` GIN 索引，
支持本模块使用的 `ILIKE` 和 `~*` 运算符。没有增加未经测量的频次或词数索引。

第一次部署时，在目标数据库使用 psql 自动提交模式执行：

```text
psql -v ON_ERROR_STOP=1 -f amazonqac_search_indexes.sql
```

此处 psql 使用 libpq 的 `PGHOST`、`PGPORT`、`PGDATABASE`、`PGUSER` 和安全的
`PGPASSFILE` 配置；psql 不会自动读取应用的 `PG_DSN`。不在命令行写密码，
不要把该 SQL 包在 `BEGIN` 中。
`CREATE INDEX CONCURRENTLY` 允许已有业务继续访问表。此迁移只增加扩展、索引和
统计信息，不改原始数据。已存在有效同名索引时无需重复执行；若构建中断，先核查
`pg_index.indisvalid/indisready`，再按实际状态处理，不能把无效索引当成部署成功。

每条 SQL 有 60 秒超时，超时返回 504，不返回截断的计数或假成功；数据库未配置
或连接失败返回 503，其他数据库错误返回 500，均不回传凭证或原始数据库错误。
一次请求包含独立的计数和分页语句，总耗时可能超过 60 秒。
短于三个字符的子串、非常宽泛的条件、仅排除词以及深页 OFFSET 可能较慢；
索引不保证所有条件都快，执行计划可由 PostgreSQL 根据选择性决定。
服务默认只绑定本机；上层认证和对外发布不在本阶段范围内。

## 验证

```powershell
# 复用 pg_config 的测试隔离，不能用正式库代替 PG_TEST_DSN
$env:TESTING = '1'
python -m unittest discover -s tests -p test_amazonqac_search.py -v
```

有 `PG_TEST_DSN` 时测试读取现有真实数据测试库，使用独立 Python 过滤比对；没有时
数据库测试明确跳过。正式库真实 HTTP 验收单独记录请求、完整返回、精确总数、
EXPLAIN ANALYZE/BUFFERS、索引使用和排序分页比对，不以这些测试代替全量库验收。

官方机制依据：
[pg_trgm](https://www.postgresql.org/docs/17/pgtrgm.html)、
[模式匹配](https://www.postgresql.org/docs/17/functions-matching.html)、
[稳定 LIMIT/OFFSET 排序](https://www.postgresql.org/docs/17/queries-limit.html)。
