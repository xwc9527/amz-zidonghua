# AmazonQAC CSV 全结果导出

独立接口：`POST /api/v2/amazonqac/export`，与现有检索入口使用同一个
`SearchFilters`、`build_filter()` 和 `build_order()`。
导出所有匹配词条，不截断到 1000 条，不循环调用分页接口。
数据库、索引和原始数据均不需要变更。

## 启动

在已有依赖环境、仓库目录启动原独立服务：

```powershell
. 'C:\Users\Administrator\Documents\Codex\services\amazonqac-postgresql\load-env.ps1'
$env:TESTING = '0'
python -m uvicorn amazonqac_search:app --host 127.0.0.1 --port 8091
```

当前开发环境 Python 依赖在 `work/api-python-deps`，启动前应使该目录和仓库在
`PYTHONPATH`。其他部署复用已安装的 FastAPI、Pydantic 2、uvicorn 和 psycopg2。
凭证只通过现有 `pg_config.get_pg_dsn()` 机制读取，不放在导出请求或 URL 中。

## 下载

另开终端，PowerShell 直接保存响应到文件：

```powershell
$body = @{
    query = 'storage'
    include_terms = @('kitchen')
    exclude_terms = @('bag', 'cabinet')
    popularity_min = 10
    word_count_min = 2
    word_count_max = 6
    sort = 'popularity_desc'
} | ConvertTo-Json
Invoke-WebRequest -Method Post -Uri 'http://127.0.0.1:8091/api/v2/amazonqac/export' `
    -ContentType 'application/json' -Body $body -OutFile '.\keywords.csv' -UseBasicParsing
```

支持现有全部筛选字段：`query`、`match_mode`、`include_terms`、`exclude_terms`、
`popularity_min/max`、`word_count_min/max`、`sort`，校验和组合语义完全沿用检索。
从检索请求复制条件时，去掉 `page`、`page_size`；导出不接受这两个字段，传入返回 422，
避免误以为分页参数会限制导出。空对象 `{}` 表示全部词库，行数没有应用层上限。

Python 客户端应流式写文件，不使用 `response.content` 加载大文件：

```python
import requests

with requests.post(
    'http://127.0.0.1:8091/api/v2/amazonqac/export',
    json={'query': 'sofa table', 'match_mode': 'words', 'sort': 'popularity_asc'},
    stream=True, timeout=(10, 3600),
) as response:
    response.raise_for_status()
    with open('sofa-table.csv', 'wb') as output:
        for chunk in response.iter_content(1024 * 1024):
            if chunk:
                output.write(chunk)
    print('Expected rows:', response.headers['X-Export-Row-Count'])
```

也可直接在已有 Python 环境中调用，输出目标须为二进制文件：

```python
from amazonqac_search import SearchFilters, write_csv_export

with open('storage.csv', 'wb') as output:
    result = write_csv_export(SearchFilters(query='storage'), output)
print(result)  # rows、elapsed_ms
```

## 文件格式和来源

UTF-8 编码，开头有 UTF-8 BOM；逗号分隔、首行为列名。由 PostgreSQL 标准 CSV
编码处理逗号、双引号、CR/LF 和非 ASCII 字符，不对词条添加前缀、去空格或改写。
原始 `popularity` 按十进制整数输出，不累加、不替换为其他计数。

| CSV 列 | 内容 |
| --- | --- |
| `final_search_term` | 原始搜索词，逐字保留 |
| `popularity` | 原始历史频次 |
| `source_id` | 原库来源主键 |
| `dataset_name` | 数据集名称 |
| `dataset_revision` | 原库记录的固定来源版本 |
| `source_url` | 官方数据集链接 |
| `split` | 原库 Train 来源标记 |
| `market` | 历史市场 |
| `period_start`, `period_end` | 原库统计起止日期 |
| `popularity_definition` | 原库保存的历史频次口径全文 |

来源信息逐行附带，因此文件单独保存后仍能解释频次口径；大文件会重复这些字段。
空结果返回 BOM 加同样的 CSV 表头，数据行数为 0。
读取 CSV 时应使用 CSV 解析器计数，词条内换行不能按物理行数判断记录数。
Excel 中可按 UTF-8 和文本列导入，以保留原始词条的文本含义。

排序与检索一致：`popularity ASC/DESC, final_search_term COLLATE "C" ASC`。
精确计数和 COPY 使用同一个只读 REPEATABLE READ 快照，避免单次导出内部计数漂移。

## 大结果集与失败行为

- PostgreSQL `COPY TO STDOUT` 连续写入二进制文件，不使用关键词结果 `fetchall()`。
  数据库排序由 PostgreSQL 执行，必要时使用其临时磁盘排序。
- HTTP 先生成临时 CSV，成功关闭文件和只读事务后再返回下载；生成耗时会计入首字节等待。
  这样数据库查询或写盘失败不会先返回 200 再给出不完整 CSV。
- 文件通过 FileResponse 分块发送，提供 `Content-Length`；下载后或传输异常时删除服务端临时文件。
  客户端应核对下载完整性；`X-Export-Row-Count` 为该快照下的精确行数，
  `X-Export-Generation-Ms` 为生成耗时（含数据库连接、计数和 COPY，不含下载）。
- 临时文件使用 Python 的系统临时目录，可在启动前通过标准 `TEMP/TMP` 环境配置到有足够
  空间的磁盘。导出时服务端临时文件和客户端目标文件均占磁盘。
- 为允许全量导出，此路径不使用交互式检索的 60 秒 SQL 超时；大导出需等待数据库排序和
  文件生成，并按实际规模设置客户端/代理超时。没有后台任务队列或断点续传。
- 无效条件返回 422；数据库不可用返回 503；数据库执行失败返回 500；
  文件写入失败返回 507。错误不回传连接串或原始数据库异常。
- 进程强制结束时系统临时目录可能留下文件；本阶段不增加持久化导出任务管理系统。

官方机制：[PostgreSQL COPY](https://www.postgresql.org/docs/17/sql-copy.html)、
[psycopg2 copy_expert](https://www.psycopg.org/docs/cursor.html#cursor.copy_expert)。
