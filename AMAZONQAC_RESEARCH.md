# AmazonQAC 双语关键词研究页

独立网页：`http://127.0.0.1:8093/research`。继续使用 `amazonqac_search:app` 的原检索与 CSV 接口；网页仅提交原有筛选参数，不实现另一套数据库检索。英文原词和 popularity 来自原表，译文单独保存。

## 当前 Windows 环境启动

现有 PostgreSQL 必须保持运行。普通 PowerShell 中执行；路径参数均为本地路径，不含凭证：

```powershell
powershell.exe -NoProfile -ExecutionPolicy RemoteSigned -File ".\start_qac_research.ps1" `
  -Python "C:\Users\Administrator\.cache\codex-runtimes\codex-primary-runtime\dependencies\python\python.exe" `
  -ModelRoot "C:\Users\Administrator\Documents\Codex\data\amazonqac-translation" `
  -DependencyPath "..\api-python-deps" `
  -PgEnvScript "C:\Users\Administrator\Documents\Codex\services\amazonqac-postgresql\load-env.ps1"
```

在本仓库目录运行。启动脚本以隐藏窗口启动两个本地进程：llama.cpp `18191`、研究页 `8093`，日志在模型目录 `logs/`，不会重启 PostgreSQL。已有服务可复用。模型首次加载需要几秒钟。端口被其他程序占用时请检查日志，不要结束无关服务。通过重跑脚本恢复服务；不是 Windows 自动启动服务。

依赖仍为原 `requirements.txt` 的 FastAPI、uvicorn、psycopg2；生产翻译连接使用 Python 标准库，无付费 API 或新 Python 翻译依赖。迁移机器时先安装原项目依赖并安全配置原 `PG_DSN`，不要把密码放进命令或 Git。

## 首次准备本地模型

当前机器已经完成下载及 SHA256 校验，无需重复下载。新环境可运行：

```powershell
python setup_qac_translation.py --root "D:\persistent\amazonqac-translation"
```

固定 Qwen3-4B-Instruct-2507 Q4_K_M、llama.cpp b12032 Windows CUDA 12.4 构建；下载文件约 3.17 GB，另需解压空间。现机 RTX 4060 Ti 8 GB 已验证，CPU 或其他硬件性能未验收。本脚本为该 Windows/CUDA 环境准备，不是跨平台安装器。模型、运行库及日志放在仓库外的持久化目录。

- [Qwen 官方模型卡，Apache-2.0](https://huggingface.co/Qwen/Qwen3-4B-Instruct-2507)
- [Unsloth GGUF 量化来源，Apache-2.0](https://huggingface.co/unsloth/Qwen3-4B-Instruct-2507-GGUF)
- GGUF 固定 revision：`a06e946bb6b655725eafa393f4a9745d460374c9`
- GGUF SHA256：`3605803b982cb64aead44f6c1b2ae36e3acdb41d8e46c8a94c6533bc4c67e597`
- [llama.cpp 官方 b12032 发布](https://github.com/ggml-org/llama.cpp/releases/tag/b12032)

下载器内固定全部三个文件的 SHA256，生成 `verified-runtime.json`。不自动更新模型。推理仅访问环回地址，关键词不发送至外部翻译服务。

## 使用与数据边界

- 输入检索词，选择包含匹配、完整词或词组、整条精确匹配；包含/排除词每行一个，与频次、词数范围组合后点击检索。
- 原接口负责过滤、总数和 popularity 排序；相同频次按原词 C 排序保证稳定分页。概念词仍为文字匹配，不做同义词扩展。
- 先显示英文和原始频次，随后每批最多 10 条自动翻译当前页；可选每页 5/10/25/50/100 条。翻页会取消旧页前端等待；已经在本地推理的批次可能完成并缓存。
- 全文自动换行，可弹窗对照完整原文与译文；支持复制。浏览器禁止剪贴板权限时可直接选中文本复制。
- 中文是辅助机译，不能替代原文。失败显示原因及重试按钮；未能生成中文、仅原样保留时明确提示。模型可能仍有词义误判或未译普通词，不能把自动校验通过当作翻译完全准确。
- 不更改 `amazonqac.keywords`、原始 popularity、既有索引或商品采集业务。不增加 CSV 网页操作；原 `/api/v2/amazonqac/export` 合约保持不变。

## 翻译接口及缓存

```http
POST /api/v2/amazonqac/translate
Content-Type: application/json

{"terms":["behind couch table","24 inch monitor"]}
```

每次 1–10 条，原词不 trim/改写；返回 `items`（`original/chinese/status/preserved_terms/cached`）、`cache_hits`、`translated`、`failed`、`elapsed_ms`、`engine`。`status` 为 `ok`、`name_only` 或 `failed`；失败时 `chinese=null`，不会将原文充作中文。原样保留的 `name_only` 与正常中文区分显示，且不提供“复制译文”，完整弹窗的中文复制禁用。

缓存独立于原始词库：首次翻译创建 `amazonqac_ui.translations`，需要当前开发角色具备建 schema/table 权限。主键 `(engine, original COLLATE "C")`，附中文、状态、保留品牌/型号列表、时间。缓存随 PostgreSQL 持久化，重启网页或模型不丢失。只缓存通过当前校验的结果，不缓存失败；升级翻译规则会换 engine，不把旧版错误译文复用到新版。原有版本记录保留供追溯。

数字串出现次数、已识别品牌/混合字母数字型号及模型声明的专名原样校验；不满足则拒绝译文。该校验不等于全自动语义审查，也不保证识别所有未知品牌。每个原词独立上下文、温度 0，最多 4 路本地推理。默认 `QAC_LOCAL_TRANSLATOR_URL=http://127.0.0.1:18191`，只允许环回 HTTP。

## 验证

```powershell
# 必须先加载独立 PG_TEST_DSN；pg_config.py 按原规则校验隔离。
python -m unittest discover -s tests -p "test_amazonqac*.py" -v
```

本轮实际 PostgreSQL 测试 12 项通过，其中原检索/CSV 9 项，新翻译校验与持久缓存 3 项。测试中的模型替身仅用于故障及缓存断言；交付验收另经真实 39,588,974 条词库、真实本地模型和浏览器完成，详见 `AMAZONQAC_RESEARCH_ACCEPTANCE.md`。
