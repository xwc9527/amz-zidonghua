# AmazonQAC 双语关键词研究界面：真实验收报告

验收日期：2026-11-02（Asia/Singapore）；仓库 xwc9527/amz-zidonghua；开发分支 chatgpt/oxylabs-amazon-probe-v1。

## 交付结论与范围

已提供可运行的独立网页 `http://127.0.0.1:8093/research`，用现有检索接口查询真实 PostgreSQL 词库，按页自动调用本地模型翻译，并持久缓存。支持检索模式、组合包含/排除、频次/词数范围、排序、分页、完整文本对照及复制。没有修改原商品看板、采集流程或检索/CSV 的筛选 SQL。

可以用于对照英文原文开展人工研究。中文仍属于辅助机译：格式与数字/型号保护通过并不等于语义准确；不能把本轮结果解释为翻译全面准确或长期运行稳定性已获证明。纯专名与无法可靠生成中文的条目有单独状态。

## 数据与运行环境

- PostgreSQL 17.11，复用本机既有持久化 amazonqac，127.0.0.1:55432。
- 原表 amazonqac.keywords：实际 count = **39,588,974**；sum(popularity) = **324,657,797**；sum(observations) = **395,550,004**，均与原导入验收值一致。
- 原词和 popularity 与独立 SQL 按当前页逐条核对；未执行原表 UPDATE、DELETE 或数据重导入，也未添加检索索引。
- 数据来源 AmazonQAC Train，US，2023-09-01 至 2023-09-30。Popularity 是历史原值，不是当前月搜索量。
- 本地 RTX 4060 Ti 8 GB；Qwen3-4B-Instruct-2507 Q4_K_M + llama.cpp b12032 CUDA 12.4。无外部翻译 API 调用或新增 API 费用。
- 模型目录实测 4,557,040,479 字节（约 4.24 GiB，含候选模型试验文件、压缩包、解压运行库和日志）；GPU 实测总占用约 5,770 MiB，含本机其他 GPU 负载，非隔离模型峰值。
- 最终翻译引擎：`qwen3-4b-instruct-2507-q4km-3605803b-translation-v5`。缓存以 engine + 原词区分版本，开发期间旧版本记录保留，不会在最终版本命中。

## 真实接口性能与检索核验

下表均为本机真实 HTTP 计时（秒），模型已加载。初次翻译列有的包含前面测试已缓存的词，**不能当作整页全部冷缓存时间**；缓存数明确列出。Repeat 对同一页再次请求，返回译文与缓存状态。测试顺序、完整请求体、原词、popularity、翻译状态和服务端计时都在 http-acceptance.json。

| 测试 | 匹配总数 | 本页行数 | 检索 s | 初次翻译 s | 初次缓存行数 | Repeat s | Repeat缓存行数 |
|---|---:|---:|---:|---:|---:|---:|---:|
| behind-5 | 133 | 5 | 0.1801 | 1.3394 | 0 | 0.1359 | 5 |
| behind-10 | 133 | 10 | 0.1868 | 1.4319 | 5 | 0.0148 | 10 |
| behind-25 | 133 | 25 | 0.1804 | 2.8686 | 10 | 0.0767 | 25 |
| behind-50 | 133 | 50 | 0.1837 | 4.5743 | 25 | 0.2274 | 50 |
| behind-100 | 133 | 100 | 0.1792 | 8.1813 | 50 | 0.4171 | 100 |
| kitchen-25 | 1000 | 25 | 0.2091 | 3.8497 | 0 | 0.2937 | 25 |
| combo | 41 | 5 | 0.1134 | 0.9286 | 0 | 0.0144 | 5 |
| storage-words | 5719 | 5 | 0.1543 | 0.6387 | 1 | 0.0296 | 5 |
| couch-asc-page1 | 3622 | 10 | 0.2301 | 1.5699 | 0 | 0.0148 | 10 |
| couch-asc-page2 | 3622 | 10 | 0.2435 | 1.6907 | 0 | 0.1295 | 10 |
| iphone-exact | 1 | 1 | 0.1741 | 0.4169 | 0 | 0.0141 | 1 |
| dimensions | 17525 | 10 | 0.1528 | 1.8 | 0 | 0.0144 | 10 |
| brand | 204 | 5 | 0.0231 | 1.0256 | 0 | 0.1372 | 5 |

- behind couch table：实际 133 条匹配，5/10/25/50/100 条页均完成；100 条页面为 2 页。
- 多维组合：sofa table，包含 outlet，排除 white，popularity 2–10000，词数 3–8，匹配 **41** 条；浏览器操作结果与真实接口一致。
- storage 完整词 + kitchen 包含 + bag 排除：**5719** 条。完整词边界沿用原有 PostgreSQL 逻辑。
- couch，popularity >=10、最多4词、升序，**3622** 条；前两页各10条无交集，20条联合满足 popularity 升序、同频次原词 C 排序。
- iphone 15 精确匹配 **1** 条，popularity **53720**；24 inch 包含匹配 **17525** 条；hoobro **204** 条。
- 浏览器实际观察：5行缓存约0.02s；50行缓存约0.22s；100行缓存约0.33s；厨房橱柜25行新译约4.12s。这些浏览器观测来自开发阶段相应引擎和当时缓存状态，不能与上表最终版本的服务端计时混为一次请求。
- 最终 HTTP 测试网页进程 RSS 采样最大 **68.57 MiB**，不是持续采样峰值，且不含模型进程/GPU、浏览器或数据库。

## 翻译结果、质量及限制

最终13组页面覆盖 **170** 个不同原词：169 条返回含中文译文，1 条原样保留（未生成中文），0 条校验失败。额外品牌/型号/尺寸核验18条：12 条含中文、4 条原样保留（未生成中文）、2 条明确失败。这是定向样本的状态统计，**不是随机抽样准确率或语义正确率**。

| 原词 | 最终中文辅助信息 | 原 popularity | 状态 |
|---|---|---:|---|
| behind couch table | 沙发后方桌子 | 1054 | ok |
| behind couch table skinny 36 tall | 沙发后方细长36高桌 | 9 | ok |
| behind couch table skinny 60 inch long | 沙发后方细长60英寸长桌 | 7 | ok |
| behind couch table 8ft | 沙发后方8ft桌 | 4 | ok |
| 24 inch monitor | 24英寸显示器 | 3394 | ok |
| squishmallows 24 inch jumbo | squishmallows 24英寸大号 | 3840 | ok |
| hoobro console table | hoobro玄关桌 | 30 | ok |
| kitchen organizer | 厨房收纳用品 | 2443 | ok |
| iphone 15 | iphone 15 | 53720 | name_only |

数字串、混合字母数字型号以及已识别/模型声明专名执行原样校验；不会用变更数字的译文充当成功。例：36 tall 没有原始单位，最终译文也未擅自加“英寸”；60 inch ->60英寸；8ft 原样保留；hoobro/iphone/vasagle 等不做品牌重命名。数字校验并不构成任意新词条语义正确或所有未知品牌均被识别的保证。

最终补充核验的明确失败：
- `geforce rtx`：未生成中文译文 / No Chinese translation generated，中文为 null。
- `nvidia rtx`：未生成中文译文 / No Chinese translation generated，中文为 null。

纯专名 ikea、vasagle、iphone 15 等没有可靠中文译名时显示“原样保留，未生成中文译文”，不把未译英文当作已完成中文。原词保留在独立英文列。失败不进入成功缓存，可重试。

人工阅读发现的残余质量问题：最终版本 kitchen organizer cart ->“厨房收纳 carts”，普通名词未完全翻译；under sink kitchen organizer ->“橱柜下收纳用品”，水槽位置被泛化。开发阶段还发现 pantry/rattan 未译、shelves 曾被译为“柜子”、cookie sheet 词义不够准确等情况；最终版本已把 shelves 样本译为“隔板”。这些说明本地小模型只适合辅助阅读，采购或品类判断仍应对照原文。没有用词典批量改写原始搜索数据，也没有伪造“人工审核全部准确”的结论。

方案选择有真实证据：Argos en_zh1.9 的24个真实词条试验把 ikea、vasagle 等译错，因此未用于正式网页；初版 Qwen 批量上下文出现跨词条信息串入，改为各词独立上下文。之后针对实测 console table / gaming console 混淆、Arabic数字转汉字等问题修正提示，并重跑最终验收。开发阶段 JSON 与截图保留供追溯，最终结果以 http-acceptance.json 和 final-fidelity-and-restart.json 为准。

## 缓存与失败恢复

- 独立缓存表 amazonqac_ui.translations，仅新增该 schema/table；原表保持不变。engine + original COLLATE C 主键，中文、状态、preserved_terms、created_at。
- Repeat 页命中全部可缓存结果；失败不缓存。缓存表在补充核验时实际占用 **237,568 字节**（包括旧引擎记录及索引），会随实际阅览量增长。
- 已实际重启网页及本地模型：原有缓存可重新读取。最终网页重启的3条全命中证据见 final-fidelity-and-restart.json 的 cache_after_web_restart；不是仅浏览器内存缓存。
- 实际停用本地模型时，kitchen storage 搜索仍返回 **1890** 条总数，当前5条英文和频次正常；其中3条缓存译文可读，2条未缓存显示不可用及重试。见04截图（开发版本v3，故障机制与最终一致）。模型恢复后可重试。
- 页面异步返回有版本号及取消控制，快速换页不会用旧页译文覆盖新页。服务端已开始的本地批次可能继续完成并写缓存。

## 浏览器、回归与交付文件

- 实际浏览器覆盖检索、全部筛选控件、升降序、下一页、页码跳转、5/10/25/50/100行、三列表头、原/译文复制、全文弹窗和品牌型号状态。desktop 1440×1000 临时视口测试后恢复默认视口；页面窄屏可横向滚动表格。完整 DOM 证据见 browser-acceptance.json；不能把截图看不到的下方行当作已截入截图。
- 本次12项自动测试全部通过，使用 PG_TEST_DSN 独立真实数据库，详见 unit-integration-tests.txt。模型替身只测试校验/缓存分支，未替代上述真实模型和真实浏览器验收。
- 原 CSV 回归：kitchen organizer **1000** 行、**250479** 字节、UTF-8 BOM、HTTP **0.2056s**；原词、popularity及顺序与独立 SQL 逐行一致。SHA256 `54d8e7c5035418fd07551745827948a50a8ba656ea3c68470717077a60369806`。未为本轮重做39M全库导出。
- 截图01/05/06/07分别为开发版本双语页、尺寸品牌、厨房橱柜、长词条弹窗，保留当时真实现象。最终版本截图以09/10的 final-v5 文件为准。原始 JSON、截图、CSV 和测试日志在本地交付目录及证据包；源码仓库提交代码、启动说明及本报告，不提交模型和大体积运行产物。
- 启动方式见 AMAZONQAC_RESEARCH.md；本地一键恢复脚本 start-research.ps1 直接复用现有 PG 配置，不含密码。模型与数据库在仓库外持久化，本地服务仅绑定环回地址；不是开机自启服务。
- Commit SHA 与远程一致性见 delivery-receipt.json（提交后写入，避免把提交 SHA 写进自身提交导致循环）。gpt 基准保持 a2098cf79211a9257a673a6932edac2a5d710f26。

## 依据与来源

- [Amazon 官方 AmazonQAC 数据](https://huggingface.co/datasets/amazon/AmazonQAC)
- [Qwen 官方模型卡：Apache-2.0](https://huggingface.co/Qwen/Qwen3-4B-Instruct-2507)
- [Unsloth GGUF 模型来源](https://huggingface.co/unsloth/Qwen3-4B-Instruct-2507-GGUF)
- [llama.cpp 官方 b12032 发布](https://github.com/ggml-org/llama.cpp/releases/tag/b12032)

未对全库翻译，未增加生成式拓词、语义检索、前端选品看板、CSV新入口或外部翻译服务。
