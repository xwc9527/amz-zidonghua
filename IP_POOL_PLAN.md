# IP 池动态化改造计划（L0–L5）

> 前置条件：**当前 probe 抓取任务（US/DE/JP）全部跑完后再执行**，避免中途停任务。
> 目标定义「动态最佳」= ① 始终用满当前可用独立 IP ② 自愈（掉线剔除/恢复纳入，worker 跟随增减）③ 免人工（开爬前不用手动 refresh）④ 不自伤（健康检测本身不打死池子）。
> 现实天花板：动态池只保证「永远用满」，上限仍是机场独立出口 IP 总数；对 `/s` 这类 RTT 受限页面，IP 增加只线性提吞吐，不破单 IP 速度墙。

---

## L0：榨干现有 IP ✅ 已完成（8 → 20 独立IP）

> **结果**：诊断发现 51 节点被两刀砍剩 11——① Hysteria2 协议砍 18 个（误杀，配置完整、16 个独立 server 与现有零重叠）② CDN 域名砍 18 个（砍对，12+5+1 挤 3 个 CDN 域名，出口不独立）。
> **动作**：`SKIP_PROTO` 去掉 `hysteria2`，只保留 `hysteria` v1。CDN 过滤保持。
> **验收**：refresh 后候选 11→29，去重 **20 个独立出口 IP**，hy2 节点 Mihomo 零报错正常加载。worker 上限 8→20。

### 原始分析（保留）


**问题**：实测链路 `probe 48 可用 → _load_nodes 过滤剩 11 → 去重 8 独立IP`。48→11 这一刀砍在 `start_lb_proxy._load_nodes` 的 `CDN_SERVERS` 黑名单 + 「profile 找不到配置即丢弃」。

**动作**
- 读 `start_lb_proxy.py` `_load_nodes()`，逐条统计 48→11 被砍原因（CDN 命中 / profile 缺失 / SKIP_PROTO / SKIP_NAMES）。
- 区分「误杀」（出口 IP 独立的好节点）与「应砍」（真 CDN/重复出口）。
- 放宽过滤，把误杀的节点放回候选。

**验收**：放宽后重新 `start`，独立 IP 数 > 8（目标十几个）。
**风险**：低，纯过滤调整，不改运行逻辑。

---

## L1–L3 ✅ 已完成（动态池核心，新模块 proxy_pool_manager.py）

> **L1 地基**：Mihomo 常驻开全部 29 端口，`lb_pid.json` 记录全端口列表，`proxy_pool.json` = 去重活跃快照。已就绪，无需改动。
> **L2 健康检测**：`PoolManager._scan()` 并发校验全部端口，判据改用 ipify/checkip.amazonaws.com/ifconfig.me（弃用会限流的 ip-api），按出口 IP 去重。实测 29 端口→20 独立IP，5.1s，与 start_lb_proxy 完全一致。被动信号：worker `report(port, ok)` 上报真实请求成败，连续失败端口主动复核。
> **L3 弹性 worker**：`probe_na_valid.py` 接入 PoolManager——bootstrap 后为每个活跃 IP spawn worker，`start_monitor` 周期复核，新 IP 回调 spawn、掉线端口 worker 自身 `should_stop` 优雅退出（队列共享不丢任务）。实测 20 worker 端到端跑通，143 节点 34s。
> **net**：worker 上限 8/10 → **20**，且运行中节点掉线/恢复自动增减。

### 原始设计（保留）

## L1：Mihomo 预开满 + 常驻（解耦地基）

**问题**：现在 Mihomo 只开「去重后 8 端口」，池子一变就要重启，`reload` 会中断在途连接。

**动作**
- `start_lb_proxy.py`：启动时把**放宽过滤后的全部候选节点**（40+）都开成 listener，端口 18001–180NN 全部常驻。
- Mihomo 进程不再因池变化而重启；「动态」上移到爬虫侧。
- `proxy_pool.json` 语义改为「全部端口 + 各自当前出口 IP 的快照」，而非「去重后的存活子集」。

**验收**：Mihomo 单次启动后常驻，端口全集稳定可查。
**风险**：低，多开 listener 资源开销可忽略。

---

## L2：健康检测 = 被动为主 + 主动兜底（避免自伤）

**问题**：ip-api.com 二次校验对本机高频查询限流，是之前 8→6 的元凶。

**动作**
- **被动（主力）**：worker 统计每端口最近 N 次**真实业务请求**成功率，连续失败 → 标记可疑。零额外开销。
- **主动（兜底）**：仅对「可疑端口」或低频轮（默认 30 分钟，可调）做轻量校验；**判据弃用 ip-api.com**，换稳定 echo-ip 端点或复用 Amazon 轻页面。
- 校验后按出口 IP 去重，产出「当前活跃独立 IP 集」。

**验收**：节点掉线能在一轮内被剔除，恢复能被重新纳入；校验不再误杀。
**决策点**：主动探活频率——节点稳则拉长到 30 分钟甚至仅告警触发（倾向「被动为主」）。

---

## L3：弹性 worker（跟随活跃集增减）

**动作**
- 引入 `PoolManager` 持有活跃 IP 集，定期刷新。
- 共享任务队列 `q`（已有）。
- 新 IP 上线 → spawn 绑该端口的 worker，直接 `q.get()` 干活。
- IP 掉线 → 给对应 worker 发退出信号，做完当前节点优雅退出。
- worker 数量实时 = 活跃独立 IP 数。

**验收**：跑测过程中手动断一个节点，worker 自动减；恢复后自动加；总数始终 = 活跃 IP 数。
**风险**：中，涉及线程生命周期管理，需测试优雅退出与队列不丢任务。
**适用**：`probe_na_valid.py` 和 `fetch_subtree.py` 共用同一套 PoolManager。

---

## L4：免人工自动化 ✅ 已完成

> **实现**：`proxy_pool_manager.py` 新增 `ensure_proxy_ready()` 函数——检查 Mihomo 进程存活 + `proxy_pool.json` 是否在 10 分钟内。任一不满足 → 自动调用 `start_lb_proxy.py refresh`。
> `probe_na_valid.py` 的 `main()` 在 `mgr.bootstrap()` 前调用 `ensure_proxy_ready()`。
> **效果**：直接 `python probe_na_valid.py` 即可，无需先手动 refresh。

---

## L5：自适应叠加 — 跳过（ROI 不足）

> BFS 已够快（900/min），`/s` 落地页不适用叠加（叠即 robot）。边际收益不值 2h 开发 + 熔断调参风险。

---

## 执行顺序与节奏

| 阶段 | 工作量 | 收益 | 状态 |
|------|--------|------|------|
| L0 放宽过滤 | ~30 分钟 | 8→20 IP | ✅ |
| L1 预开满常驻 | ~1 小时 | 解耦地基 | ✅ |
| L2 健康检测 | ~2 小时 | 自愈核心 | ✅ |
| L3 弹性 worker | ~2 小时 | 动态核心 | ✅ |
| L4 全自动 | ~30 分钟 | 免维护 | ✅ |
| L5 自适应叠加 | — | — | 跳过 |

**完结**。L0–L4 全部交付，IP 池动态化改造收工。
