# 归因服务设计

## 目标

把当前基于本地数据库、`stock-wave-attribution` skill、`chatgpt-plus-browser` skill 的单票归因流程，收敛成一个本机可调用的“归因服务”。

这个服务只服务当前项目和当前使用者，不做平台化扩展，不做多用户，不做鉴权，不做分布式调度。

核心要求：

- 服务本身不重写归因逻辑
- 由服务负责任务编排与状态管理
- 由 `Codex CLI` 作为执行内核
- 由 `stock-wave-attribution` 作为归因事实来源
- 由 `chatgpt-plus-browser` 负责联网搜索补强

## 设计边界

第一版只支持：

- 单机运行
- 单用户使用
- 单任务串行执行
- 单票归因
- 本地 Markdown 报告产出
- 本地 PostgreSQL 和本地 Chrome profile

第一版明确不做：

- Web 前端
- 用户体系
- 权限控制
- 分布式任务队列
- 批量并发归因
- 通用多项目托管

## 核心架构

服务调用链固定为：

1. 归因服务接收任务
2. 服务生成标准任务上下文
3. 服务调用 `Codex CLI`
4. `Codex CLI` 按任务上下文执行 `stock-wave-attribution`
5. `stock-wave-attribution` 内部按需要调用：
   - 本地 `event_quant / event_news`
   - `chatgpt-plus-browser`
6. 结果落回：
   - `docs/analysis/*.md`
   - `data/plots/*.png`
   - `skills/chatgpt-plus-browser/.state/*.json`
   - 服务自身的任务状态目录

换句话说：

- 归因知识和报告口径在 skill
- 服务只做壳和调度
- `Codex CLI` 是执行引擎

## 服务能力

第一版服务只保留 5 个核心动作：

1. `create_attribution_task`
- 创建任务
- 输入：股票、时间窗、样本标签、可选主线提示
- 输出：`task_id`

2. `run_attribution_task`
- 启动任务
- 实际上生成并执行一条 `codex` 命令
- 由 `codex` 去调用 `stock-wave-attribution`

3. `get_task_status`
- 返回任务当前阶段
- 阶段至少包括：
  - `queued`
  - `running`
  - `waiting_chatgpt`
  - `completed`
  - `failed`

4. `get_task_result`
- 返回报告路径、图片路径、ChatGPT task id、错误信息摘要

5. `retry_chatgpt_step`
- 只针对 ChatGPT 联网归因失败或未完成时使用
- 不重跑本地量价和 news 抽取
- 只重发联网搜索或只做 `.state -> 报告` 回填

## 任务模型

建议服务内部维护一个轻量任务记录，例如 JSON 文件：

```json
{
  "task_id": "attr-20260326-001",
  "stock_name": "腾景科技",
  "ts_code": "688195.SH",
  "start_date": "2025-09-10",
  "end_date": "2026-03-09",
  "sample_label": "数据中心",
  "status": "running",
  "stage": "chatgpt_search",
  "report_path": "docs/analysis/2026-03-26-688195SH-腾景科技-wave-attribution.md",
  "plot_path": "data/plots/688195_SH_wave_candles.png",
  "chatgpt_task_id": "xxx",
  "error": ""
}
```

任务状态目录建议独立，例如：

- `data/service_tasks/`

这样服务层和 `chatgpt-plus-browser/.state` 分层清楚：

- `service_tasks` 管服务任务
- `.state` 管 ChatGPT 网页任务

## Codex CLI 集成

服务不直接 import 全量归因逻辑，而是调用 `Codex CLI`。

建议执行模式：

- 服务生成一条结构化 prompt
- prompt 明确要求：
  - 使用 `stock-wave-attribution`
  - 使用本地 PostgreSQL
  - 输出正式报告到 `docs/analysis`
  - 若 ChatGPT 登录态异常，报告里必须写占位与 `.state` 路径

这样做的原因：

- 复用已经沉淀的 skill 边界
- 避免再在服务里复制一套归因编排
- 以后改归因逻辑，优先改 skill，不改服务

## 错误处理

服务必须把错误显式分层：

1. 数据层错误
- PostgreSQL 不可用
- 数据为空
- 概念映射缺失

2. 执行层错误
- `Codex CLI` 调用失败
- 任务中途退出

3. ChatGPT 层错误
- `blank_page`
- `workspace_deactivated`
- `logged_out`
- `missing_composer`
- `.state` 未完成

4. 产物层错误
- 报告未生成
- 图片未生成
- 报告回填失败

原则：

- 不能模糊报“失败”
- 必须写清楚失败阶段和原因

## 产物约束

服务成功完成后，至少要能提供：

- 报告路径
- 图片路径
- 波段主结论摘要
- ChatGPT task id
- ChatGPT state 路径

即使 ChatGPT 联网没有完成，也必须有本地证据版正式报告。

## 推荐技术实现

第一版最小化建议：

- Python 服务
- `FastAPI` 或极简 `Flask`
- 本地 JSON 任务状态
- `subprocess` 调 `codex`

推荐 `FastAPI`，因为：

- 后面若需要本地 HTTP 调用更自然
- 状态接口和结果接口表达更清楚
- 不会引入过重复杂度

## 建议落地顺序

1. 先做任务模型和本地状态目录
2. 再做 `Codex CLI` 调用封装
3. 再做 3 个最小接口：
   - 创建任务
   - 查状态
   - 查结果
4. 最后做 `retry_chatgpt_step`

## 最终判断

第一版归因服务的本质不是“重写归因引擎”，而是：

**给现有 skill 套一层可重复调用、可查状态、可续跑的本地任务服务。**
