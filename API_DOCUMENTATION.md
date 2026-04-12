# API Documentation

## 归因服务接口

当前项目新增本机单用户归因服务，默认由 `uvicorn service.app:app --reload` 启动。

### 1. 创建归因任务

- 方法：`POST`
- 路径：`/tasks/attribution`

请求体：

```json
{
  "stock_name": "腾景科技",
  "ts_code": "688195.SH",
  "start_date": "2025-09-10",
  "end_date": "2026-03-09",
  "sample_label": "数据中心"
}
```

返回：

```json
{
  "task_id": "attr-xxx",
  "stock_name": "腾景科技",
  "ts_code": "688195.SH",
  "start_date": "2025-09-10",
  "end_date": "2026-03-09",
  "sample_label": "数据中心",
  "status": "queued",
  "stage": "created",
  "report_path": "",
  "plot_path": "",
  "log_path": "",
  "chatgpt_task_id": "",
  "progress_summary": "",
  "last_event_type": "",
  "last_command": "",
  "error": ""
}
```

### 2. 查询任务状态

- 方法：`GET`
- 路径：`/tasks/{task_id}`

返回任务当前状态对象。

### 3. 查询任务结果

- 方法：`GET`
- 路径：`/tasks/{task_id}/result`

返回：

```json
{
  "task_id": "attr-xxx",
  "report_path": "",
  "plot_path": "",
  "report_exists": false,
  "plot_exists": false,
  "chatgpt_task_id": ""
}
```

补充说明：

- `plot_path` 优先以正式报告中的图片引用为准；只有报告里没有图片引用时，才退回固定命名规则。
- 若任务对象本身尚未写入 `chatgpt_task_id`，服务会尝试从已生成报告的 `ChatGPT 联网归因` 小节中自动提取。

### 4. 执行归因任务

- 方法：`POST`
- 路径：`/tasks/{task_id}/run`

行为：

- 先执行一次数据库健康检查
  - 当前会校验 `event_news` 与 `event_quant` 是否可连接
  - 同时校验锚点表是否存在：
    - `event_news.event_metadata`
    - `event_quant.raw_stock_daily_qfq`
- 若数据库预检失败，任务会直接返回：
  - `status=failed`
  - `stage=datastore_health_check_failed`
  - `error=<明确失败原因>`
- 将任务状态切到 `running`
- 调用 `Codex App Server`
- 由 `Codex App Server` 去驱动 `stock-wave-attribution`
- 当前默认不是开放式探索，而是直接要求 `Codex` 执行：
  - `python skills/stock-wave-attribution/scripts/orchestrator.py run --stock-name ... --ts-code ... --start-date ... --end-date ... --sample-label ...`
- `codex app-server` 子进程默认优先注入本机代理：
  - `HTTP_PROXY=http://127.0.0.1:7897`
  - `HTTPS_PROXY=http://127.0.0.1:7897`
  - `ALL_PROXY=http://127.0.0.1:7897`
  - `NO_PROXY=localhost,127.0.0.1`
- 如果服务启动环境里已经显式设置同名代理变量，默认保留显式值，不强行覆盖
- 执行期间始终写任务日志到 `log_path`
- 执行期间会从 `App Server` 事件流持续提炼：
  - `progress_summary`
  - `last_event_type`
  - `last_command`
- 日志里会记录每一步收到的请求和返回，包括：
  - JSON-RPC request
  - response
  - notification
  - 命令输出增量
  - 代理环境摘要（`[proxy_env]`）
- 默认带超时控制；超时会返回 `failed/codex_timeout`
- 若事件流里提前出现 `stream_error`，会直接返回 `failed/codex_stream_error`
- 若执行成功，自动回写：
  - `report_path`
  - `plot_path`
  - `log_path`
  - `chatgpt_task_id`（若能从报告识别）
- 若执行失败，返回 `failed` 和错误信息

数据库预检失败示例：

```json
{
  "task_id": "attr-xxx",
  "status": "failed",
  "stage": "datastore_health_check_failed",
  "report_path": "",
  "plot_path": "",
  "log_path": "",
  "chatgpt_task_id": "",
  "progress_summary": "数据库健康检查失败",
  "last_event_type": "",
  "last_command": "",
  "error": "event_news 连接或表校验失败: connection refused"
}
```

成功返回示例：

```json
{
  "task_id": "attr-xxx",
  "stock_name": "国博电子",
  "ts_code": "688375.SH",
  "start_date": "2025-12-10",
  "end_date": "2026-01-14",
  "sample_label": "5G",
  "status": "completed",
  "stage": "completed",
  "report_path": "/abs/path/outputs/analysis/2026-03-25-688375SH-国博电子-wave-attribution.md",
  "plot_path": "/abs/path/data/plots/688375_SH_wave_candles.png",
  "log_path": "/abs/path/data/service_logs/attr-xxx.log",
  "chatgpt_task_id": "d7b0a15f-688a-41af-b738-ec8d9ab5290a",
  "progress_summary": "最近命令: python scripts/run.py",
  "last_event_type": "turn/completed",
  "last_command": "python scripts/run.py",
  "error": ""
}
```

超时返回示例：

```json
{
  "task_id": "attr-xxx",
  "status": "failed",
  "stage": "codex_timeout",
  "report_path": "",
  "plot_path": "",
  "log_path": "/abs/path/data/service_logs/attr-xxx.log",
  "chatgpt_task_id": "",
  "progress_summary": "最近命令: python - <<'PY'",
  "last_event_type": "item/started",
  "last_command": "python - <<'PY'",
  "error": "codex app-server timed out after 30s"
}
```

流错误返回示例：

```json
{
  "task_id": "attr-xxx",
  "status": "failed",
  "stage": "codex_stream_error",
  "report_path": "",
  "plot_path": "",
  "log_path": "/abs/path/data/service_logs/attr-xxx.log",
  "chatgpt_task_id": "",
  "progress_summary": "最近事件: stream_error",
  "last_event_type": "stream_error",
  "last_command": "",
  "error": "stream disconnected before completion: failed to lookup address information"
}
```

### 5. 续跑 ChatGPT 步骤

- 方法：`POST`
- 路径：`/tasks/{task_id}/retry-chatgpt`

行为：

- 先检查当前 ChatGPT 自动化会话状态
- 若 `loginCheck.ok=false`，返回 `409`
- 返回体里带明确原因，例如：
  - `blank_page`
  - `workspace_deactivated`
  - `logged_out`
  - `missing_composer`

### 6. 查询数据库健康状态

- 方法：`GET`
- 路径：`/health/datastores`

返回：

```json
{
  "ok": true,
  "summary": "event_news / event_quant 已就绪",
  "config_path": "/abs/path/skills/stock-wave-attribution/stock-wave-attribution.yaml",
  "datastores": [
    {
      "name": "event_news",
      "ok": true,
      "dsn": "postgresql://postgres:***@localhost:5432/event_news",
      "current_database": "event_news",
      "current_user": "postgres",
      "latency_ms": 3.21,
      "required_tables": {
        "event_metadata": true
      },
      "error": ""
    }
  ]
}
```

## 当前约束

- 仅支持本机单用户
- 仅支持单票归因
- 任务状态存储在 `data/service_tasks/`
- 执行日志存储在 `data/service_logs/`
- 报告继续落在 `outputs/analysis/`
