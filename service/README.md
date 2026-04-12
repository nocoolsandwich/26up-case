# attribution service

这是当前项目的本机单用户归因服务。

## 目标

- 接收单票归因任务
- 用 `Codex App Server` 作为执行内核
- 调用 `stock-wave-attribution` skill 生成正式报告
- 维护本地任务状态
- 暴露最小任务查询与续跑接口

## 当前能力

- `GET /health/datastores`
  - 查询 `event_news / event_quant` 是否可连，以及关键锚点表是否存在
- `POST /tasks/attribution`
  - 创建归因任务
- `GET /tasks/{task_id}`
  - 查询任务状态
- `GET /tasks/{task_id}/result`
  - 查询结果路径
- `POST /tasks/{task_id}/run`
  - 触发任务执行，并在成功后回写报告路径、图片路径、日志路径和进度摘要
- `POST /tasks/{task_id}/retry-chatgpt`
  - 历史兼容接口，当前正式服务链默认不使用

## 当前约束

- 只支持本机单用户
- 只支持单票归因
- 只支持本地 JSON 任务状态
- 只支持 `Codex App Server` 驱动 skill，不直接重写归因逻辑

## 本地启动

```bash
uvicorn service.app:app --reload
```

## 任务状态目录

- `data/service_tasks/`
- `data/service_logs/`

## 产物目录

- 报告：`outputs/analysis/`
- 图片：`data/plots/`
- 粗排工件：`data/service_tasks/<task-id>/agent_rerank/`

## 当前执行口径

- `/tasks/{task_id}/run` 通过 `Codex App Server` 驱动 `stock-wave-attribution`
- `/tasks/{task_id}/run` 开始前会先做数据库预检
  - 当前检查：
    - `event_news` 可连接且存在 `event_metadata`
    - `event_quant` 可连接且存在 `raw_stock_daily_qfq`
  - 若失败，任务直接回写：
    - `status=failed`
    - `stage=datastore_health_check_failed`
    - `error=<明确失败原因>`
- 服务 prompt 默认不是让 Codex 自己探索仓库，而是直接执行两阶段链路：

```bash
python skills/stock-wave-attribution/scripts/orchestrator.py prepare-agent-rerank \
  --stock-name <名称> \
  --ts-code <代码> \
  --start-date <开始日期> \
  --end-date <结束日期> \
  --sample-label <标签> \
  [--skip-concept] \
  --task-id <任务ID>

python skills/stock-wave-attribution/scripts/orchestrator.py finalize-agent-rerank \
  --stock-name <名称> \
  --ts-code <代码> \
  --start-date <开始日期> \
  --end-date <结束日期> \
  --sample-label <标签> \
  [--skip-concept] \
  --task-id <任务ID> \
  --selection-path data/service_tasks/<任务ID>/agent_rerank/final_selection.json
```

- 当单票概念底表缺失、但你仍然要先产出归因时，可显式追加 `--skip-concept`
  - 此时量价、news、波段归因仍正常执行
  - `概念联动验证表` 会允许为空

- 两阶段之间由 Codex 读取 `summary.json` 和各波段 `rough_chunks/chunk_*.md`
  - 每个 chunk 直接 `100选3-5`
  - 不做逐条打分
  - 再从粗排并集里直接精选最终 10 条

- App Server 会话会显式注入收敛后的 `baseInstructions / developerInstructions`
  - 默认禁止先做无关 skill 阅读和全仓库扫描
  - 默认只允许先读 `orchestrator.py`、`scripts/attribution_data.py`、`scripts/wave_segmentation.py`、`scripts/wave_plotting.py`
- `codex app-server` 子进程默认优先走本机代理 `http://127.0.0.1:7897`
  - 默认注入：`HTTP_PROXY / HTTPS_PROXY / ALL_PROXY`
  - 同时注入：`NO_PROXY=localhost,127.0.0.1`
  - 如果外部已经显式设置这些代理环境变量，服务不会覆盖
- 任务运行期间始终写 `log_path`
- 默认有超时控制；超时会回写 `failed / codex_timeout / error / log_path`
- 任务执行期间会从 `App Server` 事件流持续提炼：
  - `progress_summary`
  - `last_event_type`
  - `last_command`
- 日志里会按步骤记录：
  - 发出的 JSON-RPC request
  - 收到的 response
  - 收到的 notification
  - 命令输出增量
  - 当前生效的代理环境摘要（`[proxy_env]`）
- 任务成功后，服务会自动扫描归因产物并回写：
  - `report_path`
  - `plot_path`
  - `log_path`
  - `chatgpt_task_id`
- `chatgpt_task_id` 目前只作为历史兼容字段保留；正式服务链默认为空
- `plot_path` 以正式报告里的图片引用为优先真相源；只有报告里没有图片引用时，才退回固定命名规则
- `chatgpt_task_id` 优先读取任务对象；为空时，再尝试从报告中的 `.state/<task-id>.json` 路径提取

## 数据库异常时先查什么

默认先查服务层暴露的预检接口：

```bash
curl http://127.0.0.1:8000/health/datastores
```

如果返回 `ok=false`，再去看：

```bash
brew services list | grep postgresql@16
python - <<'PY'
import socket
s = socket.socket()
s.settimeout(1)
print(s.connect_ex(('127.0.0.1', 5432)))
s.close()
PY
```

确认 `5432` 已监听后，再按 [docs/project-datastores.md](/Users/zhengshenghua/Library/Mobile%20Documents/com~apple~CloudDocs/work/my/case_data/docs/project-datastores.md) 的恢复步骤检查数据库和 dump。 
