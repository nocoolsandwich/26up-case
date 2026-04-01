# attribution service

这是当前项目的本机单用户归因服务。

## 目标

- 接收单票归因任务
- 用 `Codex App Server` 作为执行内核
- 调用 `stock-wave-attribution` skill 生成正式报告
- 维护本地任务状态
- 暴露最小任务查询与续跑接口

## 当前能力

- `POST /tasks/attribution`
  - 创建归因任务
- `GET /tasks/{task_id}`
  - 查询任务状态
- `GET /tasks/{task_id}/result`
  - 查询结果路径
- `POST /tasks/{task_id}/run`
  - 触发任务执行，并在成功后回写报告路径、图片路径、日志路径、进度摘要和可识别的 ChatGPT task id
- `POST /tasks/{task_id}/retry-chatgpt`
  - 检查 ChatGPT 会话后准备续跑联网步骤

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

- 报告：`docs/analysis/`
- 图片：`data/plots/`
- ChatGPT state：`skills/chatgpt-plus-browser/.state/`

## 当前执行口径

- `/tasks/{task_id}/run` 通过 `Codex App Server` 驱动 `stock-wave-attribution`
- 服务 prompt 默认不是让 Codex 自己探索仓库，而是直接执行：

```bash
python skills/stock-wave-attribution/scripts/orchestrator.py run \
  --stock-name <名称> \
  --ts-code <代码> \
  --start-date <开始日期> \
  --end-date <结束日期> \
  --sample-label <标签>
```

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
- `plot_path` 以正式报告里的图片引用为优先真相源；只有报告里没有图片引用时，才退回固定命名规则
- `chatgpt_task_id` 优先读取任务对象；为空时，再尝试从报告中的 `.state/<task-id>.json` 路径提取
