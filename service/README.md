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
- 任务成功后，服务会自动扫描归因产物并回写：
  - `report_path`
  - `plot_path`
  - `log_path`
  - `chatgpt_task_id`
- `chatgpt_task_id` 优先读取任务对象；为空时，再尝试从报告中的 `.state/<task-id>.json` 路径提取
