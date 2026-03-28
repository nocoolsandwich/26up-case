# chatgpt-plus-browser 自动 watcher 设计

## 背景

当前 `chatgpt-plus-browser` 的任务协作方式是：

- `submit` / `submit-search` 负责把任务发到 ChatGPT 页面
- 主代理之后再用 `task-status` / `result` / `wait` 轮询结果

这会带来两个问题：

- 任务已经在网页里完成，但主代理没有持续轮询时，不会自动感知
- 上游 skill 需要依赖人工提醒“结果已经好了”，协作链路不完整

## 目标

在不引入全局 daemon 的前提下，为每个 `submit` / `submit-search` 任务自动启动一个后台 watcher，使其在任务完成后把最终状态和结果稳定落到本地状态文件，后续主代理只需要读取本地任务记录即可。

## 方案

采用“每个任务一个后台 watcher”的最小方案：

1. `submit` / `submit-search` 成功后，立即后台启动同一脚本的内部命令 `watch <task-id>`
2. watcher 进程独立轮询 `getTaskStatus(taskId)`，直到：
   - 任务 `done`
   - 超时
   - 运行异常
3. watcher 不直接改业务文档，只更新任务状态文件
4. 任务结果仍继续保存在 `skills/chatgpt-plus-browser/.state/<task-id>.json`

## 为什么不用全局守护进程

- 当前需求只是补齐“提交后自动盯任务”
- 全局 daemon 需要处理启动、停机、多任务恢复、重复回填和孤儿进程清理
- 现阶段复杂度明显过高，不符合 YAGNI

## 行为边界

### 自动 watcher 负责

- 后台轮询任务状态
- 在状态文件中写入 watcher 元信息
- 在任务完成时确保 `resultText` 已落盘
- 在 watcher 失败时留下 `watcherError`

### 自动 watcher 不负责

- 不直接回填分析报告
- 不发送系统通知
- 不改上游业务 skill 的执行流
- 不替代显式 `wait`

## 数据约定

任务状态文件在原有字段基础上增加：

- `watcherStartedAt`
- `watcherPid`
- `watcherMode`
- `watcherError`
- `watcherFinishedAt`

这些字段只用于运行态可观测性，不影响既有 `result` / `task-status` 的外部契约。

## 失败处理

- 如果 watcher 启动失败：
  - 不影响 `submit` 主流程返回 task id
  - 在任务文件中写 `watcherError`
- 如果 watcher 超时：
  - 不改写已存在的 `resultText`
  - 保留 `watcherError`
- 如果用户显式调用 `wait`：
  - 允许与后台 watcher 并存
  - 最终以状态文件中的最新结果为准

## 测试策略

采用 TDD，覆盖：

1. watcher 启动命令构造正确
2. watcher 会以 detached 模式后台启动
3. 可通过环境变量关闭自动 watcher，便于测试和特殊场景排障
4. watcher 失败时会把错误写回任务文件
5. 现有 `submit/send/wait/result` 行为不回归

## 文档影响

需要同步更新：

- `skills/chatgpt-plus-browser/README.md`
- `skills/chatgpt-plus-browser/SKILL.md`

重点说明：

- `submit` / `submit-search` 现在默认会自动启动后台 watcher
- `wait` 仍保留，适合当前会话里同步阻塞等待
- 状态文件会记录 watcher 元信息
