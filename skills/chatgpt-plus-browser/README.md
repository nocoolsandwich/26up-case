# chatgpt-plus-browser 项目内化说明

## 为什么单独放 skill

这是外部浏览器自动化与结果验收链路，不应混入 `case_data/scripts/` 里的通用业务脚本。

## 当前状态

当前已经形成项目内可独立运行的真实运行时：

- `scripts/chatgpt_cdp.mjs`
- `scripts/start_chrome.sh`
- `tests/chatgpt_cdp.test.mjs`

这意味着项目内已经可以独立执行基础的 `status/send/submit-search/wait` 命令，不再需要从 `~/.codex/skills` 启动运行时。

当前已固化的运行时行为：

- CDP 执行上下文异常会做恢复重试
- 搜索任务仍以 `喵喵` 作为完成标记
- 提交前会校验当前 ChatGPT 页面是否真实可用，不能再把 `about:blank`、`workspace/deactivated` 或缺少输入框的页面误判成“已登录”
- `submit` / `submit-search` 默认每次新开独立 ChatGPT 任务页，不复用已有会话
- 同一批多票任务默认串行提交，不并发复用浏览器提交流程
- `submit` / `submit-search` 成功后会自动启动后台 watcher，持续轮询任务状态
- `submit` / `submit-search` 返回的 task 记录会显式带上 `taskStoreDir`
- 任务完成后会自动关闭对应 ChatGPT 任务页
- 自动关闭只影响任务页，不会主动退出整个 Chrome 进程

## 与项目脚本的关系

- 本 skill 当前不依赖 `case_data/scripts/` 中的业务模块
- 它会作为上游/下游能力被 `stock-wave-attribution` 调用

## 当前默认路径

- 状态目录：`skills/chatgpt-plus-browser/.state/`
- Chrome profile：`$HOME/.case_data/browser-profiles/chatgpt-plus-browser`
- 调试端口：`9222`
- 默认任务超时：`1800000ms`（`30 分钟`），可继续通过环境变量 `CHATGPT_PLUS_TIMEOUT_MS` 覆盖
- 默认自动 watcher：开启，可通过 `CHATGPT_PLUS_DISABLE_AUTO_WATCH=1` 临时关闭

## 当前产物

- `SKILL.md`：项目内 skill 使用口径
- `skill_manifest.json`：边界和迁移元数据
- `scripts/project_skill.py`：迁移元数据入口
- `scripts/chatgpt_cdp.mjs`：Node/CDP 运行时
- `scripts/start_chrome.sh`：Chrome 启动脚本
- `tests/chatgpt_cdp.test.mjs`：Node 运行时测试
- `tests/test_project_skill.py`：入口回归测试

## 当前限制

- 仍依赖真实 ChatGPT 页面 DOM 和登录态
- `status` 虽然会返回 `loggedOut`，但真正的会话是否可用应以 `loginCheck` 为准
- 关闭的是任务页，不是整个 Chrome
- 页面结构继续变化时，仍可能需要更新运行时脚本和测试
- 自动 watcher 只负责把结果稳定落到任务状态文件，不直接回填上游业务文档

## 与监听 subagent 的协作口径

如果上游 workflow 想在不阻塞当前会话的情况下等待结果，可以这样做：

1. 先执行 `submit` 或 `submit-search`
2. 从返回的 task JSON 中读取：
   - `id`
   - `taskStoreDir`
3. 监听 subagent 轮询时显式设置：
   - `CHATGPT_PLUS_TASK_DIR=<taskStoreDir>`
4. subagent 再执行：
   - `task-status <task-id>`
   - 或 `result <task-id>`

这样可以确保监听 subagent 读取的是项目内 task store，而不是误读到 `~/.codex/...` 下的其他状态目录。

但默认推荐的恢复口径更简单：

1. `submit` / `submit-search` 后让后台 watcher 把结果写入 `.state/<task-id>.json`
2. 如果业务报告生成时结果还没写入，不阻塞报告
3. 在报告里写明：
   - `ChatGPT 结果未写入`
   - 原始 `prompt`
   - 结果文件路径
4. 后续优先查这个结果文件；没有有效结果时，再按原 `prompt` 重提

补充判断规则：

- `status=done` 且 `resultText` 非空时，优先认定结果已经可用
- `closeError` 只说明任务页关闭记账不干净，不直接推翻已写回的结果文本
