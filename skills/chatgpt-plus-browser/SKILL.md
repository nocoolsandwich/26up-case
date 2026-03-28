# chatgpt-plus-browser

## 目标

把 ChatGPT Plus 浏览器链路正式收拢到 `case_data/skills/` 下，作为 `stock-wave-attribution` 的项目内下游依赖。

## 当前阶段

本轮已经完成项目内真实运行时收拢，当前不再依赖 `~/.codex/skills` 作为运行路径。

当前项目内统一口径：

- skill 自身负责：
  - ChatGPT 页面任务提交
  - 提交前校验登录态和会话可用性，不能把 `about:blank`、`workspace/deactivated`、缺失输入框页面误判成已登录
  - 每次提交默认新开独立 ChatGPT 任务页，不复用已有会话
  - 多个任务默认串行提交，不把两个 `submit` / `submit-search` 并发打到同一批页面上下文
  - Chrome CDP 连接
  - 执行上下文恢复重试
  - 任务状态轮询
  - `submit` / `submit-search` 后自动启动后台 watcher
  - 在 task 记录里写出 `taskStoreDir`
  - 结果文本提取
  - 同会话任务锚点隔离
  - 搜索模式收尾标记判断
  - 任务完成后的任务页自动关闭
- 项目继续保留：
  - 设计文档
  - 上游业务 skill 对该能力的调用约束

## 实际可运行入口

```bash
./skills/chatgpt-plus-browser/scripts/start_chrome.sh
node skills/chatgpt-plus-browser/scripts/chatgpt_cdp.mjs status
node skills/chatgpt-plus-browser/scripts/chatgpt_cdp.mjs send "<prompt>"
node skills/chatgpt-plus-browser/scripts/chatgpt_cdp.mjs submit-search "<prompt>"
node skills/chatgpt-plus-browser/scripts/chatgpt_cdp.mjs wait "<task-id>"
```

补充说明：

- `status` 现在除了 `loggedOut` 之外，还会返回 `hasComposer` 和 `loginCheck`
- 只有 `loginCheck.ok=true` 才能把当前页面视为可提交任务的有效会话
- 若 `loginCheck.reason` 是 `blank_page / workspace_deactivated / logged_out / missing_composer`，上游必须先恢复 Chrome 登录态或会话页面，再继续提交
- `submit` / `submit-search` 默认会自动启动后台 watcher
- `submit` / `submit-search` 默认一票一窗口，不复用已有 ChatGPT 对话页
- 同一批研究任务默认按“上一票提交完成后，再发下一票”执行，不要并发提交多个搜索任务
- 如果当前会话需要同步等待结果，继续用 `wait <task-id>`
- 如果只想提交任务并让它自己在后台盯住结果，不需要额外人工轮询
- 如果上游要起监听 subagent，必须把返回 task 里的 `taskStoreDir` 一并传给 subagent，并设置 `CHATGPT_PLUS_TASK_DIR=<taskStoreDir>`
- 任务结果的事实来源是 `skills/chatgpt-plus-browser/.state/<task-id>.json`
- 如果 `status=done` 且 `resultText` 非空，即使存在 `closeError`，也优先视为“结果可用、关页记账未完全成功”

补充的元数据入口仍保留：

```bash
python3 skills/chatgpt-plus-browser/scripts/project_skill.py summary --json
```

## 迁移边界

- 不再以 `~/.codex/skills/chatgpt-plus-browser` 作为主线依赖
- 历史运行时若仍需参考，只作为迁移来源，不作为项目事实来源
- 当前已迁入：
  - `skills/chatgpt-plus-browser/scripts/chatgpt_cdp.mjs`
  - `skills/chatgpt-plus-browser/scripts/start_chrome.sh`
  - `skills/chatgpt-plus-browser/tests/chatgpt_cdp.test.mjs`
- 默认本地状态目录：
  - `skills/chatgpt-plus-browser/.state/`
- 默认 Chrome profile 目录：
  - `$HOME/.case_data/browser-profiles/chatgpt-plus-browser`
- 默认调试端口：
  - `9222`
- 默认任务超时：
  - `1800000ms`（`30 分钟`）
- 默认自动 watcher：
  - 开启
- 临时关闭自动 watcher：
  - `CHATGPT_PLUS_DISABLE_AUTO_WATCH=1`
- 当前清理策略：
  - 每个任务绑定独立 `tabId`
  - 任务完成后自动关闭对应 ChatGPT 任务页
  - 不主动退出整个 Chrome 进程

## 下一步

1. 把页面结果抓取继续升级成更稳定的结构化快照提取
2. 增加真实样本端到端回归
3. 视页面变化再补更细的结果完整性断言
4. 继续把 watcher 结果和上游业务回填动作解耦
5. 持续配合 `stock-wave-attribution` 的真实归因链路
