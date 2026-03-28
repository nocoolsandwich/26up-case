# ChatGPT Plus Browser Auto Watch Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 为 `chatgpt-plus-browser` 的 `submit` / `submit-search` 增加自动后台 watcher，避免任务完成后仍依赖人工提醒。

**Architecture:** 在现有单文件 Node CLI 中新增内部 `watch` 命令和 watcher 启动辅助函数。`submit` 成功后后台拉起 detached 子进程轮询同一个 task id，状态和结果仍统一落在 `.state/<task-id>.json`，不直接侵入上游业务文档。

**Tech Stack:** Node.js ESM、`node:child_process`、现有 JSON 状态文件、`node:test`

---

### Task 1: 设计 watcher 的测试约束

**Files:**
- Modify: `skills/chatgpt-plus-browser/tests/chatgpt_cdp.test.mjs`
- Test: `skills/chatgpt-plus-browser/tests/chatgpt_cdp.test.mjs`

**Step 1: Write the failing test**

- 为 watcher 启动辅助函数增加测试：
  - 默认会后台启动 `watch <task-id>`
  - detached 为 `true`
  - 会把 task dir 传给子进程环境
  - 可通过环境变量关闭自动 watcher

**Step 2: Run test to verify it fails**

Run: `node --test skills/chatgpt-plus-browser/tests/chatgpt_cdp.test.mjs`
Expected: FAIL，提示新增导出或行为尚不存在

**Step 3: Write minimal implementation**

- 在 `chatgpt_cdp.mjs` 中实现 watcher 启动辅助函数与禁用开关

**Step 4: Run test to verify it passes**

Run: `node --test skills/chatgpt-plus-browser/tests/chatgpt_cdp.test.mjs`
Expected: PASS

### Task 2: 增加后台 watcher 运行时

**Files:**
- Modify: `skills/chatgpt-plus-browser/scripts/chatgpt_cdp.mjs`
- Test: `skills/chatgpt-plus-browser/tests/chatgpt_cdp.test.mjs`

**Step 1: Write the failing test**

- 为 watcher 失败回写状态增加测试：
  - 启动失败时任务文件写入 `watcherError`
- 为内部 `watch` 命令可复用现有等待逻辑增加测试或最小可测辅助函数

**Step 2: Run test to verify it fails**

Run: `node --test skills/chatgpt-plus-browser/tests/chatgpt_cdp.test.mjs`
Expected: FAIL

**Step 3: Write minimal implementation**

- 新增：
  - watcher 启动辅助函数
  - watcher 错误回写辅助函数
  - `watch <task-id>` CLI 分支
- 在 `submit` / `submit-search` 中接入自动 watcher

**Step 4: Run test to verify it passes**

Run: `node --test skills/chatgpt-plus-browser/tests/chatgpt_cdp.test.mjs`
Expected: PASS

### Task 3: 更新文档

**Files:**
- Modify: `skills/chatgpt-plus-browser/README.md`
- Modify: `skills/chatgpt-plus-browser/SKILL.md`

**Step 1: Update docs**

- 写清：
  - `submit` / `submit-search` 默认自动启动后台 watcher
  - `wait` 仍保留
  - 状态文件新增 watcher 元信息
  - 可通过环境变量关闭自动 watcher

**Step 2: Verify docs are consistent**

Run: `rg -n "watcher|submit-search|submit" skills/chatgpt-plus-browser/README.md skills/chatgpt-plus-browser/SKILL.md`
Expected: 能看到自动 watcher 的新说明

### Task 4: 回归验证

**Files:**
- Modify: `skills/chatgpt-plus-browser/scripts/chatgpt_cdp.mjs`
- Modify: `skills/chatgpt-plus-browser/tests/chatgpt_cdp.test.mjs`

**Step 1: Run focused tests**

Run: `node --test skills/chatgpt-plus-browser/tests/chatgpt_cdp.test.mjs`
Expected: PASS

**Step 2: Run syntax check**

Run: `node --check skills/chatgpt-plus-browser/scripts/chatgpt_cdp.mjs`
Expected: PASS

**Step 3: Spot-check docs and state contract**

Run: `rg -n "watcherStartedAt|watcherError|watch <task-id>" skills/chatgpt-plus-browser/scripts/chatgpt_cdp.mjs skills/chatgpt-plus-browser/README.md skills/chatgpt-plus-browser/SKILL.md`
Expected: 运行时和文档都能找到对应字段/命令说明
