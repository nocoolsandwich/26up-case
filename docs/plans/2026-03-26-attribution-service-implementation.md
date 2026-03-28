# Attribution Service Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 构建一个本机单用户的归因任务服务，用 `Codex CLI` 驱动 `stock-wave-attribution` skill 执行单票归因，并提供任务状态与结果查询能力。

**Architecture:** 服务层只负责任务入库、状态管理、`Codex CLI` 调度和结果回收；归因事实来源继续保留在 `stock-wave-attribution` 与 `chatgpt-plus-browser`。任务状态使用本地 JSON 文件，产物继续落在现有 `docs/analysis`、`data/plots` 和 `.state` 目录。

**Tech Stack:** Python 3、FastAPI、uvicorn、pytest、subprocess、现有项目 skill/runtime。

---

### Task 1: 定义服务目录与任务状态模型

**Files:**
- Create: `service/__init__.py`
- Create: `service/models.py`
- Create: `tests/test_service_models.py`

**Step 1: Write the failing test**

在 `tests/test_service_models.py` 写测试，覆盖：
- 创建任务对象时默认 `status=queued`
- 任务对象能序列化成 JSON 友好字典
- 任务对象包含 `task_id / ts_code / start_date / end_date / sample_label / status`

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_service_models.py -v`

Expected: FAIL，提示模块或类不存在

**Step 3: Write minimal implementation**

在 `service/models.py` 定义最小任务模型，例如：
- `AttributionTask`
- `TaskStatus`

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_service_models.py -v`

Expected: PASS

### Task 2: 实现本地任务状态仓库

**Files:**
- Create: `service/task_store.py`
- Create: `tests/test_task_store.py`

**Step 1: Write the failing test**

覆盖：
- 能创建任务状态目录
- 能写入任务 JSON
- 能按 `task_id` 读取任务
- 能更新状态为 `running/completed/failed`

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_task_store.py -v`

Expected: FAIL

**Step 3: Write minimal implementation**

实现：
- `TaskStore`
- `save_task`
- `load_task`
- `update_task`

默认目录建议：
- `data/service_tasks/`

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_task_store.py -v`

Expected: PASS

### Task 3: 定义 Codex CLI 调用参数生成器

**Files:**
- Create: `service/codex_runner.py`
- Create: `tests/test_codex_runner.py`

**Step 1: Write the failing test**

覆盖：
- 能根据任务生成 `codex` 命令
- prompt 中明确要求使用 `stock-wave-attribution`
- prompt 中包含股票、时间窗、样本标签
- prompt 中要求报告落到 `docs/analysis`

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_codex_runner.py -v`

Expected: FAIL

**Step 3: Write minimal implementation**

实现：
- `build_codex_prompt(task)`
- `build_codex_command(task)`

先不真正执行，只负责命令和 prompt 生成

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_codex_runner.py -v`

Expected: PASS

### Task 4: 实现 Codex CLI 执行封装

**Files:**
- Modify: `service/codex_runner.py`
- Update: `tests/test_codex_runner.py`

**Step 1: Write the failing test**

新增测试覆盖：
- 能调用 `subprocess`
- 执行开始前将任务状态更新为 `running`
- 执行失败时写入 `failed + error`

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_codex_runner.py -v`

Expected: FAIL

**Step 3: Write minimal implementation**

实现：
- `run_codex_task(task, task_store, runner=subprocess.run)`

第一版只要求同步执行

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_codex_runner.py -v`

Expected: PASS

### Task 5: 定义任务结果发现逻辑

**Files:**
- Create: `service/result_locator.py`
- Create: `tests/test_result_locator.py`

**Step 1: Write the failing test**

覆盖：
- 能从任务对象推导报告路径
- 能识别报告是否存在
- 能返回 plot 路径
- 能附带 ChatGPT `.state` 路径

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_result_locator.py -v`

Expected: FAIL

**Step 3: Write minimal implementation**

实现：
- `locate_task_result(task)`

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_result_locator.py -v`

Expected: PASS

### Task 6: 搭 FastAPI 最小服务壳

**Files:**
- Create: `service/app.py`
- Create: `tests/test_service_app.py`

**Step 1: Write the failing test**

覆盖 3 个最小接口：
- `POST /tasks/attribution`
- `GET /tasks/{task_id}`
- `GET /tasks/{task_id}/result`

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_service_app.py -v`

Expected: FAIL

**Step 3: Write minimal implementation**

实现 FastAPI app：
- 创建任务
- 返回任务状态
- 返回任务结果

先不接真实 `codex` 执行线程，只打通接口和状态仓库

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_service_app.py -v`

Expected: PASS

### Task 7: 接入真实任务执行

**Files:**
- Modify: `service/app.py`
- Modify: `service/codex_runner.py`
- Update: `tests/test_service_app.py`

**Step 1: Write the failing test**

新增覆盖：
- 创建任务后可以显式触发执行
- 执行后状态从 `queued -> running`
- 失败时能在接口里看到错误信息

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_service_app.py -v`

Expected: FAIL

**Step 3: Write minimal implementation**

实现一个最小动作：
- `POST /tasks/{task_id}/run`

第一版仍允许同步执行，避免先引入复杂后台任务框架

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_service_app.py -v`

Expected: PASS

### Task 8: 增加 ChatGPT 续跑接口

**Files:**
- Modify: `service/app.py`
- Create: `tests/test_service_retry_chatgpt.py`

**Step 1: Write the failing test**

覆盖：
- `POST /tasks/{task_id}/retry-chatgpt`
- 若已有报告且 `.state` 缺结果，允许续跑
- 若当前 ChatGPT status 不可用，返回明确错误

**Step 2: Run test to verify it fails**

Run: `pytest tests/test_service_retry_chatgpt.py -v`

Expected: FAIL

**Step 3: Write minimal implementation**

实现：
- `retry_chatgpt_step(task_id)`

先只更新状态和调用 `Codex CLI` 的续跑 prompt

**Step 4: Run test to verify it passes**

Run: `pytest tests/test_service_retry_chatgpt.py -v`

Expected: PASS

### Task 9: 补服务说明文档

**Files:**
- Create: `service/README.md`
- Modify: `README.md`

**Step 1: Write the doc changes**

说明：
- 服务目标
- 如何启动
- 如何提任务
- 如何查状态
- 如何续跑 ChatGPT

**Step 2: Verify documentation paths and commands**

Run: 手工检查文档里的路径和命令

Expected: 路径与当前项目一致

### Task 10: 全量回归

**Files:**
- No code changes expected

**Step 1: Run focused tests**

Run:

```bash
pytest tests/test_service_models.py -v
pytest tests/test_task_store.py -v
pytest tests/test_codex_runner.py -v
pytest tests/test_result_locator.py -v
pytest tests/test_service_app.py -v
pytest tests/test_service_retry_chatgpt.py -v
```

Expected: PASS

**Step 2: Run broader related suite**

Run:

```bash
PYTHONPATH=. pytest -q tests skills/stock-wave-attribution/tests skills/chatgpt-plus-browser/tests
```

Expected: 无新增失败

**Step 3: Manual smoke test**

Run:

```bash
uvicorn service.app:app --reload
```

然后手工调用：
- 创建一条任务
- 查询状态
- 查询结果

Expected:
- 能生成任务 JSON
- 能返回明确状态
- 若未执行或 ChatGPT 不可用，错误信息清晰
