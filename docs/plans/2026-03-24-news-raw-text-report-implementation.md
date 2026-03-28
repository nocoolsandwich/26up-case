# News Raw Text Report Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 把 `本地 news 库证据` 章节统一改成原文口径，并先完成单票试迁移，再批量替换所有历史报告。

**Architecture:** 保持数据库查询来源不变，继续从 `event_news.event_metadata.summary` 取文本，但在 skill 内部统一映射成 `raw_text`。报告渲染改为 `原文` 列，同时新增一个历史报告迁移脚本，先对单票执行，再对 `docs/analysis` 全量执行。

**Tech Stack:** Python, pytest, PostgreSQL, Markdown 文本替换

---

### Task 1: 更新 runtime 的 news 标准字段

**Files:**
- Modify: `skills/stock-wave-attribution/runtime/attribution_data.py`
- Test: `tests/test_attribution_data.py`

**Step 1: Write the failing test**

在 `tests/test_attribution_data.py` 新增测试，断言：

- `standardize_news_evidence_rows()` 返回字段包含 `raw_text`
- 不再依赖 `summary` 作为报告侧字段

**Step 2: Run test to verify it fails**

Run: `PYTHONPATH=. pytest -q tests/test_attribution_data.py -k raw_text`
Expected: FAIL

**Step 3: Write minimal implementation**

修改 `skills/stock-wave-attribution/runtime/attribution_data.py`：

- `standardize_news_evidence_rows()` 将文本字段映射为 `raw_text`
- `fetch_news_evidence()` 的标准化结果同步使用 `raw_text`

**Step 4: Run test to verify it passes**

Run: `PYTHONPATH=. pytest -q tests/test_attribution_data.py -k raw_text`
Expected: PASS

### Task 2: 更新报告渲染契约

**Files:**
- Modify: `skills/stock-wave-attribution/scripts/orchestrator.py`
- Modify: `skills/stock-wave-attribution/templates/detailed_report_contract.md`
- Test: `skills/stock-wave-attribution/tests/test_orchestrator.py`

**Step 1: Write the failing test**

新增测试，断言：

- `render_detailed_markdown()` 输出表头为 `| 时间 | 来源 | 标题 | 原文 | 链接 |`
- 使用 `raw_text` 作为证据列

**Step 2: Run test to verify it fails**

Run: `PYTHONPATH=. pytest -q skills/stock-wave-attribution/tests/test_orchestrator.py -k 原文`
Expected: FAIL

**Step 3: Write minimal implementation**

修改渲染逻辑和合同文档：

- 表头改成 `原文`
- 渲染列从 `summary` 改成 `raw_text`

**Step 4: Run test to verify it passes**

Run: `PYTHONPATH=. pytest -q skills/stock-wave-attribution/tests/test_orchestrator.py -k 原文`
Expected: PASS

### Task 3: 编写历史报告单票试迁移脚本

**Files:**
- Create: `scripts/migrate_news_raw_text_reports.py`
- Test: `skills/stock-wave-attribution/tests/test_project_skill.py` or new dedicated test file under `tests/`

**Step 1: Write the failing test**

新增测试，准备一份最小 Markdown 样本，断言脚本能够：

- 找到 `本地 news 库证据` 表
- 将列名替换为 `原文`
- 用数据库返回的 `raw_text` 覆盖原证据列

**Step 2: Run test to verify it fails**

Run: `PYTHONPATH=. pytest -q tests/test_migrate_news_raw_text_reports.py`
Expected: FAIL

**Step 3: Write minimal implementation**

脚本支持：

- `--file <path>`：只迁移一份报告
- 通过 `时间 + 来源 + 标题 + 链接` 回库查原文
- 找不到匹配时退出非零

**Step 4: Run test to verify it passes**

Run: `PYTHONPATH=. pytest -q tests/test_migrate_news_raw_text_reports.py`
Expected: PASS

### Task 4: 试迁移一份真实报告

**Files:**
- Run against: `docs/analysis/2026-03-23-300102SZ-乾照光电-wave-attribution.md`

**Step 1: Run single-file migration**

Run:

```bash
python scripts/migrate_news_raw_text_reports.py --file "docs/analysis/2026-03-23-300102SZ-乾照光电-wave-attribution.md"
```

**Step 2: Verify result**

检查：

- 表头为 `原文`
- 至少一条 news 证据已替换为数据库原文
- 报告结构未损坏

### Task 5: 批量迁移全部历史报告

**Files:**
- Run against: `docs/analysis/*.md`

**Step 1: Run bulk migration**

Run:

```bash
python scripts/migrate_news_raw_text_reports.py --all
```

**Step 2: Verify result**

Run:

```bash
rg -n "完整摘要/正文要点" docs/analysis
```

Expected: no matches

### Task 6: 全量回归验证

**Files:**
- Test: `tests/test_attribution_data.py`
- Test: `skills/stock-wave-attribution/tests/test_orchestrator.py`
- Test: `tests/test_migrate_news_raw_text_reports.py`

**Step 1: Run targeted tests**

Run:

```bash
PYTHONPATH=. pytest -q tests/test_attribution_data.py skills/stock-wave-attribution/tests/test_orchestrator.py tests/test_migrate_news_raw_text_reports.py
```

Expected: PASS

**Step 2: Spot check migrated reports**

Run:

```bash
rg -n "## 本地 news 库证据|原文" docs/analysis/*.md
```

Expected: migrated reports use the new header consistently
