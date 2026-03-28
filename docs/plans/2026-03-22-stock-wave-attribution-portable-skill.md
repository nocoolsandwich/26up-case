# Stock Wave Attribution Portable Skill Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 把 `stock-wave-attribution` 收口成可复制到其他环境直接使用的自包含 skill，并通过 `stock-wave-attribution.yaml` 注入当前数据库与 `tushare` 配置。

**Architecture:** 保留 `skills/stock-wave-attribution/` 作为唯一运行时根目录，在 skill 内新增 `runtime/` 承载配置、数据读取、波段切分与绘图模块。`orchestrator.py` 仅依赖 skill 内模块和显式配置文件，不再硬依赖宿主项目根目录 `scripts/` 或 `config/project.yaml`。

**Tech Stack:** Python 3、pandas、PyYAML、pytest、unittest、Node.js（ChatGPT 浏览器脚本）。

---

### Task 1: 用失败测试锁定“自包含运行时 + 配置驱动”目标

**Files:**
- Modify: `skills/stock-wave-attribution/tests/test_orchestrator.py`
- Modify: `skills/stock-wave-attribution/tests/test_project_skill.py`

**Step 1: Write the failing test**

补测试覆盖这些行为：
- `orchestrator` 的调用链只引用 skill 内文件
- `run_chatgpt_browser` 的工作目录不再依赖宿主项目根目录
- skill 清单暴露 `stock-wave-attribution.yaml` 与 `runtime/` 文件

**Step 2: Run test to verify it fails**

Run: `PYTHONPATH=. pytest -q skills/stock-wave-attribution/tests/test_orchestrator.py skills/stock-wave-attribution/tests/test_project_skill.py`
Expected: FAIL，失败点来自旧调用链仍引用根目录 `scripts/`、manifest 尚未包含新运行时文件。

**Step 3: Write minimal implementation**

只修改必要断言和元数据，不提前搬运额外逻辑。

**Step 4: Run test to verify it passes**

Run: `PYTHONPATH=. pytest -q skills/stock-wave-attribution/tests/test_orchestrator.py skills/stock-wave-attribution/tests/test_project_skill.py`
Expected: PASS

### Task 2: 在 skill 内新增 runtime 层和默认配置

**Files:**
- Create: `skills/stock-wave-attribution/runtime/__init__.py`
- Create: `skills/stock-wave-attribution/runtime/config.py`
- Create: `skills/stock-wave-attribution/runtime/attribution_data.py`
- Create: `skills/stock-wave-attribution/runtime/wave_segmentation.py`
- Create: `skills/stock-wave-attribution/runtime/wave_plotting.py`
- Create: `skills/stock-wave-attribution/stock-wave-attribution.yaml`
- Test: `skills/stock-wave-attribution/tests/test_orchestrator.py`

**Step 1: Write the failing test**

增加测试验证：
- 可以从 `stock-wave-attribution.yaml` 读到当前项目配置
- `orchestrator` 通过 skill 内模块完成概念验证与绘图

**Step 2: Run test to verify it fails**

Run: `PYTHONPATH=. pytest -q skills/stock-wave-attribution/tests/test_orchestrator.py`
Expected: FAIL，提示缺少 `runtime` 模块或配置文件。

**Step 3: Write minimal implementation**

先把当前可复用逻辑平移到 skill 内 `runtime/`，配置文件按当前项目现状填充默认值。

**Step 4: Run test to verify it passes**

Run: `PYTHONPATH=. pytest -q skills/stock-wave-attribution/tests/test_orchestrator.py`
Expected: PASS

### Task 3: 改造 orchestrator 为显式配置驱动

**Files:**
- Modify: `skills/stock-wave-attribution/scripts/orchestrator.py`
- Test: `skills/stock-wave-attribution/tests/test_orchestrator.py`

**Step 1: Write the failing test**

补测试覆盖：
- `orchestrator` 不再通过 `sys.path` 注入宿主项目根目录找模块
- `CALL_CHAIN` 指向 skill 内 `runtime/` 文件
- ChatGPT 浏览器脚本路径按 skill 根目录解析

**Step 2: Run test to verify it fails**

Run: `PYTHONPATH=. pytest -q skills/stock-wave-attribution/tests/test_orchestrator.py`
Expected: FAIL，旧实现仍依赖 `PROJECT_ROOT/scripts/*`。

**Step 3: Write minimal implementation**

把运行根切到 `skill_root`，从 `runtime` 导入波段、绘图、数据读取逻辑，保留必要的向后兼容参数。

**Step 4: Run test to verify it passes**

Run: `PYTHONPATH=. pytest -q skills/stock-wave-attribution/tests/test_orchestrator.py`
Expected: PASS

### Task 4: 更新 skill 文档与迁移清单，避免漏文件

**Files:**
- Modify: `skills/stock-wave-attribution/SKILL.md`
- Modify: `skills/stock-wave-attribution/README.md`
- Modify: `skills/stock-wave-attribution/skill_manifest.json`
- Modify: `README.md`

**Step 1: Write the failing test**

补测试或断言验证：
- `project_priority` 与迁移边界改为可移植口径
- README/SKILL 都明确 `stock-wave-attribution.yaml` 是默认配置入口

**Step 2: Run test to verify it fails**

Run: `PYTHONPATH=. pytest -q skills/stock-wave-attribution/tests/test_project_skill.py`
Expected: FAIL，旧 manifest 仍是 `project-first`，且缺少配置文件与 runtime 文件列表。

**Step 3: Write minimal implementation**

只更新文档和 manifest，不额外扩展功能。

**Step 4: Run test to verify it passes**

Run: `PYTHONPATH=. pytest -q skills/stock-wave-attribution/tests/test_project_skill.py`
Expected: PASS

### Task 5: 全量验证迁移相关回归

**Files:**
- Test: `skills/stock-wave-attribution/tests/test_orchestrator.py`
- Test: `skills/stock-wave-attribution/tests/test_project_skill.py`
- Test: `tests/test_attribution_data.py`
- Test: `tests/test_wave_segmentation.py`
- Test: `tests/test_wave_plotting.py`

**Step 1: Run focused skill tests**

Run: `PYTHONPATH=. pytest -q skills/stock-wave-attribution/tests`
Expected: PASS

**Step 2: Run project regression tests for copied runtime logic**

Run: `PYTHONPATH=. pytest -q tests/test_attribution_data.py tests/test_wave_segmentation.py tests/test_wave_plotting.py`
Expected: PASS

**Step 3: Verify migration asset completeness**

人工核对 skill 目录至少包含：
- `scripts/orchestrator.py`
- `scripts/project_skill.py`
- `runtime/*.py`
- `templates/detailed_report_contract.md`
- `stock-wave-attribution.yaml`
- `skill_manifest.json`
- `SKILL.md`
- `README.md`

**Step 4: Record final status**

在最终说明里明确：
- 直接运行 `pytest -q` 仍可能受仓库级测试收集方式影响
- 可迁移 skill 的标准执行方式与配置入口
