from __future__ import annotations

from subprocess import CalledProcessError, TimeoutExpired

from service.codex_runner import build_codex_command, build_codex_prompt, build_skill_run_command, run_codex_task
from service.models import AttributionTask, TaskStatus
from service.task_store import TaskStore


def test_build_codex_prompt_contains_skill_and_task_context() -> None:
    task = AttributionTask(
        task_id="attr-003",
        stock_name="腾景科技",
        ts_code="688195.SH",
        start_date="2025-09-10",
        end_date="2026-03-09",
        sample_label="数据中心",
    )

    prompt = build_codex_prompt(task)

    assert "stock-wave-attribution" in prompt
    assert "腾景科技（688195.SH）" in prompt
    assert "2025-09-10" in prompt
    assert "2026-03-09" in prompt
    assert "docs/analysis" in prompt
    assert "直接在项目根目录执行下面这条命令" in prompt
    assert "python skills/stock-wave-attribution/scripts/orchestrator.py run" in prompt
    assert "当前默认不启用 ChatGPT 补强链路" in prompt


def test_build_codex_command_wraps_prompt_for_codex_cli() -> None:
    task = AttributionTask(
        task_id="attr-004",
        stock_name="国博电子",
        ts_code="688375.SH",
        start_date="2025-09-10",
        end_date="2026-03-09",
        sample_label="5G",
    )

    command = build_codex_command(task)

    assert command[0] == "codex"
    assert command[1] == "exec"
    assert "-C" in command
    assert "--json" in command
    assert "--skip-git-repo-check" in command
    assert "--dangerously-bypass-approvals-and-sandbox" in command
    assert any("国博电子" in part for part in command)


def test_build_skill_run_command_uses_direct_orchestrator_entry() -> None:
    task = AttributionTask(
        task_id="attr-004c",
        stock_name="五洲新春",
        ts_code="603667.SH",
        start_date="2025-11-05",
        end_date="2026-01-22",
        sample_label="机器人概念",
    )

    command = build_skill_run_command(task)

    assert "python" in command
    assert "skills/stock-wave-attribution/scripts/orchestrator.py" in command
    assert "--stock-name" in command
    assert "五洲新春" in command


def test_run_codex_task_marks_completed_on_success(tmp_path) -> None:
    store = TaskStore(tmp_path / "service_tasks")
    task = AttributionTask(
        task_id="attr-004b",
        stock_name="国博电子",
        ts_code="688375.SH",
        start_date="2025-09-10",
        end_date="2026-03-09",
        sample_label="5G",
    )
    store.save_task(task)
    calls = []

    def fake_runner(*args, **kwargs):
        calls.append((args, kwargs))
        class Result:
            stdout = '{"event":"done"}\n'
        return Result()

    updated = run_codex_task(task, store, runner=fake_runner)

    assert calls
    assert updated.status == TaskStatus.COMPLETED
    assert updated.stage == "completed"


def test_run_codex_task_marks_failure_and_records_error(tmp_path) -> None:
    store = TaskStore(tmp_path / "service_tasks")
    task = AttributionTask(
        task_id="attr-005",
        stock_name="腾景科技",
        ts_code="688195.SH",
        start_date="2025-09-10",
        end_date="2026-03-09",
        sample_label="数据中心",
    )
    store.save_task(task)

    def fake_runner(*_args, **_kwargs):
        raise RuntimeError("codex boom")

    updated = run_codex_task(task, store, runner=fake_runner)

    assert updated.status == TaskStatus.FAILED
    assert updated.error == "codex boom"
    assert updated.stage == "codex_failed"


def test_run_codex_task_backfills_report_plot_and_chatgpt_task_id(tmp_path) -> None:
    store = TaskStore(tmp_path / "service_tasks")
    task = AttributionTask(
        task_id="attr-005b",
        stock_name="国博电子",
        ts_code="688375.SH",
        start_date="2025-12-10",
        end_date="2026-01-14",
        sample_label="5G",
    )
    store.save_task(task)

    def fake_runner(*_args, **_kwargs):
        report_dir = tmp_path / "docs" / "analysis"
        plot_dir = tmp_path / "data" / "plots"
        report_dir.mkdir(parents=True, exist_ok=True)
        plot_dir.mkdir(parents=True, exist_ok=True)
        (report_dir / "2026-03-25-688375SH-国博电子-wave-attribution.md").write_text(
            "\n".join(
                [
                    "### ChatGPT 联网归因",
                    "- task id：",
                    "  `d7b0a15f-688a-41af-b738-ec8d9ab5290a`",
                    "- 结果文件：",
                    "  `skills/chatgpt-plus-browser/.state/d7b0a15f-688a-41af-b738-ec8d9ab5290a.json`",
                ]
            ),
            encoding="utf-8",
        )
        (plot_dir / "688375_SH_wave_candles.png").write_text("png", encoding="utf-8")

        class Result:
            stdout = "\n".join(
                [
                    '{"type":"thread.started"}',
                    '{"type":"item.started","item":{"type":"command_execution","command":"sed -n \\"1,40p\\" SKILL.md"}}',
                    '{"type":"item.completed","item":{"type":"agent_message","text":"done"}}',
                ]
            )

        return Result()

    updated = run_codex_task(task, store, runner=fake_runner, workspace_root=tmp_path)

    assert updated.status == TaskStatus.COMPLETED
    assert updated.report_path.endswith("2026-03-25-688375SH-国博电子-wave-attribution.md")
    assert updated.plot_path.endswith("688375_SH_wave_candles.png")
    assert updated.chatgpt_task_id == "d7b0a15f-688a-41af-b738-ec8d9ab5290a"
    assert updated.log_path.endswith("attr-005b.log")
    assert updated.progress_summary == '最近命令: sed -n "1,40p" SKILL.md'
    assert updated.last_event_type == "item.completed"
    assert updated.last_command == 'sed -n "1,40p" SKILL.md'


def test_run_codex_task_marks_timeout_and_persists_log(tmp_path) -> None:
    store = TaskStore(tmp_path / "service_tasks")
    task = AttributionTask(
        task_id="attr-timeout",
        stock_name="腾景科技",
        ts_code="688195.SH",
        start_date="2025-10-24",
        end_date="2025-12-12",
        sample_label="数据中心",
    )
    store.save_task(task)

    def fake_runner(*_args, **_kwargs):
        raise TimeoutExpired(
            cmd=["codex", "exec"],
            timeout=30,
            output='{"type":"thread.started"}\n',
            stderr="codex stderr timeout",
        )

    updated = run_codex_task(
        task,
        store,
        runner=fake_runner,
        workspace_root=tmp_path,
        timeout_seconds=30,
    )

    assert updated.status == TaskStatus.FAILED
    assert updated.stage == "codex_timeout"
    assert "30s" in updated.error
    assert updated.log_path.endswith("attr-timeout.log")
    assert updated.progress_summary == "最近事件: thread.started"
    assert updated.last_event_type == "thread.started"
    assert updated.last_command == ""
    log_text = (tmp_path / "data" / "service_logs" / "attr-timeout.log").read_text(encoding="utf-8")
    assert "codex stderr timeout" in log_text
    assert "thread.started" in log_text


def test_run_codex_task_preserves_progress_when_codex_returns_nonzero(tmp_path) -> None:
    store = TaskStore(tmp_path / "service_tasks")
    task = AttributionTask(
        task_id="attr-nonzero",
        stock_name="国博电子",
        ts_code="688375.SH",
        start_date="2025-12-10",
        end_date="2026-01-14",
        sample_label="5G",
    )
    store.save_task(task)

    def fake_runner(*_args, **_kwargs):
        raise CalledProcessError(
            returncode=1,
            cmd=["codex", "exec"],
            output="\n".join(
                [
                    '{"type":"thread.started"}',
                    '{"type":"item.started","item":{"type":"command_execution","command":"sed -n \\"1,40p\\" SKILL.md"}}',
                ]
            ),
            stderr="codex stderr boom",
        )

    updated = run_codex_task(task, store, runner=fake_runner, workspace_root=tmp_path)

    assert updated.status == TaskStatus.FAILED
    assert updated.stage == "codex_failed"
    assert updated.progress_summary == '最近命令: sed -n "1,40p" SKILL.md'
    assert updated.last_event_type == "item.started"
    assert updated.last_command == 'sed -n "1,40p" SKILL.md'
    log_text = (tmp_path / "data" / "service_logs" / "attr-nonzero.log").read_text(encoding="utf-8")
    assert "codex stderr boom" in log_text
    assert "thread.started" in log_text
