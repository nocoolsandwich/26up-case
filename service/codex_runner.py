from __future__ import annotations

import json
from pathlib import Path
from subprocess import CalledProcessError, TimeoutExpired, run as subprocess_run

from service.models import AttributionTask, TaskStatus
from service.result_locator import locate_task_result
from service.task_store import TaskStore

WORKSPACE_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CODEX_TIMEOUT_SECONDS = 900


def build_codex_prompt(task: AttributionTask) -> str:
    return (
        "请使用 stock-wave-attribution skill 执行一条正式 A 股波段归因任务。\n"
        f"标的：{task.stock_name}（{task.ts_code}）\n"
        f"分析窗口：{task.start_date} 到 {task.end_date}\n"
        f"样本标签：{task.sample_label}\n"
        "要求：\n"
        "1. 使用本地 PostgreSQL 的 event_quant / event_news。\n"
        "2. 正式报告必须落到 docs/analysis。\n"
        "3. 如果 ChatGPT 登录态异常，报告里必须写明 task id、prompt 和 .state 路径。\n"
        "4. 输出以正式报告为准，不要只给过程说明。\n"
    )


def build_codex_command(task: AttributionTask) -> list[str]:
    return [
        "codex",
        "exec",
        "--json",
        "--dangerously-bypass-approvals-and-sandbox",
        "--skip-git-repo-check",
        "-C",
        str(WORKSPACE_ROOT),
        build_codex_prompt(task),
    ]


def _log_path(task_id: str, workspace_root: Path) -> Path:
    return workspace_root / "data" / "service_logs" / f"{task_id}.log"


def _coerce_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return str(value)


def _write_log(
    path: Path,
    *,
    command: list[str],
    stdout: object = "",
    stderr: object = "",
    note: str = "",
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        "[command]",
        " ".join(command),
    ]
    if note:
        lines.extend(["", "[note]", note])
    stdout_text = _coerce_text(stdout)
    stderr_text = _coerce_text(stderr)
    lines.extend(["", "[stdout]", stdout_text, "", "[stderr]", stderr_text])
    path.write_text("\n".join(lines), encoding="utf-8")


def _extract_progress(stdout: object) -> dict[str, str]:
    stdout_text = _coerce_text(stdout)
    last_event_type = ""
    last_command = ""
    for line in stdout_text.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        last_event_type = str(payload.get("type") or payload.get("event") or last_event_type)
        item = payload.get("item")
        if isinstance(item, dict) and item.get("type") == "command_execution" and item.get("command"):
            last_command = str(item["command"])
    if last_command:
        progress_summary = f"最近命令: {last_command}"
    elif last_event_type:
        progress_summary = f"最近事件: {last_event_type}"
    else:
        progress_summary = ""
    return {
        "progress_summary": progress_summary,
        "last_event_type": last_event_type,
        "last_command": last_command,
    }


def run_codex_task(
    task: AttributionTask,
    task_store: TaskStore,
    *,
    runner=subprocess_run,
    workspace_root: str | Path | None = None,
    timeout_seconds: int = DEFAULT_CODEX_TIMEOUT_SECONDS,
):
    root = Path(workspace_root or WORKSPACE_ROOT)
    log_path = _log_path(task.task_id, root)
    task_store.update_task(
        task.task_id,
        status=TaskStatus.RUNNING,
        stage="codex_running",
        error="",
        log_path=str(log_path),
    )
    command = build_codex_command(task)
    try:
        result = runner(
            command,
            check=True,
            capture_output=True,
            text=True,
            cwd=str(WORKSPACE_ROOT),
            timeout=timeout_seconds,
        )
        _write_log(log_path, command=command, stdout=getattr(result, "stdout", ""), stderr=getattr(result, "stderr", ""))
        progress = _extract_progress(getattr(result, "stdout", ""))
    except TimeoutExpired as exc:
        _write_log(
            log_path,
            command=command,
            stdout=exc.stdout,
            stderr=exc.stderr,
            note=f"codex exec timed out after {timeout_seconds}s",
        )
        progress = _extract_progress(exc.stdout)
        return task_store.update_task(
            task.task_id,
            status=TaskStatus.FAILED,
            stage="codex_timeout",
            error=f"codex exec timed out after {timeout_seconds}s",
            log_path=str(log_path),
            progress_summary=progress["progress_summary"],
            last_event_type=progress["last_event_type"],
            last_command=progress["last_command"],
        )
    except CalledProcessError as exc:
        progress = _extract_progress(exc.output)
        _write_log(
            log_path,
            command=command,
            stdout=exc.output,
            stderr=exc.stderr,
            note="codex exec failed",
        )
        return task_store.update_task(
            task.task_id,
            status=TaskStatus.FAILED,
            stage="codex_failed",
            error=str(exc),
            log_path=str(log_path),
            progress_summary=progress["progress_summary"],
            last_event_type=progress["last_event_type"],
            last_command=progress["last_command"],
        )
    except Exception as exc:  # noqa: BLE001
        _write_log(log_path, command=command, stderr=str(exc), note="codex exec failed")
        return task_store.update_task(
            task.task_id,
            status=TaskStatus.FAILED,
            stage="codex_failed",
            error=str(exc),
            log_path=str(log_path),
        )
    result = locate_task_result(task_store.load_task(task.task_id), workspace_root=root)
    return task_store.update_task(
        task.task_id,
        status=TaskStatus.COMPLETED,
        stage="completed",
        report_path=str(result["report_path"]),
        plot_path=str(result["plot_path"]),
        log_path=str(log_path),
        chatgpt_task_id=str(result["chatgpt_task_id"]),
        progress_summary=progress["progress_summary"],
        last_event_type=progress["last_event_type"],
        last_command=progress["last_command"],
    )
