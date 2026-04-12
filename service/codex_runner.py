from __future__ import annotations

import json
import shlex
from pathlib import Path
from subprocess import CalledProcessError, TimeoutExpired, run as subprocess_run

from service.models import AttributionTask, TaskStatus
from service.result_locator import locate_task_result
from service.task_store import TaskStore

WORKSPACE_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CODEX_TIMEOUT_SECONDS = 900


def _append_optional_task_flags(parts: list[str], task: AttributionTask) -> list[str]:
    if task.news_lookback_days > 0:
        parts.extend(["--news-lookback-days", str(task.news_lookback_days)])
    if task.skip_concept:
        parts.append("--skip-concept")
    return parts


def build_codex_base_instructions() -> str:
    return (
        "你正在执行 case_data 的单票波段归因服务任务。"
        "目标不是探索仓库，而是尽快完成一条正式归因链，"
        "并把结果文件都写回正式报告与任务状态，"
        "包括报告、配图和必要的任务状态回写。"
    )


def build_codex_developer_instructions() -> str:
    return (
        "不要先全仓库探索，不要先阅读无关 skill，不要先运行 `rg --files .` 或同类全仓库扫描。"
        "优先直接执行给定命令。"
        "只有当命令本身失败时，才允许回退到最小必要阅读。"
        "除非指定入口文件不存在或直接报错，否则只允许先读取以下入口文件："
        "skills/stock-wave-attribution/scripts/orchestrator.py、"
        "scripts/attribution_data.py、"
        "scripts/wave_segmentation.py、"
        "scripts/wave_plotting.py。"
        "优先执行明确步骤，而不是先做开放式仓库调研。"
    )


def build_skill_run_command(task: AttributionTask) -> str:
    return shlex.join(
        _append_optional_task_flags(
            [
                "python",
                "skills/stock-wave-attribution/scripts/orchestrator.py",
                "run",
                "--stock-name",
                task.stock_name,
                "--ts-code",
                task.ts_code,
                "--start-date",
                task.start_date,
                "--end-date",
                task.end_date,
                "--sample-label",
                task.sample_label,
            ],
            task,
        )
    )


def _agent_rerank_root(task: AttributionTask) -> Path:
    return Path("data") / "service_tasks" / task.task_id / "agent_rerank"


def build_prepare_agent_rerank_command(task: AttributionTask) -> str:
    return shlex.join(
        _append_optional_task_flags(
            [
                "python",
                "skills/stock-wave-attribution/scripts/orchestrator.py",
                "prepare-agent-rerank",
                "--stock-name",
                task.stock_name,
                "--ts-code",
                task.ts_code,
                "--start-date",
                task.start_date,
                "--end-date",
                task.end_date,
                "--sample-label",
                task.sample_label,
                "--task-id",
                task.task_id,
            ],
            task,
        )
    )


def build_finalize_agent_rerank_command(task: AttributionTask) -> str:
    return shlex.join(
        _append_optional_task_flags(
            [
                "python",
                "skills/stock-wave-attribution/scripts/orchestrator.py",
                "finalize-agent-rerank",
                "--stock-name",
                task.stock_name,
                "--ts-code",
                task.ts_code,
                "--start-date",
                task.start_date,
                "--end-date",
                task.end_date,
                "--sample-label",
                task.sample_label,
                "--task-id",
                task.task_id,
                "--selection-path",
                str(_agent_rerank_root(task) / "final_selection.json"),
            ],
            task,
        )
    )


def build_codex_prompt(task: AttributionTask) -> str:
    prepare_command = build_prepare_agent_rerank_command(task)
    finalize_command = build_finalize_agent_rerank_command(task)
    rerank_root = _agent_rerank_root(task)
    summary_path = rerank_root / "summary.json"
    selection_path = rerank_root / "final_selection.json"
    return (
        "请不要先全仓库探索，按下面这条正式服务链执行：\n\n"
        "1. 先在项目根目录执行准备命令：\n"
        f"{prepare_command}\n\n"
        f"2. 阅读 `{summary_path.as_posix()}` 与各波段目录下的 `rough_chunks/chunk_*.md`。\n"
        "- 对每个 chunk 直接选 3-5 条 item_id。\n"
        "- 只做直接入围，不要逐条打分，不要输出分数。\n"
        "- 优先选择与该标的和样本标签更贴近的启动前/启动期强信号。\n"
        "- 更偏好公司直接相关公告、产业链催化、板块主线强化、量价启动前后的关键信息。\n"
        "- 样本标签只是入口线索，不是最终结论。\n"
        "- 如果公司直接催化、产业链证据或波段节奏指向更强的其他主线，应以更强主线为准。\n"
        "- 尽量排除与该标的无关的跨主题噪音，不要被其他热门赛道带偏。\n"
        f"- 把最终结果写入 `{selection_path.as_posix()}`。\n"
        "- JSON 结构必须是：\n"
        '{\n'
        '  "one_liner": "一句话主逻辑",\n'
        '  "waves": [\n'
        '    {\n'
        '      "wave_id": "W1",\n'
        '      "one_line_logic": "该波段一句话逻辑",\n'
        '      "final_picks": [\n'
        '        {"item_id": "I00001", "role": "启动前强信号|启动期强化|中后段验证", "reason": "一句话原因"}\n'
        "      ]\n"
        "    }\n"
        "  ]\n"
        "}\n\n"
        "3. 写完最终入围 JSON 后，执行收口命令：\n"
        f"{finalize_command}\n\n"
        "任务说明：\n"
        f"- 标的：{task.stock_name}（{task.ts_code}）\n"
        f"- 分析窗口：{task.start_date} 到 {task.end_date}\n"
        f"- 样本标签：{task.sample_label}\n"
        f"- 新闻窗口：波段起点前 {task.news_lookback_days} 天到波段结束\n"
        f"- 概念联动：{'已显式跳过，本次允许概念表为空' if task.skip_concept else '正常启用'}\n"
        "- 使用本地 PostgreSQL 的 event_quant / event_news，与正式报告链路一致。\n"
        "- 粗排标准：100 选 3-5。\n"
        "- 精选标准：从粗排并集里直接精选最终 10 条，不做逐条打分。\n"
        "- 正式报告必须落到 outputs/analysis，并生成配图到 data/plots。\n"
        "- 如果命令失败，再只阅读最小必要文件定位原因，不要先做开放式探索。\n"
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
