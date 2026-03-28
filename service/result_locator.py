from __future__ import annotations

from pathlib import Path
import re

from service.models import AttributionTask

CHATGPT_TASK_ID_PATTERN = re.compile(
    r"skills/chatgpt-plus-browser/\.state/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\.json",
    re.IGNORECASE,
)


def _fallback_report_path(task: AttributionTask, workspace_root: Path) -> Path | None:
    ts_code_slug = task.ts_code.replace(".", "")
    pattern = f"*-{ts_code_slug}-{task.stock_name}-wave-attribution.md"
    matches = sorted((workspace_root / "docs" / "analysis").glob(pattern))
    return matches[-1] if matches else None


def _fallback_plot_path(task: AttributionTask, workspace_root: Path) -> Path | None:
    candidate = workspace_root / "data" / "plots" / f"{task.ts_code.replace('.', '_')}_wave_candles.png"
    return candidate if candidate.exists() else None


def _extract_chatgpt_task_id(report_path: Path | None, existing_task_id: str) -> str:
    if existing_task_id or report_path is None or not report_path.exists():
        return existing_task_id
    matched = CHATGPT_TASK_ID_PATTERN.search(report_path.read_text(encoding="utf-8"))
    return matched.group(1) if matched else ""


def locate_task_result(task: AttributionTask, workspace_root: str | Path | None = None) -> dict[str, object]:
    root = Path(workspace_root or Path(__file__).resolve().parents[1])
    report_path = Path(task.report_path) if task.report_path else _fallback_report_path(task, root)
    plot_path = Path(task.plot_path) if task.plot_path else _fallback_plot_path(task, root)
    chatgpt_task_id = _extract_chatgpt_task_id(report_path, task.chatgpt_task_id)
    return {
        "task_id": task.task_id,
        "report_path": str(report_path) if report_path else "",
        "plot_path": str(plot_path) if plot_path else "",
        "report_exists": bool(report_path and report_path.exists()),
        "plot_exists": bool(plot_path and plot_path.exists()),
        "chatgpt_task_id": chatgpt_task_id,
    }
