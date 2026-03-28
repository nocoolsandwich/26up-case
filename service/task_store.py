from __future__ import annotations

import json
from pathlib import Path

from service.models import AttributionTask, TaskStatus


class TaskStore:
    def __init__(self, root_dir: str | Path) -> None:
        self.root_dir = Path(root_dir)
        self.root_dir.mkdir(parents=True, exist_ok=True)

    def task_path(self, task_id: str) -> Path:
        return self.root_dir / f"{task_id}.json"

    def save_task(self, task: AttributionTask) -> None:
        self.task_path(task.task_id).write_text(
            json.dumps(task.to_dict(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def load_task(self, task_id: str) -> AttributionTask:
        payload = json.loads(self.task_path(task_id).read_text(encoding="utf-8"))
        return AttributionTask.from_dict(payload)

    def update_task(
        self,
        task_id: str,
        *,
        status: TaskStatus | None = None,
        stage: str | None = None,
        error: str | None = None,
        report_path: str | None = None,
        plot_path: str | None = None,
        log_path: str | None = None,
        chatgpt_task_id: str | None = None,
        progress_summary: str | None = None,
        last_event_type: str | None = None,
        last_command: str | None = None,
    ) -> AttributionTask:
        task = self.load_task(task_id)
        if status is not None:
            task.status = status
        if stage is not None:
            task.stage = stage
        if error is not None:
            task.error = error
        if report_path is not None:
            task.report_path = report_path
        if plot_path is not None:
            task.plot_path = plot_path
        if log_path is not None:
            task.log_path = log_path
        if chatgpt_task_id is not None:
            task.chatgpt_task_id = chatgpt_task_id
        if progress_summary is not None:
            task.progress_summary = progress_summary
        if last_event_type is not None:
            task.last_event_type = last_event_type
        if last_command is not None:
            task.last_command = last_command
        self.save_task(task)
        return task
