from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import StrEnum


class TaskStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    WAITING_CHATGPT = "waiting_chatgpt"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass(slots=True)
class AttributionTask:
    task_id: str
    stock_name: str
    ts_code: str
    start_date: str
    end_date: str
    sample_label: str
    news_lookback_days: int = 14
    skip_concept: bool = False
    status: TaskStatus = TaskStatus.QUEUED
    stage: str = "created"
    report_path: str = ""
    plot_path: str = ""
    log_path: str = ""
    chatgpt_task_id: str = ""
    progress_summary: str = ""
    last_event_type: str = ""
    last_command: str = ""
    error: str = ""

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["status"] = self.status.value
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, object]) -> AttributionTask:
        data = dict(payload)
        data["status"] = TaskStatus(data.get("status", TaskStatus.QUEUED))
        data["news_lookback_days"] = int(data.get("news_lookback_days", 14))
        data["skip_concept"] = bool(data.get("skip_concept", False))
        return cls(**data)
