from __future__ import annotations

from service.models import AttributionTask, TaskStatus


def test_attribution_task_defaults_to_queued_status() -> None:
    task = AttributionTask(
        task_id="attr-001",
        stock_name="腾景科技",
        ts_code="688195.SH",
        start_date="2025-09-10",
        end_date="2026-03-09",
        sample_label="数据中心",
    )

    assert task.status == TaskStatus.QUEUED
    assert task.stage == "created"
    assert task.news_lookback_days == 14


def test_attribution_task_to_dict_is_json_friendly() -> None:
    task = AttributionTask(
        task_id="attr-002",
        stock_name="国博电子",
        ts_code="688375.SH",
        start_date="2025-09-10",
        end_date="2026-03-09",
        sample_label="5G",
        report_path="outputs/analysis/demo.md",
        log_path="data/service_logs/attr-002.log",
        chatgpt_task_id="chatgpt-123",
        progress_summary="最近命令: sed -n '1,40p' SKILL.md",
        last_event_type="item.started",
        last_command="sed -n '1,40p' SKILL.md",
    )

    payload = task.to_dict()

    assert payload["task_id"] == "attr-002"
    assert payload["stock_name"] == "国博电子"
    assert payload["ts_code"] == "688375.SH"
    assert payload["sample_label"] == "5G"
    assert payload["news_lookback_days"] == 14
    assert payload["status"] == "queued"
    assert payload["report_path"] == "outputs/analysis/demo.md"
    assert payload["log_path"] == "data/service_logs/attr-002.log"
    assert payload["chatgpt_task_id"] == "chatgpt-123"
    assert payload["progress_summary"] == "最近命令: sed -n '1,40p' SKILL.md"
    assert payload["last_event_type"] == "item.started"
    assert payload["last_command"] == "sed -n '1,40p' SKILL.md"


def test_attribution_task_from_dict_reads_news_lookback_days() -> None:
    task = AttributionTask.from_dict(
        {
            "task_id": "attr-010",
            "stock_name": "数据港",
            "ts_code": "603881.SH",
            "start_date": "2025-01-01",
            "end_date": "2026-04-09",
            "sample_label": "数据中心",
            "news_lookback_days": 21,
        }
    )

    assert task.news_lookback_days == 21
