from __future__ import annotations

from pathlib import Path

from service.models import AttributionTask, TaskStatus
from service.task_store import TaskStore


def test_task_store_creates_directory_and_persists_task(tmp_path: Path) -> None:
    store = TaskStore(tmp_path / "service_tasks")
    task = AttributionTask(
        task_id="attr-001",
        stock_name="腾景科技",
        ts_code="688195.SH",
        start_date="2025-09-10",
        end_date="2026-03-09",
        sample_label="数据中心",
    )

    store.save_task(task)

    task_path = tmp_path / "service_tasks" / "attr-001.json"
    assert task_path.exists()

    loaded = store.load_task("attr-001")
    assert loaded.task_id == "attr-001"
    assert loaded.status == TaskStatus.QUEUED


def test_task_store_updates_status_and_error(tmp_path: Path) -> None:
    store = TaskStore(tmp_path / "service_tasks")
    task = AttributionTask(
        task_id="attr-002",
        stock_name="国博电子",
        ts_code="688375.SH",
        start_date="2025-09-10",
        end_date="2026-03-09",
        sample_label="5G",
    )
    store.save_task(task)

    updated = store.update_task(
        "attr-002",
        status=TaskStatus.FAILED,
        stage="chatgpt_search",
        error="blank_page",
        log_path="data/service_logs/attr-002.log",
    )

    assert updated.status == TaskStatus.FAILED
    assert updated.stage == "chatgpt_search"
    assert updated.error == "blank_page"
    assert updated.log_path == "data/service_logs/attr-002.log"

    loaded = store.load_task("attr-002")
    assert loaded.status == TaskStatus.FAILED
    assert loaded.error == "blank_page"
    assert loaded.log_path == "data/service_logs/attr-002.log"
