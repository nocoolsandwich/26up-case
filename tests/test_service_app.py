from __future__ import annotations

from fastapi.testclient import TestClient
import service.app as service_app_module

from service.models import TaskStatus
from service.app import create_app


def test_create_task_returns_task_id(tmp_path) -> None:
    app = create_app(task_root=tmp_path / "service_tasks", workspace_root=tmp_path)
    client = TestClient(app)

    response = client.post(
        "/tasks/attribution",
        json={
            "stock_name": "腾景科技",
            "ts_code": "688195.SH",
            "start_date": "2025-09-10",
            "end_date": "2026-03-09",
            "sample_label": "数据中心",
        },
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["task_id"]
    assert payload["status"] == "queued"


def test_get_task_returns_saved_status(tmp_path) -> None:
    app = create_app(task_root=tmp_path / "service_tasks", workspace_root=tmp_path)
    client = TestClient(app)

    created = client.post(
        "/tasks/attribution",
        json={
            "stock_name": "国博电子",
            "ts_code": "688375.SH",
            "start_date": "2025-09-10",
            "end_date": "2026-03-09",
            "sample_label": "5G",
        },
    ).json()

    response = client.get(f"/tasks/{created['task_id']}")

    assert response.status_code == 200
    payload = response.json()
    assert payload["task_id"] == created["task_id"]
    assert payload["ts_code"] == "688375.SH"


def test_get_result_returns_report_and_plot_flags(tmp_path) -> None:
    app = create_app(task_root=tmp_path / "service_tasks", workspace_root=tmp_path)
    client = TestClient(app)

    created = client.post(
        "/tasks/attribution",
        json={
            "stock_name": "腾景科技",
            "ts_code": "688195.SH",
            "start_date": "2025-09-10",
            "end_date": "2026-03-09",
            "sample_label": "数据中心",
        },
    ).json()

    response = client.get(f"/tasks/{created['task_id']}/result")

    assert response.status_code == 200
    payload = response.json()
    assert payload["task_id"] == created["task_id"]
    assert payload["report_exists"] is False
    assert payload["plot_exists"] is False


def test_run_task_marks_failure_when_codex_runner_raises(tmp_path) -> None:
    def fake_codex_task_runner(task, task_store):
        return task_store.update_task(
            task.task_id,
            status=TaskStatus.FAILED,
            stage="codex_failed",
            error="codex boom",
        )

    app = create_app(
        task_root=tmp_path / "service_tasks",
        workspace_root=tmp_path,
        codex_task_runner=fake_codex_task_runner,
    )
    client = TestClient(app)

    created = client.post(
        "/tasks/attribution",
        json={
            "stock_name": "腾景科技",
            "ts_code": "688195.SH",
            "start_date": "2025-09-10",
            "end_date": "2026-03-09",
            "sample_label": "数据中心",
        },
    ).json()

    response = client.post(f"/tasks/{created['task_id']}/run")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "failed"
    assert payload["stage"] == "codex_failed"
    assert payload["error"] == "codex boom"


def test_run_task_returns_backfilled_result_fields(tmp_path) -> None:
    def fake_codex_task_runner(task, task_store):
        return task_store.update_task(
            task.task_id,
            status=TaskStatus.COMPLETED,
            stage="completed",
            report_path="/tmp/report.md",
            plot_path="/tmp/plot.png",
            chatgpt_task_id="chatgpt-xyz",
        )

    app = create_app(
        task_root=tmp_path / "service_tasks",
        workspace_root=tmp_path,
        codex_task_runner=fake_codex_task_runner,
    )
    client = TestClient(app)

    created = client.post(
        "/tasks/attribution",
        json={
            "stock_name": "国博电子",
            "ts_code": "688375.SH",
            "start_date": "2025-12-10",
            "end_date": "2026-01-14",
            "sample_label": "5G",
        },
    ).json()

    response = client.post(f"/tasks/{created['task_id']}/run")

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "completed"
    assert payload["report_path"] == "/tmp/report.md"
    assert payload["plot_path"] == "/tmp/plot.png"
    assert payload["chatgpt_task_id"] == "chatgpt-xyz"


def test_run_task_passes_configured_timeout_to_default_runner(tmp_path, monkeypatch) -> None:
    captured = {}

    def fake_run_app_server_task(task, task_store, *, workspace_root=None, timeout_seconds=None):
        captured["workspace_root"] = workspace_root
        captured["timeout_seconds"] = timeout_seconds
        return task_store.update_task(
            task.task_id,
            status=TaskStatus.FAILED,
            stage="codex_timeout",
            error="codex app-server timed out after 30s",
            log_path="/tmp/attr.log",
        )

    monkeypatch.setattr(service_app_module, "run_app_server_task", fake_run_app_server_task)
    app = create_app(
        task_root=tmp_path / "service_tasks",
        workspace_root=tmp_path,
        codex_timeout_seconds=30,
    )
    client = TestClient(app)

    created = client.post(
        "/tasks/attribution",
        json={
            "stock_name": "腾景科技",
            "ts_code": "688195.SH",
            "start_date": "2025-10-24",
            "end_date": "2025-12-12",
            "sample_label": "数据中心",
        },
    ).json()

    response = client.post(f"/tasks/{created['task_id']}/run")

    assert response.status_code == 200
    assert captured["workspace_root"] == tmp_path
    assert captured["timeout_seconds"] == 30
    assert response.json()["log_path"] == "/tmp/attr.log"
