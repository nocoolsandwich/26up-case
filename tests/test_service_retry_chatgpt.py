from __future__ import annotations

from fastapi.testclient import TestClient

from service.app import create_app


def test_retry_chatgpt_returns_clear_error_when_login_check_fails(tmp_path) -> None:
    def fake_status_reader():
        return {
            "url": "about:blank",
            "hasComposer": False,
            "loginCheck": {
                "ok": False,
                "reason": "blank_page",
            },
        }

    app = create_app(
        task_root=tmp_path / "service_tasks",
        chatgpt_status_reader=fake_status_reader,
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

    response = client.post(f"/tasks/{created['task_id']}/retry-chatgpt")

    assert response.status_code == 409
    payload = response.json()
    assert payload["detail"]["reason"] == "blank_page"
