from __future__ import annotations

from pathlib import Path
from uuid import uuid4

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from service.app_server_runner import run_app_server_task
from service.datastore_health import collect_datastore_health
from service.models import AttributionTask, TaskStatus
from service.result_locator import locate_task_result
from service.task_store import TaskStore


class CreateTaskRequest(BaseModel):
    stock_name: str
    ts_code: str
    start_date: str
    end_date: str
    sample_label: str
    news_lookback_days: int = 14
    skip_concept: bool = False


def create_app(
    task_root: str | Path | None = None,
    *,
    chatgpt_status_reader=None,
    datastore_health_checker=None,
    codex_task_runner=None,
    workspace_root: str | Path | None = None,
    codex_timeout_seconds: int | None = None,
) -> FastAPI:
    task_store = TaskStore(task_root or Path("data/service_tasks"))
    resolved_workspace_root = Path(workspace_root) if workspace_root else None
    app = FastAPI(title="Attribution Service")

    def read_chatgpt_status() -> dict[str, object]:
        if chatgpt_status_reader is None:
            return {
                "loginCheck": {
                    "ok": False,
                    "reason": "status_reader_not_configured",
                }
            }
        return chatgpt_status_reader()

    def read_datastore_health() -> dict[str, object]:
        if datastore_health_checker is not None:
            return datastore_health_checker()
        return collect_datastore_health(workspace_root=resolved_workspace_root)

    def execute_codex_task(task: AttributionTask) -> AttributionTask:
        if codex_task_runner is not None:
            return codex_task_runner(task, task_store)
        return run_app_server_task(
            task,
            task_store,
            workspace_root=resolved_workspace_root,
            timeout_seconds=codex_timeout_seconds or 900,
        )

    @app.get("/health/datastores")
    def get_datastore_health() -> dict[str, object]:
        return read_datastore_health()

    @app.post("/tasks/attribution")
    def create_task(request: CreateTaskRequest) -> dict[str, object]:
        task = AttributionTask(
            task_id=f"attr-{uuid4()}",
            stock_name=request.stock_name,
            ts_code=request.ts_code,
            start_date=request.start_date,
            end_date=request.end_date,
            sample_label=request.sample_label,
            news_lookback_days=request.news_lookback_days,
            skip_concept=request.skip_concept,
        )
        task_store.save_task(task)
        return task.to_dict()

    @app.get("/tasks/{task_id}")
    def get_task(task_id: str) -> dict[str, object]:
        try:
            task = task_store.load_task(task_id)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail="task not found") from exc
        return task.to_dict()

    @app.get("/tasks/{task_id}/result")
    def get_task_result(task_id: str) -> dict[str, object]:
        try:
            task = task_store.load_task(task_id)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail="task not found") from exc
        return locate_task_result(task, workspace_root=resolved_workspace_root)

    @app.post("/tasks/{task_id}/run")
    def run_task(task_id: str) -> dict[str, object]:
        try:
            task = task_store.load_task(task_id)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail="task not found") from exc
        health = read_datastore_health()
        if not bool(health.get("ok")):
            updated = task_store.update_task(
                task.task_id,
                status=TaskStatus.FAILED,
                stage="datastore_health_check_failed",
                error=str(health.get("summary") or "数据库健康检查失败"),
                progress_summary="数据库健康检查失败",
            )
            return updated.to_dict()
        updated = execute_codex_task(task)
        return updated.to_dict()

    @app.post("/tasks/{task_id}/retry-chatgpt")
    def retry_chatgpt(task_id: str) -> dict[str, object]:
        try:
            task_store.load_task(task_id)
        except FileNotFoundError as exc:
            raise HTTPException(status_code=404, detail="task not found") from exc
        status = read_chatgpt_status()
        login_check = status.get("loginCheck", {})
        if not login_check.get("ok"):
            raise HTTPException(status_code=409, detail={"reason": login_check.get("reason", "unknown")})
        return {"task_id": task_id, "status": "queued", "stage": "retry_chatgpt"}

    return app


app = create_app()
