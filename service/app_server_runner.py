from __future__ import annotations

import json
import logging
import os
import select
import shlex
import subprocess
import time
from pathlib import Path

from service.codex_runner import (
    WORKSPACE_ROOT,
    DEFAULT_CODEX_TIMEOUT_SECONDS,
    build_codex_base_instructions,
    build_codex_developer_instructions,
    build_codex_prompt,
)
from service.models import AttributionTask, TaskStatus
from service.result_locator import locate_task_result
from service.task_store import TaskStore

logger = logging.getLogger(__name__)

NOISY_NOTIFICATION_METHODS = {
    "codex/event/agent_message_delta",
    "codex/event/agent_message_content_delta",
    "item/agentMessage/delta",
    "codex/event/reasoning_content_delta",
    "codex/event/reasoning_raw_content_delta",
    "item/reasoning/textDelta",
    "item/reasoning/summaryTextDelta",
}


def build_app_server_command() -> list[str]:
    return ["codex", "app-server"]


def build_app_server_env(base_env: dict[str, str] | None = None) -> dict[str, str]:
    env = dict(base_env or os.environ)
    proxy_url = "http://127.0.0.1:7897"
    for key in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"):
        env.setdefault(key, proxy_url)
    no_proxy_value = "localhost,127.0.0.1"
    for key in ("NO_PROXY", "no_proxy"):
        env.setdefault(key, no_proxy_value)
    return env


def _log_path(task_id: str, workspace_root: Path) -> Path:
    return workspace_root / "data" / "service_logs" / f"{task_id}.log"


def _append_log(path: Path, section: str, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(f"{section}\n{content}\n")


def _safe_read_stderr(process: subprocess.Popen[str] | object) -> str:
    stderr = getattr(process, "stderr", None)
    if stderr is None:
        return ""
    try:
        return stderr.read() or ""
    except Exception:  # noqa: BLE001
        return ""


def _terminate_process(process: subprocess.Popen[str] | object) -> None:
    try:
        process.terminate()
    except Exception:  # noqa: BLE001
        return
    try:
        process.wait(timeout=2)
    except Exception:  # noqa: BLE001
        return


def _read_message(
    process: subprocess.Popen[str] | object,
    *,
    deadline: float,
    monotonic=time.monotonic,
    sleep=time.sleep,
) -> dict[str, object]:
    stdout = getattr(process, "stdout", None)
    if stdout is None:
        raise RuntimeError("codex app-server stdout unavailable")
    while True:
        line = ""
        used_select = False
        if hasattr(stdout, "fileno"):
            try:
                remaining = max(deadline - monotonic(), 0.0)
                ready, _, _ = select.select([stdout], [], [], min(0.1, remaining))
                used_select = True
                if ready:
                    line = stdout.readline()
            except Exception:  # noqa: BLE001
                used_select = False
        if not used_select:
            line = stdout.readline()
        if line:
            return json.loads(line)
        if monotonic() > deadline:
            raise TimeoutError
        poll = getattr(process, "poll", None)
        returncode = poll() if callable(poll) else None
        if returncode is not None:
            raise RuntimeError(f"codex app-server exited before completion (code {returncode})")
        sleep(0.05)


def _write_request(process: subprocess.Popen[str] | object, payload: dict[str, object], log_path: Path) -> None:
    stdin = getattr(process, "stdin", None)
    if stdin is None:
        raise RuntimeError("codex app-server stdin unavailable")
    text = json.dumps(payload, ensure_ascii=False)
    stdin.write(f"{text}\n")
    stdin.flush()
    _append_log(log_path, f"[request] {payload['method']}", text)


def _update_progress(
    task_store: TaskStore,
    task_id: str,
    state: dict[str, str],
    *,
    stage: str | None = None,
) -> None:
    if state["last_command"]:
        state["progress_summary"] = f"最近命令: {state['last_command']}"
    elif state["last_event_type"]:
        state["progress_summary"] = f"最近事件: {state['last_event_type']}"
    task_store.update_task(
        task_id,
        stage=stage,
        progress_summary=state["progress_summary"],
        last_event_type=state["last_event_type"],
        last_command=state["last_command"],
    )


def _handle_notification(
    message: dict[str, object],
    *,
    task_store: TaskStore,
    task_id: str,
    log_path: Path,
    state: dict[str, str],
) -> dict[str, str] | None:
    method = str(message.get("method", ""))
    params = message.get("params") if isinstance(message.get("params"), dict) else {}
    _append_log(log_path, f"[notification] {method}", json.dumps(message, ensure_ascii=False))
    if method.startswith("codex/event/") and isinstance(params, dict):
        inner = params.get("msg")
        if isinstance(inner, dict):
            inner_type = str(inner.get("type", ""))
            if inner_type == "exec_command_begin":
                command = inner.get("command")
                if isinstance(command, list):
                    state["last_command"] = shlex.join(str(part) for part in command)
                elif isinstance(command, str):
                    state["last_command"] = command
                state["last_event_type"] = "exec_command_begin"
                _update_progress(task_store, task_id, state, stage="command_execution")
                return None
            if inner_type == "stream_error":
                state["last_event_type"] = "stream_error"
                state["progress_summary"] = "最近事件: stream_error"
                details = str(inner.get("additional_details", "")).strip()
                message_text = str(inner.get("message", "")).strip()
                error_text = details or message_text or "codex stream disconnected"
                _update_progress(task_store, task_id, state)
                return {"status": "failed", "error": error_text, "stage": "codex_stream_error"}
            if method not in NOISY_NOTIFICATION_METHODS and inner_type:
                state["last_event_type"] = inner_type
    elif method not in NOISY_NOTIFICATION_METHODS:
        state["last_event_type"] = method
    if method == "item/started" and isinstance(params, dict):
        item = params.get("item")
        if isinstance(item, dict) and item.get("type") == "commandExecution":
            state["last_command"] = str(item.get("command", ""))
            _update_progress(task_store, task_id, state, stage="command_execution")
            return None
    if method == "thread/started":
        _update_progress(task_store, task_id, state, stage="thread_started")
        return None
    if method == "turn/started":
        _update_progress(task_store, task_id, state, stage="turn_started")
        return None
    if method == "item/commandExecution/outputDelta" and isinstance(params, dict):
        delta = str(params.get("delta", ""))
        if delta:
            _append_log(log_path, "[command_output_delta]", delta)
        _update_progress(task_store, task_id, state)
        return None
    if method == "turn/completed" and isinstance(params, dict):
        turn = params.get("turn")
        if not isinstance(turn, dict):
            return {"status": "failed", "error": "codex turn payload missing"}
        if turn.get("status") == "completed":
            _update_progress(task_store, task_id, state)
            return {"status": "completed", "error": ""}
        error = turn.get("error")
        if isinstance(error, dict):
            message_text = str(error.get("message") or error.get("details") or json.dumps(error, ensure_ascii=False))
        else:
            message_text = "codex turn failed"
        _update_progress(task_store, task_id, state)
        return {"status": "failed", "error": message_text}
    _update_progress(task_store, task_id, state)
    return None


def _wait_for_response(
    process: subprocess.Popen[str] | object,
    *,
    request_id: int,
    request_method: str,
    deadline: float,
    task_store: TaskStore,
    task_id: str,
    log_path: Path,
    state: dict[str, str],
    monotonic=time.monotonic,
    sleep=time.sleep,
) -> dict[str, object]:
    while True:
        message = _read_message(process, deadline=deadline, monotonic=monotonic, sleep=sleep)
        if "method" in message:
            _handle_notification(message, task_store=task_store, task_id=task_id, log_path=log_path, state=state)
            continue
        if message.get("id") != request_id:
            _append_log(log_path, "[response] unexpected", json.dumps(message, ensure_ascii=False))
            continue
        if "error" in message:
            _append_log(log_path, f"[response] {request_method}", json.dumps(message, ensure_ascii=False))
            raise RuntimeError(str(message["error"]))
        _append_log(log_path, f"[response] {request_method}", json.dumps(message, ensure_ascii=False))
        result = message.get("result")
        return result if isinstance(result, dict) else {}


def _wait_for_turn_completion(
    process: subprocess.Popen[str] | object,
    *,
    deadline: float,
    task_store: TaskStore,
    task_id: str,
    log_path: Path,
    state: dict[str, str],
    monotonic=time.monotonic,
    sleep=time.sleep,
) -> dict[str, str]:
    while True:
        message = _read_message(process, deadline=deadline, monotonic=monotonic, sleep=sleep)
        if "method" not in message:
            _append_log(log_path, "[response] unexpected", json.dumps(message, ensure_ascii=False))
            continue
        result = _handle_notification(message, task_store=task_store, task_id=task_id, log_path=log_path, state=state)
        if result is not None:
            return result


def run_app_server_task(
    task: AttributionTask,
    task_store: TaskStore,
    *,
    process_factory=subprocess.Popen,
    workspace_root: str | Path | None = None,
    timeout_seconds: int = DEFAULT_CODEX_TIMEOUT_SECONDS,
    monotonic=time.monotonic,
    sleep=time.sleep,
):
    root = Path(workspace_root or WORKSPACE_ROOT)
    log_path = _log_path(task.task_id, root)
    if log_path.exists():
        log_path.unlink()
    task_store.update_task(
        task.task_id,
        status=TaskStatus.RUNNING,
        stage="codex_running",
        error="",
        log_path=str(log_path),
    )
    state = {
        "progress_summary": "",
        "last_event_type": "",
        "last_command": "",
    }
    command = build_app_server_command()
    process = None
    try:
        env = build_app_server_env()
        process = process_factory(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            cwd=str(root),
            env=env,
        )
        _append_log(log_path, "[command]", " ".join(command))
        _append_log(
            log_path,
            "[proxy_env]",
            json.dumps(
                {
                    key: env.get(key, "")
                    for key in (
                        "HTTP_PROXY",
                        "HTTPS_PROXY",
                        "ALL_PROXY",
                        "NO_PROXY",
                    )
                },
                ensure_ascii=False,
            ),
        )
        deadline = monotonic() + timeout_seconds

        request_id = 1
        _write_request(
            process,
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "method": "initialize",
                "params": {
                    "clientInfo": {"name": "attribution-service", "version": "0.1.0"},
                    "capabilities": None,
                },
            },
            log_path,
        )
        _wait_for_response(
            process,
            request_id=request_id,
            request_method="initialize",
            deadline=deadline,
            task_store=task_store,
            task_id=task.task_id,
            log_path=log_path,
            state=state,
            monotonic=monotonic,
            sleep=sleep,
        )

        request_id += 1
        _write_request(
            process,
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "method": "newConversation",
                "params": {
                    "model": None,
                    "modelProvider": None,
                    "profile": None,
                    "cwd": str(root),
                    "approvalPolicy": "never",
                    "sandbox": "danger-full-access",
                    "config": None,
                    "baseInstructions": build_codex_base_instructions(),
                    "developerInstructions": build_codex_developer_instructions(),
                    "compactPrompt": None,
                    "includeApplyPatchTool": True,
                },
            },
            log_path,
        )
        conversation = _wait_for_response(
            process,
            request_id=request_id,
            request_method="newConversation",
            deadline=deadline,
            task_store=task_store,
            task_id=task.task_id,
            log_path=log_path,
            state=state,
            monotonic=monotonic,
            sleep=sleep,
        )
        conversation_id = str(conversation.get("conversationId", ""))
        if not conversation_id:
            raise RuntimeError("codex app-server missing conversationId")

        request_id += 1
        _write_request(
            process,
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "method": "addConversationListener",
                "params": {
                    "conversationId": conversation_id,
                    "experimentalRawEvents": True,
                },
            },
            log_path,
        )
        _wait_for_response(
            process,
            request_id=request_id,
            request_method="addConversationListener",
            deadline=deadline,
            task_store=task_store,
            task_id=task.task_id,
            log_path=log_path,
            state=state,
            monotonic=monotonic,
            sleep=sleep,
        )

        request_id += 1
        _write_request(
            process,
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "method": "sendUserMessage",
                "params": {
                    "conversationId": conversation_id,
                    "items": [
                        {
                            "type": "text",
                            "data": {
                                "text": build_codex_prompt(task),
                                "text_elements": [],
                            },
                        }
                    ],
                },
            },
            log_path,
        )
        _wait_for_response(
            process,
            request_id=request_id,
            request_method="sendUserMessage",
            deadline=deadline,
            task_store=task_store,
            task_id=task.task_id,
            log_path=log_path,
            state=state,
            monotonic=monotonic,
            sleep=sleep,
        )

        turn_result = _wait_for_turn_completion(
            process,
            deadline=deadline,
            task_store=task_store,
            task_id=task.task_id,
            log_path=log_path,
            state=state,
            monotonic=monotonic,
            sleep=sleep,
        )
        _terminate_process(process)
        stderr_text = _safe_read_stderr(process)
        if stderr_text:
            _append_log(log_path, "stderr", stderr_text)
        if turn_result["status"] != "completed":
            return task_store.update_task(
                task.task_id,
                status=TaskStatus.FAILED,
                stage=turn_result.get("stage", "codex_failed"),
                error=turn_result["error"],
                log_path=str(log_path),
                progress_summary=state["progress_summary"],
                last_event_type=state["last_event_type"],
                last_command=state["last_command"],
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
            progress_summary=state["progress_summary"],
            last_event_type=state["last_event_type"],
            last_command=state["last_command"],
        )
    except TimeoutError:
        if process is not None:
            _terminate_process(process)
            stderr_text = _safe_read_stderr(process)
            if stderr_text:
                _append_log(log_path, "[stderr]", stderr_text)
        return task_store.update_task(
            task.task_id,
            status=TaskStatus.FAILED,
            stage="codex_timeout",
            error=f"codex app-server timed out after {timeout_seconds}s",
            log_path=str(log_path),
            progress_summary=state["progress_summary"],
            last_event_type=state["last_event_type"],
            last_command=state["last_command"],
        )
    except Exception as exc:  # noqa: BLE001
        logger.error("运行 codex app-server 失败", exc_info=True)
        if process is not None:
            _terminate_process(process)
            stderr_text = _safe_read_stderr(process)
            if stderr_text:
                _append_log(log_path, "[stderr]", stderr_text)
        _append_log(log_path, "[error]", str(exc))
        return task_store.update_task(
            task.task_id,
            status=TaskStatus.FAILED,
            stage="codex_failed",
            error=str(exc),
            log_path=str(log_path),
            progress_summary=state["progress_summary"],
            last_event_type=state["last_event_type"],
            last_command=state["last_command"],
        )
