from __future__ import annotations

import io
import json

from service.app_server_runner import build_app_server_command, run_app_server_task
from service.models import AttributionTask, TaskStatus
from service.task_store import TaskStore


class _FakeStdin:
    def __init__(self) -> None:
        self.writes: list[str] = []

    def write(self, text: str) -> int:
        self.writes.append(text)
        return len(text)

    def flush(self) -> None:
        return None

    def close(self) -> None:
        return None


class _FakeStdout:
    def __init__(self, lines: list[str]) -> None:
        self._lines = [line if line.endswith("\n") else f"{line}\n" for line in lines]
        self._index = 0

    def readline(self) -> str:
        if self._index >= len(self._lines):
            return ""
        line = self._lines[self._index]
        self._index += 1
        return line


class _FakeProcess:
    def __init__(self, lines: list[str], stderr: str = "") -> None:
        self.stdin = _FakeStdin()
        self.stdout = _FakeStdout(lines)
        self.stderr = io.StringIO(stderr)
        self.returncode: int | None = None
        self.terminated = False

    def poll(self) -> int | None:
        return self.returncode

    def terminate(self) -> None:
        self.terminated = True
        self.returncode = 0

    def wait(self, timeout: float | None = None) -> int:
        self.returncode = 0 if self.returncode is None else self.returncode
        return self.returncode


def _make_time(values: list[float]):
    state = {"index": 0}

    def _now() -> float:
        index = state["index"]
        if index >= len(values):
            return values[-1]
        state["index"] += 1
        return values[index]

    return _now


def test_build_app_server_command_uses_codex_app_server() -> None:
    assert build_app_server_command() == ["codex", "app-server"]


def test_run_app_server_task_completes_and_records_requests_progress_and_log(tmp_path) -> None:
    store = TaskStore(tmp_path / "service_tasks")
    task = AttributionTask(
        task_id="attr-app-server-ok",
        stock_name="国博电子",
        ts_code="688375.SH",
        start_date="2025-12-10",
        end_date="2026-01-14",
        sample_label="5G",
    )
    store.save_task(task)

    report_dir = tmp_path / "docs" / "analysis"
    plot_dir = tmp_path / "data" / "plots"
    report_dir.mkdir(parents=True, exist_ok=True)
    plot_dir.mkdir(parents=True, exist_ok=True)
    (report_dir / "2026-03-25-688375SH-国博电子-wave-attribution.md").write_text(
        "\n".join(
            [
                "### ChatGPT 联网归因",
                "- task id：",
                "  `d7b0a15f-688a-41af-b738-ec8d9ab5290a`",
                "- 结果文件：",
                "  `skills/chatgpt-plus-browser/.state/d7b0a15f-688a-41af-b738-ec8d9ab5290a.json`",
            ]
        ),
        encoding="utf-8",
    )
    (plot_dir / "688375_SH_wave_candles.png").write_text("png", encoding="utf-8")

    process = _FakeProcess(
        [
            json.dumps({"jsonrpc": "2.0", "id": 1, "result": {"protocolVersion": "0"}}),
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": 2,
                    "result": {
                        "conversationId": "conv-1",
                        "rolloutPath": str(tmp_path / "rollout.jsonl"),
                        "model": "gpt-5.4",
                        "reasoningEffort": None,
                    },
                }
            ),
            json.dumps({"jsonrpc": "2.0", "id": 3, "result": {}}),
            json.dumps({"jsonrpc": "2.0", "method": "thread/started", "params": {"thread": {"id": "conv-1"}}}),
            json.dumps({"jsonrpc": "2.0", "id": 4, "result": {}}),
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "method": "item/started",
                    "params": {
                        "threadId": "conv-1",
                        "turnId": "turn-1",
                        "item": {
                            "type": "commandExecution",
                            "id": "item-1",
                            "command": "python scripts/run.py",
                            "cwd": str(tmp_path),
                            "processId": "p1",
                            "status": "in_progress",
                            "commandActions": [],
                            "aggregatedOutput": None,
                            "exitCode": None,
                            "durationMs": None,
                        },
                    },
                }
            ),
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "method": "item/commandExecution/outputDelta",
                    "params": {
                        "threadId": "conv-1",
                        "turnId": "turn-1",
                        "itemId": "item-1",
                        "delta": "wave plotting done\n",
                    },
                }
            ),
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "method": "turn/completed",
                    "params": {
                        "threadId": "conv-1",
                        "turn": {"id": "turn-1", "items": [], "status": "completed", "error": None},
                    },
                }
            ),
        ],
        stderr="app server stderr noise",
    )

    updated = run_app_server_task(
        task,
        store,
        process_factory=lambda *_args, **_kwargs: process,
        workspace_root=tmp_path,
    )

    assert updated.status == TaskStatus.COMPLETED
    assert updated.stage == "completed"
    assert updated.report_path.endswith("2026-03-25-688375SH-国博电子-wave-attribution.md")
    assert updated.plot_path.endswith("688375_SH_wave_candles.png")
    assert updated.chatgpt_task_id == "d7b0a15f-688a-41af-b738-ec8d9ab5290a"
    assert updated.progress_summary == "最近命令: python scripts/run.py"
    assert updated.last_event_type == "turn/completed"
    assert updated.last_command == "python scripts/run.py"

    requests = [json.loads(item) for item in process.stdin.writes]
    assert [request["method"] for request in requests] == [
        "initialize",
        "newConversation",
        "addConversationListener",
        "sendUserMessage",
    ]
    assert requests[1]["params"]["cwd"] == str(tmp_path)
    assert requests[1]["params"]["approvalPolicy"] == "never"
    assert requests[1]["params"]["sandbox"] == "danger-full-access"
    assert requests[3]["params"]["conversationId"] == "conv-1"
    assert requests[3]["params"]["items"][0]["type"] == "text"

    log_text = (tmp_path / "data" / "service_logs" / "attr-app-server-ok.log").read_text(encoding="utf-8")
    assert "[request] initialize" in log_text
    assert "[response] newConversation" in log_text
    assert "[notification] item/started" in log_text
    assert "wave plotting done" in log_text
    assert "app server stderr noise" in log_text


def test_run_app_server_task_marks_timeout_with_last_progress(tmp_path) -> None:
    store = TaskStore(tmp_path / "service_tasks")
    task = AttributionTask(
        task_id="attr-app-server-timeout",
        stock_name="精测电子",
        ts_code="300567.SZ",
        start_date="2025-09-10",
        end_date="2026-03-09",
        sample_label="芯片概念",
    )
    store.save_task(task)
    process = _FakeProcess(
        [
            json.dumps({"jsonrpc": "2.0", "id": 1, "result": {"protocolVersion": "0"}}),
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": 2,
                    "result": {
                        "conversationId": "conv-timeout",
                        "rolloutPath": str(tmp_path / "rollout-timeout.jsonl"),
                        "model": "gpt-5.4",
                        "reasoningEffort": None,
                    },
                }
            ),
            json.dumps({"jsonrpc": "2.0", "id": 3, "result": {}}),
            json.dumps({"jsonrpc": "2.0", "id": 4, "result": {}}),
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "method": "item/started",
                    "params": {
                        "threadId": "conv-timeout",
                        "turnId": "turn-timeout",
                        "item": {
                            "type": "commandExecution",
                            "id": "item-1",
                            "command": "python - <<'PY'",
                            "cwd": str(tmp_path),
                            "processId": "p1",
                            "status": "in_progress",
                            "commandActions": [],
                            "aggregatedOutput": None,
                            "exitCode": None,
                            "durationMs": None,
                        },
                    },
                }
            ),
        ]
    )

    updated = run_app_server_task(
        task,
        store,
        process_factory=lambda *_args, **_kwargs: process,
        workspace_root=tmp_path,
        timeout_seconds=5,
        monotonic=_make_time([0.0, 0.1, 0.2, 6.0]),
        sleep=lambda _seconds: None,
    )

    assert updated.status == TaskStatus.FAILED
    assert updated.stage == "codex_timeout"
    assert updated.error == "codex app-server timed out after 5s"
    assert updated.progress_summary == "最近命令: python - <<'PY'"
    assert updated.last_event_type == "item/started"
    assert updated.last_command == "python - <<'PY'"
    assert process.terminated is True


def test_run_app_server_task_marks_failed_when_turn_failed(tmp_path) -> None:
    store = TaskStore(tmp_path / "service_tasks")
    task = AttributionTask(
        task_id="attr-app-server-failed",
        stock_name="腾景科技",
        ts_code="688195.SH",
        start_date="2025-10-24",
        end_date="2025-12-12",
        sample_label="数据中心",
    )
    store.save_task(task)
    process = _FakeProcess(
        [
            json.dumps({"jsonrpc": "2.0", "id": 1, "result": {"protocolVersion": "0"}}),
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": 2,
                    "result": {
                        "conversationId": "conv-failed",
                        "rolloutPath": str(tmp_path / "rollout-failed.jsonl"),
                        "model": "gpt-5.4",
                        "reasoningEffort": None,
                    },
                }
            ),
            json.dumps({"jsonrpc": "2.0", "id": 3, "result": {}}),
            json.dumps({"jsonrpc": "2.0", "id": 4, "result": {}}),
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "method": "turn/completed",
                    "params": {
                        "threadId": "conv-failed",
                        "turn": {
                            "id": "turn-failed",
                            "items": [],
                            "status": "failed",
                            "error": {"message": "chatgpt login invalid"},
                        },
                    },
                }
            ),
        ]
    )

    updated = run_app_server_task(
        task,
        store,
        process_factory=lambda *_args, **_kwargs: process,
        workspace_root=tmp_path,
    )

    assert updated.status == TaskStatus.FAILED
    assert updated.stage == "codex_failed"
    assert updated.error == "chatgpt login invalid"
    assert updated.progress_summary == "最近事件: turn/completed"
    assert updated.last_event_type == "turn/completed"


def test_run_app_server_task_prefers_exec_command_begin_over_agent_delta_noise(tmp_path) -> None:
    store = TaskStore(tmp_path / "service_tasks")
    task = AttributionTask(
        task_id="attr-app-server-noise",
        stock_name="精测电子",
        ts_code="300567.SZ",
        start_date="2025-09-10",
        end_date="2026-03-09",
        sample_label="芯片概念",
    )
    store.save_task(task)
    process = _FakeProcess(
        [
            json.dumps({"jsonrpc": "2.0", "id": 1, "result": {"protocolVersion": "0"}}),
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": 2,
                    "result": {
                        "conversationId": "conv-noise",
                        "rolloutPath": str(tmp_path / "rollout-noise.jsonl"),
                        "model": "gpt-5.4",
                        "reasoningEffort": None,
                    },
                }
            ),
            json.dumps({"jsonrpc": "2.0", "id": 3, "result": {}}),
            json.dumps({"jsonrpc": "2.0", "id": 4, "result": {}}),
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "method": "codex/event/exec_command_begin",
                    "params": {
                        "conversationId": "conv-noise",
                        "id": "turn-noise",
                        "msg": {
                            "type": "exec_command_begin",
                            "command": ["/bin/zsh", "-lc", "rg --files ."],
                            "cwd": str(tmp_path),
                        },
                    },
                }
            ),
            json.dumps(
                {
                    "jsonrpc": "2.0",
                    "method": "codex/event/agent_message_delta",
                    "params": {
                        "conversationId": "conv-noise",
                        "id": "turn-noise",
                        "msg": {"type": "agent_message_delta", "delta": "我先看一下仓库"},
                    },
                }
            ),
        ]
    )

    updated = run_app_server_task(
        task,
        store,
        process_factory=lambda *_args, **_kwargs: process,
        workspace_root=tmp_path,
        timeout_seconds=5,
        monotonic=_make_time([0.0, 0.1, 0.2, 0.3, 0.4, 6.0]),
        sleep=lambda _seconds: None,
    )

    assert updated.status == TaskStatus.FAILED
    assert updated.stage == "codex_timeout"
    assert updated.progress_summary == "最近命令: /bin/zsh -lc 'rg --files .'"
    assert updated.last_event_type == "exec_command_begin"
    assert updated.last_command == "/bin/zsh -lc 'rg --files .'"
