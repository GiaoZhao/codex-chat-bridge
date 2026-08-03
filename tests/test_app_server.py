from __future__ import annotations

import json
import socket
import tempfile
import threading
import unittest
from collections import deque
from pathlib import Path
from types import SimpleNamespace
from typing import Callable, Dict, List, Optional, Tuple
from unittest.mock import Mock, patch

from bridge import select_codex_runner
from bridge_core import CodexRunner
from codex_app_server import (
    AppServerCodexRunner,
    AppServerProtocolError,
    AppServerRPCClient,
    AppServerUnavailableError,
)


THREAD_ID = "00000000-0000-4000-8000-000000000001"
OTHER_THREAD_ID = "00000000-0000-4000-8000-000000000002"
TURN_ID = "00000000-0000-4000-8000-000000000011"
OTHER_TURN_ID = "00000000-0000-4000-8000-000000000012"


def item_completed(
    text: str,
    *,
    thread_id: str = THREAD_ID,
    turn_id: str = TURN_ID,
    phase: str = "final_answer",
) -> dict:
    return {
        "method": "item/completed",
        "params": {
            "threadId": thread_id,
            "turnId": turn_id,
            "item": {
                "id": "item-1",
                "type": "agentMessage",
                "text": text,
                "phase": phase,
            },
            "completedAtMs": 1,
        },
    }


def turn_completed(
    status: str = "completed",
    *,
    thread_id: str = THREAD_ID,
    turn_id: str = TURN_ID,
    error: object = None,
) -> dict:
    return {
        "method": "turn/completed",
        "params": {
            "threadId": thread_id,
            "turn": {
                "id": turn_id,
                "status": status,
                "items": [],
                "error": error,
            },
        },
    }


class ScriptedClient:
    def __init__(
        self,
        responses: dict,
        *,
        events: Optional[List[object]] = None,
        before_response: Optional[Dict[str, List[dict]]] = None,
        receive_hook: Optional[Callable[[], dict]] = None,
    ) -> None:
        self.responses = responses
        self.events = deque(events or [])
        self.before_response = {
            method: list(messages)
            for method, messages in (before_response or {}).items()
        }
        self.receive_hook = receive_hook
        self.calls: List[Tuple] = []
        self.connected = False
        self.closed = False

    def connect(self) -> None:
        self.connected = True

    def close(self) -> None:
        self.closed = True

    def request(self, method: str, params: dict, on_message=None):
        self.calls.append(("request", method, params))
        for message in self.before_response.pop(method, []):
            if on_message is not None:
                on_message(message)
        response = self.responses[method]
        if isinstance(response, BaseException):
            raise response
        if callable(response):
            return response(params)
        return response

    def notify(self, method: str, params: Optional[dict] = None) -> None:
        self.calls.append(("notify", method, params))

    def receive(self) -> dict:
        if self.receive_hook is not None:
            return self.receive_hook()
        if not self.events:
            raise AssertionError("scripted client has no event")
        value = self.events.popleft()
        if isinstance(value, BaseException):
            raise value
        return value


def runner_config(root: Path, *, timeout: int = 60) -> SimpleNamespace:
    return SimpleNamespace(
        codex_app_server_socket=root / "app-server.sock",
        codex_thread_id=THREAD_ID,
        codex_workdir=root,
        codex_turn_timeout=timeout,
    )


def resume_responses() -> dict:
    return {
        "initialize": {"userAgent": "codex-cli/0.146.0"},
        "thread/resume": {"thread": {"id": THREAD_ID}},
        "turn/start": {"turn": {"id": TURN_ID}},
    }


class RPCClientTests(unittest.TestCase):
    def test_server_request_with_same_id_does_not_replace_client_response(self) -> None:
        raw_socket = Mock()
        ws = Mock()
        ws.recv.side_effect = [
            json.dumps(
                {
                    "id": 1,
                    "method": "item/commandExecution/requestApproval",
                    "params": {"threadId": THREAD_ID, "turnId": TURN_ID},
                }
            ),
            json.dumps({"id": 1, "result": {"ok": True}}),
        ]
        client = AppServerRPCClient(
            Path("/tmp/app-server.sock"),
            socket_factory=Mock(return_value=raw_socket),
            websocket_factory=Mock(return_value=ws),
        )
        client.connect()
        received = []

        result = client.request("initialize", {}, received.append)

        self.assertEqual(result, {"ok": True})
        self.assertEqual(len(received), 1)
        self.assertIn("method", received[0])

    def test_deferred_notification_is_delivered_once(self) -> None:
        raw_socket = Mock()
        ws = Mock()
        notification = {"method": "thread/started", "params": {"thread": {}}}
        ws.recv.side_effect = [
            json.dumps(notification),
            json.dumps({"id": 1, "result": {}}),
            json.dumps({"id": 2, "result": {"ok": True}}),
        ]
        client = AppServerRPCClient(
            Path("/tmp/app-server.sock"),
            socket_factory=Mock(return_value=raw_socket),
            websocket_factory=Mock(return_value=ws),
        )
        client.connect()

        client.request("initialize", {})
        received = []
        result = client.request("thread/list", {}, received.append)

        self.assertEqual(result, {"ok": True})
        self.assertEqual(received, [notification])

    def test_connect_failure_is_wrapped_as_unavailable(self) -> None:
        raw_socket = Mock()
        raw_socket.connect.side_effect = OSError("connection refused")
        client = AppServerRPCClient(
            Path("/tmp/missing.sock"),
            socket_factory=Mock(return_value=raw_socket),
            websocket_factory=Mock(),
        )

        with self.assertRaisesRegex(AppServerUnavailableError, "connection refused"):
            client.connect()

        raw_socket.close.assert_called_once()


class AppServerRunnerTests(unittest.TestCase):
    def test_resume_returns_current_turn_final_and_completes_handshake(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            client = ScriptedClient(
                resume_responses(),
                events=[item_completed("done"), turn_completed()],
            )
            runner = AppServerCodexRunner(
                runner_config(root), client_factory=lambda: client
            )

            result = runner.run("question", THREAD_ID, root)

        self.assertEqual(result, (0, "done", ""))
        self.assertEqual(client.calls[0][1], "initialize")
        self.assertEqual(client.calls[1], ("notify", "initialized", None))
        self.assertTrue(client.closed)

    def test_new_thread_sets_name_before_starting_turn(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            responses = {
                "initialize": {"userAgent": "codex-cli/0.146.0"},
                "thread/start": {"thread": {"id": THREAD_ID}},
                "thread/name/set": {},
                "turn/start": {"turn": {"id": TURN_ID}},
            }
            client = ScriptedClient(
                responses,
                events=[item_completed("created"), turn_completed()],
            )
            runner = AppServerCodexRunner(
                runner_config(root), client_factory=lambda: client
            )

            result = runner.run_new("first question", root, thread_name="First question")

        self.assertEqual(result, (0, THREAD_ID, "created", ""))
        methods = [call[1] for call in client.calls]
        self.assertEqual(
            methods,
            ["initialize", "initialized", "thread/start", "thread/name/set", "turn/start"],
        )
        name_call = next(call for call in client.calls if call[1] == "thread/name/set")
        self.assertEqual(
            name_call[2], {"threadId": THREAD_ID, "name": "First question"}
        )

    def test_local_images_are_sent_as_protocol_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            image = root / "input image.png"
            image.write_bytes(b"image")
            client = ScriptedClient(
                resume_responses(),
                events=[item_completed("seen"), turn_completed()],
            )
            runner = AppServerCodexRunner(
                runner_config(root), client_factory=lambda: client
            )

            runner.run("inspect", THREAD_ID, root, [image])

        turn_call = next(call for call in client.calls if call[1] == "turn/start")
        self.assertEqual(
            turn_call[2]["input"],
            [
                {"type": "text", "text": "inspect"},
                {"type": "localImage", "path": str(image.resolve())},
            ],
        )

    def test_events_before_turn_start_response_are_replayed_for_returned_turn(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            client = ScriptedClient(
                resume_responses(),
                before_response={
                    "turn/start": [
                        item_completed(
                            "other",
                            thread_id=OTHER_THREAD_ID,
                            turn_id=OTHER_TURN_ID,
                        ),
                        turn_completed(
                            thread_id=OTHER_THREAD_ID,
                            turn_id=OTHER_TURN_ID,
                        ),
                        item_completed("fast result"),
                        turn_completed(),
                    ]
                },
            )
            runner = AppServerCodexRunner(
                runner_config(root), client_factory=lambda: client
            )

            result = runner.run("fast", THREAD_ID, root)

        self.assertEqual(result, (0, "fast result", ""))

    def test_other_turn_broadcast_does_not_contaminate_final(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            client = ScriptedClient(
                resume_responses(),
                events=[
                    item_completed("wrong", turn_id=OTHER_TURN_ID),
                    turn_completed(turn_id=OTHER_TURN_ID),
                    item_completed("right"),
                    turn_completed(),
                ],
            )
            runner = AppServerCodexRunner(
                runner_config(root), client_factory=lambda: client
            )

            result = runner.run("question", THREAD_ID, root)

        self.assertEqual(result, (0, "right", ""))

    def test_only_current_turn_approval_emits_diagnostic(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            current_approval = {
                "id": 41,
                "method": "item/commandExecution/requestApproval",
                "params": {"threadId": THREAD_ID, "turnId": TURN_ID},
            }
            other_approval = {
                "id": 42,
                "method": "item/commandExecution/requestApproval",
                "params": {
                    "threadId": OTHER_THREAD_ID,
                    "turnId": OTHER_TURN_ID,
                },
            }
            client = ScriptedClient(
                resume_responses(),
                events=[
                    other_approval,
                    current_approval,
                    item_completed("approved"),
                    turn_completed(),
                ],
            )
            runner = AppServerCodexRunner(
                runner_config(root), client_factory=lambda: client
            )
            diagnostics = []

            result = runner.run(
                "question",
                THREAD_ID,
                root,
                on_diagnostic=diagnostics.append,
            )

        self.assertEqual(result, (0, "approved", ""))
        self.assertEqual(
            diagnostics,
            [
                "desktop interaction required: "
                "item/commandExecution/requestApproval"
            ],
        )

    def test_failed_and_external_interrupted_statuses_are_distinct(self) -> None:
        cases = [
            ("failed", {"message": "upstream failed"}, 1, "upstream failed", ""),
            ("interrupted", None, -15, "turn status: interrupted", "interrupted"),
        ]
        for status, error, code, detail, reason in cases:
            with self.subTest(status=status), tempfile.TemporaryDirectory() as raw_dir:
                root = Path(raw_dir)
                client = ScriptedClient(
                    resume_responses(),
                    events=[turn_completed(status, error=error)],
                )
                runner = AppServerCodexRunner(
                    runner_config(root), client_factory=lambda: client
                )

                result_code, final, result_error = runner.run(
                    "question", THREAD_ID, root
                )

                self.assertEqual((result_code, final), (code, None))
                self.assertEqual(result_error, detail)
                self.assertEqual(runner.termination_reason(), reason)

    def test_cancel_sends_turn_interrupt_and_preserves_cancel_reason(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            release_receive = threading.Event()
            started = threading.Event()

            def receive_completed() -> dict:
                if not release_receive.wait(2):
                    raise AssertionError("cancel did not release the active turn")
                return turn_completed("interrupted")

            main_client = ScriptedClient(
                resume_responses(), receive_hook=receive_completed
            )

            def interrupt_response(_params: dict) -> dict:
                release_receive.set()
                return {}

            interrupt_client = ScriptedClient(
                {
                    "initialize": {"userAgent": "codex-cli/0.146.0"},
                    "turn/interrupt": interrupt_response,
                }
            )
            clients = deque([main_client, interrupt_client])
            runner = AppServerCodexRunner(
                runner_config(root), client_factory=clients.popleft
            )
            result: List[Tuple] = []
            worker = threading.Thread(
                target=lambda: result.append(
                    runner.run(
                        "question",
                        THREAD_ID,
                        root,
                        on_started=lambda _pid: started.set(),
                    )
                )
            )
            worker.start()
            self.assertTrue(started.wait(2))

            self.assertTrue(runner.cancel())
            worker.join(2)

        self.assertFalse(worker.is_alive())
        self.assertEqual(result[0][0], -15)
        self.assertEqual(runner.termination_reason(), "cancelled")
        interrupt_call = next(
            call for call in interrupt_client.calls if call[1] == "turn/interrupt"
        )
        self.assertEqual(
            interrupt_call[2], {"threadId": THREAD_ID, "turnId": TURN_ID}
        )

    def test_cancel_failure_clears_cancel_reason(self) -> None:
        runner = AppServerCodexRunner(
            SimpleNamespace(codex_app_server_socket=Path("/tmp/app.sock")),
            client_factory=Mock(side_effect=AppServerProtocolError("unavailable")),
        )
        runner._active_thread_id = THREAD_ID
        runner._active_turn_id = TURN_ID

        self.assertFalse(runner.cancel())
        self.assertEqual(runner.termination_reason(), "")

    def test_timeout_requests_interrupt(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            main_client = ScriptedClient(resume_responses())
            interrupt_client = ScriptedClient(
                {
                    "initialize": {"userAgent": "codex-cli/0.146.0"},
                    "turn/interrupt": {},
                }
            )
            clients = deque([main_client, interrupt_client])
            runner = AppServerCodexRunner(
                runner_config(root, timeout=1), client_factory=clients.popleft
            )

            with patch("codex_app_server.time.monotonic", side_effect=[0.0, 2.0]):
                result = runner.run("question", THREAD_ID, root)

        self.assertEqual(result[0], 124)
        self.assertIn("超过 1 秒", result[2])
        self.assertEqual(runner.termination_reason(), "timed_out")
        self.assertTrue(
            any(call[1] == "turn/interrupt" for call in interrupt_client.calls)
        )


class TransportSelectionTests(unittest.TestCase):
    def config(self, mode: str) -> SimpleNamespace:
        return SimpleNamespace(
            codex_transport=mode,
            codex_app_server_socket=Path("/tmp/app-server.sock"),
        )

    def test_exec_mode_never_probes_socket(self) -> None:
        with patch("bridge.AppServerCodexRunner") as app_runner:
            runner = select_codex_runner(self.config("exec"))

        self.assertIsInstance(runner, CodexRunner)
        app_runner.assert_not_called()

    def test_auto_uses_verified_app_server_when_socket_exists(self) -> None:
        app_runner = Mock()
        app_runner.probe.return_value = {"userAgent": "codex-cli/0.146.0"}
        with patch("bridge.Path.is_socket", return_value=True), patch(
            "bridge.AppServerCodexRunner", return_value=app_runner
        ):
            runner = select_codex_runner(self.config("auto"))

        self.assertIs(runner, app_runner)
        self.assertEqual(runner.probe_info["userAgent"], "codex-cli/0.146.0")

    def test_existing_socket_with_failed_handshake_does_not_fallback(self) -> None:
        app_runner = Mock()
        app_runner.probe.side_effect = AppServerProtocolError("bad handshake")
        with patch("bridge.Path.is_socket", return_value=True), patch(
            "bridge.AppServerCodexRunner", return_value=app_runner
        ):
            with self.assertRaisesRegex(ValueError, "握手失败"):
                select_codex_runner(self.config("auto"))

    def test_auto_falls_back_to_exec_without_socket(self) -> None:
        with patch("bridge.Path.is_socket", return_value=False):
            runner = select_codex_runner(self.config("auto"))

        self.assertIsInstance(runner, CodexRunner)

    def test_explicit_app_server_requires_supported_socket(self) -> None:
        with patch("bridge.sys.platform", "win32"):
            with self.assertRaisesRegex(ValueError, "不支持 Windows"):
                select_codex_runner(self.config("app-server"))


if __name__ == "__main__":
    unittest.main()
