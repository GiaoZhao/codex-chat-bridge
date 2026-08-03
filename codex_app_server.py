from __future__ import annotations

import json
import socket
import threading
import time
from collections import deque
from pathlib import Path
from typing import Any, Callable, Deque, Dict, List, Optional, Tuple

try:
    import websocket
except ImportError:  # pragma: no cover - reported by configuration checks
    websocket = None


MessageHandler = Callable[[Dict[str, Any]], None]
StartedHandler = Callable[[Optional[int]], None]
DiagnosticHandler = Callable[[str], None]


class AppServerProtocolError(RuntimeError):
    pass


class AppServerUnavailableError(AppServerProtocolError):
    pass


def _error_text(value: Any) -> str:
    if isinstance(value, dict):
        message = value.get("message")
        if isinstance(message, str) and message.strip():
            return message.strip()
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    text = str(value or "").strip()
    return text or "app-server request failed"


class AppServerRPCClient:
    def __init__(
        self,
        socket_path: Path,
        *,
        timeout_seconds: float = 1.0,
        socket_factory: Callable[..., socket.socket] = socket.socket,
        websocket_factory: Optional[Callable[..., Any]] = None,
    ) -> None:
        self.socket_path = socket_path.expanduser()
        self.timeout_seconds = max(0.1, timeout_seconds)
        self.socket_factory = socket_factory
        self.websocket_factory = websocket_factory
        self.ws: Optional[Any] = None
        self._next_request_id = 1
        self._deferred: Deque[Dict[str, Any]] = deque()

    def connect(self) -> None:
        if websocket is None and self.websocket_factory is None:
            raise AppServerUnavailableError("缺少 Python 依赖 websocket-client")
        raw = self.socket_factory(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            raw.settimeout(self.timeout_seconds)
            raw.connect(str(self.socket_path))
            factory = self.websocket_factory or websocket.create_connection
            self.ws = factory(
                "ws://localhost/rpc",
                socket=raw,
                timeout=self.timeout_seconds,
                suppress_origin=True,
            )
        except Exception as exc:
            try:
                raw.close()
            except OSError:
                pass
            raise AppServerUnavailableError(
                f"无法连接 Codex shared daemon: {self.socket_path}: {exc}"
            ) from exc

    def close(self) -> None:
        ws = self.ws
        self.ws = None
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass

    def request(
        self,
        method: str,
        params: Dict[str, Any],
        on_message: Optional[MessageHandler] = None,
    ) -> Any:
        ws = self._require_ws()
        if on_message is not None:
            while self._deferred:
                on_message(self._deferred.popleft())
        request_id = self._next_request_id
        self._next_request_id += 1
        ws.send(
            json.dumps(
                {"id": request_id, "method": method, "params": params},
                ensure_ascii=False,
                separators=(",", ":"),
            )
        )
        postponed: List[Dict[str, Any]] = []
        try:
            while True:
                message = self._read_socket()
                if message.get("id") == request_id and "method" not in message:
                    if "error" in message:
                        raise AppServerProtocolError(
                            f"{method}: {_error_text(message.get('error'))}"
                        )
                    return message.get("result")
                if on_message is not None:
                    on_message(message)
                else:
                    postponed.append(message)
        finally:
            self._deferred.extend(postponed)

    def notify(
        self,
        method: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> None:
        message: Dict[str, Any] = {"method": method}
        if params is not None:
            message["params"] = params
        self._require_ws().send(
            json.dumps(message, ensure_ascii=False, separators=(",", ":"))
        )

    def receive(self) -> Dict[str, Any]:
        if self._deferred:
            return self._deferred.popleft()
        return self._read_socket()

    def _read_socket(self) -> Dict[str, Any]:
        ws = self._require_ws()
        raw = ws.recv()
        try:
            message = json.loads(raw)
        except (TypeError, json.JSONDecodeError) as exc:
            raise AppServerProtocolError("app-server 返回了无效 JSON") from exc
        if not isinstance(message, dict):
            raise AppServerProtocolError("app-server 返回了非对象消息")
        return message

    def _require_ws(self) -> Any:
        if self.ws is None:
            raise AppServerUnavailableError("app-server WebSocket 尚未连接")
        return self.ws


class AppServerCodexRunner:
    transport_name = "app-server"

    def __init__(
        self,
        config: Any,
        *,
        client_factory: Optional[Callable[[], AppServerRPCClient]] = None,
    ) -> None:
        self.config = config
        self.socket_path = Path(config.codex_app_server_socket).expanduser()
        self._client_factory = client_factory or (
            lambda: AppServerRPCClient(self.socket_path)
        )
        self._lock = threading.Lock()
        self._active_thread_id = ""
        self._active_turn_id = ""
        self._termination_reason = ""

    def probe(self) -> Dict[str, Any]:
        client = self._open_client()
        try:
            result = self._initialize(client)
            return result if isinstance(result, dict) else {}
        finally:
            client.close()

    def running(self) -> bool:
        with self._lock:
            return bool(self._active_thread_id and self._active_turn_id)

    def cancel(self) -> bool:
        with self._lock:
            thread_id = self._active_thread_id
            turn_id = self._active_turn_id
            if not thread_id or not turn_id:
                return False
            self._termination_reason = "cancelled"
        try:
            self._interrupt(thread_id, turn_id)
            return True
        except Exception:
            with self._lock:
                if (
                    self._active_thread_id == thread_id
                    and self._active_turn_id == turn_id
                    and self._termination_reason == "cancelled"
                ):
                    self._termination_reason = ""
            return False

    def termination_reason(self) -> str:
        with self._lock:
            return self._termination_reason

    def run(
        self,
        prompt: str,
        thread_id: Optional[str] = None,
        workdir: Optional[Path] = None,
        image_paths: Optional[List[Path]] = None,
        on_started: Optional[StartedHandler] = None,
        on_diagnostic: Optional[DiagnosticHandler] = None,
    ) -> Tuple[int, Optional[str], str]:
        selected_thread_id = thread_id or self.config.codex_thread_id
        selected_workdir = workdir or self.config.codex_workdir
        code, _thread_id, final, error = self._run_turn(
            prompt,
            selected_workdir,
            image_paths or [],
            resume_thread_id=selected_thread_id,
            on_started=on_started,
            on_diagnostic=on_diagnostic,
        )
        return code, final, error

    def run_new(
        self,
        prompt: str,
        workdir: Path,
        image_paths: Optional[List[Path]] = None,
        on_started: Optional[StartedHandler] = None,
        on_diagnostic: Optional[DiagnosticHandler] = None,
        thread_name: Optional[str] = None,
    ) -> Tuple[int, Optional[str], Optional[str], str]:
        return self._run_turn(
            prompt,
            workdir,
            image_paths or [],
            resume_thread_id=None,
            thread_name=thread_name,
            on_started=on_started,
            on_diagnostic=on_diagnostic,
        )

    def _run_turn(
        self,
        prompt: str,
        workdir: Path,
        image_paths: List[Path],
        *,
        resume_thread_id: Optional[str],
        thread_name: Optional[str] = None,
        on_started: Optional[StartedHandler],
        on_diagnostic: Optional[DiagnosticHandler],
    ) -> Tuple[int, Optional[str], Optional[str], str]:
        if not workdir.is_dir():
            raise ValueError(f"任务工作目录不存在: {workdir}")
        client = self._open_client()
        final_messages: List[Tuple[str, str]] = []
        completed_turn: Optional[Dict[str, Any]] = None
        thread_id = resume_thread_id or ""
        turn_id = ""
        awaiting_turn_start = False
        pending_turn_messages: List[Dict[str, Any]] = []

        def handle_message(message: Dict[str, Any]) -> None:
            nonlocal completed_turn
            method = str(message.get("method") or "")
            params = message.get("params")
            params = params if isinstance(params, dict) else {}
            message_thread_id = str(params.get("threadId") or "")
            if message_thread_id and thread_id and message_thread_id != thread_id:
                return

            message_turn_id = str(params.get("turnId") or "")
            if not message_turn_id:
                message_turn = params.get("turn")
                if isinstance(message_turn, dict):
                    message_turn_id = str(message_turn.get("id") or "")
            if message_turn_id:
                if not turn_id:
                    if awaiting_turn_start:
                        pending_turn_messages.append(message)
                    return
                if message_turn_id != turn_id:
                    return

            if "id" in message and method:
                if on_diagnostic is not None:
                    on_diagnostic(f"desktop interaction required: {method}")
                return
            if method == "item/completed":
                item = params.get("item")
                item = item if isinstance(item, dict) else {}
                if item.get("type") in {"agentMessage", "agent_message"}:
                    text = item.get("text") or item.get("message")
                    if isinstance(text, str) and text.strip():
                        final_messages.append((str(item.get("phase") or ""), text.strip()))
            elif method == "error" and on_diagnostic is not None:
                on_diagnostic(_error_text(params.get("error") or params))
            elif method == "turn/completed":
                turn = params.get("turn")
                if isinstance(turn, dict) and turn.get("id") == turn_id:
                    completed_turn = turn

        try:
            self._initialize(client, handle_message)
            if resume_thread_id:
                resumed = client.request(
                    "thread/resume",
                    {"threadId": resume_thread_id},
                    handle_message,
                )
                resumed_thread = resumed.get("thread") if isinstance(resumed, dict) else None
                if not isinstance(resumed_thread, dict) or resumed_thread.get("id") != resume_thread_id:
                    raise AppServerProtocolError("thread/resume 未返回目标任务")
            else:
                started = client.request(
                    "thread/start",
                    {"cwd": str(workdir), "ephemeral": False},
                    handle_message,
                )
                started_thread = started.get("thread") if isinstance(started, dict) else None
                value = started_thread.get("id") if isinstance(started_thread, dict) else None
                if not isinstance(value, str) or not value:
                    raise AppServerProtocolError("thread/start 未返回新任务编号")
                thread_id = value
                if thread_name:
                    client.request(
                        "thread/name/set",
                        {"threadId": thread_id, "name": thread_name},
                        handle_message,
                    )

            inputs: List[Dict[str, Any]] = [{"type": "text", "text": prompt}]
            inputs.extend(
                {"type": "localImage", "path": str(path.resolve())}
                for path in image_paths
            )
            awaiting_turn_start = True
            try:
                started_turn = client.request(
                    "turn/start",
                    {
                        "threadId": thread_id,
                        "input": inputs,
                        "cwd": str(workdir.resolve()),
                    },
                    handle_message,
                )
            finally:
                awaiting_turn_start = False
            turn = started_turn.get("turn") if isinstance(started_turn, dict) else None
            value = turn.get("id") if isinstance(turn, dict) else None
            if not isinstance(value, str) or not value:
                raise AppServerProtocolError("turn/start 未返回回合编号")
            turn_id = value
            buffered = list(pending_turn_messages)
            pending_turn_messages.clear()
            for message in buffered:
                handle_message(message)
            with self._lock:
                self._active_thread_id = thread_id
                self._active_turn_id = turn_id
                self._termination_reason = ""
            if on_started is not None:
                try:
                    on_started(None)
                except Exception:
                    try:
                        self._interrupt(thread_id, turn_id)
                    finally:
                        raise

            deadline = time.monotonic() + self.config.codex_turn_timeout
            while completed_turn is None:
                if time.monotonic() >= deadline:
                    with self._lock:
                        self._termination_reason = "timed_out"
                    try:
                        self._interrupt(thread_id, turn_id)
                    except AppServerProtocolError as exc:
                        if on_diagnostic is not None:
                            on_diagnostic(str(exc))
                    return (
                        124,
                        thread_id,
                        None,
                        f"任务超过 {self.config.codex_turn_timeout} 秒，已请求终止。",
                    )
                try:
                    handle_message(client.receive())
                except Exception as exc:
                    if websocket is not None and isinstance(
                        exc, websocket.WebSocketTimeoutException
                    ):
                        continue
                    raise

            status = str(completed_turn.get("status") or "")
            final = None
            for phase, text in reversed(final_messages):
                if phase == "final_answer":
                    final = text
                    break
            if final is None and final_messages:
                final = final_messages[-1][1]
            if status == "completed":
                return 0, thread_id, final, ""
            error = _error_text(completed_turn.get("error") or f"turn status: {status}")
            if status == "interrupted":
                with self._lock:
                    if not self._termination_reason:
                        self._termination_reason = "interrupted"
                return -15, thread_id, final, error
            return 1, thread_id, final, error
        finally:
            with self._lock:
                if not turn_id or self._active_turn_id == turn_id:
                    self._active_thread_id = ""
                    self._active_turn_id = ""
            client.close()

    def _interrupt(self, thread_id: str, turn_id: str) -> None:
        client = self._open_client()
        try:
            self._initialize(client)
            client.request(
                "turn/interrupt",
                {"threadId": thread_id, "turnId": turn_id},
            )
        finally:
            client.close()

    def _open_client(self) -> AppServerRPCClient:
        client = self._client_factory()
        client.connect()
        return client

    @staticmethod
    def _initialize(
        client: AppServerRPCClient,
        on_message: Optional[MessageHandler] = None,
    ) -> Any:
        result = client.request(
            "initialize",
            {
                "clientInfo": {
                    "name": "codex-chat-bridge",
                    "title": "Codex Chat Bridge",
                    "version": "0.1.0",
                },
                "capabilities": {"experimentalApi": True},
            },
            on_message,
        )
        client.notify("initialized")
        return result
