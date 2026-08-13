#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import queue
import signal
import shutil
import sqlite3
import subprocess
import sys
import threading
import time
import traceback
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from codex_app_server import AppServerCodexRunner, AppServerProtocolError
from bridge_core import (
    ActiveThread,
    CodexDesktopRefresher,
    CodexRunner,
    Config,
    SessionMonitor,
    StateStore,
    ThreadIndex,
    ThreadInfo,
    configure_log_file,
    find_session_file,
    final_message_from_record,
    latest_complete_turn,
    latest_final_message,
    log_event,
    qq_safe_final,
    session_record_event_id,
    session_busy,
    split_message,
    summarize_codex_error,
)
from bridge_store import (
    BridgeInstanceLock,
    BridgeStore,
    OutboxItem,
    OutboxSpec,
    StoredJob,
)
from channel_factory import (
    build_channel_registry,
    channel_configuration_summary,
    channel_dependency_errors,
)
from chat_channel import (
    ActionRows,
    ChannelLimits,
    InboundMessage,
    Recipient,
    action_rows,
    actions_from_payload,
    actions_to_payload,
    redact_identifier,
)


BASE_DIR = Path(__file__).resolve().parent
THREAD_PAGE_SIZE = 6


@dataclass(frozen=True)
class JobRunResult:
    thread_id: str
    status: str
    error: str = ""
    notification: str = ""
    final_message: str = ""
    event_id: str = ""


class JobDispatchUncertainError(RuntimeError):
    pass


def select_codex_runner(config: Config, verify: bool = True) -> Any:
    mode = str(getattr(config, "codex_transport", "exec") or "exec").lower()
    if mode == "exec":
        log_event("codex-transport", "using exec transport")
        return CodexRunner(config)

    socket_path = Path(
        getattr(
            config,
            "codex_app_server_socket",
            Path.home()
            / ".codex"
            / "app-server-control"
            / "app-server-control.sock",
        )
    ).expanduser()
    supported = sys.platform != "win32"
    socket_exists = False
    try:
        socket_exists = socket_path.is_socket()
    except OSError:
        socket_exists = False

    if supported and socket_exists:
        runner = AppServerCodexRunner(config)
        if verify:
            try:
                info = runner.probe()
            except AppServerProtocolError as exc:
                raise ValueError(
                    f"Codex shared daemon socket 存在但握手失败: {exc}"
                ) from exc
            user_agent = str(info.get("userAgent") or "unknown")
            normalized_user_agent = user_agent.casefold()
            official_daemon = normalized_user_agent.startswith(
                ("codex desktop/", "codex-cli/")
            )
            if not official_daemon:
                detail = (
                    "shared daemon 不是由 Codex 官方客户端建立: "
                    f"userAgent={user_agent}"
                )
                if mode == "app-server":
                    raise ValueError(detail)
                log_event(
                    "codex-transport",
                    f"using exec fallback: {detail}",
                    level="WARNING",
                )
                return CodexRunner(config)
            log_event(
                "codex-transport",
                f"using app-server socket={socket_path} userAgent={user_agent}",
            )
            runner.probe_info = info
        return runner

    if mode == "app-server":
        if not supported:
            raise ValueError("Codex shared daemon 当前不支持 Windows")
        raise ValueError(f"未找到 Codex shared daemon socket: {socket_path}")

    reason = "unsupported platform" if not supported else f"socket missing: {socket_path}"
    log_event("codex-transport", f"using exec fallback: {reason}", level="WARNING")
    return CodexRunner(config)


class BridgeService:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.instance_lock = BridgeInstanceLock(config.base_dir / "data" / "bridge.lock")
        try:
            self.instance_lock.acquire()
            self.state = StateStore(config.base_dir / "data" / "state.json")
            self.store = BridgeStore(config.base_dir / "data" / "bridge.sqlite3")
            stored_state = self.state.load()
            self.thread_index = ThreadIndex(config.codex_state_db)
            self.active_thread = ActiveThread(config, self.state, self.thread_index)
            self.runner = select_codex_runner(config)
            self.desktop_refresher = CodexDesktopRefresher(
                config.codex_desktop_refresh,
                config.codex_desktop_cdp_url,
                config.codex_desktop_refresh_delay_ms / 1000,
            )
            self.monitor = SessionMonitor(
                config,
                self.state,
                self._on_final,
                thread_provider=lambda: self.active_thread.snapshot().thread_id,
                threads_provider=self.thread_index.list_monitorable,
                on_final_event=self._on_final_event,
                on_interrupted=self._on_interrupted,
            )
            self.jobs: "queue.Queue[str]" = queue.Queue()
            self.thread_choices: Dict[int, str] = {}
            self.stop_event = threading.Event()
            self._stop_started = threading.Event()
            self._stop_complete = threading.Event()
            self._stop_state_lock = threading.Lock()
            self._components_stopped = False
            self._fatal_shutdown_requested = threading.Event()
            self._online_notice_sent: set[str] = set()
            self.worker_busy = threading.Event()
            self.outbox_wakeup = threading.Event()
            self._runtime_lock = threading.Lock()
            self._outbox_image_lock = threading.RLock()
            self.channel_states: Dict[str, str] = {}
            self.channel_details: Dict[str, str] = {}
            self.worker_stage = "idle"
            self.current_job_id = ""
            self.last_worker_error = ""
            self.worker = threading.Thread(
                target=self._worker_loop,
                name="codex-worker",
                daemon=True,
            )
            self.outbox_worker = threading.Thread(
                target=self._outbox_loop,
                name="chat-outbox",
                daemon=True,
            )
            self.channels = build_channel_registry(
                config,
                on_message=self._on_channel_event,
                on_ready=self._on_channel_ready,
                on_state=self._on_channel_state,
            )
            self.channel_threads = {
                channel.name: threading.Thread(
                    target=channel.run_forever,
                    name=f"{channel.name}-gateway",
                    daemon=True,
                )
                for channel in self.channels.values()
            }
            stored_bindings = stored_state.get("bindings")
            if not isinstance(stored_bindings, dict):
                stored_bindings = {}
            self.bound_recipients: Dict[str, str] = {}
            for channel in self.channels.values():
                raw_binding = stored_bindings.get(channel.name)
                stored_recipient = ""
                if isinstance(raw_binding, dict):
                    stored_recipient = str(raw_binding.get("recipient_id") or "")
                if not stored_recipient:
                    stored_recipient = channel.legacy_recipient(stored_state)
                recipient_id = channel.configured_recipient or stored_recipient
                if recipient_id:
                    self.bound_recipients[channel.name] = recipient_id
                elif not channel.bind_code:
                    raise ValueError(
                        f"首次运行必须配置 {channel.name} 绑定码或允许的用户标识"
                    )
                self.channel_states[channel.name] = "stopped"
                self.channel_details[channel.name] = ""
            discarded_outbox = self.store.discard_pending_outbox()
            self._cleanup_unreferenced_outbox_images()
            self._restore_persisted_jobs()
            if discarded_outbox:
                log_event("recovery", f"discarded_outbox={discarded_outbox}")
        except BaseException:
            self.instance_lock.release()
            raise

    def _request_fatal_shutdown(self, reason: str) -> None:
        if self._fatal_shutdown_requested.is_set():
            return
        self._fatal_shutdown_requested.set()
        log_event("bridge", f"fatal shutdown requested: {reason}", level="ERROR")
        self.stop_event.set()
        self.outbox_wakeup.set()
        threading.Thread(
            target=self.stop,
            kwargs={"force_if_running": False},
            name="bridge-fatal-stop",
            daemon=True,
        ).start()

    def run(self) -> None:
        log_event("bridge", "service starting")
        try:
            with self._stop_state_lock:
                if self._stop_started.is_set():
                    return
                self.monitor.start()
                self.worker.start()
                self.outbox_worker.start()
                for channel_thread in self.channel_threads.values():
                    channel_thread.start()
            self.stop_event.wait()
        finally:
            self.stop(force_if_running=False)
            if self._components_stopped:
                self.instance_lock.release()
            else:
                log_event(
                    "bridge",
                    "instance lock retained because shutdown did not complete cleanly",
                    level="ERROR",
                )

    def stop(self, force_if_running: bool = True) -> None:
        with self._stop_state_lock:
            first_request = not self._stop_started.is_set()
            if first_request:
                self._stop_started.set()
        if not first_request:
            if force_if_running and self.runner.running():
                log_event("bridge", "second stop request: cancelling Codex task", level="WARNING")
                self.runner.cancel()
            self._stop_complete.wait()
            return
        self.stop_event.set()
        clean = True
        try:
            outbox_wakeup = getattr(self, "outbox_wakeup", None)
            if outbox_wakeup is not None:
                outbox_wakeup.set()
            channels = getattr(self, "channels", None)
            try:
                if channels is not None:
                    channels.stop_all()
            except Exception as exc:
                clean = False
                log_event("bridge", f"channel stop failed: {exc}", level="ERROR")
            channel_threads = getattr(self, "channel_threads", {})
            for channel_name, channel_thread in channel_threads.items():
                try:
                    if (
                        channel_thread.is_alive()
                        and threading.current_thread() is not channel_thread
                    ):
                        channel_thread.join(timeout=5)
                except Exception as exc:
                    clean = False
                    log_event(
                        "bridge",
                        f"{channel_name} gateway stop failed: {exc}",
                        level="ERROR",
                    )
            try:
                if self.worker.is_alive() and threading.current_thread() is not self.worker:
                    if self.runner.running():
                        log_event(
                            "bridge",
                            "waiting for current Codex task; send stop again to cancel",
                        )
                    self.worker.join()
            except Exception as exc:
                clean = False
                log_event("bridge", f"worker stop failed: {exc}", level="ERROR")
            outbox_worker = getattr(self, "outbox_worker", None)
            try:
                if (
                    outbox_worker is not None
                    and outbox_worker.is_alive()
                    and threading.current_thread() is not outbox_worker
                ):
                    outbox_worker.join()
            except Exception as exc:
                clean = False
                log_event("bridge", f"outbox stop failed: {exc}", level="ERROR")
            try:
                self.monitor.stop()
            except Exception as exc:
                clean = False
                log_event("bridge", f"monitor stop failed: {exc}", level="ERROR")
            if self.worker.is_alive():
                clean = False
            if outbox_worker is not None and outbox_worker.is_alive():
                clean = False
            if any(thread.is_alive() for thread in channel_threads.values()):
                clean = False
            monitor_thread = getattr(self.monitor, "thread", None)
            if monitor_thread is not None and monitor_thread.is_alive():
                clean = False
        finally:
            self._components_stopped = clean
            self._stop_complete.set()
            log_event("bridge", "service stopped")

    def _restore_persisted_jobs(self) -> None:
        recoverable, uncertain = self.store.recover_jobs()
        for job in recoverable:
            self.jobs.put_nowait(job.job_id)
        for job in uncertain:
            recipient = self._recipient_from_payload(job.payload)
            error = "Bridge 在任务启动或执行期间退出，未自动重跑以避免重复执行。"
            self._set_job_status_with_notification(
                job.job_id,
                "interrupted",
                error,
                recipient,
                "Bridge 在该任务启动或执行期间退出，无法确认 Codex 是否已经执行。"
                "为避免重复操作，本任务不会自动重跑，请先检查任务记录后再决定是否重试。",
                dedupe_key=f"job:{job.job_id}:interrupted",
            )
            self._cleanup_job_attachments(job.payload)
        if recoverable or uncertain:
            log_event(
                "recovery",
                f"queued={len(recoverable)} interrupted={len(uncertain)}",
            )

    def _set_worker_state(self, stage: str, job_id: str = "", error: str = "") -> None:
        with self._runtime_lock:
            self.worker_stage = stage
            self.current_job_id = job_id
            if error:
                self.last_worker_error = error[-400:]

    def _on_channel_state(self, channel: str, state: str, detail: str) -> None:
        with self._runtime_lock:
            self.channel_states[channel] = state
            self.channel_details[channel] = detail[-400:]
        if state == "ready":
            self.outbox_wakeup.set()

    def _cleanup_job_attachments(self, payload: Dict[str, Any]) -> None:
        raw_dir = str(payload.get("attachment_dir") or "")
        if not raw_dir:
            return
        root = (self.config.base_dir / "data" / "attachments").resolve()
        try:
            path = Path(raw_dir).resolve()
            path.relative_to(root)
            if path != root:
                shutil.rmtree(path)
        except (OSError, ValueError) as exc:
            log_event(
                "attachments",
                f"cleanup rejected or failed: {exc}",
                level="ERROR",
            )

    def _current_recipient(self, channel: str) -> Optional[Recipient]:
        try:
            configured = self.channels.get(channel).configured_recipient
        except KeyError:
            return None
        bindings = getattr(self, "bound_recipients", {})
        bound = str(bindings.get(channel) or "") if isinstance(bindings, dict) else ""
        recipient_id = configured or bound
        return Recipient(channel, recipient_id) if recipient_id else None

    def _recipient_from_payload(self, payload: Dict[str, Any]) -> Recipient:
        try:
            return Recipient.from_payload(payload)
        except ValueError:
            for channel in self.channels.names():
                recipient = self._current_recipient(channel)
                if recipient is not None:
                    return recipient
            raise RuntimeError("任务缺少可用的消息收件人")

    def _bound_destinations(self) -> List[Recipient]:
        recipients = [
            self._current_recipient(name) for name in self.channels.names()
        ]
        return [recipient for recipient in recipients if recipient is not None]

    @staticmethod
    def _redact_recipient(error: object, recipient_id: str) -> str:
        return redact_identifier(error, recipient_id)

    @staticmethod
    def _actions(rows: List[List[tuple[str, str, int]]]) -> ActionRows:
        return action_rows(rows)

    @classmethod
    def _main_actions(cls) -> ActionRows:
        return cls._actions(
            [
                [("任务列表", "/threads", 1), ("搜索任务", "/search", 0)],
                [
                    ("当前任务", "/current", 0),
                    ("执行状态", "/status", 0),
                    ("最近结果", "/recent", 0),
                ],
                [
                    ("新建任务", "/new", 1),
                    ("取消任务", "/cancel", 2),
                    ("操作帮助", "/help", 0),
                ],
            ]
        )

    @staticmethod
    def _coerce_recipient(value: Recipient | str, channel: str = "qq") -> Recipient:
        if isinstance(value, Recipient):
            return value
        return Recipient(channel, str(value))

    def _limits_for(self, channel: str) -> ChannelLimits:
        return self.channels.get(channel).limits

    def _send_chunk(
        self,
        recipient: Recipient,
        text: str,
        reply_to: str = "",
        actions: ActionRows = (),
        sequence: int = 1,
    ) -> None:
        channel = self.channels.get(recipient.channel)
        channel.send_text(
            recipient.recipient_id,
            text,
            reply_to=reply_to,
            sequence=sequence,
            actions=actions,
        )

    def _safe_send(
        self,
        recipient: Recipient | str,
        text: str,
        reply_to: str = "",
        actions: ActionRows = (),
        start_seq: int = 1,
        dedupe_key: str = "",
    ) -> bool:
        try:
            items = self._text_outbox_specs(
                recipient,
                text,
                reply_to,
                actions,
                start_seq,
                dedupe_key,
            )
            if not items:
                return False
            self.store.enqueue_outbox_batch(items)
            self.outbox_wakeup.set()
            return True
        except Exception as exc:
            log_event("chat-outbox", f"enqueue failed: {exc}", level="ERROR")
            return False

    def _text_outbox_specs(
        self,
        recipient: Recipient | str,
        text: str,
        reply_to: str = "",
        actions: ActionRows = (),
        start_seq: int = 1,
        dedupe_key: str = "",
    ) -> List[OutboxSpec]:
        target = self._coerce_recipient(recipient)
        limits = self._limits_for(target.channel)
        max_chunks = limits.reply_chunks
        if reply_to and limits.passive_reply_limit:
            max_chunks = min(
                max_chunks,
                max(1, limits.passive_reply_limit + 1 - start_seq),
            )
        chunks = split_message(text, limits.reply_chars, max_chunks)
        if dedupe_key:
            group_key = f"{target.channel}:{dedupe_key}"
        elif reply_to:
            fingerprint = hashlib.sha256(
                json.dumps(
                    {
                        "channel": target.channel,
                        "recipient_id": target.recipient_id,
                        "text": text,
                        "reply_to": reply_to,
                        "start_seq": start_seq,
                        "actions": actions_to_payload(actions),
                    },
                    ensure_ascii=False,
                    sort_keys=True,
                ).encode("utf-8")
            ).hexdigest()
            group_key = f"{target.channel}:reply:{reply_to}:{fingerprint}"
        else:
            group_key = f"{target.channel}:notice:{uuid.uuid4().hex}"
        last_seq = start_seq + len(chunks) - 1
        return [
            OutboxSpec(
                f"{group_key}:chunk:{index}",
                {
                    "kind": "text",
                    **target.to_payload(),
                    "text": chunk,
                    "reply_to": reply_to,
                    "sequence": index,
                    "actions": (
                        actions_to_payload(actions) if index == last_seq else []
                    ),
                },
                group_key,
                target.channel,
            )
            for index, chunk in enumerate(chunks, start_seq)
        ]

    def _set_job_status_with_notification(
        self,
        job_id: str,
        status: str,
        error: str,
        recipient: Recipient,
        text: str,
        *,
        dedupe_key: str,
        process_id: Optional[int] = None,
    ) -> None:
        items = self._text_outbox_specs(
            recipient,
            text,
            dedupe_key=dedupe_key,
        )
        if not items:
            raise RuntimeError("任务状态通知缺少聊天渠道收件人")
        self.store.set_job_status_with_outbox(
            job_id,
            status,
            items,
            process_id=process_id,
            error=error,
        )
        self.outbox_wakeup.set()

    def _safe_typing(self, recipient: Recipient, reply_to: str) -> None:
        try:
            self.channels.get(recipient.channel).send_typing(
                recipient.recipient_id,
                reply_to,
            )
        except Exception as exc:
            log_event(
                f"{recipient.channel}-typing",
                self.channels.get(recipient.channel).redact_error(
                    exc,
                    recipient.recipient_id,
                ),
                level="WARNING",
            )

    def _safe_send_images(
        self,
        recipient: Recipient | str,
        paths: List[Path],
        dedupe_key: str = "",
    ) -> bool:
        target = self._coerce_recipient(recipient)
        if not self._limits_for(target.channel).supports_outbound_images:
            return not paths
        with self._outbox_image_lock:
            items, success = self._image_outbox_specs(target, paths, dedupe_key)
            if items:
                try:
                    self.store.enqueue_outbox_batch(items)
                    self.outbox_wakeup.set()
                except Exception as exc:
                    success = False
                    log_event(
                        f"{target.channel}-image",
                        f"enqueue failed: {exc}",
                        level="ERROR",
                    )
                finally:
                    self._cleanup_unreferenced_outbox_images()
            return success

    def _outbox_image_root(self) -> Path:
        return (self.config.base_dir / "data" / "outbox-images").resolve()

    def _spool_outbox_image(self, path: Path, group_key: str, index: int) -> Path:
        source = path.resolve()
        root = self._outbox_image_root()
        group_dir = root / hashlib.sha256(group_key.encode("utf-8")).hexdigest()
        group_dir.mkdir(parents=True, exist_ok=True)
        try:
            root.chmod(0o700)
            group_dir.chmod(0o700)
        except OSError:
            pass
        suffix = source.suffix.lower()
        target = group_dir / f"{index:02d}{suffix}"
        if source == target or (
            target.is_file()
            and not target.is_symlink()
            and target.stat().st_size == source.stat().st_size
        ):
            return target.resolve()
        staging = group_dir / f".{target.name}.{uuid.uuid4().hex}.tmp"
        try:
            shutil.copyfile(source, staging)
            try:
                staging.chmod(0o600)
            except OSError:
                pass
            staging.replace(target)
        finally:
            try:
                staging.unlink(missing_ok=True)
            except OSError:
                pass
        return target.resolve()

    def _cleanup_unreferenced_outbox_images(self) -> None:
        with self._outbox_image_lock:
            root = self._outbox_image_root()
            if not root.is_dir():
                return
            try:
                referenced = {
                    str(Path(value).resolve())
                    for value in self.store.pending_outbox_image_paths()
                }
                for candidate in root.rglob("*"):
                    if not candidate.is_file():
                        continue
                    resolved = candidate.resolve()
                    resolved.relative_to(root)
                    if str(resolved) not in referenced:
                        resolved.unlink(missing_ok=True)
                directories = sorted(
                    (path for path in root.rglob("*") if path.is_dir()),
                    key=lambda path: len(path.parts),
                    reverse=True,
                )
                for directory in directories:
                    try:
                        directory.rmdir()
                    except OSError:
                        pass
            except (OSError, ValueError, sqlite3.Error) as exc:
                log_event("qq-image", f"spool cleanup failed: {exc}", level="WARNING")

    def _image_outbox_specs(
        self,
        recipient: Recipient | str,
        paths: List[Path],
        dedupe_key: str = "",
    ) -> tuple[List[OutboxSpec], bool]:
        target = self._coerce_recipient(recipient)
        limits = self._limits_for(target.channel)
        if not limits.supports_outbound_images:
            return [], not paths
        success = True
        group_key = (
            f"{target.channel}:{dedupe_key}"
            if dedupe_key
            else f"{target.channel}:image:{uuid.uuid4().hex}"
        )
        items: List[OutboxSpec] = []
        for index, path in enumerate(paths, 1):
            try:
                if path.stat().st_size > limits.attachment_max_bytes:
                    log_event(
                        f"{target.channel}-image",
                        f"skipped oversized image: {path.name}",
                        level="WARNING",
                    )
                    continue
                spooled_path = self._spool_outbox_image(path, group_key, index)
                payload = {
                    "kind": "image",
                    **target.to_payload(),
                    "path": str(spooled_path),
                    "sequence": index,
                }
                item_key = (
                    f"{target.channel}:{dedupe_key}:image:{index}"
                    if dedupe_key
                    else f"{group_key}:item:{index}"
                )
                items.append(
                    OutboxSpec(item_key, payload, group_key, target.channel)
                )
            except Exception as exc:
                success = False
                log_event(
                    f"{target.channel}-image",
                    f"{path.name}: {exc}",
                    level="ERROR",
                )
        return items, success

    def _deliver_outbox(self, item: OutboxItem) -> None:
        payload = item.payload
        kind = str(payload.get("kind") or "")
        recipient = Recipient.from_payload(payload, default_channel=item.channel)
        channel = self.channels.get(recipient.channel)
        if kind == "text":
            channel.send_text(
                recipient.recipient_id,
                str(payload.get("text") or ""),
                reply_to=str(payload.get("reply_to") or ""),
                sequence=int(payload.get("sequence") or 1),
                actions=actions_from_payload(payload.get("actions")),
            )
            return
        if kind == "image":
            path = Path(str(payload.get("path") or ""))
            if not path.is_file():
                raise FileNotFoundError(f"待发送图片不存在: {path.name}")
            channel.send_image(
                recipient.recipient_id,
                path,
                sequence=int(payload.get("sequence") or 1),
            )
            return
        raise ValueError(f"未知 outbox 类型: {kind}")

    def _outbox_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                ready_channels = self.channels.ready_names()
                if not ready_channels:
                    self.outbox_wakeup.wait(1)
                    self.outbox_wakeup.clear()
                    continue
                item = self.store.claim_due_outbox(channels=ready_channels)
                if item is None:
                    self.outbox_wakeup.wait(1)
                    self.outbox_wakeup.clear()
                    continue
                try:
                    self._deliver_outbox(item)
                except Exception as exc:
                    delay = min(300, max(2, 2 ** min(item.attempts + 1, 8)))
                    recipient_id = str(
                        item.payload.get("recipient_id")
                        or item.payload.get("openid")
                        or ""
                    )
                    detail = self.channels.get(item.channel).redact_error(
                        exc,
                        recipient_id,
                    )
                    log_event(
                        "chat-outbox",
                        f"delivery failed channel={item.channel} id={item.outbox_id} "
                        f"retry_in={delay}s error={detail}",
                        level="ERROR",
                    )
                    self.store.retry_outbox(item.outbox_id, detail, delay)
                    self.stop_event.wait(min(1, delay))
                else:
                    self.store.mark_outbox_sent(item.outbox_id)
                    if item.payload.get("kind") == "image":
                        self._cleanup_unreferenced_outbox_images()
                    log_event(
                        "chat-outbox",
                        f"delivered channel={item.channel} id={item.outbox_id}",
                    )
                    time.sleep(0.35)
            except Exception as exc:
                log_event(
                    "chat-outbox",
                    f"worker recovered from storage error: {exc}",
                    level="ERROR",
                )
                self.stop_event.wait(2)

    def _on_channel_ready(self, channel_name: str) -> None:
        self._on_channel_state(channel_name, "ready", "")
        log_event("bridge", f"{channel_name} channel is ready")
        channel = self.channels.get(channel_name)
        if not channel.notify_on_ready:
            return
        recipient = self._current_recipient(channel_name)
        if recipient:
            if channel_name in self._online_notice_sent:
                return
            active = self.active_thread.snapshot()
            queued = self._safe_send(
                recipient,
                "## Codex Chat Bridge 已上线\n\n"
                f"当前任务：**{self._short_title(active.title)}**",
                actions=self._main_actions(),
            )
            if queued:
                self._online_notice_sent.add(channel_name)
        else:
            log_event("bridge", f"{channel_name} waiting for /bind <code>")

    def _bind(self, event: InboundMessage) -> None:
        content = event.content
        supplied = content.partition(" ")[2].strip()
        channel = self.channels.get(event.channel)
        if not supplied or not hmac.compare_digest(supplied, channel.bind_code):
            self._safe_send(event.recipient, "绑定码不正确。", event.message_id)
            return
        self.bound_recipients[event.channel] = event.sender_id
        bindings = {
            name: {"recipient_id": recipient_id}
            for name, recipient_id in self.bound_recipients.items()
        }
        self.state.update(bindings=bindings)
        self._safe_send(
            event.recipient,
            "绑定成功。此机器人现在只接受你的私聊消息。发送 /help 查看命令。",
            event.message_id,
            actions=self._main_actions(),
        )
        log_event("bridge", f"{event.channel} user binding completed")

    def _status_text(self) -> str:
        active = self.active_thread.snapshot()
        path = self.monitor.path or find_session_file(
            self.config.codex_sessions_dir, active.thread_id
        )
        busy = path is not None and self._session_busy_without_checkpoint(path)
        queue_size = self.store.active_job_count()
        pending_notifications = self.store.pending_outbox_count()
        latest_job = self.store.latest_job()
        with self._runtime_lock:
            channel_states = dict(self.channel_states)
            worker_stage = self.worker_stage
            last_worker_error = self.last_worker_error
        safe_error = ""
        if last_worker_error:
            safe_error, _images = qq_safe_final(last_worker_error, active.workdir, max_images=0)
        return "\n".join(
            [
                "Codex Chat Bridge 状态",
                f"标题：{self._short_title(active.title, 60)}",
                f"任务：{active.thread_id}",
                f"Codex：{'执行中' if busy or self.runner.running() else '空闲'}",
                f"传输：{getattr(self.runner, 'transport_name', 'exec')}",
                f"Worker 阶段：{worker_stage}",
                f"最近任务状态：{latest_job.status if latest_job else '无'}",
                f"持久任务：{queue_size}",
                "渠道："
                + "，".join(
                    f"{name}={channel_states.get(name, 'unknown')}"
                    for name in self.channels.names()
                ),
                f"待发送通知：{pending_notifications}",
                f"最近执行错误：{safe_error or '无'}",
                f"会话文件：{'已找到' if path else '未找到'}",
                f"工作目录：{active.workdir}",
                f"日志：{self.config.base_dir / 'data' / 'logs' / 'bridge.jsonl'}",
            ]
        )

    def _help_text(self) -> str:
        return "\n".join(
            [
                "直接发送文字：继续当前 Codex 任务",
                "/threads [页码]：列出可切换任务",
                "/search <关键词>：检索任务",
                "/search <页码> <关键词>：查看后续检索结果",
                "/use <编号或UUID>：切换并返回最新一轮原文",
                "/current：查看当前任务",
                "/new <第一条消息>：在当前项目中新建任务",
                "/status：查看任务和队列状态",
                "/recent：查看最近一次 Codex 最终结果",
                "/cancel：取消由聊天渠道启动的当前任务",
                "/help：查看帮助",
                "桌面任务执行中发送的新消息会自动排队。",
                "可随时切换到空闲任务；新建任务不受现有桌面任务状态影响。",
            ]
        )

    @staticmethod
    def _short_title(title: str, limit: int = 36) -> str:
        normalized = " ".join(title.split()) or "未命名任务"
        return normalized if len(normalized) <= limit else normalized[: limit - 1] + "…"

    def _current_text(self) -> str:
        active = self.active_thread.snapshot()
        return "\n".join(
            [
                f"当前任务：{self._short_title(active.title, 60)}",
                f"编号：{active.thread_id}",
                f"项目：{active.project_name}",
                f"目录：{active.workdir}",
            ]
        )

    def _threads_text(self, page: int) -> str:
        page_size = THREAD_PAGE_SIZE
        threads, total = self.thread_index.list_page(page, page_size)
        total_pages = max(1, (total + page_size - 1) // page_size)
        if page > total_pages:
            return f"页码超出范围。当前共 {total_pages} 页。"
        active_id = self.active_thread.snapshot().thread_id
        lines = [f"Codex 任务（第 {page}/{total_pages} 页，共 {total} 个）"]
        start = (page - 1) * page_size
        for offset, info in enumerate(threads, 1):
            self.thread_choices[start + offset] = info.thread_id
            marker = "*" if info.thread_id == active_id else " "
            lines.append(
                f"{marker}{start + offset}. [{info.project_name}] {self._short_title(info.title)}"
            )
        lines.extend(["", "点击任务按钮，或发送 /use 编号切换；* 表示当前任务。"])
        return "\n".join(lines)

    def _search_text(self, query: str, page: int) -> str:
        page_size = THREAD_PAGE_SIZE
        threads, total = self.thread_index.search_page(query, page, page_size)
        if total == 0:
            return f"没有找到包含“{self._short_title(query, 40)}”的任务。"
        total_pages = max(1, (total + page_size - 1) // page_size)
        if page > total_pages:
            return f"页码超出范围。当前检索结果共 {total_pages} 页。"
        active_id = self.active_thread.snapshot().thread_id
        lines = [
            f"检索“{self._short_title(query, 40)}”（第 {page}/{total_pages} 页，共 {total} 个）"
        ]
        start = (page - 1) * page_size
        for offset, info in enumerate(threads, 1):
            self.thread_choices[start + offset] = info.thread_id
            marker = "*" if info.thread_id == active_id else " "
            lines.append(
                f"{marker}{start + offset}. [{info.project_name}] {self._short_title(info.title)}"
            )
        lines.extend(["", "点击任务按钮，或发送 /use 编号切换；* 表示当前任务。"])
        return "\n".join(lines)

    @staticmethod
    def _parse_search_argument(argument: str) -> tuple[int, str]:
        value = " ".join(argument.split())
        if not value:
            raise ValueError
        parts = value.split(None, 1)
        if len(parts) == 2 and parts[0].isdigit():
            page = int(parts[0])
            if page < 1:
                raise ValueError
            return page, parts[1]
        return 1, value

    def _thread_actions(self, page: int, query: str = "") -> ActionRows:
        page_size = THREAD_PAGE_SIZE
        if query:
            threads, total = self.thread_index.search_page(query, page, page_size)
        else:
            threads, total = self.thread_index.list_page(page, page_size)
        start = (page - 1) * page_size
        rows: List[List[tuple[str, str, int]]] = []
        active_id = self.active_thread.snapshot().thread_id
        for pair_start in range(0, len(threads), 2):
            row = []
            for offset, info in enumerate(
                threads[pair_start : pair_start + 2], pair_start + 1
            ):
                number = start + offset
                label = f"{number}. {self._short_title(info.title, 12)}"
                row.append(
                    (
                        label,
                        f"/use {info.thread_id}",
                        1 if info.thread_id == active_id else 0,
                    )
                )
            rows.append(row)
        total_pages = max(1, (total + page_size - 1) // page_size)
        navigation = []
        command = "/search {page} {query}" if query else "/threads {page}"
        if page > 1:
            navigation.append(
                ("上一页", command.format(page=page - 1, query=query), 0)
            )
        if page < total_pages:
            navigation.append(
                ("下一页", command.format(page=page + 1, query=query), 0)
            )
        if navigation:
            rows.append(navigation)
        rows.append(
            [
                ("搜索任务", "/search", 0),
                ("当前任务", "/current", 0),
                ("操作菜单", "/help", 0),
            ]
        )
        return self._actions(rows)

    def _thread_is_busy(self, thread_id: str) -> bool:
        path = find_session_file(self.config.codex_sessions_dir, thread_id)
        return path is not None and self._session_busy_without_checkpoint(path)

    def _switch_thread(self, selector: str) -> str:
        if not selector:
            return "用法：/use 编号或完整 UUID"
        if selector.isdigit():
            listed_thread_id = self.thread_choices.get(int(selector))
            if not listed_thread_id:
                return "这个编号还没有列出。先发送 /threads 查看对应页。"
            info = self.thread_index.get(listed_thread_id)
        else:
            info = self.thread_index.get(selector)
        if info is None:
            return "没有找到该任务。先发送 /threads 查看编号。"
        if self._thread_is_busy(info.thread_id):
            return "目标任务仍在执行，结束后才能切换。"
        self.active_thread.switch(info)
        lines = [
            f"已切换：{self._short_title(info.title, 60)}",
            f"项目：{info.project_name}",
            "之后的普通消息会继续这个任务。",
        ]
        path = find_session_file(self.config.codex_sessions_dir, info.thread_id)
        turn = latest_complete_turn(path) if path else None
        if turn:
            user_message, assistant_message = turn
            lines.extend(
                [
                    "",
                    "最新完整一轮（本地原文）",
                    "",
                    "【你】",
                    user_message,
                    "",
                    "【Codex】",
                    assistant_message,
                ]
            )
        else:
            lines.extend(["", "该任务暂时没有完整的本地对话记录。"])
        return "\n".join(lines)

    def _persist_inbound_images(
        self,
        event: InboundMessage,
    ) -> tuple[List[Path], str]:
        attachments = event.attachments
        if not attachments:
            return [], ""
        event_key = f"{event.channel}:{event.message_id}"
        directory_name = hashlib.sha256(event_key.encode("utf-8")).hexdigest()
        root = self.config.base_dir / "data" / "attachments"
        target = root / directory_name
        if target.is_dir():
            existing = sorted(path for path in target.iterdir() if path.is_file())
            if existing:
                return existing, str(target.resolve())
        root.mkdir(parents=True, exist_ok=True)
        staging = root / f".{directory_name}-{uuid.uuid4().hex}.tmp"
        try:
            paths = self.channels.get(event.channel).download_images(
                attachments,
                staging,
            )
            if not paths:
                raise ValueError("消息中没有可处理的图片")
            if target.exists():
                shutil.rmtree(target)
            staging.replace(target)
            persisted = [target / path.name for path in paths]
            return persisted, str(target.resolve())
        except Exception:
            shutil.rmtree(staging, ignore_errors=True)
            raise

    def _accept_remote_job(
        self,
        event: InboundMessage,
        *,
        action: str,
        thread_id: str,
        workdir: Path,
        content: str,
        accepted_text: str,
    ) -> bool:
        if self.stop_event.is_set():
            log_event("bridge", "ignored remote job during shutdown")
            return False
        message_id = event.message_id
        event_key = f"{event.channel}:{message_id}"
        existing = self.store.get_job_by_message(event_key)
        if existing is not None:
            self._safe_send(
                event.recipient,
                accepted_text,
                message_id,
                start_seq=2,
                dedupe_key=f"job:{existing.job_id}:accepted",
            )
            self.outbox_wakeup.set()
            return True
        attachment_dir = ""
        image_paths: List[Path] = []
        try:
            image_paths, attachment_dir = self._persist_inbound_images(event)
            if self.stop_event.is_set():
                if attachment_dir:
                    self._cleanup_job_attachments({"attachment_dir": attachment_dir})
                log_event("bridge", "deferred remote job acceptance during shutdown")
                return False
            payload: Dict[str, Any] = {
                **event.recipient.to_payload(),
                "message_id": message_id,
                "content": content,
                "action": action,
                "thread_id": thread_id,
                "workdir": str(workdir.resolve()),
                "image_paths": [str(path.resolve()) for path in image_paths],
                "attachment_dir": attachment_dir,
            }
            job, created = self.store.enqueue_job(event_key, payload, max_active=20)
        except OverflowError:
            if attachment_dir:
                self._cleanup_job_attachments({"attachment_dir": attachment_dir})
            self._safe_send(
                event.recipient,
                "远程任务队列已满，请稍后再试。",
                message_id,
                start_seq=2,
            )
            return False
        except Exception as exc:
            if attachment_dir:
                self._cleanup_job_attachments({"attachment_dir": attachment_dir})
            detail = summarize_codex_error(str(exc), max_chars=300)
            safe_detail, _images = qq_safe_final(detail, workdir, max_images=0)
            self._safe_send(
                event.recipient,
                f"消息保存失败，本条任务未进入队列：\n{safe_detail}",
                message_id,
                start_seq=2,
            )
            log_event("job-store", f"accept failed: {exc}", level="ERROR")
            return False
        if created:
            self.jobs.put_nowait(job.job_id)
            log_event(
                "job-store",
                f"accepted job={job.job_id} action={action} thread={thread_id}",
            )
        self._safe_send(
            event.recipient,
            accepted_text,
            message_id,
            start_seq=2,
            dedupe_key=f"job:{job.job_id}:accepted",
        )
        return True

    def _safe_final_for_recipient(
        self,
        message: str,
        workdir: Path,
        recipient: Recipient,
    ) -> tuple[str, List[Path]]:
        limits = self._limits_for(recipient.channel)
        max_images = (
            limits.attachment_max_images if limits.supports_outbound_images else 0
        )
        return qq_safe_final(message, workdir, max_images=max_images)

    def _on_channel_event(self, event: InboundMessage) -> None:
        if self.stop_event.is_set():
            log_event("bridge", f"ignored {event.channel} event during shutdown")
            return
        recipient = event.recipient
        content = event.content.strip()
        bound = self._current_recipient(event.channel)
        if bound is None:
            if content.startswith("/bind "):
                self._bind(event)
            else:
                self._safe_send(
                    recipient,
                    "请先发送：/bind 你的绑定码",
                    event.message_id,
                )
            return
        if recipient != bound:
            log_event(
                "bridge",
                f"blocked message from unbound {event.channel} user",
                level="WARNING",
            )
            return

        parts = content.split(None, 1)
        command = parts[0].lower() if parts else ""
        argument = parts[1].strip() if len(parts) > 1 else ""
        if command == "/bind":
            self._safe_send(recipient, "当前渠道已绑定。", event.message_id)
            return
        if command == "/status":
            self._safe_send(
                recipient,
                self._status_text(),
                event.message_id,
                actions=self._actions(
                    [[("当前任务", "/current", 0), ("取消任务", "/cancel", 2)]]
                ),
            )
            return
        if command == "/help":
            self._safe_send(
                recipient,
                self._help_text(),
                event.message_id,
                actions=self._main_actions(),
            )
            return
        if command == "/threads":
            try:
                page = int(argument or "1")
                if page < 1:
                    raise ValueError
                text = self._threads_text(page)
            except ValueError:
                text = "用法：/threads 或 /threads 正整数页码"
            except Exception as exc:
                log_event("thread-index", str(exc), level="ERROR")
                text = "暂时无法读取本机 Codex 任务列表。"
            actions: ActionRows = ()
            if not text.startswith("用法") and not text.startswith("暂时"):
                actions = self._thread_actions(page)
            self._safe_send(
                recipient,
                text,
                event.message_id,
                actions=actions,
            )
            return
        if command == "/search":
            page = 1
            query = ""
            try:
                page, query = self._parse_search_argument(argument)
                text = self._search_text(query, page)
            except ValueError:
                text = (
                    "用法：/search 关键词\n"
                    "后续页：/search 页码 关键词\n"
                    "示例：/search 支付退款"
                )
            except Exception as exc:
                log_event("thread-index", str(exc), level="ERROR")
                text = "暂时无法检索本机 Codex 任务。"
            actions: ActionRows = ()
            if query and not text.startswith("暂时"):
                actions = self._thread_actions(page, query)
            if not actions:
                actions = self._main_actions()
            self._safe_send(
                recipient,
                text,
                event.message_id,
                actions=actions,
            )
            return
        if command == "/use":
            try:
                text = self._switch_thread(argument)
                active = self.active_thread.snapshot()
                text, images = self._safe_final_for_recipient(
                    text,
                    active.workdir,
                    recipient,
                )
            except Exception as exc:
                log_event("thread-switch", str(exc), level="ERROR")
                text = f"切换失败：{exc}"
                images = []
            self._safe_send(
                recipient,
                text,
                event.message_id,
                actions=self._main_actions(),
            )
            self._safe_send_images(recipient, images)
            return
        if command == "/current":
            self._safe_send(
                recipient,
                self._current_text(),
                event.message_id,
                actions=self._main_actions(),
            )
            return
        if command == "/recent":
            active = self.active_thread.snapshot()
            path = find_session_file(self.config.codex_sessions_dir, active.thread_id)
            latest = latest_final_message(path) if path else None
            if latest:
                text, images = self._safe_final_for_recipient(
                    latest,
                    active.workdir,
                    recipient,
                )
                self._safe_send(
                    recipient,
                    text,
                    event.message_id,
                    actions=self._main_actions(),
                )
                self._safe_send_images(recipient, images)
            else:
                self._safe_send(
                    recipient,
                    "当前会话还没有最终结果。",
                    event.message_id,
                    actions=self._main_actions(),
                )
            return
        if command == "/cancel":
            cancelled = self.runner.cancel()
            message = (
                "已请求取消聊天渠道启动的当前任务。"
                if cancelled
                else "当前没有可取消的聊天渠道任务。"
            )
            self._safe_send(recipient, message, event.message_id)
            return
        if command == "/new":
            if not argument:
                self._safe_send(
                    recipient,
                    (
                        "请发送：/new 新任务的第一条消息\n"
                        "示例：/new 排查支付回调失败原因"
                    ),
                    event.message_id,
                    actions=self._main_actions(),
                )
                return
            active = self.active_thread.snapshot()
            self._safe_typing(recipient, event.message_id)
            self._accept_remote_job(
                event,
                action="new",
                thread_id=active.thread_id,
                workdir=active.workdir,
                content=argument,
                accepted_text=(
                    f"已接收并保存，等待 Bridge 在项目 {active.project_name} 中调度新任务。"
                ),
            )
            return
        if content.startswith("/"):
            self._safe_send(
                recipient,
                "未知命令。发送 /help 查看可用命令。",
                event.message_id,
            )
            return

        attachments = event.attachments
        if attachments and not any(
            str(item.get("content_type") or "").startswith("image/")
            for item in attachments
            if isinstance(item, dict)
        ):
            self._safe_send(
                recipient,
                "当前只支持接收图片附件。",
                event.message_id,
            )
            return

        active = self.active_thread.snapshot()
        self._safe_typing(recipient, event.message_id)
        self._accept_remote_job(
            event,
            action="resume",
            thread_id=active.thread_id,
            workdir=active.workdir,
            content=content,
            accepted_text="已接收并保存，等待 Bridge 调度当前 Codex 任务。",
        )

    def _wait_until_idle(self, thread_id: str) -> bool:
        deadline = time.time() + self.config.queue_wait_timeout
        while not self.stop_event.is_set() and time.time() < deadline:
            path = find_session_file(self.config.codex_sessions_dir, thread_id)
            busy = path is not None and self._session_busy_without_checkpoint(path)
            if not busy and not self.runner.running():
                return True
            self.stop_event.wait(1)
        return False

    def _session_busy_without_checkpoint(self, path: Path) -> bool:
        state = self.state.load()
        if state.get("known_idle_session_path") == str(path):
            try:
                checkpoint = int(state.get("known_idle_session_size", -1))
            except (TypeError, ValueError):
                checkpoint = -1
            if checkpoint == path.stat().st_size:
                return False
        return session_busy(path)

    def _mark_thread_idle(self, thread_id: str) -> None:
        path = find_session_file(self.config.codex_sessions_dir, thread_id)
        if not path:
            return
        try:
            size = path.stat().st_size
        except OSError:
            return
        if self.active_thread.snapshot().thread_id == thread_id:
            self.monitor.mark_idle()
        self.state.update(
            known_idle_session_path=str(path),
            known_idle_session_size=size,
        )

    def _refresh_desktop(self, thread_id: str, workdir: Optional[Path] = None) -> None:
        try:
            if self.desktop_refresher.refresh(thread_id, workdir):
                detail = getattr(self.desktop_refresher, "last_detail", "")
                suffix = f" detail={detail}" if isinstance(detail, str) and detail else ""
                log_event("desktop", f"refreshed thread={thread_id}{suffix}")
            elif self.config.codex_desktop_refresh:
                detail = getattr(self.desktop_refresher, "last_detail", "")
                suffix = f" detail={detail}" if isinstance(detail, str) and detail else ""
                log_event(
                    "desktop",
                    f"refresh skipped or failed thread={thread_id}{suffix}",
                    level="WARNING",
                )
        except Exception:
            log_event(
                "desktop",
                f"refresh raised thread={thread_id}\n{traceback.format_exc()}",
            )

    def _worker_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                job_id = self.jobs.get(timeout=0.5)
            except queue.Empty:
                continue
            job: Optional[StoredJob] = None
            result: Optional[JobRunResult] = None
            terminal = False
            stage = "prepare"
            try:
                try:
                    job = self.store.claim_job(job_id)
                except sqlite3.Error as exc:
                    log_event(
                        "job-store",
                        f"claim failed; requeued in memory job={job_id}: {exc}",
                        level="ERROR",
                    )
                    if not self.stop_event.is_set():
                        self.jobs.put_nowait(job_id)
                    self.stop_event.wait(1)
                    continue
                if job is None:
                    continue
                event = job.payload
                self.worker_busy.set()
                self._set_worker_state(stage, job_id)
                action = event.get("action", "resume")
                target_thread_id = event.get("thread_id") or self.active_thread.snapshot().thread_id
                log_event(
                    "worker",
                    f"claimed job={job_id} action={action} thread={target_thread_id}",
                )
                if action != "new":
                    stage = "wait-for-idle"
                    self.store.set_job_status(job_id, "waiting")
                    self._set_worker_state(stage, job_id)
                    path = find_session_file(self.config.codex_sessions_dir, target_thread_id)
                    already_waiting = self.runner.running() or (
                        path is not None and self._session_busy_without_checkpoint(path)
                    )
                    if already_waiting:
                        self._safe_send(
                            self._recipient_from_payload(event),
                            "当前 Codex 任务仍在执行，本条消息已保存并进入等待队列。",
                            dedupe_key=f"job:{job_id}:waiting",
                        )
                    if not self._wait_until_idle(target_thread_id):
                        if self.stop_event.is_set():
                            self.store.set_job_status(job_id, "queued")
                            log_event("worker", f"deferred during shutdown job={job_id}")
                            continue
                        error = "等待当前任务结束超时，本条消息未执行。"
                        result = JobRunResult(target_thread_id, "failed", error, error)
                        stage = "finalize"
                        terminal = self._persist_job_result_with_retry(job, result)
                        if not terminal:
                            self._set_worker_state("persistence-failed", job_id, error)
                            continue
                        self._set_worker_state("failed", job_id, error)
                        log_event(
                            "worker",
                            f"timed out waiting job={job_id} thread={target_thread_id}",
                            level="ERROR",
                        )
                        continue
                stage = "prepare-input"
                self.store.set_job_status(job_id, "preparing")
                self._set_worker_state(stage, job_id)
                workdir = Path(event.get("workdir") or str(self.config.codex_workdir))
                prompt = str(event.get("content") or "").strip()
                image_paths = [Path(str(value)) for value in event.get("image_paths") or []]
                if image_paths and not prompt:
                    prompt = "请查看我发送的图片，并根据图片内容继续处理。"
                    event["content"] = prompt
                missing = [path.name for path in image_paths if not path.is_file()]
                if missing:
                    raise FileNotFoundError(f"已保存的输入图片不存在: {', '.join(missing)}")
                if self.stop_event.is_set():
                    self.store.set_job_status(job_id, "queued")
                    log_event("worker", f"deferred before dispatch job={job_id}")
                    continue
                stage = "run-codex"
                self.store.set_job_status(job_id, "dispatching")
                self._set_worker_state("dispatching", job_id)
                result = self._run_job(
                    job_id,
                    event,
                    action,
                    target_thread_id,
                    workdir,
                    prompt,
                    image_paths,
                )
                stage = "finalize"
                terminal = self._persist_job_result_with_retry(job, result)
                if not terminal:
                    self._set_worker_state(
                        "persistence-failed",
                        job_id,
                        result.error or "terminal persistence failed",
                    )
                    continue
                self._set_worker_state(
                    result.status,
                    job_id,
                    result.error if result.status != "succeeded" else "",
                )
                refreshed_thread_id = result.thread_id
                refresh_result = action != "new" or refreshed_thread_id != target_thread_id
                if refresh_result:
                    self._mark_thread_idle(refreshed_thread_id)
                    self._refresh_desktop(refreshed_thread_id, workdir)
                log_event(
                    "worker",
                    f"finished job={job_id} status={result.status} "
                    f"action={action} thread={refreshed_thread_id}",
                    level="INFO" if result.status == "succeeded" else "ERROR",
                )
            except Exception as exc:
                error = summarize_codex_error(str(exc), max_chars=400)
                log_event(
                    "worker",
                    f"failed job={job.job_id if job else job_id} stage={stage}\n"
                    f"{traceback.format_exc()}",
                    level="ERROR",
                )
                if job is not None and not terminal:
                    persisted: Optional[StoredJob] = None
                    try:
                        persisted = self.store.get_job(job.job_id)
                    except Exception as status_exc:
                        log_event(
                            "job-store",
                            f"failed to inspect job state id={job.job_id}: {status_exc}",
                            level="ERROR",
                        )
                    if result is None:
                        workdir = Path(
                            job.payload.get("workdir") or str(self.config.codex_workdir)
                        )
                        safe_detail, _images = qq_safe_final(
                            error,
                            workdir,
                            max_images=0,
                        )
                        uncertain = isinstance(exc, JobDispatchUncertainError) or (
                            persisted is not None and persisted.status == "running"
                        )
                        if uncertain:
                            status = "interrupted"
                            message = (
                                "Codex 子进程已经启动，但 Bridge 无法确认任务是否完整执行。"
                                "为避免重复操作，请先检查任务记录后再决定是否重试。\n"
                                f"详情：{safe_detail}"
                            )
                        else:
                            status = "failed"
                            message = f"Codex 任务处理失败（阶段：{stage}）：\n{safe_detail}"
                        result = JobRunResult(
                            str(job.payload.get("thread_id") or ""),
                            status,
                            error,
                            message,
                        )
                    terminal = self._persist_job_result_with_retry(job, result)
                    self._set_worker_state(
                        result.status if terminal else "persistence-failed",
                        job.job_id,
                        result.error,
                    )
            finally:
                if terminal and job is not None:
                    self._cleanup_job_attachments(job.payload)
                self.worker_busy.clear()
                self._set_worker_state("idle")
                self.jobs.task_done()

    def _persist_job_result_with_retry(
        self,
        job: StoredJob,
        result: JobRunResult,
    ) -> bool:
        for attempt in range(1, 3):
            try:
                self._persist_job_result(job, result)
                return True
            except Exception as exc:
                log_event(
                    "job-store",
                    f"terminal persistence attempt={attempt} job={job.job_id}: {exc}",
                    level="ERROR",
                )
                if attempt == 1:
                    time.sleep(0.2)
        self._cleanup_unreferenced_outbox_images()
        self._request_fatal_shutdown(
            f"job={job.job_id} terminal persistence failed twice"
        )
        return False

    def _persist_job_result(self, job: StoredJob, result: JobRunResult) -> None:
        if result.status == "succeeded":
            if not result.final_message:
                raise RuntimeError("成功任务缺少最终结果")
            if not result.event_id:
                raise RuntimeError("成功任务缺少可去重的会话事件标识")
            thread = self._thread_info_for_job_result(job, result)
            recipient = self._recipient_from_payload(job.payload)
            dedupe_key = f"session-final:{result.event_id}"
            with self._outbox_image_lock:
                items = self._final_outbox_specs(
                    recipient,
                    thread,
                    result.final_message,
                    dedupe_key,
                    question=str(job.payload.get("content") or "").strip(),
                )
                if not items:
                    raise RuntimeError("成功任务通知缺少聊天渠道收件人")
                self.store.set_job_status_with_outbox(
                    job.job_id,
                    "succeeded",
                    items,
                )
                self._cleanup_unreferenced_outbox_images()
            self.outbox_wakeup.set()
            return
        if not result.notification:
            raise RuntimeError("终态任务缺少用户通知")
        dedupe_key = (
            f"session-interrupted:{result.event_id}"
            if result.event_id
            else f"job:{job.job_id}:terminal"
        )
        self._set_job_status_with_notification(
            job.job_id,
            result.status,
            result.error,
            self._recipient_from_payload(job.payload),
            result.notification,
            dedupe_key=dedupe_key,
        )

    def _thread_info_for_job_result(
        self,
        job: StoredJob,
        result: JobRunResult,
    ) -> ThreadInfo:
        active = self.active_thread.snapshot()
        if active.thread_id == result.thread_id:
            return active
        try:
            indexed = self.thread_index.get(result.thread_id)
        except sqlite3.Error as exc:
            log_event("thread-index", f"result lookup failed: {exc}", level="WARNING")
            indexed = None
        if indexed is not None:
            return indexed
        workdir = Path(job.payload.get("workdir") or str(self.config.codex_workdir)).resolve()
        return ThreadInfo(result.thread_id, result.thread_id, workdir)

    def _on_job_started(
        self,
        job_id: str,
        event: Dict[str, Any],
        action: str,
        process_id: Optional[int],
    ) -> None:
        runner = getattr(self, "runner", None)
        shared_daemon = getattr(runner, "transport_name", "exec") == "app-server"
        if shared_daemon:
            message = (
                "Codex 共享回合已启动，正在创建新任务。"
                if action == "new"
                else "Codex 共享回合已启动，正在执行当前任务。"
            )
        else:
            message = (
                "Codex 子进程已启动，正在创建新任务。"
                if action == "new"
                else "Codex 子进程已启动，正在执行当前任务。"
            )
        try:
            self._set_job_status_with_notification(
                job_id,
                "running",
                "",
                self._recipient_from_payload(event),
                message,
                dedupe_key=f"job:{job_id}:started",
                process_id=process_id,
            )
        except Exception as exc:
            raise JobDispatchUncertainError(
                "Codex 已启动，但 Bridge 无法持久化启动状态"
            ) from exc
        self._set_worker_state("running", job_id)

    def _on_codex_diagnostic(
        self,
        job_id: str,
        event: Dict[str, Any],
        line: str,
    ) -> None:
        filtered = summarize_codex_error(line, max_chars=500)
        if filtered == "没有错误详情":
            return
        lowered = filtered.lower()
        category = ""
        message = ""
        if any(
            marker in lowered
            for marker in (
                "unauthorized",
                "authentication",
                "not logged in",
                "login required",
                "invalid api key",
                "http 401",
            )
        ):
            category = "authentication"
            message = "Codex 报告登录或鉴权异常，当前任务可能无法继续。"
        elif any(marker in lowered for marker in ("rate limit", "too many requests", "http 429")):
            category = "rate-limit"
            message = "Codex 报告服务限流，CLI 可能正在等待后重试。"
        elif any(
            marker in lowered
            for marker in (
                "upstream request failed",
                "service unavailable",
                "bad gateway",
                "gateway timeout",
                "server error",
                "http 502",
                "http 503",
                "http 504",
            )
        ):
            category = "upstream"
            message = "Codex 报告上游服务异常，CLI 可能正在自动重试。"
        elif any(
            marker in lowered
            for marker in (
                "stream disconnected",
                "connection reset",
                "connection refused",
                "failed to connect",
                "network is unreachable",
                "dns error",
                "timed out",
            )
        ):
            category = "network"
            message = "Codex 报告网络连接异常，CLI 可能正在自动重试。"
        elif any(
            marker in lowered
            for marker in ("desktop interaction required", "approval required")
        ):
            category = "approval"
            message = "Codex 正在等待桌面端交互或审批，请打开对应任务处理。"
        if not category:
            return
        log_event(
            "codex-diagnostic",
            f"job={job_id} category={category} detail={filtered}",
            level="WARNING",
        )
        self._safe_send(
            self._recipient_from_payload(event),
            f"{message}\n\n任务尚未结束；最终成功或失败后会继续通知。",
            dedupe_key=f"job:{job_id}:diagnostic:{category}",
        )

    def _rollout_checkpoint(self, thread_id: str) -> tuple[Optional[Path], int]:
        path = find_session_file(self.config.codex_sessions_dir, thread_id)
        if path is None:
            return None, 0
        try:
            resolved = path.resolve()
            return resolved, resolved.stat().st_size
        except OSError:
            return None, 0

    def _latest_rollout_event_id(
        self,
        thread_id: str,
        checkpoint: tuple[Optional[Path], int],
        *,
        final_message: str = "",
        event_type: str = "",
    ) -> str:
        path = find_session_file(self.config.codex_sessions_dir, thread_id)
        if path is None:
            return ""
        try:
            path = path.resolve()
            checkpoint_path, checkpoint_offset = checkpoint
            start_offset = checkpoint_offset if checkpoint_path == path else 0
            if start_offset > path.stat().st_size:
                start_offset = 0
            expected_final = final_message.strip()
            latest = ""
            with path.open("rb") as handle:
                handle.seek(start_offset)
                while True:
                    line_start = handle.tell()
                    line = handle.readline()
                    if not line:
                        break
                    if not line.endswith(b"\n"):
                        break
                    try:
                        record = json.loads(line)
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        continue
                    if not isinstance(record, dict):
                        continue
                    if expected_final:
                        if final_message_from_record(record) != expected_final:
                            continue
                    elif event_type:
                        payload = (
                            record.get("payload")
                            if record.get("type") == "event_msg"
                            else None
                        )
                        if not isinstance(payload, dict) or payload.get("type") != event_type:
                            continue
                    else:
                        continue
                    latest = session_record_event_id(path, line_start, record)
            return latest
        except OSError as exc:
            log_event(
                "session-event",
                f"failed to identify terminal event thread={thread_id}: {exc}",
                level="WARNING",
            )
            return ""

    def _wait_for_rollout_event_id(
        self,
        thread_id: str,
        checkpoint: tuple[Optional[Path], int],
        *,
        final_message: str = "",
        event_type: str = "",
        timeout_seconds: float = 1.0,
    ) -> str:
        deadline = time.monotonic() + max(0.0, timeout_seconds)
        while True:
            event_id = self._latest_rollout_event_id(
                thread_id,
                checkpoint,
                final_message=final_message,
                event_type=event_type,
            )
            if event_id or time.monotonic() >= deadline:
                return event_id
            time.sleep(0.05)

    def _run_job(
        self,
        job_id: str,
        event: Dict[str, Any],
        action: str,
        target_thread_id: str,
        workdir: Path,
        prompt: str,
        image_paths: List[Path],
    ) -> JobRunResult:
        result_thread_id = target_thread_id
        checkpoint = (
            self._rollout_checkpoint(target_thread_id)
            if action != "new"
            else (None, 0)
        )
        on_started = lambda process_id: self._on_job_started(
            job_id,
            event,
            action,
            process_id,
        )
        on_diagnostic = lambda line: self._on_codex_diagnostic(job_id, event, line)
        if action == "new":
            log_event(
                "codex",
                f"new cwd={workdir} chars={len(prompt)} images={len(image_paths)}",
            )
            title = self._short_title(prompt, 80)
            code, new_thread_id, final, stderr = self.runner.run_new(
                prompt,
                workdir,
                image_paths,
                on_started=on_started,
                on_diagnostic=on_diagnostic,
                thread_name=title,
            )
            if new_thread_id:
                result_thread_id = new_thread_id
                switched = self.active_thread.switch_if_current(
                    target_thread_id,
                    ThreadInfo(new_thread_id, title, workdir.resolve()),
                )
                if switched:
                    created_text = f"已新建并切换任务：{title}"
                else:
                    created_text = f"已新建任务：{title}\n当前任务选择保持不变。"
                self._safe_send(
                    self._recipient_from_payload(event),
                    f"{created_text}\n编号：{new_thread_id}",
                    dedupe_key=f"job:{job_id}:created",
                )
            elif code == 0:
                self._safe_send(
                    self._recipient_from_payload(event),
                    "任务已执行，但没有解析到新任务编号，因此未切换当前任务。",
                    dedupe_key=f"job:{job_id}:missing-thread-id",
                )
        else:
            log_event(
                "codex",
                f"resume thread={target_thread_id} chars={len(prompt)} "
                f"images={len(image_paths)}",
            )
            code, final, stderr = self.runner.run(
                prompt,
                target_thread_id,
                workdir,
                image_paths,
                on_started=on_started,
                on_diagnostic=on_diagnostic,
            )
        if code != 0:
            event_id = self._wait_for_rollout_event_id(
                result_thread_id,
                checkpoint,
                event_type="turn_aborted",
            )
            reason = self.runner.termination_reason()
            if reason == "timed_out":
                status = "timed_out"
                detail = summarize_codex_error(stderr)
                heading = "Codex 任务执行超时"
            elif reason == "cancelled":
                status = "cancelled"
                detail = "任务已按用户请求取消。"
                heading = "Codex 任务已取消"
            elif reason == "interrupted":
                status = "interrupted"
                detail = summarize_codex_error(stderr)
                heading = "Codex 任务已中断"
            elif code == -15:
                status = "failed"
                detail = summarize_codex_error(stderr)
                if detail == "没有错误详情":
                    detail = "Codex 子进程收到终止信号。"
                heading = "Codex 任务执行失败"
            else:
                status = "failed"
                detail = summarize_codex_error(stderr)
                heading = "Codex 任务执行失败"
            safe_detail, _images = qq_safe_final(detail, workdir, max_images=0)
            return JobRunResult(
                result_thread_id,
                status,
                detail,
                f"{heading}（退出码 {code}）：\n{safe_detail}",
                event_id=event_id,
            )
        elif not final:
            detail = "Codex 任务已结束，但未解析到最终文本。"
            event_id = self._wait_for_rollout_event_id(
                result_thread_id,
                checkpoint,
                event_type="turn_aborted",
            )
            return JobRunResult(
                result_thread_id,
                "failed",
                detail,
                f"{detail}发送 /recent 查看会话记录。",
                event_id=event_id,
            )
        event_id = self._wait_for_rollout_event_id(
            result_thread_id,
            checkpoint,
            final_message=final,
        )
        if not event_id:
            detail = "Codex 已返回结果，但 Bridge 无法在本地会话中确认对应的终态记录。"
            safe_final, _images = qq_safe_final(final, workdir, max_images=0)
            return JobRunResult(
                result_thread_id,
                "interrupted",
                detail,
                f"{detail}\n为避免重复通知，本任务未标记为成功。\n\nCodex 返回：\n{safe_final}",
            )
        return JobRunResult(
            result_thread_id,
            "succeeded",
            final_message=final,
            event_id=event_id,
        )

    def _on_final(self, thread: ThreadInfo, message: str) -> None:
        question = ""
        sessions_dir = getattr(self.config, "codex_sessions_dir", None)
        path = thread.rollout_path
        if path is None and sessions_dir is not None:
            path = find_session_file(sessions_dir, thread.thread_id)
        turn = latest_complete_turn(path) if path else None
        if turn and turn[1] == message:
            question = turn[0]
        self._enqueue_final_notification(thread, message, question=question)

    def _on_final_event(
        self,
        thread: ThreadInfo,
        message: str,
        event_id: str,
        question: str = "",
    ) -> None:
        self._enqueue_final_notification(thread, message, event_id, question)

    def _enqueue_final_notification(
        self,
        thread: ThreadInfo,
        message: str,
        event_id: str = "",
        question: str = "",
    ) -> None:
        recipients = self._bound_destinations()
        if not recipients:
            return
        dedupe_key = f"session-final:{event_id}" if event_id else ""
        with self._outbox_image_lock:
            try:
                items: List[OutboxSpec] = []
                for recipient in recipients:
                    items.extend(
                        self._final_outbox_specs(
                            recipient,
                            thread,
                            message,
                            dedupe_key,
                            question,
                        )
                    )
                if not items:
                    raise RuntimeError("Codex 最终结果通知缺少聊天渠道收件人")
                self.store.enqueue_outbox_batch(items)
            finally:
                self._cleanup_unreferenced_outbox_images()
        self.outbox_wakeup.set()

    def _prepare_final_notification(
        self,
        recipient: Recipient,
        thread: ThreadInfo,
        message: str,
        question: str = "",
    ) -> tuple[str, ActionRows, List[Path]]:
        text, images = self._safe_final_for_recipient(
            message,
            thread.workdir,
            recipient,
        )
        title = self._short_title(thread.title, 60)
        if text:
            body = text
        elif images:
            body = "Codex 已返回图片。"
        else:
            body = "Codex 返回了本地图片，请在 Desktop 中查看。"
        safe_question = ""
        if question.strip():
            safe_question, _question_images = qq_safe_final(
                question,
                thread.workdir,
                max_images=0,
            )
        normalized_question = " ".join(safe_question.split())
        if len(normalized_question) > 400:
            normalized_question = normalized_question[:399].rstrip() + "…"
        escaped_question = normalized_question.replace("\\", "\\\\")
        for marker in ("`", "*", "_", "[", "]", "#", "|", ">"):
            escaped_question = escaped_question.replace(marker, f"\\{marker}")
        lines = [
            "## Codex 任务已回复",
            "",
            f"**任务：{title}**",
            f"项目：{thread.project_name}",
            f"编号：`{thread.thread_id}`",
        ]
        if escaped_question:
            lines.extend(
                [
                    "",
                    "### 本轮问题",
                    f"> {escaped_question}",
                    "",
                    "### Codex 回复",
                ]
            )
        lines.extend(["", body])
        notification = "\n".join(lines)
        actions = self._actions(
            [
                [("切换到此任务", f"/use {thread.thread_id}", 1)],
                [("任务列表", "/threads", 0), ("当前任务", "/current", 0)],
            ]
        )
        return notification, actions, images

    def _final_outbox_specs(
        self,
        recipient: Recipient,
        thread: ThreadInfo,
        message: str,
        dedupe_key: str,
        question: str = "",
    ) -> List[OutboxSpec]:
        notification, actions, images = self._prepare_final_notification(
            recipient,
            thread,
            message,
            question,
        )
        items = self._text_outbox_specs(
            recipient,
            notification,
            actions=actions,
            dedupe_key=dedupe_key,
        )
        image_items, complete = self._image_outbox_specs(
            recipient,
            images,
            dedupe_key=dedupe_key,
        )
        if not complete:
            raise RuntimeError("无法持久化 Codex 结果图片通知")
        return [*items, *image_items]

    def _on_interrupted(self, thread: ThreadInfo, detail: str, event_id: str) -> None:
        recipients = self._bound_destinations()
        if not recipients:
            return
        safe_detail, _images = qq_safe_final(detail, thread.workdir, max_images=0)
        title = self._short_title(thread.title, 60)
        notification = "\n".join(
            [
                "## Codex 任务已中断",
                "",
                f"**任务：{title}**",
                f"项目：{thread.project_name}",
                "",
                safe_detail,
            ]
        )
        queued = [
            self._safe_send(
                recipient,
                notification,
                dedupe_key=f"session-interrupted:{event_id}",
            )
            for recipient in recipients
        ]
        if not all(queued):
            raise RuntimeError("无法将 Codex 中断事件写入通知队列")


def check_installation(config: Config) -> int:
    errors = []
    transport = "unknown"
    transport_detail = ""
    state = StateStore(config.base_dir / "data" / "state.json").load()
    stored_id = str(state.get("active_thread_id") or "")
    thread_id = stored_id if stored_id else config.codex_thread_id
    path = find_session_file(config.codex_sessions_dir, thread_id)
    if not path:
        errors.append("未找到当前 Codex 会话文件")
    try:
        ThreadIndex(config.codex_state_db).list_page(1, 1)
    except Exception as exc:
        errors.append(f"Codex 任务索引读取失败: {exc}")
    errors.extend(channel_dependency_errors(config.enabled_channels))
    try:
        runner = select_codex_runner(config)
        transport = getattr(runner, "transport_name", "exec")
        probe_info = getattr(runner, "probe_info", {})
        if isinstance(probe_info, dict):
            transport_detail = str(probe_info.get("userAgent") or "")
    except Exception as exc:
        errors.append(f"Codex transport 检查失败: {exc}")
    try:
        completed = subprocess.run(
            [*config.codex_command, "--version"],
            cwd=str(config.codex_workdir),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=10,
            check=False,
        )
        version = (completed.stdout or "").strip() or "unknown"
    except Exception as exc:
        errors.append(f"Codex 命令检查失败: {exc}")
        version = "unknown"
    print(f"Codex: {version}")
    print(
        f"Transport: {transport}"
        + (f" ({transport_detail})" if transport_detail else "")
    )
    print(f"Thread: {thread_id}")
    print(f"Session: {path or 'not found'}")
    print(f"State DB: {config.codex_state_db}")
    print(f"Channels: {','.join(config.enabled_channels)}")
    for line in channel_configuration_summary(config, config.enabled_channels):
        print(line)
    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1
    print("Local check passed. Network and channel credentials have not been tested.")
    return 0


def main() -> int:
    configure_log_file(BASE_DIR / "data" / "logs" / "bridge.jsonl")
    parser = argparse.ArgumentParser(description="Bridge Codex Desktop tasks to chat channels")
    parser.add_argument("--check", action="store_true", help="validate local configuration only")
    args = parser.parse_args()
    try:
        config = Config.from_env(BASE_DIR, require_channel_credentials=True)
    except ValueError as exc:
        log_event("bridge", f"configuration error: {exc}", level="ERROR")
        print(f"配置错误：{exc}", file=sys.stderr)
        return 2
    if args.check:
        return check_installation(config)
    try:
        service = BridgeService(config)
    except Exception as exc:
        log_event("bridge", f"startup failed: {exc}", level="ERROR")
        print(f"启动失败：{exc}", file=sys.stderr)
        return 2

    def stop_service(_signum: int, _frame: Optional[object]) -> None:
        threading.Thread(
            target=service.stop,
            name="bridge-signal-stop",
            daemon=True,
        ).start()

    signal.signal(signal.SIGINT, stop_service)
    signal.signal(signal.SIGTERM, stop_service)
    active = service.active_thread.snapshot()
    log_event(
        "bridge",
        f"starting thread={active.thread_id} cwd={active.workdir}",
    )
    try:
        service.run()
    except Exception as exc:
        log_event(
            "bridge",
            f"service failed: {exc}\n{traceback.format_exc()}",
            level="ERROR",
        )
        print(f"Bridge 异常退出：{exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
