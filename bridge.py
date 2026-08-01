#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hmac
import queue
import signal
import subprocess
import sys
import tempfile
import threading
import time
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional

from bridge_core import (
    ActiveThread,
    CodexDesktopRefresher,
    CodexRunner,
    Config,
    SessionMonitor,
    StateStore,
    ThreadIndex,
    ThreadInfo,
    find_session_file,
    latest_complete_turn,
    latest_final_message,
    log_event,
    qq_safe_final,
    session_busy,
    split_message,
    summarize_codex_error,
)
from qq_gateway import QQApi, QQGatewayClient, download_c2c_images, websocket


BASE_DIR = Path(__file__).resolve().parent


class BridgeService:
    def __init__(self, config: Config) -> None:
        self.config = config
        self.state = StateStore(config.base_dir / "data" / "state.json")
        stored_state = self.state.load()
        self.bound_openid = config.qq_allowed_openid or str(stored_state.get("bound_openid") or "")
        if not self.bound_openid and not config.qq_bind_code:
            raise ValueError("首次运行必须配置 QQ_BIND_CODE，或直接配置 QQ_ALLOWED_OPENID")

        self.api = QQApi(config)
        self.thread_index = ThreadIndex(config.codex_state_db)
        self.active_thread = ActiveThread(config, self.state, self.thread_index)
        self.runner = CodexRunner(config)
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
        )
        self.jobs: "queue.Queue[Dict[str, Any]]" = queue.Queue(maxsize=20)
        self.thread_choices: Dict[int, str] = {}
        self.stop_event = threading.Event()
        self._stop_started = threading.Event()
        self._online_notice_sent = threading.Event()
        self.worker_busy = threading.Event()
        self.worker = threading.Thread(target=self._worker_loop, name="codex-worker", daemon=True)
        self.gateway = QQGatewayClient(
            config,
            self.api,
            on_event=self._on_qq_event,
            on_ready=self._on_gateway_ready,
        )
        self._send_lock = threading.Lock()

    def run(self) -> None:
        self.monitor.start()
        self.worker.start()
        try:
            self.gateway.run_forever()
        finally:
            self.stop()

    def stop(self) -> None:
        if self._stop_started.is_set():
            if self.runner.running():
                print("[bridge] second stop request: cancelling Codex task", flush=True)
                self.runner.cancel()
            return
        self._stop_started.set()
        self.stop_event.set()
        self.gateway.stop()
        if self.worker.is_alive() and threading.current_thread() is not self.worker:
            if self.runner.running():
                print(
                    "[bridge] waiting for current Codex task; send stop again to cancel",
                    flush=True,
                )
            self.worker.join()
        self.monitor.stop()

    def _current_openid(self) -> str:
        return self.config.qq_allowed_openid or self.bound_openid

    @staticmethod
    def _button(label: str, command: str, style: int = 0) -> Dict[str, Any]:
        return {
            "id": command,
            "render_data": {"label": label, "visited_label": label, "style": style},
            "action": {
                "type": 2,
                "permission": {"type": 2},
                "data": command,
                "enter": True,
            },
        }

    @classmethod
    def _keyboard(cls, rows: List[List[tuple[str, str, int]]]) -> Dict[str, Any]:
        return {
            "content": {
                "rows": [
                    {
                        "buttons": [
                            cls._button(label, command, style)
                            for label, command, style in row
                        ]
                    }
                    for row in rows
                    if row
                ]
            }
        }

    @classmethod
    def _main_keyboard(cls) -> Dict[str, Any]:
        return cls._keyboard(
            [
                [("任务列表", "/threads", 1), ("当前任务", "/current", 0)],
                [("执行状态", "/status", 0), ("最近结果", "/recent", 0)],
            ]
        )

    def _send_text(
        self,
        openid: str,
        text: str,
        msg_id: str = "",
        keyboard: Optional[Dict[str, Any]] = None,
        start_seq: int = 1,
    ) -> None:
        if not openid:
            return
        max_chunks = self.config.qq_reply_chunks
        if msg_id:
            max_chunks = min(max_chunks, max(1, 5 - start_seq))
        chunks = split_message(text, self.config.qq_reply_chars, max_chunks)
        with self._send_lock:
            for index, chunk in enumerate(chunks, start_seq):
                try:
                    self.api.send_c2c_markdown(
                        openid,
                        chunk,
                        msg_id=msg_id,
                        msg_seq=index,
                        keyboard=keyboard if index == start_seq else None,
                    )
                except Exception as exc:
                    print(f"[qq-markdown-fallback] {exc}", flush=True)
                    self.api.send_c2c(openid, chunk, msg_id=msg_id, msg_seq=index)
                if len(chunks) > 1:
                    time.sleep(0.35)

    def _safe_send(
        self,
        openid: str,
        text: str,
        msg_id: str = "",
        keyboard: Optional[Dict[str, Any]] = None,
        start_seq: int = 1,
    ) -> None:
        try:
            self._send_text(openid, text, msg_id, keyboard, start_seq)
        except Exception as exc:
            print(f"[qq-send] {exc}", flush=True)

    def _safe_typing(self, openid: str, msg_id: str) -> None:
        try:
            self.api.send_c2c_typing(openid, msg_id, seconds=60)
        except Exception as exc:
            print(f"[qq-typing] {exc}", flush=True)

    def _safe_send_images(self, openid: str, paths: List[Path]) -> None:
        for index, path in enumerate(paths, 1):
            try:
                if path.stat().st_size > self.config.qq_attachment_max_bytes:
                    print(f"[qq-image] skipped oversized image: {path.name}", flush=True)
                    continue
                file_info = self.api.upload_c2c_image(openid, path)
                self.api.send_c2c_media(openid, file_info, msg_seq=index)
            except Exception as exc:
                print(f"[qq-image] {path.name}: {exc}", flush=True)

    def _on_gateway_ready(self) -> None:
        log_event("bridge", "QQ Gateway is ready")
        if not self.config.qq_notify_on_ready:
            return
        openid = self._current_openid()
        if openid:
            if self._online_notice_sent.is_set():
                return
            self._online_notice_sent.set()
            active = self.active_thread.snapshot()
            self._safe_send(
                openid,
                "## Codex Chat Bridge 已上线\n\n"
                f"当前任务：**{self._short_title(active.title)}**",
                keyboard=self._main_keyboard(),
            )
        else:
            log_event("bridge", "waiting for /bind <code>")

    def _bind(self, event: Dict[str, Any]) -> None:
        content = event["content"]
        supplied = content.partition(" ")[2].strip()
        if not supplied or not hmac.compare_digest(supplied, self.config.qq_bind_code):
            self._safe_send(event["openid"], "绑定码不正确。", event["message_id"])
            return
        self.bound_openid = event["openid"]
        self.state.update(bound_openid=self.bound_openid)
        self._safe_send(
            self.bound_openid,
            "绑定成功。此机器人现在只接受你的私聊消息。发送 /help 查看命令。",
            event["message_id"],
            keyboard=self._main_keyboard(),
        )
        print(f"[bridge] bound openid={self.bound_openid[:8]}...", flush=True)

    def _status_text(self) -> str:
        active = self.active_thread.snapshot()
        path = self.monitor.path or find_session_file(
            self.config.codex_sessions_dir, active.thread_id
        )
        busy = path is not None and self._session_busy_without_checkpoint(path)
        queue_size = self.jobs.qsize()
        return "\n".join(
            [
                "Codex Chat Bridge 状态",
                f"标题：{self._short_title(active.title, 60)}",
                f"任务：{active.thread_id}",
                f"Codex：{'执行中' if busy or self.runner.running() else '空闲'}",
                f"QQ 队列：{queue_size}",
                f"会话文件：{'已找到' if path else '未找到'}",
                f"工作目录：{active.workdir}",
            ]
        )

    def _help_text(self) -> str:
        return "\n".join(
            [
                "直接发送文字：继续当前 Codex 任务",
                "/threads [页码]：列出可切换任务",
                "/use <编号或UUID>：切换并返回最新一轮原文",
                "/current：查看当前任务",
                "/new <第一条消息>：在当前项目中新建任务",
                "/status：查看任务和队列状态",
                "/recent：查看最近一次 Codex 最终结果",
                "/cancel：取消由 QQ 启动的当前任务",
                "/help：查看帮助",
                "桌面任务执行中发送的新消息会自动排队。",
                "切换或新建任务时，队列必须为空且当前任务必须空闲。",
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
        page_size = 8
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
        lines.extend(["", "发送 /use 编号 切换；* 表示当前任务。"])
        if page < total_pages:
            lines.append(f"下一页：/threads {page + 1}")
        return "\n".join(lines)

    def _threads_keyboard(self, page: int) -> Dict[str, Any]:
        page_size = 8
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
                    (label, f"/use {number}", 1 if info.thread_id == active_id else 0)
                )
            rows.append(row)
        total_pages = max(1, (total + page_size - 1) // page_size)
        navigation = []
        if page > 1:
            navigation.append(("上一页", f"/threads {page - 1}", 0))
        if page < total_pages:
            navigation.append(("下一页", f"/threads {page + 1}", 0))
        if navigation:
            rows.append(navigation)
        rows.append([("当前任务", "/current", 0), ("执行状态", "/status", 0)])
        return self._keyboard(rows)

    def _can_change_thread(self) -> bool:
        active = self.active_thread.snapshot()
        path = self.monitor.path or find_session_file(
            self.config.codex_sessions_dir, active.thread_id
        )
        session_is_busy = path is not None and self._session_busy_without_checkpoint(path)
        return not (
            self.worker_busy.is_set()
            or self.runner.running()
            or session_is_busy
            or not self.jobs.empty()
        )

    def _switch_thread(self, selector: str) -> str:
        if not selector:
            return "用法：/use 编号或完整 UUID"
        if not self._can_change_thread():
            return "当前任务或 QQ 队列仍在执行，结束后才能切换。"
        if selector.isdigit():
            listed_thread_id = self.thread_choices.get(int(selector))
            if not listed_thread_id:
                return "这个编号还没有列出。先发送 /threads 查看对应页。"
            info = self.thread_index.get(listed_thread_id)
        else:
            info = self.thread_index.get(selector)
        if info is None:
            return "没有找到该任务。先发送 /threads 查看编号。"
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

    def _on_qq_event(self, event: Dict[str, Any]) -> None:
        openid = event["openid"]
        content = event["content"].strip()
        if not self._current_openid():
            if content.startswith("/bind "):
                self._bind(event)
            else:
                self._safe_send(openid, "请先发送：/bind 你的绑定码", event["message_id"])
            return
        if openid != self._current_openid():
            print(f"[bridge] blocked openid={openid[:8]}...", flush=True)
            return

        parts = content.split(None, 1)
        command = parts[0].lower() if parts else ""
        argument = parts[1].strip() if len(parts) > 1 else ""
        if command == "/bind":
            self._safe_send(openid, "当前 QQ 已绑定。", event["message_id"])
            return
        if command == "/status":
            self._safe_send(
                openid,
                self._status_text(),
                event["message_id"],
                keyboard=self._keyboard(
                    [[("当前任务", "/current", 0), ("取消任务", "/cancel", 2)]]
                ),
            )
            return
        if command == "/help":
            self._safe_send(
                openid,
                self._help_text(),
                event["message_id"],
                keyboard=self._main_keyboard(),
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
                print(f"[thread-index] {exc}", flush=True)
                text = "暂时无法读取本机 Codex 任务列表。"
            keyboard = None
            if not text.startswith("用法") and not text.startswith("暂时"):
                keyboard = self._threads_keyboard(page)
            self._safe_send(openid, text, event["message_id"], keyboard=keyboard)
            return
        if command == "/use":
            try:
                text = self._switch_thread(argument)
                active = self.active_thread.snapshot()
                text, images = qq_safe_final(
                    text,
                    active.workdir,
                    self.config.qq_attachment_max_images,
                )
            except Exception as exc:
                print(f"[thread-switch] {exc}", flush=True)
                text = f"切换失败：{exc}"
                images = []
            self._safe_send(
                openid,
                text,
                event["message_id"],
                keyboard=self._main_keyboard(),
            )
            self._safe_send_images(openid, images)
            return
        if command == "/current":
            self._safe_send(
                openid,
                self._current_text(),
                event["message_id"],
                keyboard=self._main_keyboard(),
            )
            return
        if command == "/recent":
            active = self.active_thread.snapshot()
            path = find_session_file(self.config.codex_sessions_dir, active.thread_id)
            latest = latest_final_message(path) if path else None
            if latest:
                text, images = qq_safe_final(
                    latest,
                    active.workdir,
                    self.config.qq_attachment_max_images,
                )
                self._safe_send(openid, text, event["message_id"], keyboard=self._main_keyboard())
                self._safe_send_images(openid, images)
            else:
                self._safe_send(
                    openid,
                    "当前会话还没有最终结果。",
                    event["message_id"],
                    keyboard=self._main_keyboard(),
                )
            return
        if command == "/cancel":
            cancelled = self.runner.cancel()
            message = "已请求取消 QQ 启动的当前任务。" if cancelled else "当前没有可取消的 QQ 任务。"
            self._safe_send(openid, message, event["message_id"])
            return
        if command == "/new":
            if not argument:
                self._safe_send(
                    openid,
                    "用法：/new 新任务的第一条消息",
                    event["message_id"],
                )
                return
            if not self._can_change_thread():
                self._safe_send(
                    openid,
                    "当前任务或 QQ 队列仍在执行，结束后才能新建任务。",
                    event["message_id"],
                )
                return
            active = self.active_thread.snapshot()
            self._safe_typing(openid, event["message_id"])
            job = dict(event)
            job.update(action="new", workdir=str(active.workdir), content=argument)
            try:
                self.jobs.put_nowait(job)
            except queue.Full:
                self._safe_send(
                    openid,
                    "远程任务队列已满，请稍后再试。",
                    event["message_id"],
                    start_seq=2,
                )
                return
            self._safe_send(
                openid,
                f"收到，正在项目 {active.project_name} 中新建 Codex 任务。",
                event["message_id"],
                start_seq=2,
            )
            return
        if content.startswith("/"):
            self._safe_send(openid, "未知命令。发送 /help 查看可用命令。", event["message_id"])
            return

        attachments = event.get("attachments") or []
        if attachments and not any(
            str(item.get("content_type") or "").startswith("image/")
            for item in attachments
            if isinstance(item, dict)
        ):
            self._safe_send(openid, "当前只支持接收图片附件。", event["message_id"])
            return

        active = self.active_thread.snapshot()
        self._safe_typing(openid, event["message_id"])
        job = dict(event)
        job.update(
            action="resume",
            thread_id=active.thread_id,
            workdir=str(active.workdir),
        )
        try:
            self.jobs.put_nowait(job)
        except queue.Full:
            self._safe_send(
                openid,
                "远程任务队列已满，请稍后再试。",
                event["message_id"],
                start_seq=2,
            )
            return
        path = self.monitor.path or find_session_file(
            self.config.codex_sessions_dir, active.thread_id
        )
        waiting = (
            self.runner.running()
            or (path is not None and self._session_busy_without_checkpoint(path))
            or self.jobs.qsize() > 1
        )
        message = "已排队，当前任务结束后继续。" if waiting else "收到，正在继续当前 Codex 任务。"
        self._safe_send(openid, message, event["message_id"], start_seq=2)

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
        self.monitor.mark_idle()
        self.state.update(
            known_idle_session_path=str(path),
            known_idle_session_size=size,
        )

    def _refresh_desktop(self, thread_id: str) -> None:
        try:
            if self.desktop_refresher.refresh(thread_id):
                log_event("desktop", f"refreshed thread={thread_id}")
            elif self.config.codex_desktop_refresh:
                log_event("desktop", f"refresh skipped or failed thread={thread_id}")
        except Exception:
            log_event(
                "desktop",
                f"refresh raised thread={thread_id}\n{traceback.format_exc()}",
            )

    def _worker_loop(self) -> None:
        while not self.stop_event.is_set():
            try:
                event = self.jobs.get(timeout=0.5)
            except queue.Empty:
                continue
            self.worker_busy.set()
            stage = "prepare"
            try:
                action = event.get("action", "resume")
                target_thread_id = event.get("thread_id") or self.active_thread.snapshot().thread_id
                log_event(
                    "worker",
                    f"started action={action} thread={target_thread_id}",
                )
                stage = "wait-for-idle"
                if not self._wait_until_idle(target_thread_id):
                    self._safe_send(event["openid"], "等待当前任务结束超时，本条消息未执行。")
                    log_event("worker", f"timed out stage={stage} thread={target_thread_id}")
                    continue
                stage = "prepare-input"
                workdir = Path(event.get("workdir") or str(self.config.codex_workdir))
                attachments = event.get("attachments") or []
                prompt = str(event.get("content") or "").strip()
                if attachments and not prompt:
                    prompt = "请查看我发送的图片，并根据图片内容继续处理。"
                temp_parent = self.config.base_dir / "data"
                temp_parent.mkdir(parents=True, exist_ok=True)
                with tempfile.TemporaryDirectory(prefix="qq-images-", dir=temp_parent) as raw_dir:
                    stage = "download-attachments"
                    image_paths = download_c2c_images(
                        attachments,
                        Path(raw_dir),
                        self.config.qq_attachment_max_bytes,
                        self.config.qq_attachment_max_images,
                    )
                    if attachments and not image_paths:
                        raise ValueError("消息中没有可处理的图片")
                    refreshed_thread_id = target_thread_id
                    try:
                        stage = "run-codex"
                        refreshed_thread_id = self._run_job(
                            event,
                            action,
                            target_thread_id,
                            workdir,
                            prompt,
                            image_paths,
                        )
                    except Exception:
                        if action == "new":
                            refreshed_thread_id = self.active_thread.snapshot().thread_id
                        self._mark_thread_idle(refreshed_thread_id)
                        self._refresh_desktop(refreshed_thread_id)
                        raise
                    stage = "finalize"
                    if action == "new":
                        refreshed_thread_id = self.active_thread.snapshot().thread_id
                    self._mark_thread_idle(refreshed_thread_id)
                    self._refresh_desktop(refreshed_thread_id)
                log_event(
                    "worker",
                    f"completed action={action} thread={refreshed_thread_id}",
                )
            except Exception as exc:
                log_event(
                    "worker",
                    f"failed stage={stage}\n{traceback.format_exc()}",
                )
                workdir = Path(event.get("workdir") or str(self.config.codex_workdir))
                detail = summarize_codex_error(str(exc), max_chars=400)
                safe_detail, _images = qq_safe_final(detail, workdir, max_images=0)
                self._safe_send(
                    event["openid"],
                    f"Codex 任务处理失败（阶段：{stage}）：\n{safe_detail}",
                )
            finally:
                self.worker_busy.clear()
                self.jobs.task_done()

    def _run_job(
        self,
        event: Dict[str, Any],
        action: str,
        target_thread_id: str,
        workdir: Path,
        prompt: str,
        image_paths: List[Path],
    ) -> str:
        result_thread_id = target_thread_id
        if action == "new":
            log_event(
                "codex",
                f"new cwd={workdir} chars={len(prompt)} images={len(image_paths)}",
            )
            code, new_thread_id, final, stderr = self.runner.run_new(
                prompt, workdir, image_paths
            )
            if new_thread_id:
                result_thread_id = new_thread_id
                title = self._short_title(prompt, 80)
                self.active_thread.switch(
                    ThreadInfo(new_thread_id, title, workdir.resolve())
                )
                self._safe_send(
                    event["openid"],
                    f"已新建并切换任务：{title}\n编号：{new_thread_id}",
                )
            elif code == 0:
                self._safe_send(
                    event["openid"],
                    "任务已执行，但没有解析到新任务编号，因此未切换当前任务。",
                )
        else:
            log_event(
                "codex",
                f"resume thread={target_thread_id} chars={len(prompt)} "
                f"images={len(image_paths)}",
            )
            code, final, stderr = self.runner.run(
                prompt, target_thread_id, workdir, image_paths
            )
        if code != 0:
            if code == -15:
                detail = "Codex 子进程收到终止信号。"
            else:
                detail = summarize_codex_error(stderr)
            safe_detail, _images = qq_safe_final(detail, workdir, max_images=0)
            self._safe_send(
                event["openid"],
                f"Codex 任务执行失败（退出码 {code}）：\n{safe_detail}",
            )
        elif not final:
            self._safe_send(
                event["openid"],
                "Codex 任务已结束，但未解析到最终文本。发送 /recent 查看会话记录。",
            )
        return result_thread_id

    def _on_final(self, thread: ThreadInfo, message: str) -> None:
        openid = self._current_openid()
        if not openid:
            return
        text, images = qq_safe_final(
            message,
            thread.workdir,
            self.config.qq_attachment_max_images,
        )
        title = self._short_title(thread.title, 60)
        body = text or "Codex 已返回图片。"
        notification = "\n".join(
            [
                "## Codex 任务已回复",
                "",
                f"**任务：{title}**",
                f"项目：{thread.project_name}",
                f"编号：`{thread.thread_id}`",
                "",
                body,
            ]
        )
        keyboard = self._keyboard(
            [
                [("切换到此任务", f"/use {thread.thread_id}", 1)],
                [("任务列表", "/threads", 0), ("当前任务", "/current", 0)],
            ]
        )
        self._safe_send(openid, notification, keyboard=keyboard)
        self._safe_send_images(openid, images)


def check_installation(config: Config) -> int:
    errors = []
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
    if websocket is None:
        errors.append("缺少 Python 依赖 websocket-client")
    try:
        completed = subprocess.run(
            [*config.codex_command, "--version"],
            cwd=str(config.codex_workdir),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=10,
            check=False,
        )
        version = (completed.stdout or "").strip() or "unknown"
    except Exception as exc:
        errors.append(f"Codex 命令检查失败: {exc}")
        version = "unknown"
    print(f"Codex: {version}")
    print(f"Thread: {thread_id}")
    print(f"Session: {path or 'not found'}")
    print(f"State DB: {config.codex_state_db}")
    print(f"QQ AppID: {'configured' if config.qq_app_id else 'missing'}")
    print(f"QQ binding: {'configured' if config.qq_allowed_openid or config.qq_bind_code else 'missing'}")
    if errors:
        for error in errors:
            print(f"ERROR: {error}", file=sys.stderr)
        return 1
    print("Local check passed. Network and QQ credentials have not been tested.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Bridge Codex Desktop tasks to QQ")
    parser.add_argument("--check", action="store_true", help="validate local configuration only")
    args = parser.parse_args()
    try:
        config = Config.from_env(BASE_DIR, require_qq=True)
    except ValueError as exc:
        print(f"配置错误：{exc}", file=sys.stderr)
        return 2
    if args.check:
        return check_installation(config)
    try:
        service = BridgeService(config)
    except (ValueError, RuntimeError) as exc:
        print(f"启动失败：{exc}", file=sys.stderr)
        return 2

    def stop_service(_signum: int, _frame: Optional[object]) -> None:
        service.stop()

    signal.signal(signal.SIGINT, stop_service)
    signal.signal(signal.SIGTERM, stop_service)
    active = service.active_thread.snapshot()
    log_event(
        "bridge",
        f"starting thread={active.thread_id} cwd={active.workdir}",
    )
    service.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
