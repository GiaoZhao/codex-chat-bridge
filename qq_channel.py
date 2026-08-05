from __future__ import annotations

import threading
from pathlib import Path
from typing import Any, Mapping, Sequence, Tuple

from bridge_core import Config, log_event
from chat_channel import (
    ActionRows,
    ChannelLimits,
    InboundHandler,
    InboundMessage,
    ReadyHandler,
    StateHandler,
    redact_identifier,
)
from qq_gateway import QQApi, QQGatewayClient, download_c2c_images, websocket


class QQChannel:
    name = "qq"

    @staticmethod
    def dependency_error() -> str:
        return "" if websocket is not None else "缺少 Python 依赖 websocket-client"

    @staticmethod
    def configuration_summary(config: Config) -> Tuple[str, ...]:
        return (
            f"QQ AppID: {'configured' if config.qq_app_id else 'missing'}",
            "QQ binding: "
            + (
                "configured"
                if config.qq_allowed_openid or config.qq_bind_code
                else "missing"
            ),
        )

    def __init__(
        self,
        config: Config,
        on_message: InboundHandler,
        on_ready: ReadyHandler,
        on_state: StateHandler,
    ) -> None:
        self.config = config
        self.bind_code = config.qq_bind_code
        self.configured_recipient = config.qq_allowed_openid
        self.notify_on_ready = config.qq_notify_on_ready
        self.limits = ChannelLimits(
            reply_chars=config.qq_reply_chars,
            reply_chunks=config.qq_reply_chunks,
            attachment_max_bytes=config.qq_attachment_max_bytes,
            attachment_max_images=config.qq_attachment_max_images,
            passive_reply_limit=4,
        )
        self._on_message = on_message
        self._on_ready = on_ready
        self._on_state = on_state
        self._ready = threading.Event()
        self._send_lock = threading.Lock()
        self.api = QQApi(config)
        self.gateway = QQGatewayClient(
            config,
            self.api,
            on_event=self._handle_event,
            on_ready=self._handle_ready,
            on_state=self._handle_state,
        )

    def legacy_recipient(self, stored_state: Mapping[str, Any]) -> str:
        return str(stored_state.get("bound_openid") or "")

    @staticmethod
    def _button(label: str, command: str, style: int = 0) -> dict[str, Any]:
        return {
            "id": command,
            "render_data": {
                "label": label,
                "visited_label": label,
                "style": style,
            },
            "action": {
                "type": 2,
                "permission": {"type": 2},
                "data": command,
                "enter": True,
            },
        }

    @classmethod
    def keyboard(cls, rows: ActionRows) -> dict[str, Any] | None:
        if not rows:
            return None
        return {
            "content": {
                "rows": [
                    {
                        "buttons": [
                            cls._button(action.label, action.command, action.style)
                            for action in row
                        ]
                    }
                    for row in rows
                ]
            }
        }

    def _handle_event(self, event: dict[str, Any]) -> None:
        self._on_message(
            InboundMessage(
                channel=self.name,
                sender_id=str(event["openid"]),
                message_id=str(event["message_id"]),
                content=str(event.get("content") or ""),
                attachments=tuple(event.get("attachments") or ()),
            )
        )

    def _handle_ready(self) -> None:
        self._handle_state("ready", "")
        self._on_ready(self.name)

    def _handle_state(self, state: str, detail: str) -> None:
        if state == "ready":
            self._ready.set()
        else:
            self._ready.clear()
        self._on_state(self.name, state, detail)

    def run_forever(self) -> None:
        self.gateway.run_forever()

    def stop(self) -> None:
        self.gateway.stop()
        self._ready.clear()

    def is_ready(self) -> bool:
        return self._ready.is_set()

    def send_text(
        self,
        recipient_id: str,
        text: str,
        *,
        reply_to: str = "",
        sequence: int = 1,
        actions: ActionRows = (),
    ) -> None:
        keyboard = self.keyboard(actions)
        with self._send_lock:
            try:
                self.api.send_c2c_markdown(
                    recipient_id,
                    text,
                    msg_id=reply_to,
                    msg_seq=sequence,
                    keyboard=keyboard,
                )
            except Exception as exc:
                detail = self.redact_error(exc, recipient_id)
                log_event("qq-markdown", f"fallback: {detail}", level="WARNING")
                self.api.send_c2c(
                    recipient_id,
                    text,
                    msg_id=reply_to,
                    msg_seq=sequence,
                )

    def send_typing(self, recipient_id: str, reply_to: str) -> None:
        self.api.send_c2c_typing(recipient_id, reply_to, seconds=60)

    def send_image(self, recipient_id: str, path: Path, sequence: int = 1) -> None:
        file_info = self.api.upload_c2c_image(recipient_id, path)
        self.api.send_c2c_media(recipient_id, file_info, msg_seq=sequence)

    def download_images(
        self,
        attachments: Sequence[Mapping[str, Any]],
        target_dir: Path,
    ) -> list[Path]:
        return download_c2c_images(
            list(attachments),
            target_dir,
            self.limits.attachment_max_bytes,
            self.limits.attachment_max_images,
        )

    def redact_error(self, error: object, recipient_id: str) -> str:
        return redact_identifier(error, recipient_id)
