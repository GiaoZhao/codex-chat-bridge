from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Protocol, Sequence, Tuple
from urllib.parse import quote


@dataclass(frozen=True)
class ChannelLimits:
    reply_chars: int
    reply_chunks: int
    attachment_max_bytes: int
    attachment_max_images: int
    passive_reply_limit: int = 0
    supports_outbound_images: bool = True


@dataclass(frozen=True)
class Recipient:
    channel: str
    recipient_id: str

    def __post_init__(self) -> None:
        if not self.channel.strip():
            raise ValueError("channel 不能为空")
        if not self.recipient_id.strip():
            raise ValueError("recipient_id 不能为空")

    def to_payload(self) -> Dict[str, str]:
        return {
            "channel": self.channel,
            "recipient_id": self.recipient_id,
        }

    @classmethod
    def from_payload(
        cls,
        payload: Mapping[str, Any],
        default_channel: str = "qq",
    ) -> "Recipient":
        channel = str(payload.get("channel") or default_channel).strip()
        recipient_id = str(
            payload.get("recipient_id") or payload.get("openid") or ""
        ).strip()
        return cls(channel, recipient_id)


@dataclass(frozen=True)
class InboundMessage:
    channel: str
    sender_id: str
    message_id: str
    content: str = ""
    attachments: Tuple[Dict[str, Any], ...] = field(default_factory=tuple)

    @property
    def recipient(self) -> Recipient:
        return Recipient(self.channel, self.sender_id)


@dataclass(frozen=True)
class ChannelAction:
    label: str
    command: str
    style: int = 0

    def to_payload(self) -> Dict[str, Any]:
        return {
            "label": self.label,
            "command": self.command,
            "style": self.style,
        }

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> "ChannelAction":
        return cls(
            label=str(payload.get("label") or ""),
            command=str(payload.get("command") or ""),
            style=int(payload.get("style") or 0),
        )


ActionRows = Tuple[Tuple[ChannelAction, ...], ...]
InboundHandler = Callable[[InboundMessage], None]
ReadyHandler = Callable[[str], None]
StateHandler = Callable[[str, str, str], None]


def action_rows(rows: Sequence[Sequence[tuple[str, str, int]]]) -> ActionRows:
    return tuple(
        tuple(ChannelAction(label, command, style) for label, command, style in row)
        for row in rows
        if row
    )


def actions_to_payload(rows: ActionRows) -> List[List[Dict[str, Any]]]:
    return [[action.to_payload() for action in row] for row in rows]


def actions_from_payload(value: Any) -> ActionRows:
    if not isinstance(value, list):
        return ()
    rows: List[Tuple[ChannelAction, ...]] = []
    for raw_row in value:
        if not isinstance(raw_row, list):
            continue
        row = tuple(
            ChannelAction.from_payload(item)
            for item in raw_row
            if isinstance(item, dict)
        )
        if row:
            rows.append(row)
    return tuple(rows)


def redact_identifier(error: object, identifier: str) -> str:
    detail = str(error)
    if not identifier:
        return detail
    for value in {identifier, quote(identifier, safe="")}:
        if value:
            detail = detail.replace(value, "[recipient]")
    return detail


class ChatChannel(Protocol):
    name: str
    limits: ChannelLimits
    bind_code: str
    configured_recipient: str
    notify_on_ready: bool

    @staticmethod
    def dependency_error() -> str:
        ...

    @staticmethod
    def configuration_summary(config: Any) -> Tuple[str, ...]:
        ...

    def legacy_recipient(self, stored_state: Mapping[str, Any]) -> str:
        ...

    def run_forever(self) -> None:
        ...

    def stop(self) -> None:
        ...

    def is_ready(self) -> bool:
        ...

    def send_text(
        self,
        recipient_id: str,
        text: str,
        *,
        reply_to: str = "",
        sequence: int = 1,
        actions: ActionRows = (),
    ) -> None:
        ...

    def send_typing(self, recipient_id: str, reply_to: str) -> None:
        ...

    def send_image(self, recipient_id: str, path: Path, sequence: int = 1) -> None:
        ...

    def download_images(
        self,
        attachments: Sequence[Mapping[str, Any]],
        target_dir: Path,
    ) -> List[Path]:
        ...

    def redact_error(self, error: object, recipient_id: str) -> str:
        ...


class ChannelRegistry:
    def __init__(self, channels: Iterable[ChatChannel]) -> None:
        self._channels: Dict[str, ChatChannel] = {}
        for channel in channels:
            name = channel.name.strip().lower()
            if not name:
                raise ValueError("渠道名称不能为空")
            if name in self._channels:
                raise ValueError(f"重复渠道: {name}")
            self._channels[name] = channel
        if not self._channels:
            raise ValueError("至少需要启用一个聊天渠道")

    def get(self, name: str) -> ChatChannel:
        normalized = name.strip().lower()
        try:
            return self._channels[normalized]
        except KeyError as exc:
            raise KeyError(f"未启用聊天渠道: {normalized}") from exc

    def names(self) -> Tuple[str, ...]:
        return tuple(self._channels)

    def values(self) -> Tuple[ChatChannel, ...]:
        return tuple(self._channels.values())

    def ready_names(self) -> Tuple[str, ...]:
        return tuple(
            name for name, channel in self._channels.items() if channel.is_ready()
        )

    def stop_all(self) -> None:
        first_error: Exception | None = None
        for channel in self._channels.values():
            try:
                channel.stop()
            except Exception as exc:
                if first_error is None:
                    first_error = exc
        if first_error is not None:
            raise first_error
