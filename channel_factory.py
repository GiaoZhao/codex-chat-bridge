from __future__ import annotations

from typing import Dict, Iterable, List, Tuple, Type

from bridge_core import Config
from chat_channel import (
    ChannelRegistry,
    ChatChannel,
    InboundHandler,
    ReadyHandler,
    StateHandler,
)
from dingtalk_gateway import DingTalkChannel
from qq_channel import QQChannel


CHANNEL_TYPES: Dict[str, Type[ChatChannel]] = {
    QQChannel.name: QQChannel,
    DingTalkChannel.name: DingTalkChannel,
}


def build_channel_registry(
    config: Config,
    on_message: InboundHandler,
    on_ready: ReadyHandler,
    on_state: StateHandler,
) -> ChannelRegistry:
    channels: List[ChatChannel] = []
    for raw_name in config.enabled_channels:
        name = raw_name.strip().lower()
        try:
            channel_type = CHANNEL_TYPES[name]
        except KeyError as exc:
            raise ValueError(f"不支持的聊天渠道: {name}") from exc
        channels.append(channel_type(config, on_message, on_ready, on_state))
    return ChannelRegistry(channels)


def channel_dependency_errors(channel_names: Iterable[str]) -> List[str]:
    errors: List[str] = []
    for raw_name in channel_names:
        name = raw_name.strip().lower()
        channel_type = CHANNEL_TYPES.get(name)
        if channel_type is None:
            errors.append(f"不支持的聊天渠道: {name}")
            continue
        error = channel_type.dependency_error()
        if error:
            errors.append(error)
    return errors


def channel_configuration_summary(
    config: Config,
    channel_names: Iterable[str],
) -> Tuple[str, ...]:
    lines: List[str] = []
    for raw_name in channel_names:
        name = raw_name.strip().lower()
        channel_type = CHANNEL_TYPES.get(name)
        if channel_type is not None:
            lines.extend(channel_type.configuration_summary(config))
    return tuple(lines)
