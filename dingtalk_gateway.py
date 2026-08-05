from __future__ import annotations

import asyncio
import base64
import binascii
import json
import queue
import threading
import time
import uuid
from pathlib import Path
from typing import Any, Callable, Dict, Mapping, Optional, Sequence, Tuple
from urllib.parse import quote_plus, urlparse

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

try:
    import dingtalk_stream
    import requests
    import websockets
except ImportError:
    dingtalk_stream = None
    requests = None
    websockets = None


DINGTALK_AI_CARD_TEMPLATE_ID = "382e4302-551d-4880-bf29-a30acfab2e71.schema"
DINGTALK_CARD_CALLBACK_TOPIC = "/v1.0/card/instances/callback"
DINGTALK_CARD_ACTION_PREFIX = "codex_cmd_"


def extract_dingtalk_event(data: Mapping[str, Any]) -> Optional[InboundMessage]:
    if str(data.get("conversationType") or "") != "1":
        return None
    sender_id = str(data.get("senderStaffId") or data.get("senderId") or "").strip()
    message_id = str(data.get("msgId") or "").strip()
    message_type = str(data.get("msgtype") or "").strip()
    content = ""
    attachments: list[Dict[str, Any]] = []

    if message_type == "text":
        text = data.get("text")
        if isinstance(text, dict):
            content = str(text.get("content") or "").strip()
    elif message_type == "picture":
        raw_content = data.get("content")
        if isinstance(raw_content, dict) and raw_content.get("downloadCode"):
            attachments.append(
                {
                    "download_code": str(raw_content["downloadCode"]),
                    "content_type": "image/dingtalk",
                    "filename": "image.png",
                }
            )
    elif message_type == "richText":
        raw_content = data.get("content")
        rich_text = raw_content.get("richText") if isinstance(raw_content, dict) else None
        texts: list[str] = []
        if isinstance(rich_text, list):
            for item in rich_text:
                if not isinstance(item, dict):
                    continue
                if item.get("text"):
                    texts.append(str(item["text"]))
                if item.get("downloadCode"):
                    attachments.append(
                        {
                            "download_code": str(item["downloadCode"]),
                            "content_type": "image/dingtalk",
                            "filename": f"image-{len(attachments) + 1}.png",
                        }
                    )
        content = "\n".join(texts).strip()
    else:
        return None

    if not sender_id or not message_id or (not content and not attachments):
        return None
    return InboundMessage(
        channel="dingtalk",
        sender_id=sender_id,
        message_id=message_id,
        content=content,
        attachments=tuple(attachments),
    )


def _json_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    if not isinstance(value, str) or not value.strip():
        return {}
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError):
        return {}
    return parsed if isinstance(parsed, Mapping) else {}


def _encode_dingtalk_card_action(command: str) -> str:
    encoded = base64.urlsafe_b64encode(command.encode("utf-8")).decode("ascii")
    return DINGTALK_CARD_ACTION_PREFIX + encoded.rstrip("=")


def _decode_dingtalk_card_action(action_id: Any) -> str:
    value = str(action_id or "")
    if not value.startswith(DINGTALK_CARD_ACTION_PREFIX):
        return ""
    encoded = value[len(DINGTALK_CARD_ACTION_PREFIX) :]
    padding = "=" * (-len(encoded) % 4)
    try:
        command = base64.b64decode(
            (encoded + padding).encode("ascii"),
            altchars=b"-_",
            validate=True,
        ).decode("utf-8")
    except (binascii.Error, UnicodeDecodeError, UnicodeEncodeError, ValueError):
        return ""
    return command if command.startswith("/") else ""


def _find_dingtalk_card_action(value: Any, depth: int = 0) -> str:
    if depth > 8:
        return ""
    if isinstance(value, Mapping):
        for nested in value.values():
            command = _find_dingtalk_card_action(nested, depth + 1)
            if command:
                return command
        return ""
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for nested in value:
            command = _find_dingtalk_card_action(nested, depth + 1)
            if command:
                return command
        return ""
    if not isinstance(value, str):
        return ""
    command = _decode_dingtalk_card_action(value)
    if command:
        return command
    stripped = value.strip()
    if not stripped or stripped[0] not in "[{":
        return ""
    try:
        parsed = json.loads(stripped)
    except (TypeError, ValueError):
        return ""
    return _find_dingtalk_card_action(parsed, depth + 1)


def _dingtalk_card_callback_shape(value: Any, depth: int = 0) -> str:
    if depth > 5:
        return "..."
    if isinstance(value, Mapping):
        entries = []
        for key, nested in list(value.items())[:20]:
            entries.append(
                f"{str(key)[:40]}:{_dingtalk_card_callback_shape(nested, depth + 1)}"
            )
        return "{" + ",".join(entries) + "}"
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        samples = [
            _dingtalk_card_callback_shape(item, depth + 1)
            for item in list(value)[:3]
        ]
        return f"list[{len(value)}](" + ",".join(samples) + ")"
    if isinstance(value, str):
        stripped = value.strip()
        if stripped and stripped[0] in "[{":
            try:
                parsed = json.loads(stripped)
            except (TypeError, ValueError):
                pass
            else:
                return "json:" + _dingtalk_card_callback_shape(parsed, depth + 1)
        return "str"
    return type(value).__name__


def extract_dingtalk_card_event(
    data: Mapping[str, Any],
    message_id: str = "",
) -> Optional[InboundMessage]:
    if str(data.get("type") or "actionCallback") != "actionCallback":
        return None
    sender_id = str(data.get("userId") or "").strip()
    content = _json_mapping(data.get("content"))
    private_data = _json_mapping(content.get("cardPrivateData"))
    params = _json_mapping(private_data.get("params"))
    command = str(params.get("command") or params.get("text") or "").strip()
    if not command.startswith("/"):
        command = _find_dingtalk_card_action(data)
    if not sender_id or not command.startswith("/"):
        return None

    event_id = message_id.strip()
    if not event_id:
        action_ids = private_data.get("actionIds") or []
        fingerprint = json.dumps(
            [data.get("outTrackId"), sender_id, action_ids, command],
            ensure_ascii=False,
            sort_keys=True,
        )
        event_id = "card-" + uuid.uuid5(uuid.NAMESPACE_URL, fingerprint).hex
    return InboundMessage(
        channel="dingtalk",
        sender_id=sender_id,
        message_id=event_id,
        content=command,
    )


class DingTalkApi:
    def __init__(self, config: Config) -> None:
        if requests is None:
            raise RuntimeError("缺少 dingtalk-stream，请先运行安装脚本")
        self.config = config
        self._token = ""
        self._expires_at = 0.0
        self._token_lock = threading.Lock()
        self._send_lock = threading.Lock()

    def _url(self, path: str) -> str:
        return f"{self.config.dingtalk_api_base}/{path.lstrip('/')}"

    @staticmethod
    def _response_json(response: Any) -> Dict[str, Any]:
        try:
            value = response.json()
        except Exception as exc:
            raise RuntimeError(
                f"钉钉 API 返回非 JSON 响应: {str(getattr(response, 'text', ''))[:500]}"
            ) from exc
        if not isinstance(value, dict):
            raise RuntimeError("钉钉 API 返回的不是 JSON 对象")
        return value

    def access_token(self, force: bool = False) -> str:
        with self._token_lock:
            if not force and self._token and self._expires_at > time.time() + 60:
                return self._token
            response = requests.post(
                self._url("/v1.0/oauth2/accessToken"),
                json={
                    "appKey": self.config.dingtalk_client_id,
                    "appSecret": self.config.dingtalk_client_secret,
                },
                timeout=15,
            )
            response.raise_for_status()
            value = self._response_json(response)
            token = str(value.get("accessToken") or "")
            if not token:
                raise RuntimeError("钉钉 access token 响应缺少 accessToken")
            expires_in = int(value.get("expireIn") or 7200)
            self._token = token
            self._expires_at = time.time() + max(60, expires_in - 300)
            return token

    def _post_openapi(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        response = requests.post(
            self._url(path),
            headers={"x-acs-dingtalk-access-token": self.access_token()},
            json=payload,
            timeout=20,
        )
        if response.status_code == 401:
            response = requests.post(
                self._url(path),
                headers={"x-acs-dingtalk-access-token": self.access_token(force=True)},
                json=payload,
                timeout=20,
            )
        response.raise_for_status()
        return self._response_json(response) if response.content else {}

    def _send_oto(self, recipient_id: str, msg_key: str, msg_param: Dict[str, Any]) -> None:
        robot_code = self.config.dingtalk_robot_code or self.config.dingtalk_client_id
        self._post_openapi(
            "/v1.0/robot/oToMessages/batchSend",
            {
                "robotCode": robot_code,
                "userIds": [recipient_id],
                "msgKey": msg_key,
                "msgParam": json.dumps(msg_param, ensure_ascii=False),
            },
        )

    def send_markdown(
        self,
        recipient_id: str,
        text: str,
        *,
        title: str = "Codex",
    ) -> None:
        with self._send_lock:
            self._send_oto(
                recipient_id,
                "sampleMarkdown",
                {"title": title, "text": text},
            )

    def send_text(self, recipient_id: str, text: str) -> None:
        with self._send_lock:
            self._send_oto(recipient_id, "sampleText", {"content": text})

    def send_ai_card(
        self,
        recipient_id: str,
        title: str,
        markdown: str,
        actions: Sequence[tuple[str, str, int]],
    ) -> str:
        buttons = [
            {
                "text": label,
                "color": {1: "blue", 2: "red"}.get(style, "gray"),
                "id": _encode_dingtalk_card_action(command),
                "request": True,
                "params": {"command": command, "text": command},
            }
            for label, command, style in actions
        ]
        order = ["msgTitle", "staticMsgContent"]
        if buttons:
            order.append("msgButtons")
        card_data = {
            "flowStatus": "3",
            "msgTitle": title,
            "msgContent": "",
            "staticMsgContent": markdown,
            "sys_full_json_obj": json.dumps(
                {"order": order, "msgButtons": buttons},
                ensure_ascii=False,
            ),
        }
        out_track_id = "codex-" + uuid.uuid4().hex
        with self._send_lock:
            result = self._post_openapi(
                "/v1.0/card/instances/createAndDeliver",
                {
                    "cardTemplateId": DINGTALK_AI_CARD_TEMPLATE_ID,
                    "outTrackId": out_track_id,
                    "callbackType": "STREAM",
                    "cardData": {"cardParamMap": card_data},
                    "imRobotOpenSpaceModel": {"supportForward": False},
                    "openSpaceId": f"dtv1.card//IM_ROBOT.{recipient_id}",
                    "imRobotOpenDeliverModel": {"spaceType": "IM_ROBOT"},
                    "userIdType": 1,
                },
            )
        if result.get("success") is False:
            raise RuntimeError("钉钉互动卡片发送失败")
        return out_track_id

    def download_image(
        self,
        download_code: str,
        target: Path,
        max_bytes: int,
    ) -> Path:
        robot_code = self.config.dingtalk_robot_code or self.config.dingtalk_client_id
        value = self._post_openapi(
            "/v1.0/robot/messageFiles/download",
            {"robotCode": robot_code, "downloadCode": download_code},
        )
        download_url = str(value.get("downloadUrl") or "")
        parsed = urlparse(download_url)
        if parsed.scheme != "https" or not parsed.hostname:
            raise ValueError("钉钉图片下载地址无效")
        response = requests.get(download_url, stream=True, timeout=30)
        response.raise_for_status()
        declared_size = int(response.headers.get("Content-Length") or 0)
        if declared_size > max_bytes:
            raise ValueError("钉钉图片超过大小限制")
        data = bytearray()
        for chunk in response.iter_content(chunk_size=64 * 1024):
            data.extend(chunk)
            if len(data) > max_bytes:
                raise ValueError("钉钉图片超过大小限制")
        if not data:
            raise ValueError("钉钉图片内容为空")
        target.write_bytes(bytes(data))
        return target


_HandlerBase = dingtalk_stream.ChatbotHandler if dingtalk_stream is not None else object
_CallbackHandlerBase = (
    dingtalk_stream.CallbackHandler if dingtalk_stream is not None else object
)


class _DingTalkMessageHandler(_HandlerBase):
    def __init__(self, enqueue: Callable[[InboundMessage], None]) -> None:
        super().__init__()
        self._enqueue = enqueue

    async def process(self, callback: Any) -> tuple[int, str]:
        try:
            event = extract_dingtalk_event(callback.data)
            if event is not None:
                self._enqueue(event)
        except Exception as exc:
            log_event("dingtalk-event", str(exc), level="ERROR")
        return dingtalk_stream.AckMessage.STATUS_OK, "OK"


class _DingTalkCardCallbackHandler(_CallbackHandlerBase):
    def __init__(self, enqueue: Callable[[InboundMessage], None]) -> None:
        super().__init__()
        self._enqueue = enqueue

    async def process(self, callback: Any) -> tuple[int, str]:
        try:
            headers = getattr(callback, "headers", None)
            message_id = str(getattr(headers, "message_id", "") or "")
            event = extract_dingtalk_card_event(callback.data, message_id)
            if event is not None:
                self._enqueue(event)
                log_event("dingtalk-card", "callback accepted")
            else:
                log_event(
                    "dingtalk-card",
                    "callback ignored: missing bridge command; shape="
                    + _dingtalk_card_callback_shape(callback.data),
                    level="WARNING",
                )
        except Exception as exc:
            log_event("dingtalk-card", str(exc), level="ERROR")
        return dingtalk_stream.AckMessage.STATUS_OK, "OK"


class DingTalkGatewayClient:
    def __init__(
        self,
        config: Config,
        on_message: InboundHandler,
        on_ready: Callable[[], None],
        on_state: Callable[[str, str], None],
    ) -> None:
        if dingtalk_stream is None or websockets is None:
            raise RuntimeError("缺少 dingtalk-stream，请先运行安装脚本")
        self.config = config
        self.on_message = on_message
        self.on_ready = on_ready
        self.on_state = on_state
        self.stop_event = threading.Event()
        self._ready = threading.Event()
        self._events: "queue.Queue[InboundMessage]" = queue.Queue()
        self._seen: Dict[str, float] = {}
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._event_worker = threading.Thread(
            target=self._dispatch_events,
            name="dingtalk-inbound",
            daemon=True,
        )
        credential = dingtalk_stream.Credential(
            config.dingtalk_client_id,
            config.dingtalk_client_secret,
        )
        self.stream_client = dingtalk_stream.DingTalkStreamClient(credential)
        self.stream_client.register_callback_handler(
            dingtalk_stream.ChatbotMessage.TOPIC,
            _DingTalkMessageHandler(self._enqueue),
        )
        self.stream_client.register_callback_handler(
            DINGTALK_CARD_CALLBACK_TOPIC,
            _DingTalkCardCallbackHandler(self._enqueue),
        )

    def _emit_state(self, state: str, detail: str = "") -> None:
        if state == "ready":
            self._ready.set()
        else:
            self._ready.clear()
        self.on_state(state, detail)

    def _enqueue(self, event: InboundMessage) -> None:
        now = time.time()
        self._seen = {
            key: expires for key, expires in self._seen.items() if expires > now
        }
        key = f"{event.channel}:{event.message_id}"
        if key in self._seen:
            return
        self._seen[key] = now + 600
        self._events.put_nowait(event)

    def _dispatch_events(self) -> None:
        while not self.stop_event.is_set() or not self._events.empty():
            try:
                event = self._events.get(timeout=0.5)
            except queue.Empty:
                continue
            try:
                self.on_message(event)
            except Exception as exc:
                log_event("dingtalk-event", f"handler failed: {exc}", level="ERROR")
            finally:
                self._events.task_done()

    async def _wait_before_reconnect(self, seconds: float) -> None:
        deadline = time.monotonic() + seconds
        while not self.stop_event.is_set() and time.monotonic() < deadline:
            await asyncio.sleep(min(0.25, deadline - time.monotonic()))

    async def _connection_loop(self) -> None:
        self.stream_client.pre_start()
        while not self.stop_event.is_set():
            self._emit_state("connecting")
            connection = await asyncio.to_thread(self.stream_client.open_connection)
            if not connection:
                self._emit_state("error", "open connection failed")
                await self._wait_before_reconnect(5)
                continue
            endpoint = str(connection.get("endpoint") or "")
            ticket = str(connection.get("ticket") or "")
            if not endpoint or not ticket:
                self._emit_state("error", "connection response incomplete")
                await self._wait_before_reconnect(5)
                continue
            uri = f"{endpoint}?ticket={quote_plus(ticket)}"
            try:
                async with websockets.connect(uri, ping_interval=30) as websocket:
                    self.stream_client.websocket = websocket
                    self._emit_state("ready")
                    self.on_ready()
                    while not self.stop_event.is_set():
                        try:
                            raw = await asyncio.wait_for(websocket.recv(), timeout=1)
                        except asyncio.TimeoutError:
                            continue
                        result = await self.stream_client.route_message(json.loads(raw))
                        if result == self.stream_client.TAG_DISCONNECT:
                            break
            except Exception as exc:
                if not self.stop_event.is_set():
                    self._emit_state("error", str(exc))
                    log_event("dingtalk-gateway", str(exc), level="ERROR")
            finally:
                self.stream_client.websocket = None
                if not self.stop_event.is_set():
                    self._emit_state("disconnected")
            await self._wait_before_reconnect(3)

    def run_forever(self) -> None:
        if not self._event_worker.is_alive():
            self._event_worker.start()
        self._loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._loop)
        try:
            self._loop.run_until_complete(self._connection_loop())
        finally:
            self._ready.clear()
            self._loop.close()
            self._loop = None
            if self._event_worker.is_alive():
                self._event_worker.join(timeout=2)

    def stop(self) -> None:
        self.stop_event.set()
        loop = self._loop
        websocket = self.stream_client.websocket
        if loop is not None and websocket is not None:
            def close_websocket() -> None:
                asyncio.create_task(websocket.close())

            loop.call_soon_threadsafe(close_websocket)
        self._emit_state("stopped")

    def is_ready(self) -> bool:
        return self._ready.is_set()


class DingTalkChannel:
    name = "dingtalk"

    @staticmethod
    def dependency_error() -> str:
        return (
            ""
            if dingtalk_stream is not None
            and requests is not None
            and websockets is not None
            else "缺少 Python 依赖 dingtalk-stream"
        )

    @staticmethod
    def configuration_summary(config: Config) -> Tuple[str, ...]:
        return (
            "DingTalk Client ID: "
            + ("configured" if config.dingtalk_client_id else "missing"),
            "DingTalk binding: "
            + (
                "configured"
                if config.dingtalk_allowed_user_id or config.dingtalk_bind_code
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
        self.bind_code = config.dingtalk_bind_code
        self.configured_recipient = config.dingtalk_allowed_user_id
        self.notify_on_ready = config.dingtalk_notify_on_ready
        self.limits = ChannelLimits(
            reply_chars=config.dingtalk_reply_chars,
            reply_chunks=config.dingtalk_reply_chunks,
            attachment_max_bytes=config.dingtalk_attachment_max_bytes,
            attachment_max_images=config.dingtalk_attachment_max_images,
            supports_outbound_images=False,
        )
        self.api = DingTalkApi(config)
        self.gateway = DingTalkGatewayClient(
            config,
            on_message=on_message,
            on_ready=lambda: on_ready(self.name),
            on_state=lambda state, detail: on_state(self.name, state, detail),
        )

    def legacy_recipient(self, stored_state: Mapping[str, Any]) -> str:
        del stored_state
        return ""

    def run_forever(self) -> None:
        self.gateway.run_forever()

    def stop(self) -> None:
        self.gateway.stop()

    def is_ready(self) -> bool:
        return self.gateway.is_ready()

    @staticmethod
    def _action_entries(actions: ActionRows) -> list[tuple[str, str, int]]:
        entries: list[tuple[str, str, int]] = []
        seen: set[str] = set()
        for row in actions:
            for action in row:
                if action.command and action.command not in seen:
                    seen.add(action.command)
                    entries.append((action.label, action.command, action.style))
        return entries

    @staticmethod
    def _card_markdown(text: str) -> str:
        lines = text.strip("\n").splitlines()
        rendered: list[str] = []
        fence = ""
        for index, line in enumerate(lines):
            stripped = line.lstrip()
            marker = (
                "```"
                if stripped.startswith("```")
                else "~~~" if stripped.startswith("~~~") else ""
            )
            if marker:
                rendered.append(line)
                if not fence:
                    fence = marker
                elif marker == fence:
                    fence = ""
                continue
            next_line = lines[index + 1] if index + 1 < len(lines) else ""
            if line and next_line and not fence and not line.endswith("  "):
                rendered.append(line + "  ")
            else:
                rendered.append(line)
        return "\n".join(rendered)

    @classmethod
    def _with_actions(cls, text: str, actions: ActionRows) -> str:
        entries = cls._action_entries(actions)
        if not entries:
            return text
        commands = "　".join(
            f"{label}：{command}" for label, command, _style in entries
        )
        return f"{text}\n\n快捷操作：{commands}"

    @classmethod
    def _render_markdown(
        cls,
        text: str,
        actions: ActionRows,
        sequence: int,
    ) -> tuple[str, str]:
        title = "Codex 回复" if sequence <= 1 else f"Codex 回复 · {sequence}"
        sections = [f"#### {title}", "", text.strip("\n")]
        entries = cls._action_entries(actions)
        if entries:
            sections.extend(["", "> **快捷操作**"])
            sections.extend(
                f"> **{label}**　{command}"
                for label, command, _style in entries
            )
        return title, "\n".join(sections)

    def send_text(
        self,
        recipient_id: str,
        text: str,
        *,
        reply_to: str = "",
        sequence: int = 1,
        actions: ActionRows = (),
    ) -> None:
        del reply_to
        entries = self._action_entries(actions)
        title = "Codex 回复" if sequence <= 1 else f"Codex 回复 · {sequence}"
        try:
            self.api.send_ai_card(
                recipient_id,
                title,
                self._card_markdown(text),
                entries,
            )
            return
        except Exception as exc:
            log_event(
                "dingtalk-card",
                f"fallback: {self.redact_error(exc, recipient_id)}",
                level="WARNING",
            )

        title, rendered = self._render_markdown(text, actions, sequence)
        try:
            self.api.send_markdown(recipient_id, rendered, title=title)
        except Exception as exc:
            log_event(
                "dingtalk-markdown",
                f"fallback: {self.redact_error(exc, recipient_id)}",
                level="WARNING",
            )
            self.api.send_text(recipient_id, self._with_actions(text, actions))

    def send_typing(self, recipient_id: str, reply_to: str) -> None:
        del recipient_id, reply_to

    def send_image(self, recipient_id: str, path: Path, sequence: int = 1) -> None:
        del recipient_id, path, sequence
        raise NotImplementedError("钉钉渠道暂不支持发送本地图片")

    def download_images(
        self,
        attachments: Sequence[Mapping[str, Any]],
        target_dir: Path,
    ) -> list[Path]:
        target_dir.mkdir(parents=True, exist_ok=True)
        paths: list[Path] = []
        for index, attachment in enumerate(attachments[: self.limits.attachment_max_images], 1):
            download_code = str(attachment.get("download_code") or "")
            if not download_code:
                continue
            path = target_dir / f"{index:02d}-image.png"
            paths.append(
                self.api.download_image(
                    download_code,
                    path,
                    self.limits.attachment_max_bytes,
                )
            )
        return paths

    def redact_error(self, error: object, recipient_id: str) -> str:
        return redact_identifier(error, recipient_id)
