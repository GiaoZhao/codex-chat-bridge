from __future__ import annotations

import asyncio
import json
import queue
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

import channel_factory
from channel_factory import build_channel_registry
from chat_channel import InboundMessage, action_rows
from dingtalk_gateway import (
    DINGTALK_AI_CARD_TEMPLATE_ID,
    DINGTALK_CARD_CALLBACK_TOPIC,
    DingTalkApi,
    DingTalkChannel,
    DingTalkGatewayClient,
    _DingTalkCardCallbackHandler,
    _DingTalkMessageHandler,
    _dingtalk_card_callback_shape,
    _encode_dingtalk_card_action,
    extract_dingtalk_card_event,
    extract_dingtalk_event,
)


def dingtalk_config(**overrides: object) -> SimpleNamespace:
    values = {
        "enabled_channels": ("dingtalk",),
        "dingtalk_client_id": "client-id",
        "dingtalk_client_secret": "client-secret",
        "dingtalk_robot_code": "robot-code",
        "dingtalk_api_base": "https://api.dingtalk.com",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


class FakeResponse:
    def __init__(
        self,
        value: dict | None = None,
        *,
        status_code: int = 200,
        content: bytes = b"{}",
        headers: dict | None = None,
        chunks: list[bytes] | None = None,
    ) -> None:
        self._value = value or {}
        self.status_code = status_code
        self.content = content
        self.text = content.decode("utf-8", errors="replace")
        self.headers = headers or {}
        self._chunks = chunks or []

    def json(self) -> dict:
        return self._value

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def iter_content(self, chunk_size: int) -> list[bytes]:
        del chunk_size
        return self._chunks


class DingTalkEventTests(unittest.TestCase):
    def test_private_text_uses_staff_id_and_message_id(self) -> None:
        event = extract_dingtalk_event(
            {
                "conversationType": "1",
                "senderStaffId": "staff-1",
                "senderId": "fallback-id",
                "msgId": "message-1",
                "msgtype": "text",
                "text": {"content": "  hello  "},
            }
        )

        self.assertEqual(event.channel, "dingtalk")
        self.assertEqual(event.sender_id, "staff-1")
        self.assertEqual(event.message_id, "message-1")
        self.assertEqual(event.content, "hello")
        self.assertEqual(event.attachments, ())

    def test_picture_and_rich_text_are_normalized(self) -> None:
        picture = extract_dingtalk_event(
            {
                "conversationType": "1",
                "senderStaffId": "staff-1",
                "msgId": "picture-1",
                "msgtype": "picture",
                "content": {"downloadCode": "download-1"},
            }
        )
        rich_text = extract_dingtalk_event(
            {
                "conversationType": "1",
                "senderStaffId": "staff-1",
                "msgId": "rich-1",
                "msgtype": "richText",
                "content": {
                    "richText": [
                        {"text": "first"},
                        {"downloadCode": "download-2"},
                        {"text": "second"},
                    ]
                },
            }
        )

        self.assertEqual(picture.attachments[0]["download_code"], "download-1")
        self.assertEqual(rich_text.content, "first\nsecond")
        self.assertEqual(rich_text.attachments[0]["download_code"], "download-2")

    def test_group_chat_is_rejected(self) -> None:
        event = extract_dingtalk_event(
            {
                "conversationType": "2",
                "senderStaffId": "staff-1",
                "msgId": "message-1",
                "msgtype": "text",
                "text": {"content": "hello"},
            }
        )

        self.assertIsNone(event)

    def test_stream_handler_acks_after_enqueue(self) -> None:
        received: list[InboundMessage] = []
        handler = _DingTalkMessageHandler(received.append)
        callback = SimpleNamespace(
            data={
                "conversationType": "1",
                "senderStaffId": "staff-1",
                "msgId": "message-1",
                "msgtype": "text",
                "text": {"content": "hello"},
            }
        )

        status, message = asyncio.run(handler.process(callback))

        self.assertEqual((status, message), (200, "OK"))
        self.assertEqual([event.message_id for event in received], ["message-1"])

    def test_card_callback_becomes_a_command_message(self) -> None:
        event = extract_dingtalk_card_event(
            {
                "type": "actionCallback",
                "outTrackId": "card-1",
                "userId": "staff-1",
                "content": json.dumps(
                    {
                        "cardPrivateData": {
                            "actionIds": ["status"],
                            "params": {"command": "/status"},
                        }
                    }
                ),
            },
            "callback-1",
        )

        self.assertEqual(event.channel, "dingtalk")
        self.assertEqual(event.sender_id, "staff-1")
        self.assertEqual(event.message_id, "callback-1")
        self.assertEqual(event.content, "/status")

    def test_card_callback_rejects_non_command_content(self) -> None:
        event = extract_dingtalk_card_event(
            {
                "userId": "staff-1",
                "content": {"cardPrivateData": {"params": {"text": "hello"}}},
            }
        )

        self.assertIsNone(event)

    def test_card_callback_decodes_command_from_action_id(self) -> None:
        event = extract_dingtalk_card_event(
            {
                "userId": "staff-1",
                "content": {
                    "cardPrivateData": {
                        "actionIds": [_encode_dingtalk_card_action("/current")]
                    }
                },
            },
            "callback-2",
        )

        self.assertEqual(event.sender_id, "staff-1")
        self.assertEqual(event.message_id, "callback-2")
        self.assertEqual(event.content, "/current")

    def test_card_callback_finds_nested_action_id(self) -> None:
        event = extract_dingtalk_card_event(
            {
                "userId": "staff-1",
                "content": json.dumps(
                    {
                        "cardPrivateData": {
                            "actionIds": [
                                {
                                    "button": {
                                        "id": _encode_dingtalk_card_action(
                                            "/threads 2"
                                        )
                                    }
                                }
                            ]
                        }
                    }
                ),
            },
            "callback-3",
        )

        self.assertEqual(event.content, "/threads 2")

    def test_card_callback_ignores_button_label_and_decodes_params_id(self) -> None:
        action_id = _encode_dingtalk_card_action("/current")
        private_data = {
            "actionIds": [action_id],
            "params": {"id": action_id, "text": "当前任务"},
        }
        event = extract_dingtalk_card_event(
            {
                "userId": "staff-1",
                "content": json.dumps({"cardPrivateData": private_data}),
                "value": json.dumps({"cardPrivateData": private_data}),
            },
            "callback-4",
        )

        self.assertEqual(event.content, "/current")

    def test_callback_shape_reports_structure_without_values(self) -> None:
        shape = _dingtalk_card_callback_shape(
            {
                "userId": "sensitive-user-id",
                "content": json.dumps(
                    {"cardPrivateData": {"actionIds": ["sensitive-action"]}}
                ),
            }
        )

        self.assertIn("cardPrivateData", shape)
        self.assertIn("actionIds", shape)
        self.assertNotIn("sensitive-user-id", shape)
        self.assertNotIn("sensitive-action", shape)

    def test_card_callback_handler_acks_after_enqueue(self) -> None:
        received: list[InboundMessage] = []
        handler = _DingTalkCardCallbackHandler(received.append)
        callback = SimpleNamespace(
            headers=SimpleNamespace(message_id="callback-1"),
            data={
                "userId": "staff-1",
                "content": {
                    "cardPrivateData": {"params": {"text": "/current"}}
                },
            },
        )

        status, message = asyncio.run(handler.process(callback))

        self.assertEqual((status, message), (200, "OK"))
        self.assertEqual([event.content for event in received], ["/current"])

    def test_gateway_deduplicates_before_dispatch(self) -> None:
        gateway = DingTalkGatewayClient.__new__(DingTalkGatewayClient)
        gateway._seen = {}
        gateway._events = queue.Queue()
        gateway.stop_event = threading.Event()
        received: list[InboundMessage] = []
        gateway.on_message = received.append
        event = InboundMessage("dingtalk", "staff-1", "message-1", "hello")

        gateway._enqueue(event)
        gateway._enqueue(event)
        gateway.stop_event.set()
        gateway._dispatch_events()

        self.assertEqual([item.message_id for item in received], ["message-1"])


class DingTalkApiTests(unittest.TestCase):
    def test_access_token_is_cached(self) -> None:
        api = DingTalkApi(dingtalk_config())
        response = FakeResponse(
            {"accessToken": "token-1", "expireIn": 7200}
        )

        with patch("dingtalk_gateway.requests.post", return_value=response) as posted:
            self.assertEqual(api.access_token(), "token-1")
            self.assertEqual(api.access_token(), "token-1")

        posted.assert_called_once()

    def test_openapi_refreshes_token_once_after_401(self) -> None:
        api = DingTalkApi(dingtalk_config())
        responses = [
            FakeResponse({"accessToken": "token-1", "expireIn": 7200}),
            FakeResponse(status_code=401),
            FakeResponse({"accessToken": "token-2", "expireIn": 7200}),
            FakeResponse({"processQueryKey": "sent"}),
        ]

        with patch("dingtalk_gateway.requests.post", side_effect=responses) as posted:
            result = api._post_openapi("/v1.0/test", {"hello": "world"})

        self.assertEqual(result, {"processQueryKey": "sent"})
        self.assertEqual(posted.call_count, 4)
        self.assertEqual(
            posted.call_args_list[1].kwargs["headers"][
                "x-acs-dingtalk-access-token"
            ],
            "token-1",
        )
        self.assertEqual(
            posted.call_args_list[3].kwargs["headers"][
                "x-acs-dingtalk-access-token"
            ],
            "token-2",
        )

    def test_markdown_uses_active_single_chat_payload(self) -> None:
        api = DingTalkApi(dingtalk_config())
        api._post_openapi = Mock(return_value={})

        api.send_markdown("staff-1", "hello", title="Codex 回复")

        path, payload = api._post_openapi.call_args.args
        self.assertEqual(path, "/v1.0/robot/oToMessages/batchSend")
        self.assertEqual(payload["robotCode"], "robot-code")
        self.assertEqual(payload["userIds"], ["staff-1"])
        self.assertEqual(payload["msgKey"], "sampleMarkdown")
        self.assertEqual(
            json.loads(payload["msgParam"]),
            {"title": "Codex 回复", "text": "hello"},
        )

    def test_ai_card_uses_stream_callback_and_request_buttons(self) -> None:
        api = DingTalkApi(dingtalk_config())
        api._post_openapi = Mock(return_value={"success": True})

        card_id = api.send_ai_card(
            "staff-1",
            "Codex 回复",
            "**任务完成**",
            [("查看状态", "/status", 1), ("取消任务", "/cancel", 2)],
        )

        self.assertTrue(card_id.startswith("codex-"))
        path, payload = api._post_openapi.call_args.args
        self.assertEqual(path, "/v1.0/card/instances/createAndDeliver")
        self.assertEqual(payload["cardTemplateId"], DINGTALK_AI_CARD_TEMPLATE_ID)
        self.assertEqual(payload["callbackType"], "STREAM")
        self.assertEqual(payload["openSpaceId"], "dtv1.card//IM_ROBOT.staff-1")
        card_data = payload["cardData"]["cardParamMap"]
        self.assertEqual(card_data["msgContent"], "")
        self.assertEqual(card_data["staticMsgContent"], "**任务完成**")
        system_data = json.loads(card_data["sys_full_json_obj"])
        self.assertEqual(
            system_data["order"],
            ["msgTitle", "staticMsgContent", "msgButtons"],
        )
        self.assertEqual(
            system_data["msgButtons"],
            [
                {
                    "text": "查看状态",
                    "color": "blue",
                    "id": _encode_dingtalk_card_action("/status"),
                    "request": True,
                    "params": {"command": "/status", "text": "/status"},
                },
                {
                    "text": "取消任务",
                    "color": "red",
                    "id": _encode_dingtalk_card_action("/cancel"),
                    "request": True,
                    "params": {"command": "/cancel", "text": "/cancel"},
                },
            ],
        )

    def test_download_image_requires_https_and_enforces_size(self) -> None:
        api = DingTalkApi(dingtalk_config())
        with tempfile.TemporaryDirectory() as raw_dir:
            target = Path(raw_dir) / "image.png"
            api._post_openapi = Mock(
                return_value={"downloadUrl": "http://example.com/image.png"}
            )
            with self.assertRaisesRegex(ValueError, "地址无效"):
                api.download_image("download-1", target, 10)

            api._post_openapi = Mock(
                return_value={"downloadUrl": "https://example.com/image.png"}
            )
            response = FakeResponse(headers={"Content-Length": "11"})
            with patch("dingtalk_gateway.requests.get", return_value=response):
                with self.assertRaisesRegex(ValueError, "超过大小限制"):
                    api.download_image("download-1", target, 10)

    def test_download_image_writes_streamed_content(self) -> None:
        api = DingTalkApi(dingtalk_config())
        api._post_openapi = Mock(
            return_value={"downloadUrl": "https://example.com/image.png"}
        )
        response = FakeResponse(
            headers={"Content-Length": "6"},
            chunks=[b"abc", b"def"],
        )

        with tempfile.TemporaryDirectory() as raw_dir, patch(
            "dingtalk_gateway.requests.get", return_value=response
        ):
            target = Path(raw_dir) / "image.png"
            result = api.download_image("download-1", target, 10)

            self.assertEqual(result.read_bytes(), b"abcdef")


class DingTalkChannelTests(unittest.TestCase):
    def test_channel_prefers_ai_card(self) -> None:
        channel = DingTalkChannel.__new__(DingTalkChannel)
        channel.api = Mock()
        actions = action_rows([[("查看状态", "/status", 0)]])

        channel.send_text("staff-1", "**任务完成**", actions=actions)

        channel.api.send_ai_card.assert_called_once_with(
            "staff-1",
            "Codex 回复",
            "**任务完成**",
            [("查看状态", "/status", 0)],
        )
        channel.api.send_markdown.assert_not_called()
        channel.api.send_text.assert_not_called()

    def test_markdown_has_heading_sequence_and_distinct_actions(self) -> None:
        actions = action_rows(
            [
                [("查看状态", "/status", 0), ("当前任务", "/current", 0)],
                [("重复状态", "/status", 0)],
            ]
        )

        title, rendered = DingTalkChannel._render_markdown(
            "**任务完成**\n\n结果如下。",
            actions,
            sequence=2,
        )

        self.assertEqual(title, "Codex 回复 · 2")
        self.assertTrue(rendered.startswith("#### Codex 回复 · 2\n\n**任务完成**"))
        self.assertIn("> **快捷操作**", rendered)
        self.assertIn("> **查看状态**　/status", rendered)
        self.assertIn("> **当前任务**　/current", rendered)
        self.assertEqual(rendered.count("/status"), 1)

    def test_card_markdown_preserves_plain_line_breaks_but_not_code_fences(self) -> None:
        rendered = DingTalkChannel._card_markdown(
            "运行状态\n当前任务：测试\n\n```text\nfirst\nsecond\n```"
        )

        self.assertEqual(
            rendered,
            "运行状态  \n当前任务：测试\n\n```text\nfirst\nsecond\n```",
        )

    def test_markdown_failure_falls_back_to_clean_text(self) -> None:
        channel = DingTalkChannel.__new__(DingTalkChannel)
        channel.api = Mock()
        channel.api.send_ai_card.side_effect = RuntimeError("card unavailable")
        channel.api.send_markdown.side_effect = RuntimeError("markdown unavailable")
        actions = action_rows([[("查看状态", "/status", 0)]])

        with patch("dingtalk_gateway.log_event"):
            channel.send_text("staff-1", "任务完成。", actions=actions)

        channel.api.send_ai_card.assert_called_once_with(
            "staff-1",
            "Codex 回复",
            "任务完成。",
            [("查看状态", "/status", 0)],
        )
        channel.api.send_markdown.assert_called_once_with(
            "staff-1",
            "#### Codex 回复\n\n任务完成。\n\n> **快捷操作**\n> **查看状态**　/status",
            title="Codex 回复",
        )
        channel.api.send_text.assert_called_once_with(
            "staff-1",
            "任务完成。\n\n快捷操作：查看状态：/status",
        )


class DingTalkGatewayRegistrationTests(unittest.TestCase):
    def test_gateway_registers_chat_and_card_callbacks(self) -> None:
        stream_client = Mock()

        with patch("dingtalk_gateway.dingtalk_stream.Credential"), patch(
            "dingtalk_gateway.dingtalk_stream.DingTalkStreamClient",
            return_value=stream_client,
        ):
            DingTalkGatewayClient(
                dingtalk_config(),
                on_message=Mock(),
                on_ready=Mock(),
                on_state=Mock(),
            )

        topics = [
            call.args[0]
            for call in stream_client.register_callback_handler.call_args_list
        ]
        self.assertIn(DINGTALK_CARD_CALLBACK_TOPIC, topics)


class ChannelFactoryTests(unittest.TestCase):
    def test_only_configured_channel_is_constructed(self) -> None:
        qq_builder = Mock()
        dingtalk_builder = Mock(
            return_value=SimpleNamespace(name="dingtalk")
        )
        callbacks = (Mock(), Mock(), Mock())

        with patch.dict(
            channel_factory.CHANNEL_TYPES,
            {"qq": qq_builder, "dingtalk": dingtalk_builder},
            clear=True,
        ):
            registry = build_channel_registry(dingtalk_config(), *callbacks)

        self.assertEqual(registry.names(), ("dingtalk",))
        qq_builder.assert_not_called()
        dingtalk_builder.assert_called_once_with(dingtalk_config(), *callbacks)


if __name__ == "__main__":
    unittest.main()
