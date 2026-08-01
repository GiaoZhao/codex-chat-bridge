from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from bridge import BridgeService
from bridge_core import (
    CodexRunner,
    ThreadInfo,
    parse_command,
    qq_safe_final,
    resolve_command,
)
from qq_gateway import QQGatewayClient


THREAD_ID = "00000000-0000-4000-8000-000000000001"


class CommandParsingRegressionTests(unittest.TestCase):
    def test_windows_command_preserves_backslashes(self) -> None:
        command = r"C:\Tools\Codex\codex.CMD"

        self.assertEqual(parse_command(command, windows=True), (command,))

    def test_windows_command_parses_quoted_path_with_spaces(self) -> None:
        command = r'"C:\Program Files\Codex\codex.exe" --json'

        self.assertEqual(
            parse_command(command, windows=True),
            (r"C:\Program Files\Codex\codex.exe", "--json"),
        )

    def test_command_uses_executable_resolved_from_path(self) -> None:
        resolved = r"C:\Users\alice\AppData\Roaming\npm\codex.CMD"

        with patch("bridge_core.shutil.which", return_value=resolved):
            command = resolve_command(("codex", "--flag"))

        self.assertEqual(command, (resolved, "--flag"))

    def test_windows_command_expands_environment_variable_in_executable(self) -> None:
        with patch.dict(os.environ, {"APPDATA": r"C:\Users\alice\AppData\Roaming"}):
            command = parse_command(r"%APPDATA%\npm\codex.CMD --flag", windows=True)

        self.assertEqual(
            command,
            (r"C:\Users\alice\AppData\Roaming\npm\codex.CMD", "--flag"),
        )


class RunnerRegressionTests(unittest.TestCase):
    def test_execute_normalizes_none_stdout_and_stderr(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            process = Mock()
            process.communicate.return_value = (None, None)
            process.returncode = 0
            runner = CodexRunner(SimpleNamespace(codex_turn_timeout=60))

            with patch("bridge_core.subprocess.Popen", return_value=process):
                result = runner._execute(["codex", "--version"], root)

        self.assertEqual(result, (0, "", ""))
        process.communicate.assert_called_once_with(timeout=60)
        self.assertIsNone(runner._process)

    def test_execute_passes_prompt_via_stdin(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            process = Mock()
            process.communicate.return_value = ("", "")
            process.returncode = 0
            runner = CodexRunner(SimpleNamespace(codex_turn_timeout=60))
            prompt = "review & preserve | these shell characters"

            with patch("bridge_core.subprocess.Popen", return_value=process) as popen:
                result = runner._execute(["codex.CMD", "exec", "-"], root, prompt)

        self.assertEqual(result, (0, "", ""))
        self.assertEqual(popen.call_args.kwargs["stdin"], subprocess.PIPE)
        process.communicate.assert_called_once_with(input=prompt, timeout=60)


class BridgeServiceRegressionTests(unittest.TestCase):
    def _service(self, root: Path, notify_on_ready: bool) -> BridgeService:
        config = SimpleNamespace(
            base_dir=root,
            qq_allowed_openid="openid",
            qq_bind_code="",
            codex_state_db=root / "state.sqlite",
            codex_desktop_refresh=False,
            codex_desktop_cdp_url="http://127.0.0.1:9229",
            codex_desktop_refresh_delay_ms=0,
            qq_notify_on_ready=notify_on_ready,
        )
        active = ThreadInfo(THREAD_ID, "current task", root)
        with patch("bridge.QQApi"), patch("bridge.ThreadIndex"), patch(
            "bridge.ActiveThread"
        ) as active_thread, patch("bridge.CodexRunner"), patch(
            "bridge.CodexDesktopRefresher"
        ), patch("bridge.SessionMonitor"), patch("bridge.QQGatewayClient"):
            active_thread.return_value.snapshot.return_value = active
            service = BridgeService(config)
        service._safe_send = Mock()
        return service

    def test_gateway_ready_notification_is_disabled_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            service = self._service(Path(raw_dir), notify_on_ready=False)

            service._on_gateway_ready()

        service._safe_send.assert_not_called()

    def test_enabled_gateway_ready_notification_is_sent_only_once(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            service = self._service(Path(raw_dir), notify_on_ready=True)

            service._on_gateway_ready()
            service._on_gateway_ready()

        service._safe_send.assert_called_once()

    def test_refresh_desktop_contains_arbitrary_runtime_exception(self) -> None:
        service = BridgeService.__new__(BridgeService)
        service.config = SimpleNamespace(codex_desktop_refresh=True)
        service.desktop_refresher = Mock()
        service.desktop_refresher.refresh.side_effect = RuntimeError("unexpected")

        with patch("bridge.log_event"):
            self.assertIsNone(service._refresh_desktop(THREAD_ID))


class RedactionRegressionTests(unittest.TestCase):
    def test_windows_local_paths_are_redacted(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            drive_path = r"C:\Users\alice\AppData\Local\Codex\debug.log"
            unc_path = r"\\server\share\private\result.txt"
            quoted_path = r"'C:\Program Files\Codex\debug.log'"
            spaced_drive_path = r"C:\Users\Alice Smith\Codex\debug.log"
            spaced_unc_path = r"\\server\Private Share\Codex\result.txt"

            text, images = qq_safe_final(
                "\n".join(
                    [
                        drive_path,
                        unc_path,
                        quoted_path,
                        spaced_drive_path,
                        spaced_unc_path,
                    ]
                ),
                Path(raw_dir),
            )

        self.assertEqual(images, [])
        self.assertNotIn(drive_path, text)
        self.assertNotIn(unc_path, text)
        self.assertNotIn("Program Files", text)
        self.assertNotIn("Alice Smith", text)
        self.assertNotIn("Private Share", text)
        self.assertEqual(text.count("[本地路径已隐藏]"), 5)


class GatewayPlatformRegressionTests(unittest.TestCase):
    def test_identify_uses_runtime_platform_name(self) -> None:
        client = QQGatewayClient.__new__(QQGatewayClient)
        client.api = SimpleNamespace(access_token=Mock(return_value="token"))
        client.config = SimpleNamespace(qq_intents=1)
        client._send = Mock()

        with patch("qq_gateway.PLATFORM_NAME", "test-platform"):
            client._identify()

        payload = client._send.call_args.args[0]
        self.assertEqual(payload["d"]["properties"]["$os"], "test-platform")


if __name__ == "__main__":
    unittest.main()
