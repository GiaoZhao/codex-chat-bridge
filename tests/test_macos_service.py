import importlib.util
import plistlib
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest import mock


ROOT = Path(__file__).resolve().parent.parent
MODULE_PATH = ROOT / "scripts" / "macos_service.py"
SPEC = importlib.util.spec_from_file_location("macos_service", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
macos_service = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(macos_service)


class MacOSServiceTests(unittest.TestCase):
    def test_runtime_source_files_include_channel_adapters(self) -> None:
        self.assertTrue(
            {
                "channel_factory.py",
                "chat_channel.py",
                "dingtalk_gateway.py",
                "qq_channel.py",
            }.issubset(macos_service.RUNTIME_SOURCE_FILES)
        )

    def test_plist_runs_bridge_from_runtime_directory(self) -> None:
        runtime = Path("/tmp/CodexQQBridge")
        python_bin = Path("/usr/bin/python3")
        codex_bin = Path("/usr/local/bin/codex")

        payload = macos_service.build_plist(runtime, python_bin, codex_bin)

        self.assertEqual(payload["Label"], macos_service.LABEL)
        self.assertEqual(
            payload["ProgramArguments"],
            [
                str(python_bin.resolve()),
                str(runtime.resolve() / "macos_service.py"),
                "_launch",
            ],
        )
        self.assertEqual(payload["WorkingDirectory"], str(runtime.resolve()))
        self.assertEqual(
            payload["EnvironmentVariables"]["PYTHONPATH"],
            str(runtime.resolve() / "vendor"),
        )
        self.assertEqual(payload["EnvironmentVariables"]["CODEX_TRANSPORT"], "exec")
        self.assertEqual(
            payload["EnvironmentVariables"]["CODEX_COMMAND"],
            str(codex_bin),
        )
        self.assertTrue(payload["RunAtLoad"])
        self.assertTrue(payload["KeepAlive"])

    def test_write_plist_is_valid_and_private(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "bridge.plist"
            payload = macos_service.build_plist(
                Path(raw_dir),
                Path("/usr/bin/python3"),
                Path("/usr/local/bin/codex"),
            )

            macos_service.write_plist(path, payload)

            with path.open("rb") as handle:
                self.assertEqual(plistlib.load(handle), payload)
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)

    def test_plist_can_opt_into_auto_without_daemon_environment(self) -> None:
        payload = macos_service.build_plist(
            Path("/tmp/CodexQQBridge"),
            Path("/usr/bin/python3"),
            Path("/usr/local/bin/codex"),
            "auto",
        )

        environment = payload["EnvironmentVariables"]
        self.assertEqual(environment["CODEX_TRANSPORT"], "auto")
        self.assertNotIn("CODEX_APP_SERVER_USE_LOCAL_DAEMON", environment)

    def test_plist_rejects_unknown_transport(self) -> None:
        with self.assertRaisesRegex(ValueError, "unsupported Codex transport"):
            macos_service.build_plist(
                Path("/tmp/CodexQQBridge"),
                Path("/usr/bin/python3"),
                Path("/usr/local/bin/codex"),
                "unknown",
            )

    def test_deploy_runtime_copies_secrets_and_migrates_data_once(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            source = root / "source"
            runtime = root / "runtime"
            dependencies = root / "dependencies"
            source.mkdir()
            (source / "scripts").mkdir()
            dependencies.mkdir()
            for name in macos_service.RUNTIME_SOURCE_FILES:
                (source / name).write_text(f"# {name}\n", encoding="utf-8")
            (source / "scripts" / "macos_service.py").write_text(
                "# macos_service.py\n",
                encoding="utf-8",
            )
            (source / ".env").write_text("QQ_APP_SECRET=secret\n", encoding="utf-8")
            (dependencies / "dependency.py").write_text("VALUE = 1\n", encoding="utf-8")
            source_data = source / "data"
            source_data.mkdir()
            (source_data / "state.json").write_text('{"active":"source"}', encoding="utf-8")
            with sqlite3.connect(source_data / "bridge.sqlite3") as connection:
                connection.execute("CREATE TABLE sample (value TEXT)")
                connection.execute("INSERT INTO sample VALUES ('source')")

            macos_service.deploy_runtime(source, runtime, dependencies)

            self.assertEqual((runtime / ".env").stat().st_mode & 0o777, 0o600)
            self.assertEqual(
                (runtime / "vendor" / "dependency.py").read_text(encoding="utf-8"),
                "VALUE = 1\n",
            )
            self.assertEqual(
                (runtime / "macos_service.py").read_text(encoding="utf-8"),
                "# macos_service.py\n",
            )
            with sqlite3.connect(runtime / "data" / "bridge.sqlite3") as connection:
                self.assertEqual(connection.execute("SELECT value FROM sample").fetchone()[0], "source")

            (runtime / "data" / "state.json").write_text('{"active":"runtime"}', encoding="utf-8")
            (source / ".env").write_text("QQ_APP_SECRET=updated\n", encoding="utf-8")
            macos_service.deploy_runtime(source, runtime, dependencies)

            self.assertEqual(
                (runtime / "data" / "state.json").read_text(encoding="utf-8"),
                '{"active":"runtime"}',
            )
            self.assertEqual(
                (runtime / ".env").read_text(encoding="utf-8"),
                "QQ_APP_SECRET=updated\n",
            )

    def test_launch_runtime_preserves_configured_transport(self) -> None:
        runtime = Path("/tmp/CodexQQBridge")
        python_bin = Path("/usr/bin/python3")
        with mock.patch.dict(
            macos_service.os.environ,
            {"CODEX_TRANSPORT": "exec"},
            clear=False,
        ):
            with mock.patch.object(macos_service.os, "execv") as execv_mock:
                macos_service.launch_runtime(runtime, python_bin)

            self.assertEqual(macos_service.os.environ["CODEX_TRANSPORT"], "exec")
            execv_mock.assert_called_once_with(
                "/usr/bin/caffeinate",
                [
                    "/usr/bin/caffeinate",
                    "-i",
                    str(python_bin),
                    str(runtime / "bridge.py"),
                ],
            )

    def test_launch_runtime_rejects_unknown_transport(self) -> None:
        with mock.patch.dict(
            macos_service.os.environ,
            {"CODEX_TRANSPORT": "unknown"},
            clear=False,
        ):
            with mock.patch.object(macos_service.os, "execv") as execv_mock:
                with self.assertRaisesRegex(RuntimeError, "unsupported Codex transport"):
                    macos_service.launch_runtime(
                        Path("/tmp/CodexQQBridge"),
                        Path("/usr/bin/python3"),
                    )

        execv_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
