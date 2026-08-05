from __future__ import annotations

import json
import sqlite3
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from bridge_core import (
    ActiveThread,
    CodexDesktopRefresher,
    CodexRunner,
    SessionMonitor,
    StateStore,
    ThreadIndex,
    ThreadInfo,
    extract_final_from_codex_jsonl,
    extract_thread_id_from_codex_jsonl,
    final_message_from_record,
    find_session_file,
    latest_complete_turn,
    latest_final_message,
    qq_safe_final,
    session_busy,
    split_message,
    summarize_codex_error,
)
from bridge import BridgeService
from chat_channel import ChannelLimits, ChannelRegistry, Recipient
from qq_gateway import QQApi, clean_message_content, download_c2c_images, extract_c2c_event


class SessionTests(unittest.TestCase):
    def test_final_message_record(self) -> None:
        record = {
            "type": "event_msg",
            "payload": {
                "type": "agent_message",
                "phase": "final_answer",
                "message": "done",
            },
        }
        self.assertEqual(final_message_from_record(record), "done")

    def test_commentary_is_not_final(self) -> None:
        record = {
            "type": "event_msg",
            "payload": {
                "type": "agent_message",
                "phase": "commentary",
                "message": "working",
            },
        }
        self.assertIsNone(final_message_from_record(record))

    def test_session_state_and_discovery(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            path = root / "2026" / "07" / "31" / "rollout-now-abc.jsonl"
            path.parent.mkdir(parents=True)
            rows = [
                {"type": "event_msg", "payload": {"type": "task_started"}},
                {
                    "type": "event_msg",
                    "payload": {
                        "type": "agent_message",
                        "phase": "final_answer",
                        "message": "result",
                    },
                },
                {"type": "event_msg", "payload": {"type": "task_complete"}},
            ]
            path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
            self.assertEqual(find_session_file(root, "abc"), path)
            self.assertFalse(session_busy(path))
            self.assertEqual(latest_final_message(path), "result")

    def test_latest_complete_turn_ignores_incomplete_and_non_final_messages(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "session.jsonl"
            rows = [
                {
                    "type": "event_msg",
                    "payload": {"type": "user_message", "message": "first question"},
                },
                {
                    "type": "event_msg",
                    "payload": {
                        "type": "agent_message",
                        "phase": "commentary",
                        "message": "working",
                    },
                },
                {
                    "type": "event_msg",
                    "payload": {
                        "type": "agent_message",
                        "phase": "final_answer",
                        "message": "first answer",
                    },
                },
                {
                    "type": "event_msg",
                    "payload": {"type": "user_message", "message": "unfinished question"},
                },
                {
                    "type": "response_item",
                    "payload": {
                        "type": "message",
                        "role": "user",
                        "content": [{"type": "input_text", "text": "injected context"}],
                    },
                },
            ]
            path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
            self.assertEqual(latest_complete_turn(path), ("first question", "first answer"))

            with path.open("a", encoding="utf-8") as handle:
                handle.write(
                    json.dumps(
                        {
                            "type": "event_msg",
                            "payload": {
                                "type": "agent_message",
                                "phase": "final_answer",
                                "message": "finished answer",
                            },
                        }
                    )
                    + "\n"
                )
            self.assertEqual(
                latest_complete_turn(path),
                ("unfinished question", "finished answer"),
            )

    def test_codex_jsonl_final(self) -> None:
        output = "\n".join(
            [
                json.dumps({"type": "thread.started"}),
                json.dumps(
                    {
                        "type": "item.completed",
                        "item": {"type": "agent_message", "text": "final"},
                    }
                ),
            ]
        )
        self.assertEqual(extract_final_from_codex_jsonl(output), "final")

    def test_codex_jsonl_thread_id(self) -> None:
        thread_id = "00000000-0000-4000-8000-000000000001"
        output = "\n".join(
            [
                "not-json",
                json.dumps({"type": "thread.started", "thread_id": thread_id}),
            ]
        )
        self.assertEqual(extract_thread_id_from_codex_jsonl(output), thread_id)

    def test_monitor_skips_history_and_reads_new_final(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            thread_id = "00000000-0000-4000-8000-000000000001"
            path = root / f"rollout-now-{thread_id}.jsonl"
            historical = {
                "type": "event_msg",
                "payload": {
                    "type": "agent_message",
                    "phase": "final_answer",
                    "message": "historical",
                },
            }
            path.write_text(json.dumps(historical) + "\n", encoding="utf-8")
            received = []
            config = SimpleNamespace(codex_sessions_dir=root, codex_thread_id=thread_id)
            monitor = SessionMonitor(
                config,
                StateStore(root / "state.json"),
                lambda info, message: received.append((info.thread_id, message)),
                poll_seconds=0.02,
            )
            monitor.start()
            time.sleep(0.08)
            self.assertEqual(received, [])
            appended = {
                "type": "event_msg",
                "payload": {
                    "type": "agent_message",
                    "phase": "final_answer",
                    "message": "new result",
                },
            }
            with path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(appended) + "\n")
            deadline = time.time() + 2
            while not received and time.time() < deadline:
                time.sleep(0.02)
            monitor.stop()
            self.assertEqual(received, [(thread_id, "new result")])

    def test_monitor_reads_new_finals_from_all_visible_threads(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            first_id = "00000000-0000-4000-8000-000000000001"
            second_id = "00000000-0000-4000-8000-000000000002"
            first_path = root / f"rollout-now-{first_id}.jsonl"
            second_path = root / f"rollout-now-{second_id}.jsonl"

            def final(message: str) -> dict:
                return {
                    "type": "event_msg",
                    "payload": {
                        "type": "agent_message",
                        "phase": "final_answer",
                        "message": message,
                    },
                }

            first_path.write_text(json.dumps(final("first history")) + "\n", encoding="utf-8")
            second_path.write_text(json.dumps(final("second history")) + "\n", encoding="utf-8")
            received = []
            config = SimpleNamespace(codex_sessions_dir=root, codex_thread_id=first_id)
            infos = [
                ThreadInfo(first_id, "first", root, rollout_path=first_path),
                ThreadInfo(second_id, "second", root, rollout_path=second_path),
            ]
            monitor = SessionMonitor(
                config,
                StateStore(root / "state.json"),
                lambda info, message: received.append((info.thread_id, message)),
                poll_seconds=0.02,
                threads_provider=lambda: infos,
            )
            monitor.start()
            time.sleep(0.08)
            with first_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(final("same answer")) + "\n")
            with second_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(final("same answer")) + "\n")
            deadline = time.time() + 2
            while len(received) < 2 and time.time() < deadline:
                time.sleep(0.02)
            monitor.stop()
            self.assertCountEqual(
                received,
                [(first_id, "same answer"), (second_id, "same answer")],
            )

    def test_monitor_reads_first_final_from_thread_created_after_start(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            first_id = "00000000-0000-4000-8000-000000000001"
            new_id = "00000000-0000-4000-8000-000000000002"
            first_path = root / f"rollout-now-{first_id}.jsonl"
            first_path.write_text("", encoding="utf-8")
            infos = [ThreadInfo(first_id, "first", root, rollout_path=first_path)]
            received = []
            config = SimpleNamespace(codex_sessions_dir=root, codex_thread_id=first_id)
            monitor = SessionMonitor(
                config,
                StateStore(root / "state.json"),
                lambda info, message: received.append((info.thread_id, message)),
                poll_seconds=0.02,
                threads_provider=lambda: list(infos),
            )
            monitor.start()
            time.sleep(0.08)
            new_path = root / f"rollout-now-{new_id}.jsonl"
            new_path.write_text(
                json.dumps(
                    {
                        "type": "event_msg",
                        "payload": {
                            "type": "agent_message",
                            "phase": "final_answer",
                            "message": "first result",
                        },
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            infos.append(ThreadInfo(new_id, "new", root, rollout_path=new_path))
            deadline = time.time() + 2
            while not received and time.time() < deadline:
                time.sleep(0.02)
            monitor.stop()
            self.assertEqual(received, [(new_id, "first result")])

    def test_monitor_waits_for_complete_jsonl_line(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            thread_id = "00000000-0000-4000-8000-000000000001"
            path = root / f"rollout-now-{thread_id}.jsonl"
            path.write_bytes(b"")
            received = []
            config = SimpleNamespace(codex_sessions_dir=root, codex_thread_id=thread_id)
            monitor = SessionMonitor(
                config,
                StateStore(root / "state.json"),
                lambda info, message: received.append(message),
                poll_seconds=0.02,
            )
            monitor.start()
            time.sleep(0.08)
            record = json.dumps(
                {
                    "type": "event_msg",
                    "payload": {
                        "type": "agent_message",
                        "phase": "final_answer",
                        "message": "complete later",
                    },
                }
            ).encode("utf-8")
            with path.open("ab") as handle:
                handle.write(record)
            time.sleep(0.08)
            self.assertEqual(received, [])
            with path.open("ab") as handle:
                handle.write(b"\n")
            deadline = time.time() + 2
            while not received and time.time() < deadline:
                time.sleep(0.02)
            monitor.stop()
            self.assertEqual(received, ["complete later"])

    def test_monitor_startup_baseline_keeps_an_incomplete_jsonl_line(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            thread_id = "00000000-0000-4000-8000-000000000001"
            path = root / f"rollout-now-{thread_id}.jsonl"
            record = json.dumps(
                {
                    "type": "event_msg",
                    "payload": {
                        "type": "agent_message",
                        "phase": "final_answer",
                        "message": "finished after startup",
                    },
                }
            ).encode("utf-8")
            cut = len(record) // 2
            path.write_bytes(b"{}\n" + record[:cut])
            received = []
            config = SimpleNamespace(codex_sessions_dir=root, codex_thread_id=thread_id)
            monitor = SessionMonitor(
                config,
                StateStore(root / "state.json"),
                lambda _info, message: received.append(message),
                poll_seconds=0.02,
            )
            monitor.start()
            time.sleep(0.08)
            self.assertEqual(received, [])

            with path.open("ab") as handle:
                handle.write(record[cut:] + b"\n")
            deadline = time.time() + 2
            while not received and time.time() < deadline:
                time.sleep(0.02)
            monitor.stop()

            self.assertEqual(received, ["finished after startup"])

    def test_monitor_skips_offline_history_after_restart_and_reads_new_final(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            thread_id = "00000000-0000-4000-8000-000000000001"
            path = root / f"rollout-now-{thread_id}.jsonl"
            path.write_text("{}\n", encoding="utf-8")
            state = StateStore(root / "state.json")
            config = SimpleNamespace(codex_sessions_dir=root, codex_thread_id=thread_id)
            first = SessionMonitor(config, state, lambda _info, _message: None, poll_seconds=0.02)
            first.start()
            time.sleep(0.08)
            first.stop()
            with path.open("a", encoding="utf-8") as handle:
                handle.write(
                    json.dumps(
                        {
                            "type": "event_msg",
                            "payload": {
                                "type": "agent_message",
                                "phase": "final_answer",
                                "message": "while offline",
                            },
                        }
                    )
                    + "\n"
                )
            received = []
            second = SessionMonitor(
                config,
                state,
                lambda _info, message: received.append(message),
                poll_seconds=0.02,
            )
            second.start()
            time.sleep(0.08)
            self.assertEqual(received, [])
            with path.open("a", encoding="utf-8") as handle:
                handle.write(
                    json.dumps(
                        {
                            "type": "event_msg",
                            "payload": {
                                "type": "agent_message",
                                "phase": "final_answer",
                                "message": "after restart",
                            },
                        }
                    )
                    + "\n"
                )
            deadline = time.time() + 2
            while not received and time.time() < deadline:
                time.sleep(0.02)
            second.stop()
            self.assertEqual(received, ["after restart"])

    def test_final_event_includes_the_user_message_from_the_same_turn(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            thread_id = "00000000-0000-4000-8000-000000000001"
            path = root / f"rollout-now-{thread_id}.jsonl"
            user_record = {
                "type": "event_msg",
                "payload": {"type": "user_message", "message": "current question"},
            }
            path.write_text(json.dumps(user_record) + "\n", encoding="utf-8")
            final_offset = path.stat().st_size
            final_record = {
                "type": "event_msg",
                "payload": {
                    "type": "agent_message",
                    "phase": "final_answer",
                    "message": "current answer",
                },
            }
            callback = Mock()
            config = SimpleNamespace(codex_sessions_dir=root, codex_thread_id=thread_id)
            monitor = SessionMonitor(
                config,
                StateStore(root / "state.json"),
                Mock(),
                on_final_event=callback,
            )
            info = ThreadInfo(thread_id, "task", root, rollout_path=path)

            monitor._handle_record(info, final_record, path, final_offset)

            self.assertEqual(callback.call_args.args[1], "current answer")
            self.assertEqual(callback.call_args.args[3], "current question")


class ThreadIndexTests(unittest.TestCase):
    def _create_index(self, root: Path) -> ThreadIndex:
        database = root / "state.sqlite"
        connection = sqlite3.connect(database)
        connection.execute(
            """
            CREATE TABLE threads (
                id TEXT PRIMARY KEY,
                rollout_path TEXT NOT NULL,
                cwd TEXT NOT NULL,
                title TEXT NOT NULL,
                first_user_message TEXT NOT NULL,
                preview TEXT NOT NULL,
                has_user_event INTEGER NOT NULL,
                archived INTEGER NOT NULL,
                source TEXT NOT NULL,
                thread_source TEXT,
                updated_at INTEGER NOT NULL,
                updated_at_ms INTEGER
            )
            """
        )
        rows = [
            (
                "00000000-0000-4000-8000-000000000001",
                str(root / "rollout-older.jsonl"),
                str(root),
                "older task",
                "older",
                "visible",
                1,
                0,
                "vscode",
                "user",
                10,
                10000,
            ),
            (
                "00000000-0000-4000-8000-000000000002",
                str(root / "rollout-newer.jsonl"),
                str(root),
                "newer task",
                "newer",
                "visible",
                0,
                0,
                "exec",
                "user",
                20,
                20000,
            ),
            (
                "00000000-0000-4000-8000-000000000003",
                str(root / "rollout-subagent.jsonl"),
                str(root),
                "subagent",
                "subagent",
                "visible",
                1,
                0,
                '{"subagent":{"other":"guardian"}}',
                "subagent",
                30,
                30000,
            ),
            (
                "00000000-0000-4000-8000-000000000004",
                str(root / "rollout-archived.jsonl"),
                str(root),
                "archived",
                "archived",
                "visible",
                1,
                1,
                "vscode",
                "user",
                40,
                40000,
            ),
        ]
        connection.executemany(
            "INSERT INTO threads VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
        connection.commit()
        connection.close()
        return ThreadIndex(database)

    def test_lists_only_visible_root_threads_and_resolves_number(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            index = self._create_index(root)
            threads, total = index.list_page(page=1, page_size=8)
            self.assertEqual(total, 2)
            self.assertEqual([item.title for item in threads], ["newer task", "older task"])
            self.assertEqual(index.resolve("2").title, "older task")
            self.assertIsNone(index.resolve("999"))
            monitorable = index.list_monitorable()
            self.assertEqual(len(monitorable), 2)
            self.assertTrue(all(item.rollout_path is not None for item in monitorable))

    def test_active_thread_persists_switch(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            index = self._create_index(root)
            state = StateStore(root / "state.json")
            config = SimpleNamespace(
                codex_thread_id="00000000-0000-4000-8000-000000000001",
                codex_workdir=root,
            )
            active = ActiveThread(config, state, index)
            selected = index.resolve("1")
            self.assertIsNotNone(selected)
            active.switch(selected)
            stored = state.load()
            self.assertEqual(stored["active_thread_id"], selected.thread_id)
            self.assertEqual(Path(stored["active_workdir"]), root.resolve())

    def test_active_thread_switch_if_current_is_atomic(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            index = self._create_index(root)
            state = StateStore(root / "state.json")
            original_id = "00000000-0000-4000-8000-000000000001"
            config = SimpleNamespace(
                codex_thread_id=original_id,
                codex_workdir=root,
            )
            active = ActiveThread(config, state, index)
            replacement = ThreadInfo(
                "00000000-0000-4000-8000-000000000003",
                "created task",
                root,
            )

            self.assertFalse(
                active.switch_if_current(
                    "00000000-0000-4000-8000-000000000002",
                    replacement,
                )
            )
            self.assertEqual(active.snapshot().thread_id, original_id)
            self.assertTrue(active.switch_if_current(original_id, replacement))
            self.assertEqual(active.snapshot(), replacement)
            self.assertEqual(state.load()["active_thread_id"], replacement.thread_id)


class RunnerTests(unittest.TestCase):
    def test_new_thread_command_and_parsing(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            thread_id = "00000000-0000-4000-8000-000000000001"
            config = SimpleNamespace(
                codex_command=("codex",),
                codex_thread_id=thread_id,
                codex_workdir=root,
                codex_turn_timeout=60,
            )
            output = "\n".join(
                [
                    json.dumps({"type": "thread.started", "thread_id": thread_id}),
                    json.dumps(
                        {
                            "type": "item.completed",
                            "item": {"type": "agent_message", "text": "created"},
                        }
                    ),
                ]
            )
            runner = CodexRunner(config)
            with patch.object(runner, "_execute", return_value=(0, output, "")) as execute:
                code, created_id, final, error = runner.run_new("first prompt", root)
            self.assertEqual((code, created_id, final, error), (0, thread_id, "created", ""))
            self.assertEqual(
                execute.call_args.args[0],
                ["codex", "exec", "--skip-git-repo-check", "--json", "-"],
            )
            self.assertEqual(execute.call_args.args[1], root)
            self.assertEqual(execute.call_args.args[2], "first prompt")

    def test_resume_passes_each_image_to_codex(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            first = root / "one.png"
            second = root / "two.jpg"
            config = SimpleNamespace(
                codex_command=("codex",),
                codex_thread_id="00000000-0000-4000-8000-000000000001",
                codex_workdir=root,
                codex_turn_timeout=60,
            )
            runner = CodexRunner(config)
            with patch.object(runner, "_execute", return_value=(0, "", "")) as execute:
                runner.run("inspect", workdir=root, image_paths=[first, second])
            self.assertEqual(
                execute.call_args.args[0],
                [
                    "codex",
                    "exec",
                    "resume",
                    "--skip-git-repo-check",
                    "--json",
                    "-i",
                    str(first),
                    "-i",
                    str(second),
                    config.codex_thread_id,
                    "-",
                ],
            )
            self.assertEqual(execute.call_args.args[2], "inspect")

    def test_qq_safe_final_extracts_allowed_image_and_redacts_paths(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            image = root / "result.png"
            image.write_bytes(b"png")
            text, images = qq_safe_final(
                f"结果 ![图]({image})\n日志 /Users/example/private.log",
                root,
            )
            self.assertEqual(images, [image.resolve()])
            self.assertIn("图片将在下方发送", text)
            self.assertNotIn(str(image), text)
            self.assertNotIn("/Users/example", text)

    def test_qq_safe_final_rejects_image_outside_allowed_roots(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            workdir = root / "work"
            workdir.mkdir()
            outside = root / "outside.png"
            outside.write_bytes(b"png")
            text, images = qq_safe_final(
                f"![图]({outside})",
                workdir,
                visualizations_dir=root / "visualizations",
            )
            self.assertEqual(images, [])
            self.assertEqual(text, "[本地图片未转发]")

    def test_desktop_refresh_fails_closed_without_touching_desktop(self) -> None:
        thread_id = "00000000-0000-4000-8000-000000000001"
        with tempfile.TemporaryDirectory() as raw_dir:
            workdir = Path(raw_dir)
            refresher = CodexDesktopRefresher(enabled=True, delay_seconds=0)
            self.assertFalse(refresher.refresh(thread_id, workdir))

        self.assertEqual(
            refresher.last_detail,
            "automatic Desktop sync unavailable: no non-destructive Desktop API",
        )

    def test_desktop_refresh_respects_disabled_setting(self) -> None:
        refresher = CodexDesktopRefresher(enabled=False)

        self.assertFalse(
            refresher.refresh("00000000-0000-4000-8000-000000000001")
        )
        self.assertEqual(refresher.last_detail, "disabled")

    def test_error_summary_removes_unrelated_plugin_warnings(self) -> None:
        stderr = "\n".join(
            [
                "WARN codex_core_plugins::manifest: prompt too long",
                "WARN codex_core_plugins::manager: failed to warm featured plugin ids cache",
                "WARN codex_core_plugins::remote::remote_installed_plugin_sync: "
                "chatgpt authentication required for remote plugin catalog",
                "WARN codex_core_plugins::startup_sync: git sync failed: "
                "Failed to connect to github.com",
                "WARN codex_core::responses_retry: stream disconnected",
                "sampling_error=Upstream request failed",
            ]
        )
        summary = summarize_codex_error(stderr)
        self.assertNotIn("plugins::manifest", summary)
        self.assertNotIn("featured plugin", summary)
        self.assertNotIn("remote plugin catalog", summary)
        self.assertNotIn("github.com", summary)
        self.assertIn("stream disconnected", summary)
        self.assertIn("Upstream request failed", summary)


class BridgeServiceTests(unittest.TestCase):
    @staticmethod
    def _write_session(path: Path, *event_types: str) -> None:
        path.write_text(
            "".join(
                json.dumps({"type": "event_msg", "payload": {"type": event_type}})
                + "\n"
                for event_type in event_types
            ),
            encoding="utf-8",
        )

    def test_switches_to_idle_target_while_current_thread_is_running(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            current = ThreadInfo(
                "00000000-0000-4000-8000-000000000001",
                "running task",
                root,
            )
            target = ThreadInfo(
                "00000000-0000-4000-8000-000000000002",
                "finished task",
                root,
            )
            self._write_session(
                root / f"rollout-current-{current.thread_id}.jsonl",
                "task_started",
            )
            self._write_session(
                root / f"rollout-target-{target.thread_id}.jsonl",
                "task_started",
                "task_complete",
            )
            service = BridgeService.__new__(BridgeService)
            service.config = SimpleNamespace(codex_sessions_dir=root)
            service.state = StateStore(root / "state.json")
            service.monitor = SimpleNamespace(
                path=root / f"rollout-current-{current.thread_id}.jsonl"
            )
            service.worker_busy = threading.Event()
            service.runner = Mock()
            service.runner.running.return_value = False
            service.store = Mock()
            service.store.active_job_count.return_value = 0
            service.active_thread = Mock()
            service.active_thread.snapshot.return_value = current
            service.thread_index = Mock()
            service.thread_index.get.return_value = target
            service.thread_choices = {}

            text = service._switch_thread(target.thread_id)

            service.active_thread.switch.assert_called_once_with(target)
            self.assertIn("已切换", text)
            self.assertIn("finished task", text)

    def test_rejects_switch_when_target_thread_is_running(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            current = ThreadInfo(
                "00000000-0000-4000-8000-000000000001",
                "idle task",
                root,
            )
            target = ThreadInfo(
                "00000000-0000-4000-8000-000000000002",
                "running task",
                root,
            )
            self._write_session(
                root / f"rollout-current-{current.thread_id}.jsonl",
                "task_started",
                "task_complete",
            )
            self._write_session(
                root / f"rollout-target-{target.thread_id}.jsonl",
                "task_started",
            )
            service = BridgeService.__new__(BridgeService)
            service.config = SimpleNamespace(codex_sessions_dir=root)
            service.state = StateStore(root / "state.json")
            service.monitor = SimpleNamespace(
                path=root / f"rollout-current-{current.thread_id}.jsonl"
            )
            service.worker_busy = threading.Event()
            service.runner = Mock()
            service.runner.running.return_value = False
            service.store = Mock()
            service.store.active_job_count.return_value = 0
            service.active_thread = Mock()
            service.active_thread.snapshot.return_value = current
            service.thread_index = Mock()
            service.thread_index.get.return_value = target
            service.thread_choices = {}

            text = service._switch_thread(target.thread_id)

            service.active_thread.switch.assert_not_called()
            self.assertEqual(text, "目标任务仍在执行，结束后才能切换。")

    def test_all_thread_notification_uses_origin_thread_context(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            thread = ThreadInfo(
                "00000000-0000-4000-8000-000000000001",
                "another task",
                root,
            )
            service = BridgeService.__new__(BridgeService)
            channel = SimpleNamespace(
                name="qq",
                limits=ChannelLimits(1500, 8, 20 * 1024 * 1024, 3, 4),
            )
            service.channels = ChannelRegistry([channel])
            with patch("bridge.qq_safe_final", return_value=("result", [])) as safe_final:
                notification, actions, images = service._prepare_final_notification(
                    Recipient("qq", "user"),
                    thread,
                    "raw result",
                )
            safe_final.assert_called_once_with("raw result", root, max_images=3)
            self.assertEqual(images, [])
            self.assertIn("another task", notification)
            self.assertIn(thread.thread_id, notification)
            command = actions[0][0].command
            self.assertEqual(command, f"/use {thread.thread_id}")

    def test_idle_checkpoint_overrides_incomplete_owned_turn(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            path = root / "session.jsonl"
            path.write_text(
                json.dumps({"type": "event_msg", "payload": {"type": "task_started"}})
                + "\n",
                encoding="utf-8",
            )
            service = BridgeService.__new__(BridgeService)
            service.state = StateStore(root / "state.json")
            self.assertTrue(service._session_busy_without_checkpoint(path))
            service.state.update(
                known_idle_session_path=str(path),
                known_idle_session_size=path.stat().st_size,
            )
            self.assertFalse(service._session_busy_without_checkpoint(path))
            with path.open("a", encoding="utf-8") as handle:
                handle.write(
                    json.dumps(
                        {"type": "event_msg", "payload": {"type": "task_started"}}
                    )
                    + "\n"
                )
            self.assertTrue(service._session_busy_without_checkpoint(path))

    def test_marking_non_current_thread_idle_does_not_clear_current_busy_state(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            completed_id = "00000000-0000-4000-8000-000000000002"
            completed_path = root / f"rollout-finished-{completed_id}.jsonl"
            self._write_session(completed_path, "task_started", "task_complete")
            service = BridgeService.__new__(BridgeService)
            service.config = SimpleNamespace(codex_sessions_dir=root)
            service.state = StateStore(root / "state.json")
            service.monitor = Mock()
            service.active_thread = Mock()
            service.active_thread.snapshot.return_value = ThreadInfo(
                "00000000-0000-4000-8000-000000000001",
                "running task",
                root,
            )

            service._mark_thread_idle(completed_id)

            service.monitor.mark_idle.assert_not_called()
            stored = service.state.load()
            self.assertEqual(stored["known_idle_session_path"], str(completed_path))

    def test_first_stop_waits_and_second_stop_cancels(self) -> None:
        service = BridgeService.__new__(BridgeService)
        service._stop_started = threading.Event()
        service._stop_complete = threading.Event()
        service._stop_state_lock = threading.Lock()
        service._components_stopped = False
        service.stop_event = threading.Event()
        service.runner = MockRunner()
        service.gateway = MockLifecycle()
        service.monitor = MockLifecycle()
        service.worker = MockWorker()

        service.stop()
        self.assertTrue(service.stop_event.is_set())
        self.assertEqual(service.worker.join_count, 1)
        self.assertEqual(service.runner.cancel_count, 0)

        service.stop()
        self.assertEqual(service.runner.cancel_count, 1)


class MockRunner:
    def __init__(self) -> None:
        self.cancel_count = 0

    def running(self) -> bool:
        return True

    def cancel(self) -> bool:
        self.cancel_count += 1
        return True


class MockLifecycle:
    def stop(self) -> None:
        pass


class MockWorker:
    def __init__(self) -> None:
        self.join_count = 0

    def is_alive(self) -> bool:
        return True

    def join(self) -> None:
        self.join_count += 1


class QQTests(unittest.TestCase):
    def test_extract_c2c(self) -> None:
        payload = {
            "op": 0,
            "t": "C2C_MESSAGE_CREATE",
            "d": {
                "id": "msg-1",
                "content": "<@123> hello",
                "author": {"user_openid": "user-1"},
            },
        }
        self.assertEqual(
            extract_c2c_event(payload),
            {
                "openid": "user-1",
                "message_id": "msg-1",
                "content": "hello",
                "attachments": [],
            },
        )

    def test_extracts_pure_image_c2c(self) -> None:
        payload = {
            "op": 0,
            "t": "C2C_MESSAGE_CREATE",
            "d": {
                "id": "msg-image",
                "content": "",
                "author": {"user_openid": "user-1"},
                "attachments": [
                    {
                        "url": "https://example.qq.com/image.png",
                        "filename": "screen.png",
                        "content_type": "image/png",
                        "size": 123,
                        "width": 10,
                        "height": 20,
                    }
                ],
            },
        }
        event = extract_c2c_event(payload)
        self.assertEqual(event["content"], "")
        self.assertEqual(event["attachments"][0]["filename"], "screen.png")

    def test_rejects_non_https_image_download(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            with self.assertRaisesRegex(ValueError, "下载地址无效"):
                download_c2c_images(
                    [
                        {
                            "url": "http://example.com/image.png",
                            "filename": "image.png",
                            "content_type": "image/png",
                            "size": 3,
                        }
                    ],
                    Path(raw_dir),
                    1024,
                    1,
                )

    def test_markdown_typing_and_media_payloads(self) -> None:
        config = SimpleNamespace(
            qq_api_base="https://api.sgroup.qq.com",
            qq_app_id="app",
            qq_app_secret="secret",
            qq_auth_url="https://auth.example",
        )
        api = QQApi(config)
        keyboard = {"content": {"rows": []}}
        with patch.object(api, "headers", return_value={}), patch(
            "qq_gateway.http_json", return_value={}
        ) as http_json:
            api.send_c2c_markdown("user", "**ok**", "msg", 2, keyboard)
            markdown_payload = http_json.call_args.kwargs["payload"]
            self.assertEqual(markdown_payload["msg_type"], 2)
            self.assertEqual(markdown_payload["markdown"], {"content": "**ok**"})
            self.assertEqual(markdown_payload["keyboard"], keyboard)

            api.send_c2c_typing("user", "msg", seconds=90)
            typing_payload = http_json.call_args.kwargs["payload"]
            self.assertEqual(typing_payload["msg_type"], 6)
            self.assertEqual(typing_payload["input_notify"]["input_second"], 60)

            api.send_c2c_media("user", "file-info")
            media_payload = http_json.call_args.kwargs["payload"]
            self.assertEqual(media_payload["msg_type"], 7)
            self.assertEqual(media_payload["media"], {"file_info": "file-info"})

    def test_upload_image_completes_parts_and_returns_file_info(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            image = Path(raw_dir) / "test.png"
            image.write_bytes(b"abcdef")
            config = SimpleNamespace(
                qq_api_base="https://api.sgroup.qq.com",
                qq_app_id="app",
                qq_app_secret="secret",
                qq_auth_url="https://auth.example",
            )
            api = QQApi(config)

            def response(method, url, payload=None, headers=None, timeout=20):
                if url.endswith("/upload_prepare"):
                    return {
                        "upload_id": "upload",
                        "parts": [
                            {"index": 2, "block_size": 3, "presigned_url": "https://up/2"},
                            {"index": 1, "block_size": 3, "presigned_url": "https://up/1"},
                        ],
                    }
                if url.endswith("/files"):
                    return {"file_info": "ready"}
                return {}

            uploaded = []
            with patch.object(api, "headers", return_value={}), patch(
                "qq_gateway.http_json", side_effect=response
            ), patch("qq_gateway.http_put_bytes", side_effect=lambda url, body: uploaded.append((url, body))):
                self.assertEqual(api.upload_c2c_image("user", image), "ready")
            self.assertEqual(uploaded, [("https://up/1", b"abc"), ("https://up/2", b"def")])

    def test_clean_html(self) -> None:
        self.assertEqual(clean_message_content("a&amp;b"), "a&b")

    def test_split_truncates(self) -> None:
        chunks = split_message("x" * 100, 20, 2)
        self.assertEqual(len(chunks), 2)
        self.assertIn("已截断", chunks[-1])

    def test_split_keeps_markdown_links_intact(self) -> None:
        link = "[BS11](https://www.bs11.jp/program/)"
        chunks = split_message(f"前文 {'x' * 70}\n\n来源：{link}\n\n后文", 90, 8)

        self.assertTrue(all(len(chunk) <= 90 for chunk in chunks))
        self.assertEqual(sum(chunk.count(link) for chunk in chunks), 1)
        self.assertFalse(any("[BS1" in chunk and link not in chunk for chunk in chunks))

    def test_split_omits_one_oversized_link_with_nested_parentheses_as_a_unit(self) -> None:
        link = "[docs](https://example.com/path(" + "x" * 90 + ")/tail)"
        chunks = split_message(link, 70, 8)

        self.assertEqual(
            chunks,
            ["[单个 Markdown 片段超过 QQ 单条限制，已省略]"],
        )

    def test_split_protects_autolinks_bare_urls_and_reference_links(self) -> None:
        values = [
            "<https://example.com/" + "a" * 90 + ">",
            "https://example.com/path(" + "b" * 90 + ")",
            "[reference " + "c" * 70 + "][docs]",
        ]

        for value in values:
            with self.subTest(value=value[:20]):
                self.assertEqual(
                    split_message(value, 70, 8),
                    ["[单个 Markdown 片段超过 QQ 单条限制，已省略]"],
                )

        self.assertEqual(
            split_message("见https://example.com/" + "d" * 90, 70, 8),
            ["见\n\n[单个 Markdown 片段超过 QQ 单条限制，已省略]"],
        )

    def test_split_repeats_table_header_for_each_table_chunk(self) -> None:
        header = "| 时间 | 动画与集数 |\n| --- | --- |"
        rows = [f"| {index:02d}:00 | 第 {index} 集内容 |" for index in range(12)]
        chunks = split_message("\n".join([header, *rows]), 110, 20)

        self.assertGreater(len(chunks), 1)
        self.assertTrue(all(chunk.startswith(header) for chunk in chunks))
        self.assertTrue(all(len(chunk) <= 110 for chunk in chunks))

    def test_split_degrades_a_table_when_one_row_cannot_fit_with_its_header(self) -> None:
        header = "| 时间 | 动画与集数 |\n| --- | --- |"
        row_content = "x" * 170
        chunks = split_message(f"{header}\n| 10:00 | {row_content} |", 200, 8)

        self.assertTrue(all(len(chunk) <= 200 for chunk in chunks))
        self.assertIn("[表格过宽，已转为分段文本]", chunks[0])
        self.assertTrue(any("表头" in chunk for chunk in chunks))
        self.assertTrue(any("第 1 行" in chunk for chunk in chunks))
        self.assertEqual(sum(chunk.count("x") for chunk in chunks), len(row_content))
        self.assertFalse(any("| --- | --- |" in chunk for chunk in chunks))

    def test_split_closes_and_reopens_fenced_code(self) -> None:
        code = "```python\n" + "\n".join(f"print({index})" for index in range(20)) + "\n```"
        chunks = split_message(code, 80, 20)

        self.assertGreater(len(chunks), 1)
        self.assertTrue(all(chunk.startswith("```python\n") for chunk in chunks))
        self.assertTrue(all(chunk.endswith("\n```") for chunk in chunks))
        self.assertTrue(all(len(chunk) <= 80 for chunk in chunks))

    def test_split_degrades_a_code_fence_with_an_oversized_info_string(self) -> None:
        code = "```" + "language" * 20 + "\nprint(1)\n```"
        chunks = split_message(code, 50, 20)

        self.assertTrue(all(len(chunk) <= 50 for chunk in chunks))
        self.assertIn("[代码块围栏过长，已转为分段文本]", chunks[0])
        self.assertFalse(any("```" in chunk for chunk in chunks))
        self.assertTrue(any("print(1)" in chunk for chunk in chunks))


if __name__ == "__main__":
    unittest.main()
