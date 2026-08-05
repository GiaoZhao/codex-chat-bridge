from __future__ import annotations

import io
import json
import queue
import sqlite3
import subprocess
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from bridge import BridgeService, JobDispatchUncertainError, JobRunResult
from bridge_core import (
    CodexRunner,
    SessionMonitor,
    StateStore,
    ThreadInfo,
    session_record_event_id,
)
from bridge_store import BridgeInstanceLock, BridgeStore, OutboxSpec
from chat_channel import (
    ChannelLimits,
    ChannelRegistry,
    InboundMessage,
    Recipient,
    action_rows,
)


THREAD_ID = "00000000-0000-4000-8000-000000000001"


class FakeQQChannel:
    name = "qq"
    bind_code = "12345678"
    configured_recipient = "user"
    notify_on_ready = False

    def __init__(self) -> None:
        self.limits = ChannelLimits(1500, 8, 20 * 1024 * 1024, 3, 4)
        self.sent_text: list[tuple] = []
        self.sent_images: list[tuple] = []
        self.ready = True

    def run_forever(self) -> None:
        return

    def stop(self) -> None:
        return

    def is_ready(self) -> bool:
        return self.ready

    def send_text(self, recipient_id: str, text: str, **kwargs: object) -> None:
        self.sent_text.append((recipient_id, text, kwargs))

    def send_typing(self, recipient_id: str, reply_to: str) -> None:
        return

    def send_image(self, recipient_id: str, path: Path, sequence: int = 1) -> None:
        self.sent_images.append((recipient_id, path, sequence))

    def download_images(self, attachments: object, target_dir: Path) -> list[Path]:
        return []

    def redact_error(self, error: object, recipient_id: str) -> str:
        return str(error).replace(recipient_id, "[recipient]")


class FakeDingTalkChannel(FakeQQChannel):
    name = "dingtalk"
    bind_code = "87654321"

    def __init__(self) -> None:
        super().__init__()
        self.limits = ChannelLimits(
            1800,
            8,
            20 * 1024 * 1024,
            3,
            supports_outbound_images=False,
        )


class BridgeStoreTests(unittest.TestCase):
    def test_existing_qq_outbox_is_migrated_to_qq_channel(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            database = Path(raw_dir) / "bridge.sqlite3"
            with sqlite3.connect(database) as connection:
                connection.execute(
                    """
                    CREATE TABLE outbox (
                        outbox_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        dedupe_key TEXT NOT NULL UNIQUE,
                        group_key TEXT NOT NULL DEFAULT '',
                        payload_json TEXT NOT NULL,
                        status TEXT NOT NULL DEFAULT 'pending',
                        attempts INTEGER NOT NULL DEFAULT 0,
                        next_attempt_at REAL NOT NULL DEFAULT 0,
                        lease_until REAL NOT NULL DEFAULT 0,
                        last_error TEXT NOT NULL DEFAULT '',
                        created_at REAL NOT NULL,
                        updated_at REAL NOT NULL,
                        sent_at REAL
                    )
                    """
                )
                connection.execute(
                    """
                    INSERT INTO outbox(
                        dedupe_key, payload_json, created_at, updated_at
                    ) VALUES ('legacy', '{"kind":"text"}', 1, 1)
                    """
                )

            store = BridgeStore(database)
            item = store.claim_due_outbox(channels=("qq",))

            self.assertEqual(item.dedupe_key, "legacy")
            self.assertEqual(item.channel, "qq")

    def test_outbox_claim_only_uses_ready_channels(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            store = BridgeStore(Path(raw_dir) / "bridge.sqlite3")
            store.enqueue_outbox_batch(
                [
                    OutboxSpec("qq:first", {"kind": "text"}, channel="qq"),
                    OutboxSpec(
                        "dingtalk:first",
                        {"kind": "text"},
                        channel="dingtalk",
                    ),
                ]
            )

            dingtalk = store.claim_due_outbox(channels=("dingtalk",))

            self.assertEqual(dingtalk.channel, "dingtalk")
            self.assertEqual(dingtalk.dedupe_key, "dingtalk:first")
            store.mark_outbox_sent(dingtalk.outbox_id)
            self.assertIsNone(store.claim_due_outbox(channels=("dingtalk",)))
            self.assertEqual(
                store.claim_due_outbox(channels=("qq",)).dedupe_key,
                "qq:first",
            )

    def test_same_event_can_enqueue_one_notification_per_channel(self) -> None:
        service = BridgeService.__new__(BridgeService)
        service.channels = ChannelRegistry(
            [FakeQQChannel(), FakeDingTalkChannel()]
        )
        qq_items = service._text_outbox_specs(
            Recipient("qq", "user"),
            "same result",
            dedupe_key="session-final:event-1",
        )
        dingtalk_items = service._text_outbox_specs(
            Recipient("dingtalk", "user"),
            "same result",
            dedupe_key="session-final:event-1",
        )

        with tempfile.TemporaryDirectory() as raw_dir:
            store = BridgeStore(Path(raw_dir) / "bridge.sqlite3")
            created = store.enqueue_outbox_batch([*qq_items, *dingtalk_items])

        self.assertEqual(len(qq_items), 1)
        self.assertEqual(len(dingtalk_items), 1)
        self.assertNotEqual(qq_items[0].dedupe_key, dingtalk_items[0].dedupe_key)
        self.assertEqual(
            [was_created for _item_id, was_created in created],
            [True, True],
        )

    def test_job_acceptance_is_deduplicated_and_capacity_is_atomic(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            store = BridgeStore(Path(raw_dir) / "bridge.sqlite3")
            first, created = store.enqueue_job("message-1", {"content": "first"}, max_active=1)
            duplicate, duplicate_created = store.enqueue_job(
                "message-1", {"content": "changed"}, max_active=1
            )

            self.assertTrue(created)
            self.assertFalse(duplicate_created)
            self.assertEqual(duplicate.job_id, first.job_id)
            self.assertEqual(duplicate.payload["content"], "first")
            with self.assertRaises(OverflowError):
                store.enqueue_job("message-2", {"content": "second"}, max_active=1)

            store.set_job_status(first.job_id, "succeeded")
            second, second_created = store.enqueue_job(
                "message-2", {"content": "second"}, max_active=1
            )
            self.assertTrue(second_created)
            self.assertNotEqual(second.job_id, first.job_id)

    def test_recovery_requeues_safe_jobs_and_interrupts_uncertain_jobs(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            store = BridgeStore(Path(raw_dir) / "bridge.sqlite3")
            queued, _ = store.enqueue_job("queued", {"openid": "user"})
            running, _ = store.enqueue_job("running", {"openid": "user"})
            store.set_job_status(running.job_id, "dispatching")

            recoverable, interrupted = store.recover_jobs()

            self.assertEqual([job.job_id for job in recoverable], [queued.job_id])
            self.assertEqual([job.job_id for job in interrupted], [running.job_id])
            self.assertEqual(store.get_job_by_message("queued").status, "queued")
            self.assertEqual(store.get_job_by_message("running").status, "dispatching")

            store.set_job_status_with_outbox(
                running.job_id,
                "interrupted",
                [OutboxSpec("interrupted", {"kind": "text"})],
                error="uncertain",
            )
            self.assertEqual(store.get_job_by_message("running").status, "interrupted")
            self.assertEqual(store.claim_due_outbox().dedupe_key, "interrupted")

    def test_job_terminal_state_and_notification_are_atomic(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            store = BridgeStore(Path(raw_dir) / "bridge.sqlite3")
            job, _ = store.enqueue_job("message", {"openid": "user"})

            with self.assertRaises(TypeError):
                store.set_job_status_with_outbox(
                    job.job_id,
                    "failed",
                    [OutboxSpec("terminal", {"invalid": object()})],
                    error="failed",
                )

            self.assertEqual(store.get_job(job.job_id).status, "queued")
            self.assertEqual(store.pending_outbox_count(), 0)

    def test_outbox_is_deduplicated_retried_and_marked_sent(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            store = BridgeStore(Path(raw_dir) / "bridge.sqlite3")
            outbox_id, created = store.enqueue_outbox("same", {"kind": "text"})
            duplicate_id, duplicate_created = store.enqueue_outbox(
                "same", {"kind": "changed"}
            )

            self.assertTrue(created)
            self.assertFalse(duplicate_created)
            self.assertEqual(duplicate_id, outbox_id)
            item = store.claim_due_outbox()
            self.assertEqual(item.outbox_id, outbox_id)
            store.retry_outbox(item.outbox_id, "network", 0)
            retried = store.claim_due_outbox()
            self.assertEqual(retried.attempts, 1)
            store.mark_outbox_sent(retried.outbox_id)
            self.assertEqual(store.pending_outbox_count(), 0)

    def test_existing_outbox_group_rejects_an_entire_alternate_batch(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            store = BridgeStore(Path(raw_dir) / "bridge.sqlite3")
            first = store.enqueue_outbox_batch(
                [
                    OutboxSpec("event:chunk:1", {"text": "first one"}, "event"),
                    OutboxSpec("event:chunk:2", {"text": "first two"}, "event"),
                ]
            )
            alternate = store.enqueue_outbox_batch(
                [
                    OutboxSpec("event:chunk:1", {"text": "changed one"}, "event"),
                    OutboxSpec("event:chunk:2", {"text": "changed two"}, "event"),
                    OutboxSpec("event:chunk:3", {"text": "changed three"}, "event"),
                ]
            )

            self.assertTrue(all(created for _outbox_id, created in first))
            self.assertTrue(all(not created for _outbox_id, created in alternate))
            first_item = store.claim_due_outbox()
            self.assertEqual(first_item.payload["text"], "first one")
            store.mark_outbox_sent(first_item.outbox_id)
            second_item = store.claim_due_outbox()
            self.assertEqual(second_item.payload["text"], "first two")
            store.mark_outbox_sent(second_item.outbox_id)
            self.assertIsNone(store.claim_due_outbox())

    def test_restart_discards_pending_and_abandoned_outbox_items(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "bridge.sqlite3"
            first = BridgeStore(path)
            first.enqueue_outbox_batch(
                [
                    OutboxSpec("lease", {"kind": "text"}, "message"),
                    OutboxSpec("pending", {"kind": "text"}, "message"),
                ]
            )
            claimed = first.claim_due_outbox(lease_seconds=3600)
            self.assertIsNotNone(claimed)

            restarted = BridgeStore(path)
            self.assertEqual(restarted.discard_pending_outbox(), 2)
            self.assertEqual(restarted.pending_outbox_count(), 0)
            self.assertIsNone(restarted.claim_due_outbox())

            restarted.enqueue_outbox(
                "current-process",
                {"kind": "text"},
                "current-message",
            )
            current = restarted.claim_due_outbox()
            self.assertIsNotNone(current)
            self.assertEqual(current.dedupe_key, "current-process")

    def test_later_group_item_waits_for_an_earlier_retry(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            store = BridgeStore(Path(raw_dir) / "bridge.sqlite3")
            store.enqueue_outbox_batch(
                [
                    OutboxSpec("chunk-1", {"kind": "text"}, "message"),
                    OutboxSpec("chunk-2", {"kind": "text"}, "message"),
                ]
            )

            first = store.claim_due_outbox()
            self.assertEqual(first.dedupe_key, "chunk-1")
            store.retry_outbox(first.outbox_id, "network", 3600)
            self.assertIsNone(store.claim_due_outbox())

            store.retry_outbox(first.outbox_id, "network", 0)
            retried = store.claim_due_outbox()
            self.assertEqual(retried.dedupe_key, "chunk-1")
            store.mark_outbox_sent(retried.outbox_id)
            self.assertEqual(store.claim_due_outbox().dedupe_key, "chunk-2")

    def test_instance_lock_rejects_a_second_bridge(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            path = Path(raw_dir) / "bridge.lock"
            first = BridgeInstanceLock(path)
            second = BridgeInstanceLock(path)
            first.acquire()
            try:
                with self.assertRaisesRegex(RuntimeError, "另一个 Bridge"):
                    second.acquire()
            finally:
                first.release()

            second.acquire()
            second.release()

    def test_service_acquires_instance_lock_before_shared_state(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            held = BridgeInstanceLock(root / "data" / "bridge.lock")
            held.acquire()
            try:
                with patch("bridge.StateStore") as state_store, patch(
                    "bridge.BridgeStore"
                ) as bridge_store, patch("bridge.ActiveThread") as active_thread:
                    with self.assertRaisesRegex(RuntimeError, "另一个 Bridge"):
                        BridgeService(SimpleNamespace(base_dir=root))

                state_store.assert_not_called()
                bridge_store.assert_not_called()
                active_thread.assert_not_called()
            finally:
                held.release()

    def test_service_releases_instance_lock_when_construction_fails(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            with patch("bridge.StateStore", side_effect=RuntimeError("broken state")):
                with self.assertRaisesRegex(RuntimeError, "broken state"):
                    BridgeService(SimpleNamespace(base_dir=root))

            probe = BridgeInstanceLock(root / "data" / "bridge.lock")
            probe.acquire()
            probe.release()


class RunnerLifecycleTests(unittest.TestCase):
    def test_started_callback_runs_after_process_creation(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            process = Mock()
            process.pid = 123
            process.communicate.return_value = ("", "")
            process.returncode = 0
            runner = CodexRunner(SimpleNamespace(codex_turn_timeout=60))
            started = Mock()

            with patch("bridge_core.subprocess.Popen", return_value=process):
                runner._execute(["codex"], Path(raw_dir), on_started=started)

            started.assert_called_once_with(123)

    def test_timeout_is_distinct_from_user_cancellation(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            process = Mock()
            process.communicate.side_effect = [
                subprocess.TimeoutExpired("codex", 60),
                ("", "retry failed"),
            ]
            process.returncode = -15
            runner = CodexRunner(SimpleNamespace(codex_turn_timeout=60))

            with patch("bridge_core.subprocess.Popen", return_value=process):
                runner._execute(["codex"], Path(raw_dir))

            self.assertEqual(runner.termination_reason(), "timed_out")

    def test_streaming_mode_reports_stderr_diagnostics(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            process = Mock()
            process.pid = 123
            process.stdout = io.StringIO('{"type":"thread.started"}\n')
            process.stderr = io.StringIO("stream disconnected; retrying\n")
            process.stdin = None
            process.wait.return_value = 0
            process.returncode = 0
            runner = CodexRunner(SimpleNamespace(codex_turn_timeout=60))
            diagnostic = Mock()

            with patch("bridge_core.subprocess.Popen", return_value=process):
                code, stdout, stderr = runner._execute(
                    ["codex"],
                    Path(raw_dir),
                    on_diagnostic=diagnostic,
                )

            self.assertEqual(code, 0)
            self.assertIn("thread.started", stdout)
            self.assertIn("stream disconnected", stderr)
            diagnostic.assert_called_once_with("stream disconnected; retrying")


class SessionFailureTests(unittest.TestCase):
    def test_turn_aborted_emits_a_stable_terminal_event(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            session = root / f"rollout-{THREAD_ID}.jsonl"
            session.write_text("", encoding="utf-8")
            state = StateStore(root / "state.json")
            interrupted = Mock()
            config = SimpleNamespace(
                codex_thread_id=THREAD_ID,
                codex_sessions_dir=root,
                codex_workdir=root,
            )
            monitor = SessionMonitor(
                config,
                state,
                Mock(),
                on_interrupted=interrupted,
            )
            info = ThreadInfo(THREAD_ID, "task", root, rollout_path=session)
            record = {
                "type": "event_msg",
                "payload": {"type": "turn_aborted", "reason": "network unavailable"},
            }

            monitor._handle_record(info, record, session, 12)
            first_event_id = interrupted.call_args.args[2]
            monitor._handle_record(info, record, session, 12)

            self.assertEqual(interrupted.call_count, 2)
            self.assertEqual(interrupted.call_args.args[2], first_event_id)
            self.assertEqual(len(first_event_id), 64)


class TerminalNotificationTests(unittest.TestCase):
    @staticmethod
    def _final_record(message: str) -> dict:
        return {
            "type": "event_msg",
            "payload": {
                "type": "agent_message",
                "phase": "final_answer",
                "message": message,
            },
        }

    def _service(self, root: Path) -> tuple[BridgeService, ThreadInfo]:
        service = BridgeService.__new__(BridgeService)
        service.config = SimpleNamespace(
            base_dir=root,
            codex_workdir=root,
            codex_sessions_dir=root,
            qq_allowed_openid="user",
            qq_reply_chunks=8,
            qq_reply_chars=1500,
            qq_attachment_max_bytes=20 * 1024 * 1024,
            qq_attachment_max_images=3,
        )
        service.bound_recipients = {"qq": "user"}
        service.channel = FakeQQChannel()
        service.channels = ChannelRegistry([service.channel])
        service.store = BridgeStore(root / "data" / "bridge.sqlite3")
        service.outbox_wakeup = threading.Event()
        service._outbox_image_lock = threading.RLock()
        info = ThreadInfo(THREAD_ID, "task", root)
        service.active_thread = Mock()
        service.active_thread.snapshot.return_value = info
        service.thread_index = Mock()
        service.thread_index.get.return_value = info
        return service, info

    def test_checkpoint_selects_new_identical_final_event(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            service, _info = self._service(root)
            session = root / f"rollout-test-{THREAD_ID}.jsonl"
            old_record = self._final_record("same result")
            session.write_text(json.dumps(old_record) + "\n", encoding="utf-8")
            checkpoint = service._rollout_checkpoint(THREAD_ID)
            new_record = self._final_record("same result")
            new_offset = session.stat().st_size
            with session.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(new_record) + "\n")

            event_id = service._latest_rollout_event_id(
                THREAD_ID,
                checkpoint,
                final_message="same result",
            )

            self.assertEqual(
                event_id,
                session_record_event_id(session, new_offset, new_record),
            )
            self.assertNotEqual(
                event_id,
                session_record_event_id(session, 0, old_record),
            )

    def test_new_rollout_event_is_found_from_zero_checkpoint(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            service, _info = self._service(root)
            session = root / f"rollout-new-{THREAD_ID}.jsonl"
            record = self._final_record("created")
            session.write_text(json.dumps(record) + "\n", encoding="utf-8")

            event_id = service._latest_rollout_event_id(
                THREAD_ID,
                (None, 0),
                final_message="created",
            )

            self.assertEqual(event_id, session_record_event_id(session, 0, record))

    def test_success_and_monitor_notification_are_deduplicated_in_both_orders(self) -> None:
        for monitor_first in (False, True):
            with self.subTest(monitor_first=monitor_first), tempfile.TemporaryDirectory() as raw_dir:
                root = Path(raw_dir)
                service, info = self._service(root)
                payload = {
                    "openid": "user",
                    "workdir": str(root),
                    "content": "current question",
                }
                job, _created = service.store.enqueue_job("message", payload)
                service.store.set_job_status(job.job_id, "running")
                result = JobRunResult(
                    THREAD_ID,
                    "succeeded",
                    final_message="done",
                    event_id="event-1",
                )

                if monitor_first:
                    service._on_final_event(
                        info,
                        "done",
                        "event-1",
                        "monitor question",
                    )
                service._persist_job_result(job, result)
                if not monitor_first:
                    service._on_final_event(
                        info,
                        "done",
                        "event-1",
                        "monitor question",
                    )

                self.assertEqual(service.store.get_job(job.job_id).status, "succeeded")
                self.assertEqual(service.store.pending_outbox_count(), 1)
                reopened = BridgeStore(root / "data" / "bridge.sqlite3")
                notice = reopened.claim_due_outbox()
                self.assertIn("done", notice.payload["text"])
                self.assertIn("本轮问题", notice.payload["text"])
                winning_question = (
                    "monitor question" if monitor_first else "current question"
                )
                losing_question = (
                    "current question" if monitor_first else "monitor question"
                )
                self.assertIn(winning_question, notice.payload["text"])
                self.assertNotIn(losing_question, notice.payload["text"])
                self.assertEqual(notice.dedupe_key, "qq:session-final:event-1:chunk:1")

    def test_success_notification_serialization_failure_rolls_back_state(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            service, _info = self._service(root)
            job, _created = service.store.enqueue_job(
                "message",
                {"openid": "user", "workdir": str(root)},
            )
            service.store.set_job_status(job.job_id, "running")
            result = JobRunResult(
                THREAD_ID,
                "succeeded",
                final_message="done",
                event_id="event-1",
            )
            invalid = [
                OutboxSpec("valid", {"kind": "text"}),
                OutboxSpec("invalid", {"value": object()}),
            ]

            with patch.object(service, "_final_outbox_specs", return_value=invalid):
                with self.assertRaises(TypeError):
                    service._persist_job_result(job, result)

            self.assertEqual(service.store.get_job(job.job_id).status, "running")
            self.assertEqual(service.store.pending_outbox_count(), 0)

    def test_success_persists_text_and_image_in_one_terminal_transaction(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            service, _info = self._service(root)
            attachment_dir = root / "data" / "attachments" / "message"
            attachment_dir.mkdir(parents=True)
            image = attachment_dir / "result.png"
            image.write_bytes(b"png")
            job, _created = service.store.enqueue_job(
                "message",
                {
                    "openid": "user",
                    "workdir": str(root),
                    "attachment_dir": str(attachment_dir),
                },
            )
            service.store.set_job_status(job.job_id, "running")

            service._persist_job_result(
                job,
                JobRunResult(
                    THREAD_ID,
                    "succeeded",
                    final_message=f"done\n![result]({image})",
                    event_id="event-1",
                ),
            )

            self.assertEqual(service.store.get_job(job.job_id).status, "succeeded")
            self.assertEqual(service.store.pending_outbox_count(), 2)
            service._cleanup_job_attachments(job.payload)
            self.assertFalse(image.exists())
            text_notice = service.store.claim_due_outbox()
            self.assertEqual(text_notice.payload["kind"], "text")
            service.store.mark_outbox_sent(text_notice.outbox_id)
            image_notice = service.store.claim_due_outbox()
            self.assertEqual(image_notice.payload["kind"], "image")
            spooled = Path(image_notice.payload["path"])
            self.assertNotEqual(spooled, image.resolve())
            self.assertTrue(spooled.is_file())
            service._deliver_outbox(image_notice)
            self.assertEqual(service.channel.sent_images, [("user", spooled, 1)])
            service.store.mark_outbox_sent(image_notice.outbox_id)
            service._cleanup_unreferenced_outbox_images()
            self.assertFalse(spooled.exists())

    def test_success_without_rollout_event_is_not_committed(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            service, _info = self._service(root)
            job, _created = service.store.enqueue_job(
                "message",
                {"openid": "user", "workdir": str(root)},
            )
            service.store.set_job_status(job.job_id, "running")

            with self.assertRaisesRegex(RuntimeError, "会话事件标识"):
                service._persist_job_result(
                    job,
                    JobRunResult(THREAD_ID, "succeeded", final_message="done"),
                )

            self.assertEqual(service.store.get_job(job.job_id).status, "running")
            self.assertEqual(service.store.pending_outbox_count(), 0)

    def test_interruption_and_worker_terminal_notification_are_deduplicated(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            service, info = self._service(root)
            job, _created = service.store.enqueue_job(
                "message",
                {"openid": "user", "workdir": str(root)},
            )
            service.store.set_job_status(job.job_id, "running")

            service._on_interrupted(info, "network unavailable", "event-1")
            service._persist_job_result(
                job,
                JobRunResult(
                    THREAD_ID,
                    "failed",
                    "network unavailable",
                    "Codex task failed",
                    event_id="event-1",
                ),
            )

            self.assertEqual(service.store.get_job(job.job_id).status, "failed")
            self.assertEqual(service.store.pending_outbox_count(), 1)
            self.assertEqual(
                service.store.claim_due_outbox().dedupe_key,
                "qq:session-interrupted:event-1:chunk:1",
            )


class BridgeAcceptanceTests(unittest.TestCase):
    def _service(self, root: Path) -> BridgeService:
        service = BridgeService.__new__(BridgeService)
        service.config = SimpleNamespace(
            base_dir=root,
            qq_allowed_openid="user",
            qq_reply_chunks=8,
            qq_reply_chars=1500,
            qq_attachment_max_bytes=20 * 1024 * 1024,
            qq_attachment_max_images=3,
        )
        service.store = BridgeStore(root / "data" / "bridge.sqlite3")
        service.bound_recipients = {"qq": "user"}
        service.channel = FakeQQChannel()
        service.channels = ChannelRegistry([service.channel])
        service.jobs = queue.Queue()
        service.outbox_wakeup = threading.Event()
        service._outbox_image_lock = threading.RLock()
        service.stop_event = threading.Event()
        return service

    def test_ack_is_persisted_and_does_not_claim_codex_started(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            service = self._service(root)
            event = InboundMessage("qq", "user", "message-1", "continue")

            accepted = service._accept_remote_job(
                event,
                action="resume",
                thread_id=THREAD_ID,
                workdir=root,
                content="continue",
                accepted_text="已接收并保存，等待 Bridge 调度当前 Codex 任务。",
            )

            self.assertTrue(accepted)
            self.assertEqual(service.store.active_job_count(), 1)
            self.assertEqual(service.jobs.qsize(), 1)
            outbox = service.store.claim_due_outbox()
            self.assertIn("已接收并保存", outbox.payload["text"])
            self.assertNotIn("正在继续", outbox.payload["text"])
            self.assertNotIn("已启动", outbox.payload["text"])

    def test_each_channel_keeps_an_independent_binding(self) -> None:
        service = BridgeService.__new__(BridgeService)
        service.channels = ChannelRegistry(
            [FakeQQChannel(), FakeDingTalkChannel()]
        )
        service.bound_recipients = {}
        service.state = Mock()
        service._safe_send = Mock(return_value=True)

        service._bind(
            InboundMessage("qq", "qq-user", "qq-message", "/bind 12345678")
        )
        service._bind(
            InboundMessage(
                "dingtalk",
                "ding-user",
                "ding-message",
                "/bind 87654321",
            )
        )

        self.assertEqual(
            service.bound_recipients,
            {"qq": "qq-user", "dingtalk": "ding-user"},
        )
        self.assertEqual(
            service.state.update.call_args.kwargs["bindings"],
            {
                "qq": {"recipient_id": "qq-user"},
                "dingtalk": {"recipient_id": "ding-user"},
            },
        )

    def test_actions_are_attached_only_to_the_last_markdown_chunk(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            service = self._service(Path(raw_dir))
            service.channel.limits = ChannelLimits(80, 8, 20 * 1024 * 1024, 3, 4)
            actions = action_rows([[("当前任务", "/current", 0)]])

            items = service._text_outbox_specs(
                "user",
                "第一段 " + "x" * 220,
                actions=actions,
                dedupe_key="long-message",
            )

            self.assertGreater(len(items), 1)
            self.assertTrue(all(item.payload["actions"] == [] for item in items[:-1]))
            self.assertEqual(items[-1].payload["actions"][0][0]["command"], "/current")

    def test_started_notification_requires_running_transition(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            service = self._service(root)
            service._runtime_lock = threading.Lock()
            service.worker_stage = "idle"
            service.current_job_id = ""
            service.last_worker_error = ""
            payload = {"openid": "user", "workdir": str(root)}
            job, _ = service.store.enqueue_job("message-1", payload)
            service.store.set_job_status(job.job_id, "dispatching")

            service._on_job_started(job.job_id, payload, "resume", 321)

            persisted = service.store.get_job_by_message("message-1")
            self.assertEqual(persisted.status, "running")
            outbox = service.store.claim_due_outbox()
            self.assertIn("子进程已启动", outbox.payload["text"])

    def test_repeated_network_diagnostic_enqueues_one_notification(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            service = self._service(Path(raw_dir))
            event = {"openid": "user"}

            service._on_codex_diagnostic(
                "job-1",
                event,
                "WARN codex_core::responses_retry: stream disconnected",
            )
            service._on_codex_diagnostic(
                "job-1",
                event,
                "WARN codex_core::responses_retry: stream disconnected",
            )

            self.assertEqual(service.store.pending_outbox_count(), 1)
            outbox = service.store.claim_due_outbox()
            self.assertIn("网络连接异常", outbox.payload["text"])

    def test_plugin_sync_warnings_do_not_emit_auth_or_network_notifications(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            service = self._service(Path(raw_dir))
            event = {"openid": "user"}

            service._on_codex_diagnostic(
                "job-1",
                event,
                "WARN codex_core_plugins::remote::remote_installed_plugin_sync: "
                "chatgpt authentication required for remote plugin catalog; "
                "api key auth is not supported",
            )
            service._on_codex_diagnostic(
                "job-1",
                event,
                "WARN codex_core_plugins::startup_sync: git sync failed: "
                "Failed to connect to github.com",
            )

            self.assertEqual(service.store.pending_outbox_count(), 0)

            service._on_codex_diagnostic(
                "job-1",
                event,
                "ERROR codex_api::client: invalid API key; HTTP 401",
            )
            self.assertEqual(service.store.pending_outbox_count(), 1)
            outbox = service.store.claim_due_outbox()
            self.assertIn("登录或鉴权异常", outbox.payload["text"])

    def test_openid_is_redacted_from_encoded_api_errors(self) -> None:
        openid = "user/id+secret"
        error = "POST https://api.example/v2/users/user%2Fid%2Bsecret/messages failed"

        detail = BridgeService._redact_recipient(error, openid)

        self.assertNotIn(openid, detail)
        self.assertNotIn("user%2Fid%2Bsecret", detail)
        self.assertIn("[recipient]", detail)

    def test_shutdown_before_dispatch_requeues_the_job(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            service = self._service(root)
            service.config.codex_sessions_dir = root
            service.config.codex_workdir = root
            service.stop_event = threading.Event()
            service.worker_busy = threading.Event()
            service._runtime_lock = threading.Lock()
            service.worker_stage = "idle"
            service.current_job_id = ""
            service.last_worker_error = ""
            service.runner = Mock()
            service.runner.running.return_value = False
            service.active_thread = Mock()
            service.active_thread.snapshot.return_value = ThreadInfo(THREAD_ID, "task", root)
            service._safe_send = Mock()
            service._cleanup_job_attachments = Mock()
            service._run_job = Mock()

            def stop_while_waiting(_thread_id: str) -> bool:
                service.stop_event.set()
                return True

            service._wait_until_idle = stop_while_waiting
            payload = {
                "openid": "user",
                "action": "resume",
                "thread_id": THREAD_ID,
                "workdir": str(root),
                "content": "continue",
                "image_paths": [],
            }
            job, _ = service.store.enqueue_job("message-1", payload)
            service.jobs.put_nowait(job.job_id)

            service._worker_loop()

            persisted = service.store.get_job_by_message("message-1")
            self.assertEqual(persisted.status, "queued")
            service._run_job.assert_not_called()
            service._cleanup_job_attachments.assert_not_called()

    def test_image_only_job_uses_the_effective_prompt_in_its_final_notice(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            image = root / "input.png"
            image.write_bytes(b"png")
            service = self._service(root)
            service.config.codex_sessions_dir = root
            service.config.codex_workdir = root
            service.worker_busy = threading.Event()
            service._runtime_lock = threading.Lock()
            service.worker_stage = "idle"
            service.current_job_id = ""
            service.last_worker_error = ""
            service.runner = Mock()
            service.runner.running.return_value = False
            service.active_thread = Mock()
            service.active_thread.snapshot.return_value = ThreadInfo(THREAD_ID, "task", root)
            service._wait_until_idle = Mock(return_value=True)
            service._cleanup_job_attachments = Mock()
            service._mark_thread_idle = Mock()
            service._refresh_desktop = Mock()
            service._run_job = Mock(
                return_value=JobRunResult(
                    THREAD_ID,
                    "succeeded",
                    final_message="done",
                    event_id="event-1",
                )
            )
            captured_question = []

            def persist(job: object, _result: JobRunResult) -> bool:
                captured_question.append(job.payload["content"])
                service.stop_event.set()
                return True

            service._persist_job_result_with_retry = Mock(side_effect=persist)
            payload = {
                "openid": "user",
                "action": "resume",
                "thread_id": THREAD_ID,
                "workdir": str(root),
                "content": "",
                "image_paths": [str(image)],
            }
            job, _ = service.store.enqueue_job("message-1", payload)
            service.jobs.put_nowait(job.job_id)

            service._worker_loop()

            expected = "请查看我发送的图片，并根据图片内容继续处理。"
            self.assertEqual(service._run_job.call_args.args[5], expected)
            self.assertEqual(captured_question, [expected])

    def test_started_process_persistence_failure_is_uncertain(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            service = self._service(root)
            service._set_job_status_with_notification = Mock(
                side_effect=sqlite3.OperationalError("disk unavailable")
            )

            with self.assertRaises(JobDispatchUncertainError):
                service._on_job_started(
                    "job-1",
                    {"openid": "user"},
                    "resume",
                    321,
                )

    def test_worker_does_not_downgrade_uncertain_dispatch_to_failed(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            service = self._service(root)
            service.config.codex_sessions_dir = root
            service.config.codex_workdir = root
            service.worker_busy = threading.Event()
            service._runtime_lock = threading.Lock()
            service.worker_stage = "idle"
            service.current_job_id = ""
            service.last_worker_error = ""
            service.runner = Mock()
            service.runner.running.return_value = False
            service.active_thread = Mock()
            service.active_thread.snapshot.return_value = ThreadInfo(THREAD_ID, "task", root)
            service._wait_until_idle = Mock(return_value=True)
            service._cleanup_job_attachments = Mock()

            def uncertain_run(*_args: object, **_kwargs: object) -> None:
                service.stop_event.set()
                raise JobDispatchUncertainError("started state was not persisted")

            service._run_job = uncertain_run
            payload = {
                "openid": "user",
                "action": "resume",
                "thread_id": THREAD_ID,
                "workdir": str(root),
                "content": "continue",
                "image_paths": [],
            }
            job, _ = service.store.enqueue_job("message-1", payload)
            service.jobs.put_nowait(job.job_id)

            with patch("bridge.log_event"):
                service._worker_loop()

            persisted = service.store.get_job_by_message("message-1")
            self.assertEqual(persisted.status, "interrupted")
            notice = service.store.claim_due_outbox()
            self.assertIn("无法确认", notice.payload["text"])
            service._cleanup_job_attachments.assert_called_once()

    def test_worker_requests_shutdown_after_two_terminal_persistence_failures(self) -> None:
        with tempfile.TemporaryDirectory() as raw_dir:
            root = Path(raw_dir)
            service = self._service(root)
            service.config.codex_sessions_dir = root
            service.config.codex_workdir = root
            service.worker_busy = threading.Event()
            service._runtime_lock = threading.Lock()
            service.worker_stage = "idle"
            service.current_job_id = ""
            service.last_worker_error = ""
            service.runner = Mock()
            service.runner.running.return_value = False
            service.active_thread = Mock()
            service.active_thread.snapshot.return_value = ThreadInfo(THREAD_ID, "task", root)
            service._wait_until_idle = Mock(return_value=True)
            service._cleanup_job_attachments = Mock()
            service._run_job = Mock(
                return_value=JobRunResult(
                    THREAD_ID,
                    "failed",
                    "failure",
                    "Codex task failed",
                )
            )
            service._persist_job_result = Mock(
                side_effect=sqlite3.OperationalError("disk unavailable")
            )

            def request_shutdown(_reason: str) -> None:
                service.stop_event.set()

            service._request_fatal_shutdown = Mock(side_effect=request_shutdown)
            payload = {
                "openid": "user",
                "action": "resume",
                "thread_id": THREAD_ID,
                "workdir": str(root),
                "content": "continue",
                "image_paths": [],
            }
            job, _created = service.store.enqueue_job("message-1", payload)
            service.jobs.put_nowait(job.job_id)

            with patch("bridge.log_event"):
                service._worker_loop()

            self.assertEqual(service._persist_job_result.call_count, 2)
            service._request_fatal_shutdown.assert_called_once()
            self.assertTrue(service.stop_event.is_set())
            self.assertEqual(service.store.get_job(job.job_id).status, "dispatching")
            service._cleanup_job_attachments.assert_not_called()

    def test_fatal_shutdown_request_uses_a_separate_stop_thread(self) -> None:
        service = BridgeService.__new__(BridgeService)
        service._fatal_shutdown_requested = threading.Event()
        service.stop_event = threading.Event()
        service.outbox_wakeup = threading.Event()
        service.stop = Mock()
        stop_thread = Mock()

        with patch("bridge.threading.Thread", return_value=stop_thread) as thread_class, patch(
            "bridge.log_event"
        ):
            service._request_fatal_shutdown("terminal persistence failed")

        self.assertTrue(service.stop_event.is_set())
        self.assertTrue(service.outbox_wakeup.is_set())
        stop_thread.start.assert_called_once_with()
        self.assertIs(thread_class.call_args.kwargs["target"], service.stop)
        self.assertEqual(
            thread_class.call_args.kwargs["kwargs"],
            {"force_if_running": False},
        )

    def test_outbox_loop_survives_a_storage_error(self) -> None:
        service = BridgeService.__new__(BridgeService)
        stopped = {"value": False}
        service.stop_event = Mock()
        service.stop_event.is_set.side_effect = lambda: stopped["value"]
        service.stop_event.wait.return_value = False
        service.channels = ChannelRegistry([FakeQQChannel()])
        service.outbox_wakeup = Mock()
        service.store = Mock()

        calls = {"count": 0}

        def claim(*_args: object, **_kwargs: object) -> None:
            calls["count"] += 1
            if calls["count"] == 1:
                raise sqlite3.OperationalError("database busy")
            stopped["value"] = True
            return None

        service.store.claim_due_outbox.side_effect = claim

        with patch("bridge.log_event") as logged:
            service._outbox_loop()

        self.assertEqual(calls["count"], 2)
        self.assertTrue(
            any("storage error" in call.args[1] for call in logged.call_args_list)
        )


if __name__ == "__main__":
    unittest.main()
