from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


ACTIVE_JOB_STATUSES = ("queued", "waiting", "preparing", "dispatching", "running")
RECOVERABLE_JOB_STATUSES = ("queued", "waiting", "preparing")
UNCERTAIN_JOB_STATUSES = ("dispatching", "running")


@dataclass(frozen=True)
class StoredJob:
    job_id: str
    message_id: str
    payload: Dict[str, Any]
    status: str
    created_at: float


@dataclass(frozen=True)
class OutboxItem:
    outbox_id: int
    dedupe_key: str
    payload: Dict[str, Any]
    attempts: int
    channel: str = "qq"


@dataclass(frozen=True)
class OutboxSpec:
    dedupe_key: str
    payload: Dict[str, Any]
    group_key: str = ""
    channel: str = "qq"


class BridgeInstanceLock:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._handle: Optional[Any] = None

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+b")
        try:
            os.chmod(self.path, 0o600)
        except OSError:
            pass
        try:
            if os.name == "nt":
                import msvcrt

                handle.seek(0)
                if handle.read(1) == b"":
                    handle.write(b"0")
                    handle.flush()
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except (OSError, ImportError) as exc:
            handle.close()
            raise RuntimeError("已有另一个 Bridge 实例正在运行") from exc
        self._handle = handle

    def release(self) -> None:
        handle = self._handle
        if handle is None:
            return
        try:
            if os.name == "nt":
                import msvcrt

                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()
            self._handle = None


class BridgeStore:
    """Durable job and notification ledger backed by a local SQLite database."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.chmod(self.path.parent, 0o700)
        except OSError:
            pass
        self._schema_lock = threading.Lock()
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=10)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute("PRAGMA busy_timeout = 10000")
        return connection

    def _initialize(self) -> None:
        with self._schema_lock, self._connect() as connection:
            connection.execute("PRAGMA journal_mode = WAL")
            connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    job_id TEXT PRIMARY KEY,
                    message_id TEXT NOT NULL UNIQUE,
                    payload_json TEXT NOT NULL,
                    status TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    started_at REAL,
                    finished_at REAL,
                    process_id INTEGER,
                    last_error TEXT NOT NULL DEFAULT ''
                );

                CREATE INDEX IF NOT EXISTS jobs_status_created_idx
                    ON jobs(status, created_at, job_id);

                CREATE TABLE IF NOT EXISTS outbox (
                    outbox_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    dedupe_key TEXT NOT NULL UNIQUE,
                    group_key TEXT NOT NULL DEFAULT '',
                    channel TEXT NOT NULL DEFAULT 'qq',
                    payload_json TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    attempts INTEGER NOT NULL DEFAULT 0,
                    next_attempt_at REAL NOT NULL DEFAULT 0,
                    lease_until REAL NOT NULL DEFAULT 0,
                    last_error TEXT NOT NULL DEFAULT '',
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    sent_at REAL
                );

                CREATE INDEX IF NOT EXISTS outbox_due_idx
                    ON outbox(status, next_attempt_at, lease_until, outbox_id);
                """
            )
            columns = {
                str(row["name"])
                for row in connection.execute("PRAGMA table_info(outbox)").fetchall()
            }
            if "group_key" not in columns:
                connection.execute(
                    "ALTER TABLE outbox ADD COLUMN group_key TEXT NOT NULL DEFAULT ''"
                )
            if "channel" not in columns:
                connection.execute(
                    "ALTER TABLE outbox ADD COLUMN channel TEXT NOT NULL DEFAULT 'qq'"
                )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS outbox_group_order_idx
                    ON outbox(group_key, outbox_id, status)
                """
            )
            connection.execute(
                """
                CREATE INDEX IF NOT EXISTS outbox_channel_due_idx
                    ON outbox(channel, status, next_attempt_at, lease_until, outbox_id)
                """
            )
            now = time.time()
            retention_cutoff = now - 30 * 24 * 60 * 60
            connection.execute(
                """
                DELETE FROM outbox
                WHERE status IN ('sent', 'discarded') AND sent_at < ?
                """,
                (retention_cutoff,),
            )
            connection.execute(
                """
                DELETE FROM jobs
                WHERE status IN (
                    'succeeded', 'failed', 'cancelled', 'timed_out', 'interrupted'
                ) AND finished_at < ?
                """,
                (retention_cutoff,),
            )
        try:
            os.chmod(self.path, 0o600)
        except OSError:
            pass

    @staticmethod
    def _decode_json(value: str) -> Dict[str, Any]:
        parsed = json.loads(value)
        if not isinstance(parsed, dict):
            raise ValueError("stored payload is not a JSON object")
        return parsed

    @classmethod
    def _job_from_row(cls, row: sqlite3.Row) -> StoredJob:
        return StoredJob(
            job_id=str(row["job_id"]),
            message_id=str(row["message_id"]),
            payload=cls._decode_json(str(row["payload_json"])),
            status=str(row["status"]),
            created_at=float(row["created_at"]),
        )

    def get_job_by_message(self, message_id: str) -> Optional[StoredJob]:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM jobs WHERE message_id = ?", (message_id,)
            ).fetchone()
        return self._job_from_row(row) if row else None

    def get_job(self, job_id: str) -> Optional[StoredJob]:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM jobs WHERE job_id = ?", (job_id,)
            ).fetchone()
        return self._job_from_row(row) if row else None

    def enqueue_job(
        self,
        message_id: str,
        payload: Dict[str, Any],
        max_active: int = 20,
    ) -> Tuple[StoredJob, bool]:
        now = time.time()
        encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT * FROM jobs WHERE message_id = ?", (message_id,)
            ).fetchone()
            if existing:
                connection.commit()
                return self._job_from_row(existing), False
            placeholders = ",".join("?" for _ in ACTIVE_JOB_STATUSES)
            active = int(
                connection.execute(
                    f"SELECT COUNT(*) FROM jobs WHERE status IN ({placeholders})",
                    ACTIVE_JOB_STATUSES,
                ).fetchone()[0]
            )
            if active >= max_active:
                connection.rollback()
                raise OverflowError("job queue is full")
            job_id = uuid.uuid4().hex
            connection.execute(
                """
                INSERT INTO jobs(
                    job_id, message_id, payload_json, status, created_at, updated_at
                ) VALUES (?, ?, ?, 'queued', ?, ?)
                """,
                (job_id, message_id, encoded, now, now),
            )
            row = connection.execute(
                "SELECT * FROM jobs WHERE job_id = ?", (job_id,)
            ).fetchone()
            connection.commit()
        if row is None:
            raise RuntimeError("failed to read persisted job")
        return self._job_from_row(row), True

    def claim_job(self, job_id: str) -> Optional[StoredJob]:
        now = time.time()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT * FROM jobs WHERE job_id = ?", (job_id,)
            ).fetchone()
            if not row or str(row["status"]) != "queued":
                connection.commit()
                return None
            connection.execute(
                "UPDATE jobs SET status = 'preparing', updated_at = ? WHERE job_id = ?",
                (now, job_id),
            )
            connection.commit()
        return StoredJob(
            job_id=str(row["job_id"]),
            message_id=str(row["message_id"]),
            payload=self._decode_json(str(row["payload_json"])),
            status="preparing",
            created_at=float(row["created_at"]),
        )

    def set_job_status(
        self,
        job_id: str,
        status: str,
        *,
        process_id: Optional[int] = None,
        error: str = "",
    ) -> None:
        now = time.time()
        started_at = now if status == "running" else None
        finished_at = now if status in {
            "succeeded",
            "failed",
            "cancelled",
            "timed_out",
            "interrupted",
        } else None
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE jobs
                SET status = ?, updated_at = ?,
                    started_at = COALESCE(?, started_at),
                    finished_at = COALESCE(?, finished_at),
                    process_id = COALESCE(?, process_id),
                    last_error = ?
                WHERE job_id = ?
                """,
                (status, now, started_at, finished_at, process_id, error, job_id),
            )
            if cursor.rowcount != 1:
                raise KeyError(f"unknown job: {job_id}")

    def set_job_status_with_outbox(
        self,
        job_id: str,
        status: str,
        items: List[OutboxSpec],
        *,
        process_id: Optional[int] = None,
        error: str = "",
    ) -> None:
        now = time.time()
        started_at = now if status == "running" else None
        finished_at = now if status in {
            "succeeded",
            "failed",
            "cancelled",
            "timed_out",
            "interrupted",
        } else None
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            cursor = connection.execute(
                """
                UPDATE jobs
                SET status = ?, updated_at = ?,
                    started_at = COALESCE(?, started_at),
                    finished_at = COALESCE(?, finished_at),
                    process_id = COALESCE(?, process_id),
                    last_error = ?
                WHERE job_id = ?
                """,
                (status, now, started_at, finished_at, process_id, error, job_id),
            )
            if cursor.rowcount != 1:
                raise KeyError(f"unknown job: {job_id}")
            self._insert_outbox_specs(connection, items, now)
            connection.commit()

    def recover_jobs(self) -> Tuple[List[StoredJob], List[StoredJob]]:
        now = time.time()
        recoverable_placeholders = ",".join("?" for _ in RECOVERABLE_JOB_STATUSES)
        uncertain_placeholders = ",".join("?" for _ in UNCERTAIN_JOB_STATUSES)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            recoverable_rows = connection.execute(
                f"""
                SELECT * FROM jobs
                WHERE status IN ({recoverable_placeholders})
                ORDER BY created_at, job_id
                """,
                RECOVERABLE_JOB_STATUSES,
            ).fetchall()
            uncertain_rows = connection.execute(
                f"""
                SELECT * FROM jobs
                WHERE status IN ({uncertain_placeholders})
                ORDER BY created_at, job_id
                """,
                UNCERTAIN_JOB_STATUSES,
            ).fetchall()
            connection.execute(
                f"""
                UPDATE jobs SET status = 'queued', updated_at = ?
                WHERE status IN ({recoverable_placeholders})
                """,
                (now, *RECOVERABLE_JOB_STATUSES),
            )
            connection.commit()
        recoverable = [
            StoredJob(
                job_id=str(row["job_id"]),
                message_id=str(row["message_id"]),
                payload=self._decode_json(str(row["payload_json"])),
                status="queued",
                created_at=float(row["created_at"]),
            )
            for row in recoverable_rows
        ]
        uncertain = [self._job_from_row(row) for row in uncertain_rows]
        return recoverable, uncertain

    def active_job_count(self) -> int:
        placeholders = ",".join("?" for _ in ACTIVE_JOB_STATUSES)
        with self._connect() as connection:
            return int(
                connection.execute(
                    f"SELECT COUNT(*) FROM jobs WHERE status IN ({placeholders})",
                    ACTIVE_JOB_STATUSES,
                ).fetchone()[0]
            )

    def latest_job(self) -> Optional[StoredJob]:
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM jobs ORDER BY updated_at DESC, job_id DESC LIMIT 1"
            ).fetchone()
        return self._job_from_row(row) if row else None

    def enqueue_outbox(
        self,
        dedupe_key: str,
        payload: Dict[str, Any],
        group_key: str = "",
    ) -> Tuple[int, bool]:
        return self.enqueue_outbox_batch(
            [OutboxSpec(dedupe_key, payload, group_key)]
        )[0]

    @staticmethod
    def _insert_outbox_specs(
        connection: sqlite3.Connection,
        items: List[OutboxSpec],
        now: float,
    ) -> List[Tuple[int, bool]]:
        results: List[Tuple[int, bool]] = []
        existing_groups: Dict[str, int] = {}
        for group_key in {item.group_key for item in items if item.group_key}:
            existing_group = connection.execute(
                """
                SELECT outbox_id FROM outbox
                WHERE group_key = ?
                ORDER BY outbox_id
                LIMIT 1
                """,
                (group_key,),
            ).fetchone()
            if existing_group:
                existing_groups[group_key] = int(existing_group["outbox_id"])
        for item in items:
            encoded = json.dumps(
                item.payload,
                ensure_ascii=False,
                separators=(",", ":"),
            )
            existing = connection.execute(
                "SELECT outbox_id FROM outbox WHERE dedupe_key = ?",
                (item.dedupe_key,),
            ).fetchone()
            if existing:
                results.append((int(existing["outbox_id"]), False))
                continue
            if item.group_key in existing_groups:
                results.append((existing_groups[item.group_key], False))
                continue
            cursor = connection.execute(
                """
                INSERT INTO outbox(
                    dedupe_key, group_key, channel, payload_json, status, attempts,
                    next_attempt_at, lease_until, created_at, updated_at
                ) VALUES (?, ?, ?, ?, 'pending', 0, 0, 0, ?, ?)
                """,
                (
                    item.dedupe_key,
                    item.group_key,
                    item.channel,
                    encoded,
                    now,
                    now,
                ),
            )
            results.append((int(cursor.lastrowid), True))
        return results

    def enqueue_outbox_batch(
        self,
        items: List[OutboxSpec],
    ) -> List[Tuple[int, bool]]:
        if not items:
            return []
        now = time.time()
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            results = self._insert_outbox_specs(connection, items, now)
            connection.commit()
        return results

    def discard_pending_outbox(self) -> int:
        now = time.time()
        with self._connect() as connection:
            cursor = connection.execute(
                """
                UPDATE outbox
                SET status = 'discarded', lease_until = 0, next_attempt_at = 0,
                    last_error = '', sent_at = ?, updated_at = ?
                WHERE status IN ('pending', 'sending')
                """,
                (now, now),
            )
            return cursor.rowcount

    def claim_due_outbox(
        self,
        lease_seconds: float = 60,
        channels: Optional[Iterable[str]] = None,
    ) -> Optional[OutboxItem]:
        now = time.time()
        normalized_channels: Optional[Tuple[str, ...]] = None
        if channels is not None:
            normalized_channels = tuple(
                dict.fromkeys(value.strip().lower() for value in channels if value.strip())
            )
            if not normalized_channels:
                return None
        channel_clause = ""
        parameters: List[Any] = [now, now]
        if normalized_channels is not None:
            placeholders = ",".join("?" for _ in normalized_channels)
            channel_clause = f"AND candidate.channel IN ({placeholders})"
            parameters.extend(normalized_channels)
        with self._connect() as connection:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                f"""
                SELECT candidate.* FROM outbox AS candidate
                WHERE (
                    (candidate.status = 'pending' AND candidate.next_attempt_at <= ?)
                    OR (candidate.status = 'sending' AND candidate.lease_until <= ?)
                )
                {channel_clause}
                AND (
                    candidate.group_key = ''
                    OR NOT EXISTS (
                        SELECT 1 FROM outbox AS earlier
                        WHERE earlier.group_key = candidate.group_key
                          AND earlier.outbox_id < candidate.outbox_id
                          AND earlier.status NOT IN ('sent', 'discarded')
                    )
                )
                ORDER BY candidate.outbox_id
                LIMIT 1
                """,
                parameters,
            ).fetchone()
            if not row:
                connection.commit()
                return None
            connection.execute(
                """
                UPDATE outbox
                SET status = 'sending', lease_until = ?, updated_at = ?
                WHERE outbox_id = ?
                """,
                (now + lease_seconds, now, int(row["outbox_id"])),
            )
            connection.commit()
        return OutboxItem(
            outbox_id=int(row["outbox_id"]),
            dedupe_key=str(row["dedupe_key"]),
            payload=self._decode_json(str(row["payload_json"])),
            attempts=int(row["attempts"]),
            channel=str(row["channel"] or "qq"),
        )

    def mark_outbox_sent(self, outbox_id: int) -> None:
        now = time.time()
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE outbox
                SET status = 'sent', sent_at = ?, updated_at = ?, lease_until = 0,
                    last_error = ''
                WHERE outbox_id = ?
                """,
                (now, now, outbox_id),
            )

    def retry_outbox(self, outbox_id: int, error: str, delay_seconds: float) -> None:
        now = time.time()
        with self._connect() as connection:
            connection.execute(
                """
                UPDATE outbox
                SET status = 'pending', attempts = attempts + 1,
                    next_attempt_at = ?, lease_until = 0, last_error = ?, updated_at = ?
                WHERE outbox_id = ?
                """,
                (now + max(0, delay_seconds), error[-1000:], now, outbox_id),
            )

    def pending_outbox_count(self) -> int:
        with self._connect() as connection:
            return int(
                connection.execute(
                    "SELECT COUNT(*) FROM outbox WHERE status IN ('pending', 'sending')"
                ).fetchone()[0]
            )

    def pending_outbox_image_paths(self) -> set[str]:
        with self._connect() as connection:
            rows = connection.execute(
                """
                SELECT payload_json FROM outbox
                WHERE status IN ('pending', 'sending')
                """
            ).fetchall()
        paths: set[str] = set()
        for row in rows:
            payload = self._decode_json(str(row["payload_json"]))
            if payload.get("kind") == "image" and payload.get("path"):
                paths.add(str(payload["path"]))
        return paths

    def latest_outbox_error(self) -> str:
        with self._connect() as connection:
            row = connection.execute(
                """
                SELECT last_error FROM outbox
                WHERE last_error <> ''
                ORDER BY updated_at DESC, outbox_id DESC LIMIT 1
                """
            ).fetchone()
        return str(row["last_error"]) if row else ""
