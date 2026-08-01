from __future__ import annotations

import json
import ntpath
import os
import re
import shlex
import shutil
import sqlite3
import subprocess
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple
from urllib.parse import unquote, urlparse
from urllib.request import urlopen

try:
    import websocket
except ImportError:  # pragma: no cover - reported by the installation check
    websocket = None


THREAD_ID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)


def log_event(component: str, message: str) -> None:
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{component}] {message}", flush=True)


def load_env(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            continue
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        os.environ.setdefault(key, value)


def env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    raw = os.getenv(name, "").strip()
    value = int(raw) if raw else default
    return max(minimum, min(maximum, value))


def env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name, "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on"}


def parse_command(value: str, windows: Optional[bool] = None) -> Tuple[str, ...]:
    raw = value.strip()
    if not raw:
        return ()
    use_windows_rules = os.name == "nt" if windows is None else windows
    if not use_windows_rules:
        return tuple(shlex.split(raw))

    unquoted = (
        raw[1:-1]
        if len(raw) >= 2 and raw[0] == raw[-1] and raw[0] in {"'", '"'}
        else raw
    )
    expanded = Path(ntpath.expandvars(unquoted)).expanduser()
    if expanded.is_file():
        return (str(expanded),)

    parts = shlex.split(raw, posix=False)
    normalized = tuple(
        part[1:-1]
        if len(part) >= 2 and part[0] == part[-1] and part[0] in {"'", '"'}
        else part
        for part in parts
    )
    if not normalized:
        return ()
    return (ntpath.expandvars(normalized[0]), *normalized[1:])


def resolve_command(command: Tuple[str, ...]) -> Tuple[str, ...]:
    if not command:
        return ()
    executable = shutil.which(command[0])
    if not executable:
        return command
    return (executable, *command[1:])


@dataclass(frozen=True)
class Config:
    base_dir: Path
    qq_app_id: str
    qq_app_secret: str
    qq_bind_code: str
    qq_allowed_openid: str
    qq_auth_url: str
    qq_api_base: str
    qq_intents: int
    qq_reply_chars: int
    qq_reply_chunks: int
    qq_attachment_max_bytes: int
    qq_attachment_max_images: int
    qq_notify_on_ready: bool
    codex_desktop_refresh: bool
    codex_desktop_cdp_url: str
    codex_desktop_refresh_delay_ms: int
    codex_thread_id: str
    codex_workdir: Path
    codex_command: Tuple[str, ...]
    codex_sessions_dir: Path
    codex_state_db: Path
    codex_turn_timeout: int
    queue_wait_timeout: int

    @classmethod
    def from_env(cls, base_dir: Path, require_qq: bool = True) -> "Config":
        load_env(base_dir / ".env")
        app_id = os.getenv("QQ_APP_ID", "").strip()
        app_secret = os.getenv("QQ_APP_SECRET", "").strip()
        thread_id = os.getenv("CODEX_THREAD_ID", "").strip()
        workdir = Path(os.getenv("CODEX_WORKDIR", str(base_dir))).expanduser()
        sessions_dir = Path(
            os.getenv("CODEX_SESSIONS_DIR", str(Path.home() / ".codex" / "sessions"))
        ).expanduser()
        state_db = Path(
            os.getenv("CODEX_STATE_DB", str(Path.home() / ".codex" / "state_5.sqlite"))
        ).expanduser()
        command_raw = os.getenv("CODEX_COMMAND", "codex").strip() or "codex"
        command = resolve_command(parse_command(command_raw))
        desktop_cdp_url = os.getenv(
            "CODEX_DESKTOP_CDP_URL", "http://127.0.0.1:9229"
        ).strip().rstrip("/")

        errors: List[str] = []
        if require_qq and not app_id:
            errors.append("QQ_APP_ID 未配置")
        if require_qq and not app_secret:
            errors.append("QQ_APP_SECRET 未配置")
        if not thread_id or not THREAD_ID_RE.fullmatch(thread_id):
            errors.append("CODEX_THREAD_ID 必须是有效的 Codex UUID")
        if not workdir.is_dir():
            errors.append(f"CODEX_WORKDIR 不存在: {workdir}")
        if not sessions_dir.is_dir():
            errors.append(f"CODEX_SESSIONS_DIR 不存在: {sessions_dir}")
        if not state_db.is_file():
            errors.append(f"CODEX_STATE_DB 不存在: {state_db}")
        if not command:
            errors.append("CODEX_COMMAND 不能为空")
        elif shutil.which(command[0]) is None and not Path(command[0]).expanduser().is_file():
            errors.append(f"找不到 Codex 命令: {command[0]}")
        parsed_cdp_url = urlparse(desktop_cdp_url)
        if (
            parsed_cdp_url.scheme != "http"
            or parsed_cdp_url.hostname not in {"127.0.0.1", "localhost", "::1"}
        ):
            errors.append("CODEX_DESKTOP_CDP_URL 只允许本机 HTTP 地址")
        if errors:
            raise ValueError("；".join(errors))

        return cls(
            base_dir=base_dir,
            qq_app_id=app_id,
            qq_app_secret=app_secret,
            qq_bind_code=os.getenv("QQ_BIND_CODE", "").strip(),
            qq_allowed_openid=os.getenv("QQ_ALLOWED_OPENID", "").strip(),
            qq_auth_url=os.getenv(
                "QQ_AUTH_URL", "https://bots.qq.com/app/getAppAccessToken"
            ).strip(),
            qq_api_base=os.getenv("QQ_API_BASE", "https://api.sgroup.qq.com").rstrip("/"),
            qq_intents=env_int(
                "QQ_GATEWAY_INTENTS", (1 << 25) | (1 << 26), 1, (1 << 31) - 1
            ),
            qq_reply_chars=env_int("QQ_REPLY_MAX_CHARS", 1500, 200, 4000),
            qq_reply_chunks=env_int("QQ_REPLY_MAX_CHUNKS", 8, 1, 20),
            qq_attachment_max_bytes=env_int(
                "QQ_ATTACHMENT_MAX_BYTES", 20 * 1024 * 1024, 1024, 20 * 1024 * 1024
            ),
            qq_attachment_max_images=env_int("QQ_ATTACHMENT_MAX_IMAGES", 3, 1, 5),
            qq_notify_on_ready=env_bool("QQ_NOTIFY_ON_READY", False),
            codex_desktop_refresh=env_bool("CODEX_DESKTOP_REFRESH", os.name != "nt"),
            codex_desktop_cdp_url=desktop_cdp_url,
            codex_desktop_refresh_delay_ms=env_int(
                "CODEX_DESKTOP_REFRESH_DELAY_MS", 800, 100, 5000
            ),
            codex_thread_id=thread_id,
            codex_workdir=workdir.resolve(),
            codex_command=command,
            codex_sessions_dir=sessions_dir.resolve(),
            codex_state_db=state_db.resolve(),
            codex_turn_timeout=env_int("CODEX_TURN_TIMEOUT", 7200, 60, 21600),
            queue_wait_timeout=env_int("CODEX_QUEUE_WAIT_TIMEOUT", 21600, 60, 43200),
        )


class StateStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.Lock()
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def load(self) -> Dict[str, Any]:
        with self._lock:
            if not self.path.exists():
                return {}
            try:
                value = json.loads(self.path.read_text(encoding="utf-8"))
                return value if isinstance(value, dict) else {}
            except (OSError, json.JSONDecodeError):
                return {}

    def update(self, **changes: Any) -> Dict[str, Any]:
        with self._lock:
            current: Dict[str, Any] = {}
            if self.path.exists():
                try:
                    value = json.loads(self.path.read_text(encoding="utf-8"))
                    if isinstance(value, dict):
                        current = value
                except (OSError, json.JSONDecodeError):
                    pass
            current.update(changes)
            temp = self.path.with_suffix(".tmp")
            temp.write_text(
                json.dumps(current, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            os.chmod(temp, 0o600)
            os.replace(temp, self.path)
            return current


@dataclass(frozen=True)
class ThreadInfo:
    thread_id: str
    title: str
    workdir: Path
    updated_at: int = 0
    rollout_path: Optional[Path] = None

    @property
    def project_name(self) -> str:
        return self.workdir.name or str(self.workdir)


class ThreadIndex:
    """Read the Codex task index without writing to the Codex database."""

    def __init__(self, database_path: Path) -> None:
        self.database_path = database_path

    def _connect(self) -> sqlite3.Connection:
        uri = self.database_path.resolve().as_uri() + "?mode=ro"
        connection = sqlite3.connect(uri, uri=True, timeout=3)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only = ON")
        return connection

    @staticmethod
    def _visible_where() -> str:
        return (
            "archived = 0 AND preview <> '' "
            "AND source NOT LIKE '%subagent%' "
            "AND COALESCE(thread_source, '') <> 'subagent'"
        )

    def _columns(self, connection: sqlite3.Connection) -> set[str]:
        return {str(row[1]) for row in connection.execute("PRAGMA table_info(threads)")}

    def _expressions(self, connection: sqlite3.Connection) -> Tuple[str, str]:
        columns = self._columns(connection)
        title_parts = []
        if "name" in columns:
            title_parts.append("NULLIF(name, '')")
        title_parts.extend(
            ["NULLIF(title, '')", "NULLIF(first_user_message, '')", "NULLIF(preview, '')", "id"]
        )
        title_expression = "COALESCE(" + ", ".join(title_parts) + ")"
        order_expression = "updated_at"
        return title_expression, order_expression

    @staticmethod
    def _from_row(row: sqlite3.Row) -> ThreadInfo:
        title = " ".join(str(row["display_title"] or row["id"]).split())
        rollout_value = str(row["rollout_path"] or "")
        return ThreadInfo(
            thread_id=str(row["id"]),
            title=title,
            workdir=Path(str(row["cwd"])).expanduser().resolve(),
            updated_at=int(row["sort_time"] or 0),
            rollout_path=(
                Path(rollout_value).expanduser().resolve() if rollout_value else None
            ),
        )

    def list_page(self, page: int = 1, page_size: int = 8) -> Tuple[List[ThreadInfo], int]:
        page = max(1, page)
        page_size = max(1, min(20, page_size))
        with self._connect() as connection:
            title_expression, order_expression = self._expressions(connection)
            where = self._visible_where()
            total = int(
                connection.execute(f"SELECT COUNT(*) FROM threads WHERE {where}").fetchone()[0]
            )
            rows = connection.execute(
                f"""
                SELECT id, cwd, rollout_path, {title_expression} AS display_title,
                       {order_expression} AS sort_time
                FROM threads
                WHERE {where}
                ORDER BY sort_time DESC, id DESC
                LIMIT ? OFFSET ?
                """,
                (page_size, (page - 1) * page_size),
            ).fetchall()
        return [self._from_row(row) for row in rows], total

    def list_monitorable(self) -> List[ThreadInfo]:
        with self._connect() as connection:
            title_expression, order_expression = self._expressions(connection)
            rows = connection.execute(
                f"""
                SELECT id, cwd, rollout_path, {title_expression} AS display_title,
                       {order_expression} AS sort_time
                FROM threads
                WHERE {self._visible_where()}
                ORDER BY sort_time DESC, id DESC
                """
            ).fetchall()
        return [self._from_row(row) for row in rows]

    def get(self, thread_id: str) -> Optional[ThreadInfo]:
        if not THREAD_ID_RE.fullmatch(thread_id):
            return None
        with self._connect() as connection:
            title_expression, order_expression = self._expressions(connection)
            row = connection.execute(
                f"""
                SELECT id, cwd, rollout_path, {title_expression} AS display_title,
                       {order_expression} AS sort_time
                FROM threads
                WHERE {self._visible_where()} AND id = ?
                """,
                (thread_id,),
            ).fetchone()
        return self._from_row(row) if row else None

    def get_by_number(self, number: int) -> Optional[ThreadInfo]:
        if number < 1:
            return None
        rows, _total = self.list_page(page=number, page_size=1)
        return rows[0] if rows else None

    def resolve(self, selector: str) -> Optional[ThreadInfo]:
        selector = selector.strip()
        if selector.isdigit():
            return self.get_by_number(int(selector))
        return self.get(selector)


class ActiveThread:
    def __init__(self, config: Config, state: StateStore, index: ThreadIndex) -> None:
        self.state = state
        self._lock = threading.Lock()
        stored = state.load()
        stored_id = str(stored.get("active_thread_id") or "")
        thread_id = stored_id if THREAD_ID_RE.fullmatch(stored_id) else config.codex_thread_id
        info: Optional[ThreadInfo]
        try:
            info = index.get(thread_id)
        except sqlite3.Error:
            info = None
        if info is None:
            stored_workdir = str(stored.get("active_workdir") or "")
            workdir = Path(stored_workdir).expanduser() if stored_workdir else config.codex_workdir
            info = ThreadInfo(thread_id, thread_id, workdir.resolve())
        self._info = info
        self.state.update(
            active_thread_id=info.thread_id,
            active_workdir=str(info.workdir),
        )

    def snapshot(self) -> ThreadInfo:
        with self._lock:
            return self._info

    def switch(self, info: ThreadInfo) -> None:
        if not THREAD_ID_RE.fullmatch(info.thread_id):
            raise ValueError("无效的 Codex Thread UUID")
        if not info.workdir.is_dir():
            raise ValueError(f"任务工作目录不存在: {info.workdir}")
        with self._lock:
            self._info = info
            self.state.update(
                active_thread_id=info.thread_id,
                active_workdir=str(info.workdir),
            )


def find_session_file(sessions_dir: Path, thread_id: str) -> Optional[Path]:
    matches = list(sessions_dir.glob(f"**/rollout-*-{thread_id}.jsonl"))
    if not matches:
        return None
    return max(matches, key=lambda path: path.stat().st_mtime)


def iter_jsonl(path: Path) -> Iterable[Dict[str, Any]]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                try:
                    value = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(value, dict):
                    yield value
    except OSError:
        return


def final_message_from_record(record: Dict[str, Any]) -> Optional[str]:
    if record.get("type") != "event_msg":
        return None
    payload = record.get("payload")
    if not isinstance(payload, dict):
        return None
    if payload.get("type") != "agent_message" or payload.get("phase") != "final_answer":
        return None
    message = payload.get("message")
    return message.strip() if isinstance(message, str) and message.strip() else None


def session_busy(path: Path) -> bool:
    busy = False
    for record in iter_jsonl(path):
        if record.get("type") != "event_msg":
            continue
        payload = record.get("payload")
        if not isinstance(payload, dict):
            continue
        event_type = payload.get("type")
        if event_type == "task_started":
            busy = True
        elif event_type in {"task_complete", "turn_aborted", "turn_complete"}:
            busy = False
    return busy


def latest_final_message(path: Path) -> Optional[str]:
    latest = None
    for record in iter_jsonl(path):
        value = final_message_from_record(record)
        if value:
            latest = value
    return latest


def latest_complete_turn(path: Path) -> Optional[Tuple[str, str]]:
    current_user: Optional[str] = None
    latest: Optional[Tuple[str, str]] = None
    for record in iter_jsonl(path):
        if record.get("type") != "event_msg":
            continue
        payload = record.get("payload")
        if not isinstance(payload, dict):
            continue
        if payload.get("type") == "user_message":
            message = payload.get("message")
            current_user = message.strip() if isinstance(message, str) and message.strip() else None
            continue
        assistant = final_message_from_record(record)
        if current_user and assistant:
            latest = (current_user, assistant)
    return latest


class SessionMonitor:
    def __init__(
        self,
        config: Config,
        state: StateStore,
        on_final: Callable[[ThreadInfo, str], None],
        poll_seconds: float = 0.75,
        thread_provider: Optional[Callable[[], str]] = None,
        threads_provider: Optional[Callable[[], Iterable[ThreadInfo]]] = None,
    ) -> None:
        self.config = config
        self.state = state
        self.on_final = on_final
        self.poll_seconds = poll_seconds
        self.thread_provider = thread_provider or (lambda: self.config.codex_thread_id)
        self.threads_provider = threads_provider
        self.stop_event = threading.Event()
        self.thread: Optional[threading.Thread] = None
        self.path: Optional[Path] = None
        self._paths: Dict[str, Path] = {}
        self._offsets: Dict[str, int] = {}
        self._infos: Dict[str, ThreadInfo] = {}
        stored = self.state.load()
        raw_offsets = stored.get("session_offsets")
        self._saved_offsets = raw_offsets if isinstance(raw_offsets, dict) else {}
        self._sessions_root = self.config.codex_sessions_dir.resolve()
        self._startup_sizes: Dict[str, int] = {}
        for startup_path in self._sessions_root.glob("**/rollout-*.jsonl"):
            try:
                resolved = startup_path.resolve()
                self._startup_sizes[str(resolved)] = resolved.stat().st_size
            except OSError:
                continue
        self._legacy_path = str(stored.get("session_path") or "")
        try:
            self._legacy_offset = int(stored.get("session_offset", 0))
        except (TypeError, ValueError):
            self._legacy_offset = 0
        self._busy = False
        self._status_lock = threading.Lock()

    def start(self) -> None:
        self.thread = threading.Thread(target=self._run, name="codex-session-monitor", daemon=True)
        self.thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=3)

    def is_busy(self) -> bool:
        with self._status_lock:
            return self._busy

    def mark_idle(self) -> None:
        self._set_busy(False)

    def _set_busy(self, value: bool) -> None:
        with self._status_lock:
            self._busy = value

    def _target_infos(self) -> List[ThreadInfo]:
        if self.threads_provider is not None:
            return list(self.threads_provider())
        thread_id = self.thread_provider()
        path = find_session_file(self.config.codex_sessions_dir, thread_id)
        if path is None:
            return []
        workdir = Path(getattr(self.config, "codex_workdir", path.parent)).resolve()
        return [ThreadInfo(thread_id, thread_id, workdir, rollout_path=path)]

    def _initial_offset(self, thread_id: str, path: Path) -> int:
        saved = self._saved_offsets.get(thread_id)
        if isinstance(saved, dict) and saved.get("path") == str(path):
            try:
                offset = int(saved.get("offset", 0))
            except (TypeError, ValueError):
                offset = 0
            return min(max(0, offset), path.stat().st_size)
        if self._legacy_path == str(path):
            return min(max(0, self._legacy_offset), path.stat().st_size)
        return min(self._startup_sizes.get(str(path), 0), path.stat().st_size)

    def _persist_offsets(self) -> None:
        serialized = dict(self._saved_offsets)
        serialized.update({
            thread_id: {"path": str(path), "offset": self._offsets[thread_id]}
            for thread_id, path in self._paths.items()
            if thread_id in self._offsets
        })
        changes: Dict[str, Any] = {"session_offsets": serialized}
        active_id = self.thread_provider()
        active_path = self._paths.get(active_id)
        if active_path is not None and active_id in self._offsets:
            changes.update(
                session_path=str(active_path),
                session_offset=self._offsets[active_id],
            )
        self.state.update(**changes)
        self._saved_offsets = serialized

    def _sync_targets(self) -> bool:
        discovered: Dict[str, ThreadInfo] = {}
        changed = False
        for info in self._target_infos():
            raw_path = info.rollout_path
            if raw_path is None:
                continue
            try:
                path = raw_path.resolve()
                path.relative_to(self._sessions_root)
            except (OSError, ValueError):
                continue
            if info.thread_id not in path.name or not path.is_file():
                continue
            discovered[info.thread_id] = info
            if self._paths.get(info.thread_id) != path:
                self._offsets[info.thread_id] = self._initial_offset(info.thread_id, path)
                changed = True
            self._paths[info.thread_id] = path
            self._infos[info.thread_id] = info

        for thread_id in set(self._paths) - set(discovered):
            self._paths.pop(thread_id, None)
            self._offsets.pop(thread_id, None)
            self._infos.pop(thread_id, None)
            changed = True

        active_path = self._paths.get(self.thread_provider())
        if active_path != self.path:
            self.path = active_path
            self._set_busy(session_busy(active_path) if active_path else False)
        return changed

    def _handle_record(self, info: ThreadInfo, record: Dict[str, Any]) -> None:
        payload = record.get("payload") if record.get("type") == "event_msg" else None
        if isinstance(payload, dict) and info.thread_id == self.thread_provider():
            event_type = payload.get("type")
            if event_type == "task_started":
                self._set_busy(True)
            elif event_type in {"task_complete", "turn_aborted", "turn_complete"}:
                self._set_busy(False)
        message = final_message_from_record(record)
        if message:
            try:
                self.on_final(info, message)
            except Exception as exc:
                print(f"[session-monitor] final callback: {exc}", flush=True)

    def _run(self) -> None:
        while not self.stop_event.is_set():
            changed = False
            try:
                changed = self._sync_targets()
                for thread_id, path in list(self._paths.items()):
                    offset = self._offsets[thread_id]
                    size = path.stat().st_size
                    if size < offset:
                        self._offsets[thread_id] = size
                        changed = True
                        continue
                    if size <= offset:
                        continue
                    with path.open("rb") as handle:
                        handle.seek(offset)
                        while True:
                            line_start = handle.tell()
                            line = handle.readline()
                            if not line:
                                break
                            if not line.endswith(b"\n"):
                                handle.seek(line_start)
                                break
                            try:
                                record = json.loads(line)
                            except (json.JSONDecodeError, UnicodeDecodeError):
                                continue
                            if isinstance(record, dict):
                                self._handle_record(self._infos[thread_id], record)
                        self._offsets[thread_id] = handle.tell()
                    changed = True
                if changed:
                    self._persist_offsets()
            except (OSError, sqlite3.Error) as exc:
                print(f"[session-monitor] {exc}", flush=True)
            self.stop_event.wait(self.poll_seconds)


def extract_final_from_codex_jsonl(output: str) -> Optional[str]:
    latest = None
    for line in output.splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(event, dict):
            continue
        item = event.get("item")
        if isinstance(item, dict) and item.get("type") in {"agent_message", "agentMessage"}:
            text = item.get("text") or item.get("message")
            if isinstance(text, str) and text.strip():
                latest = text.strip()
        message = event.get("message")
        if event.get("type") in {"agent_message", "agentMessage"}:
            if isinstance(message, str) and message.strip():
                latest = message.strip()
    return latest


def extract_thread_id_from_codex_jsonl(output: str) -> Optional[str]:
    for line in output.splitlines():
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(event, dict) or event.get("type") != "thread.started":
            continue
        for key in ("thread_id", "threadId", "id"):
            value = event.get(key)
            if isinstance(value, str) and THREAD_ID_RE.fullmatch(value):
                return value
        thread = event.get("thread")
        if isinstance(thread, dict):
            value = thread.get("id")
            if isinstance(value, str) and THREAD_ID_RE.fullmatch(value):
                return value
    return None


IGNORED_CODEX_WARNING_PARTS = (
    "codex_core_plugins::manifest:",
    "codex_core_plugins::manager:",
    "featured plugin",
)


def summarize_codex_error(stderr: str, max_chars: int = 1000) -> str:
    lines = []
    for raw_line in stderr.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if " WARN " in f" {line} " and any(
            marker in line for marker in IGNORED_CODEX_WARNING_PARTS
        ):
            continue
        lines.append(line)
    if not lines:
        return "没有错误详情"
    text = "\n".join(lines[-12:])
    return text[-max_chars:]


class CodexRunner:
    def __init__(self, config: Config) -> None:
        self.config = config
        self._lock = threading.Lock()
        self._process: Optional[subprocess.Popen[str]] = None

    def running(self) -> bool:
        with self._lock:
            return self._process is not None and self._process.poll() is None

    def cancel(self) -> bool:
        with self._lock:
            process = self._process
            if not process or process.poll() is not None:
                return False
            process.terminate()
            return True

    def _execute(
        self,
        command: List[str],
        workdir: Path,
        stdin_text: Optional[str] = None,
    ) -> Tuple[int, str, str]:
        if not workdir.is_dir():
            raise ValueError(f"任务工作目录不存在: {workdir}")
        process = subprocess.Popen(
            command,
            cwd=str(workdir),
            stdin=subprocess.PIPE if stdin_text is not None else subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        with self._lock:
            self._process = process
        try:
            try:
                if stdin_text is None:
                    stdout, stderr = process.communicate(
                        timeout=self.config.codex_turn_timeout
                    )
                else:
                    stdout, stderr = process.communicate(
                        input=stdin_text,
                        timeout=self.config.codex_turn_timeout,
                    )
            except subprocess.TimeoutExpired:
                process.terminate()
                try:
                    stdout, stderr = process.communicate(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    stdout, stderr = process.communicate()
                stderr = (
                    f"任务超过 {self.config.codex_turn_timeout} 秒，已终止。\n"
                    f"{stderr or ''}"
                )
            stdout_text = stdout or ""
            stderr_text = stderr or ""
            return process.returncode or 0, stdout_text, stderr_text.strip()
        finally:
            with self._lock:
                self._process = None

    def run(
        self,
        prompt: str,
        thread_id: Optional[str] = None,
        workdir: Optional[Path] = None,
        image_paths: Optional[List[Path]] = None,
    ) -> Tuple[int, Optional[str], str]:
        selected_thread_id = thread_id or self.config.codex_thread_id
        selected_workdir = workdir or self.config.codex_workdir
        command = [
            *self.config.codex_command,
            "exec",
            "resume",
            "--skip-git-repo-check",
            "--json",
            *self._image_args(image_paths),
            selected_thread_id,
            "-",
        ]
        code, stdout, stderr = self._execute(command, selected_workdir, prompt)
        return code, extract_final_from_codex_jsonl(stdout), stderr

    def run_new(
        self, prompt: str, workdir: Path, image_paths: Optional[List[Path]] = None
    ) -> Tuple[int, Optional[str], Optional[str], str]:
        command = [
            *self.config.codex_command,
            "exec",
            "--skip-git-repo-check",
            "--json",
            *self._image_args(image_paths),
            "-",
        ]
        code, stdout, stderr = self._execute(command, workdir, prompt)
        return (
            code,
            extract_thread_id_from_codex_jsonl(stdout),
            extract_final_from_codex_jsonl(stdout),
            stderr,
        )

    @staticmethod
    def _image_args(image_paths: Optional[List[Path]]) -> List[str]:
        args: List[str] = []
        for path in image_paths or []:
            args.extend(["-i", str(path)])
        return args


class CodexDesktopRefresher:
    """Select one Desktop task by UUID and rebuild the renderer's history cache."""

    def __init__(
        self,
        enabled: bool = True,
        cdp_url: str = "http://127.0.0.1:9229",
        delay_seconds: float = 0.8,
        timeout_seconds: float = 3.0,
    ) -> None:
        self.enabled = enabled
        self.cdp_url = cdp_url.rstrip("/")
        self.delay_seconds = max(0.0, delay_seconds)
        self.timeout_seconds = max(0.5, timeout_seconds)

    def _main_page(self) -> Optional[Dict[str, Any]]:
        with urlopen(f"{self.cdp_url}/json/list", timeout=self.timeout_seconds) as response:
            targets = json.load(response)
        if not isinstance(targets, list):
            return None
        for target in targets:
            if (
                isinstance(target, dict)
                and target.get("type") == "page"
                and target.get("url") == "app://-/index.html"
                and isinstance(target.get("webSocketDebuggerUrl"), str)
            ):
                return target
        return None

    @staticmethod
    def _request(
        connection: Any,
        request_id: int,
        method: str,
        params: Dict[str, Any],
    ) -> Dict[str, Any]:
        connection.send(
            json.dumps(
                {"id": request_id, "method": method, "params": params},
                ensure_ascii=True,
            )
        )
        while True:
            response = json.loads(connection.recv())
            if isinstance(response, dict) and response.get("id") == request_id:
                return response

    def refresh(self, thread_id: str) -> bool:
        if (
            not self.enabled
            or websocket is None
            or not THREAD_ID_RE.fullmatch(thread_id)
        ):
            return False
        connection = None
        try:
            page = self._main_page()
            if page is None:
                return False
            connection = websocket.create_connection(
                page["webSocketDebuggerUrl"],
                timeout=self.timeout_seconds,
                origin=self.cdp_url,
            )
            selector_id = json.dumps(f"local:{thread_id}")
            expression = (
                "(() => {"
                f"const target = {selector_id};"
                "const row = [...document.querySelectorAll("
                "'[data-app-action-sidebar-thread-id]')].find("
                "element => element.getAttribute('data-app-action-sidebar-thread-id') === target"
                ");"
                "if (!row) return {clicked: false};"
                "row.click();"
                "return {clicked: true};"
                "})()"
            )
            click_response = self._request(
                connection,
                1,
                "Runtime.evaluate",
                {"expression": expression, "returnByValue": True},
            )
            click_value = (
                click_response.get("result", {})
                .get("result", {})
                .get("value", {})
            )
            if not isinstance(click_value, dict) or click_value.get("clicked") is not True:
                return False
            time.sleep(self.delay_seconds)
            reload_response = self._request(
                connection,
                2,
                "Page.reload",
                {"ignoreCache": True},
            )
            return "error" not in reload_response
        except (OSError, ValueError, json.JSONDecodeError, websocket.WebSocketException):
            return False
        finally:
            if connection is not None:
                try:
                    connection.close()
                except Exception:
                    pass


def split_message(text: str, max_chars: int, max_chunks: int) -> List[str]:
    normalized = text.strip() or "(Codex 没有返回文本结果)"
    chunks = [normalized[index : index + max_chars] for index in range(0, len(normalized), max_chars)]
    if len(chunks) <= max_chunks:
        return chunks
    chunks = chunks[:max_chunks]
    suffix = "\n\n[内容过长，已截断；回到 Codex Desktop 可查看完整结果]"
    chunks[-1] = chunks[-1][: max(0, max_chars - len(suffix))] + suffix
    return chunks


MARKDOWN_IMAGE_RE = re.compile(r"!\[[^\]]*\]\((<[^>]+>|[^)\s]+)(?:\s+[^)]*)?\)")
LOCAL_PATH_RE = re.compile(r"(?<![\w:])/(?:Users|private|var|tmp)/[^\s)\]}>]+")
WINDOWS_QUOTED_LOCAL_PATH_RE = re.compile(
    r'''(?ix)(?:
        "(?:[a-z]:[\\/]|\\\\)[^"\r\n]+"
        | '(?:[a-z]:[\\/]|\\\\)[^'\r\n]+'
        | <(?:[a-z]:[\\/]|\\\\)[^>\r\n]+>
    )'''
)
WINDOWS_LOCAL_PATH_RE = re.compile(
    r'''(?ix)(?<![\w])(?:[a-z]:[\\/]|\\\\)[^\r\n)\]}>,'"，。；;]+'''
)
QQ_IMAGE_SUFFIXES = {".png", ".jpg", ".jpeg", ".gif", ".webp"}


def _is_within(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def qq_safe_final(
    message: str,
    workdir: Path,
    max_images: int = 3,
    visualizations_dir: Optional[Path] = None,
) -> Tuple[str, List[Path]]:
    """Extract safe local Markdown images and redact local paths from QQ text."""
    roots = [workdir.expanduser().resolve()]
    roots.append(
        (visualizations_dir or Path.home() / ".codex" / "visualizations")
        .expanduser()
        .resolve()
    )
    images: List[Path] = []

    def replace_image(match: re.Match[str]) -> str:
        raw_target = match.group(1).strip("<>")
        parsed = urlparse(raw_target)
        if parsed.scheme not in {"", "file"}:
            return match.group(0)
        raw_path = unquote(parsed.path if parsed.scheme == "file" else raw_target)
        candidate = Path(raw_path).expanduser()
        if not candidate.is_absolute():
            candidate = (roots[0] / candidate).resolve()
        else:
            candidate = candidate.resolve()
        allowed = any(_is_within(candidate, root) for root in roots)
        if (
            allowed
            and candidate.is_file()
            and candidate.suffix.lower() in QQ_IMAGE_SUFFIXES
            and candidate not in images
            and len(images) < max_images
        ):
            images.append(candidate)
            return "[图片将在下方发送]"
        return "[本地图片未转发]"

    text = MARKDOWN_IMAGE_RE.sub(replace_image, message)
    text = LOCAL_PATH_RE.sub("[本地路径已隐藏]", text)
    text = WINDOWS_QUOTED_LOCAL_PATH_RE.sub("[本地路径已隐藏]", text)
    text = WINDOWS_LOCAL_PATH_RE.sub("[本地路径已隐藏]", text)
    return text.strip(), images
