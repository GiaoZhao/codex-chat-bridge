from __future__ import annotations

import json
import hashlib
import ntpath
import nturl2path
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


THREAD_ID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)


_LOG_LOCK = threading.Lock()
_LOG_PATH: Optional[Path] = None
_LOG_MAX_BYTES = 2 * 1024 * 1024
_LOG_BACKUP_COUNT = 5


def configure_log_file(
    path: Path,
    max_bytes: int = _LOG_MAX_BYTES,
    backup_count: int = _LOG_BACKUP_COUNT,
) -> None:
    global _LOG_PATH, _LOG_MAX_BYTES, _LOG_BACKUP_COUNT
    resolved = path.expanduser().resolve()
    resolved.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(resolved.parent, 0o700)
    except OSError:
        pass
    _LOG_PATH = resolved
    _LOG_MAX_BYTES = max(64 * 1024, max_bytes)
    _LOG_BACKUP_COUNT = max(1, backup_count)


def _rotate_log(path: Path) -> None:
    if not path.exists() or path.stat().st_size < _LOG_MAX_BYTES:
        return
    oldest = path.with_name(f"{path.name}.{_LOG_BACKUP_COUNT}")
    if oldest.exists():
        oldest.unlink()
    for index in range(_LOG_BACKUP_COUNT - 1, 0, -1):
        source = path.with_name(f"{path.name}.{index}")
        if source.exists():
            source.replace(path.with_name(f"{path.name}.{index + 1}"))
    path.replace(path.with_name(f"{path.name}.1"))


def log_event(component: str, message: str, level: str = "INFO") -> None:
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    normalized_level = level.strip().upper() or "INFO"
    print(f"[{timestamp}] [{normalized_level}] [{component}] {message}", flush=True)
    path = _LOG_PATH
    if path is None:
        return
    record = json.dumps(
        {
            "timestamp": timestamp,
            "level": normalized_level,
            "component": component,
            "message": message,
        },
        ensure_ascii=False,
        separators=(",", ":"),
    )
    with _LOG_LOCK:
        try:
            _rotate_log(path)
            with path.open("a", encoding="utf-8") as handle:
                handle.write(record + "\n")
            os.chmod(path, 0o600)
        except OSError:
            # Logging must never terminate the Bridge; stdout remains available.
            return


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
    enabled_channels: Tuple[str, ...]
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
    dingtalk_client_id: str
    dingtalk_client_secret: str
    dingtalk_robot_code: str
    dingtalk_bind_code: str
    dingtalk_allowed_user_id: str
    dingtalk_api_base: str
    dingtalk_reply_chars: int
    dingtalk_reply_chunks: int
    dingtalk_attachment_max_bytes: int
    dingtalk_attachment_max_images: int
    dingtalk_notify_on_ready: bool
    codex_desktop_refresh: bool
    codex_desktop_cdp_url: str
    codex_desktop_refresh_delay_ms: int
    codex_thread_id: str
    codex_workdir: Path
    codex_command: Tuple[str, ...]
    codex_transport: str
    codex_app_server_socket: Path
    codex_sessions_dir: Path
    codex_state_db: Path
    codex_turn_timeout: int
    queue_wait_timeout: int

    @classmethod
    def from_env(
        cls,
        base_dir: Path,
        require_channel_credentials: bool = True,
    ) -> "Config":
        load_env(base_dir / ".env")
        raw_channels = os.getenv("BRIDGE_CHANNELS", "qq")
        enabled_channels = tuple(
            dict.fromkeys(
                value.strip().lower()
                for value in raw_channels.split(",")
                if value.strip()
            )
        )
        app_id = os.getenv("QQ_APP_ID", "").strip()
        app_secret = os.getenv("QQ_APP_SECRET", "").strip()
        dingtalk_client_id = os.getenv("DINGTALK_CLIENT_ID", "").strip()
        dingtalk_client_secret = os.getenv("DINGTALK_CLIENT_SECRET", "").strip()
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
        transport = os.getenv("CODEX_TRANSPORT", "auto").strip().lower() or "auto"
        app_server_socket = Path(
            os.getenv(
                "CODEX_APP_SERVER_SOCKET",
                str(
                    Path.home()
                    / ".codex"
                    / "app-server-control"
                    / "app-server-control.sock"
                ),
            )
        ).expanduser()
        desktop_cdp_url = os.getenv(
            "CODEX_DESKTOP_CDP_URL", "http://127.0.0.1:9229"
        ).strip().rstrip("/")

        errors: List[str] = []
        unknown_channels = set(enabled_channels) - {"qq", "dingtalk"}
        if not enabled_channels:
            errors.append("BRIDGE_CHANNELS 至少需要启用一个渠道")
        if unknown_channels:
            errors.append(
                "BRIDGE_CHANNELS 包含未知渠道: " + ", ".join(sorted(unknown_channels))
            )
        if require_channel_credentials and "qq" in enabled_channels and not app_id:
            errors.append("QQ_APP_ID 未配置")
        if require_channel_credentials and "qq" in enabled_channels and not app_secret:
            errors.append("QQ_APP_SECRET 未配置")
        if (
            require_channel_credentials
            and "dingtalk" in enabled_channels
            and not dingtalk_client_id
        ):
            errors.append("DINGTALK_CLIENT_ID 未配置")
        if (
            require_channel_credentials
            and "dingtalk" in enabled_channels
            and not dingtalk_client_secret
        ):
            errors.append("DINGTALK_CLIENT_SECRET 未配置")
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
        if transport not in {"auto", "app-server", "exec"}:
            errors.append("CODEX_TRANSPORT 只允许 auto、app-server 或 exec")
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
            enabled_channels=enabled_channels,
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
            dingtalk_client_id=dingtalk_client_id,
            dingtalk_client_secret=dingtalk_client_secret,
            dingtalk_robot_code=os.getenv("DINGTALK_ROBOT_CODE", "").strip(),
            dingtalk_bind_code=os.getenv("DINGTALK_BIND_CODE", "").strip(),
            dingtalk_allowed_user_id=os.getenv(
                "DINGTALK_ALLOWED_USER_ID", ""
            ).strip(),
            dingtalk_api_base=os.getenv(
                "DINGTALK_API_BASE", "https://api.dingtalk.com"
            ).rstrip("/"),
            dingtalk_reply_chars=env_int(
                "DINGTALK_REPLY_MAX_CHARS", 1800, 200, 4000
            ),
            dingtalk_reply_chunks=env_int(
                "DINGTALK_REPLY_MAX_CHUNKS", 8, 1, 20
            ),
            dingtalk_attachment_max_bytes=env_int(
                "DINGTALK_ATTACHMENT_MAX_BYTES",
                20 * 1024 * 1024,
                1024,
                20 * 1024 * 1024,
            ),
            dingtalk_attachment_max_images=env_int(
                "DINGTALK_ATTACHMENT_MAX_IMAGES", 3, 1, 5
            ),
            dingtalk_notify_on_ready=env_bool(
                "DINGTALK_NOTIFY_ON_READY", False
            ),
            codex_desktop_refresh=env_bool("CODEX_DESKTOP_REFRESH", False),
            codex_desktop_cdp_url=desktop_cdp_url,
            codex_desktop_refresh_delay_ms=env_int(
                "CODEX_DESKTOP_REFRESH_DELAY_MS", 800, 100, 5000
            ),
            codex_thread_id=thread_id,
            codex_workdir=workdir.resolve(),
            codex_command=command,
            codex_transport=transport,
            codex_app_server_socket=app_server_socket.resolve(),
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

    def switch_if_current(self, expected_thread_id: str, info: ThreadInfo) -> bool:
        if not THREAD_ID_RE.fullmatch(info.thread_id):
            raise ValueError("无效的 Codex Thread UUID")
        if not info.workdir.is_dir():
            raise ValueError(f"任务工作目录不存在: {info.workdir}")
        with self._lock:
            if self._info.thread_id != expected_thread_id:
                return False
            self._info = info
            self.state.update(
                active_thread_id=info.thread_id,
                active_workdir=str(info.workdir),
            )
            return True


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


def user_message_before_offset(path: Path, end_offset: int) -> str:
    """Return the latest user message before one rollout record offset."""
    latest = ""
    try:
        with path.open("rb") as handle:
            while handle.tell() < end_offset:
                line_start = handle.tell()
                line = handle.readline()
                if not line or line_start >= end_offset:
                    break
                try:
                    record = json.loads(line)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    continue
                payload = (
                    record.get("payload")
                    if isinstance(record, dict) and record.get("type") == "event_msg"
                    else None
                )
                if not isinstance(payload, dict) or payload.get("type") != "user_message":
                    continue
                message = payload.get("message")
                if isinstance(message, str) and message.strip():
                    latest = message.strip()
    except OSError:
        return ""
    return latest


def session_record_event_id(path: Path, offset: int, record: Dict[str, Any]) -> str:
    encoded = json.dumps(record, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    resolved_path = os.path.normcase(str(path.resolve()))
    raw = f"{resolved_path}:{offset}:{encoded}".encode("utf-8", errors="replace")
    return hashlib.sha256(raw).hexdigest()


def complete_jsonl_offset(path: Path) -> int:
    """Return the byte offset immediately after the last complete JSONL line."""
    size = path.stat().st_size
    if size == 0:
        return 0
    with path.open("rb") as handle:
        position = size
        while position > 0:
            chunk_size = min(8192, position)
            position -= chunk_size
            handle.seek(position)
            chunk = handle.read(chunk_size)
            newline = chunk.rfind(b"\n")
            if newline >= 0:
                return position + newline + 1
    return 0


class SessionMonitor:
    def __init__(
        self,
        config: Config,
        state: StateStore,
        on_final: Callable[[ThreadInfo, str], None],
        poll_seconds: float = 0.75,
        thread_provider: Optional[Callable[[], str]] = None,
        threads_provider: Optional[Callable[[], Iterable[ThreadInfo]]] = None,
        on_final_event: Optional[Callable[[ThreadInfo, str, str, str], None]] = None,
        on_interrupted: Optional[Callable[[ThreadInfo, str, str], None]] = None,
    ) -> None:
        self.config = config
        self.state = state
        self.on_final = on_final
        self.poll_seconds = poll_seconds
        self.thread_provider = thread_provider or (lambda: self.config.codex_thread_id)
        self.threads_provider = threads_provider
        self.on_final_event = on_final_event
        self.on_interrupted = on_interrupted
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
        self._startup_offsets: Dict[str, int] = {}
        for startup_path in self._sessions_root.glob("**/rollout-*.jsonl"):
            try:
                resolved = startup_path.resolve()
                self._startup_offsets[str(resolved)] = complete_jsonl_offset(resolved)
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
            self.thread.join()

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
        startup_offset = self._startup_offsets.pop(str(path), None)
        if startup_offset is not None:
            return min(max(0, startup_offset), path.stat().st_size)
        saved = self._saved_offsets.get(thread_id)
        if isinstance(saved, dict) and saved.get("path") == str(path):
            try:
                offset = int(saved.get("offset", 0))
            except (TypeError, ValueError):
                offset = 0
            return min(max(0, offset), path.stat().st_size)
        if self._legacy_path == str(path):
            return min(max(0, self._legacy_offset), path.stat().st_size)
        return 0

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

    @staticmethod
    def _record_event_id(path: Path, offset: int, record: Dict[str, Any]) -> str:
        return session_record_event_id(path, offset, record)

    def _handle_record(
        self,
        info: ThreadInfo,
        record: Dict[str, Any],
        path: Path,
        offset: int,
    ) -> None:
        payload = record.get("payload") if record.get("type") == "event_msg" else None
        event_id = self._record_event_id(path, offset, record)
        if isinstance(payload, dict) and info.thread_id == self.thread_provider():
            event_type = payload.get("type")
            if event_type == "task_started":
                self._set_busy(True)
            elif event_type in {"task_complete", "turn_aborted", "turn_complete"}:
                self._set_busy(False)
        if (
            isinstance(payload, dict)
            and payload.get("type") == "turn_aborted"
            and self.on_interrupted is not None
        ):
            raw_detail = payload.get("reason") or payload.get("message") or "Codex 任务已中断"
            detail = str(raw_detail).strip()[:400] or "Codex 任务已中断"
            self.on_interrupted(info, detail, event_id)
        message = final_message_from_record(record)
        if message:
            if self.on_final_event is not None:
                self.on_final_event(
                    info,
                    message,
                    event_id,
                    user_message_before_offset(path, offset),
                )
            else:
                self.on_final(info, message)

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
                                self._handle_record(
                                    self._infos[thread_id],
                                    record,
                                    path,
                                    line_start,
                                )
                        self._offsets[thread_id] = handle.tell()
                    changed = True
                if changed:
                    self._persist_offsets()
            except (OSError, sqlite3.Error) as exc:
                log_event("session-monitor", str(exc), level="ERROR")
            except Exception as exc:
                log_event("session-monitor", str(exc), level="ERROR")
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
    "codex_core_plugins::remote::remote_installed_plugin_sync:",
    "codex_core_plugins::startup_sync:",
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
    transport_name = "exec"

    def __init__(self, config: Config) -> None:
        self.config = config
        self._lock = threading.Lock()
        self._process: Optional[subprocess.Popen[str]] = None
        self._termination_reason = ""

    def running(self) -> bool:
        with self._lock:
            return self._process is not None and self._process.poll() is None

    def cancel(self) -> bool:
        with self._lock:
            process = self._process
            if not process or process.poll() is not None:
                return False
            self._termination_reason = "cancelled"
            process.terminate()
            return True

    def termination_reason(self) -> str:
        with self._lock:
            return self._termination_reason

    def _execute(
        self,
        command: List[str],
        workdir: Path,
        stdin_text: Optional[str] = None,
        on_started: Optional[Callable[[int], None]] = None,
        on_diagnostic: Optional[Callable[[str], None]] = None,
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
            encoding="utf-8",
            errors="replace",
        )
        with self._lock:
            self._process = process
            self._termination_reason = ""
        if on_started is not None:
            try:
                on_started(process.pid)
            except Exception:
                try:
                    process.terminate()
                    process.communicate()
                finally:
                    with self._lock:
                        self._process = None
                raise
        try:
            if on_diagnostic is not None:
                stdout, stderr = self._communicate_streaming(
                    process,
                    stdin_text,
                    on_diagnostic,
                )
            else:
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
                    with self._lock:
                        if not self._termination_reason:
                            self._termination_reason = "timed_out"
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

    def _communicate_streaming(
        self,
        process: subprocess.Popen[str],
        stdin_text: Optional[str],
        on_diagnostic: Callable[[str], None],
    ) -> Tuple[str, str]:
        stdout_parts: List[str] = []
        stderr_parts: List[str] = []

        def read_stream(stream: Any, target: List[str], diagnostic: bool) -> None:
            if stream is None:
                return
            try:
                for line in stream:
                    target.append(line)
                    if diagnostic and line.strip():
                        try:
                            on_diagnostic(line.strip())
                        except Exception as exc:
                            log_event(
                                "codex-diagnostic",
                                f"callback failed: {exc}",
                                level="ERROR",
                            )
            except (OSError, ValueError) as exc:
                log_event("codex-stream", str(exc), level="ERROR")

        readers = [
            threading.Thread(
                target=read_stream,
                args=(process.stdout, stdout_parts, False),
                name="codex-stdout",
                daemon=True,
            ),
            threading.Thread(
                target=read_stream,
                args=(process.stderr, stderr_parts, True),
                name="codex-stderr",
                daemon=True,
            ),
        ]
        for reader in readers:
            reader.start()
        if stdin_text is not None and process.stdin is not None:
            try:
                process.stdin.write(stdin_text)
                process.stdin.close()
            except (BrokenPipeError, OSError) as exc:
                stderr_parts.append(f"无法向 Codex 写入任务内容: {exc}\n")
        try:
            process.wait(timeout=self.config.codex_turn_timeout)
        except subprocess.TimeoutExpired:
            with self._lock:
                if not self._termination_reason:
                    self._termination_reason = "timed_out"
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
            stderr_parts.insert(
                0,
                f"任务超过 {self.config.codex_turn_timeout} 秒，已终止。\n",
            )
        for reader in readers:
            reader.join()
        return "".join(stdout_parts), "".join(stderr_parts)

    def run(
        self,
        prompt: str,
        thread_id: Optional[str] = None,
        workdir: Optional[Path] = None,
        image_paths: Optional[List[Path]] = None,
        on_started: Optional[Callable[[int], None]] = None,
        on_diagnostic: Optional[Callable[[str], None]] = None,
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
        if on_started is None and on_diagnostic is None:
            code, stdout, stderr = self._execute(command, selected_workdir, prompt)
        else:
            code, stdout, stderr = self._execute(
                command,
                selected_workdir,
                prompt,
                on_started=on_started,
                on_diagnostic=on_diagnostic,
            )
        return code, extract_final_from_codex_jsonl(stdout), stderr

    def run_new(
        self,
        prompt: str,
        workdir: Path,
        image_paths: Optional[List[Path]] = None,
        on_started: Optional[Callable[[int], None]] = None,
        on_diagnostic: Optional[Callable[[str], None]] = None,
        thread_name: Optional[str] = None,
    ) -> Tuple[int, Optional[str], Optional[str], str]:
        del thread_name
        command = [
            *self.config.codex_command,
            "exec",
            "--skip-git-repo-check",
            "--json",
            *self._image_args(image_paths),
            "-",
        ]
        if on_started is None and on_diagnostic is None:
            code, stdout, stderr = self._execute(command, workdir, prompt)
        else:
            code, stdout, stderr = self._execute(
                command,
                workdir,
                prompt,
                on_started=on_started,
                on_diagnostic=on_diagnostic,
            )
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
    """Fail closed until Desktop exposes a non-destructive synchronization API."""

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
        self.last_detail = ""

    def refresh(self, thread_id: str, workdir: Optional[Path] = None) -> bool:
        if not self.enabled:
            self.last_detail = "disabled"
            return False
        self.last_detail = (
            "automatic Desktop sync unavailable: no non-destructive Desktop API"
        )
        return False


_FENCE_OPEN_RE = re.compile(r"^\s*(`{3,}|~{3,})")
_TABLE_DIVIDER_RE = re.compile(
    r"^\s*\|?\s*:?-{3,}:?\s*(?:\|\s*:?-{3,}:?\s*)+\|?\s*$"
)
_INLINE_CODE_RE = re.compile(r"(`+)[^\n]*?\1")
_REFERENCE_LINK_RE = re.compile(
    r"(?<!\\)!?\[(?:\\.|[^\]\\\n])+\]\[(?:\\.|[^\]\\\n])*\]"
)
_AUTOLINK_RE = re.compile(r"(?<!\\)<(?:https?://|mailto:)[^<>\s]+>", re.IGNORECASE)
_BARE_URL_START_RE = re.compile(r"(?<![A-Za-z0-9_<])https?://", re.IGNORECASE)


def _markdown_link_spans(text: str) -> List[Tuple[int, int]]:
    spans: List[Tuple[int, int]] = []
    index = 0
    while index < len(text):
        start = index
        bracket = index
        if text[index] == "!" and index + 1 < len(text) and text[index + 1] == "[":
            bracket = index + 1
        elif text[index] != "[":
            index += 1
            continue

        slash_count = 0
        cursor = start - 1
        while cursor >= 0 and text[cursor] == "\\":
            slash_count += 1
            cursor -= 1
        if slash_count % 2:
            index = bracket + 1
            continue

        cursor = bracket + 1
        bracket_depth = 1
        while cursor < len(text) and bracket_depth:
            char = text[cursor]
            if char == "\n":
                break
            if char == "\\":
                cursor += 2
                continue
            if char == "[":
                bracket_depth += 1
            elif char == "]":
                bracket_depth -= 1
            cursor += 1
        if bracket_depth or cursor >= len(text) or text[cursor] != "(":
            index = bracket + 1
            continue

        cursor += 1
        paren_depth = 1
        while cursor < len(text) and paren_depth:
            char = text[cursor]
            if char == "\n":
                break
            if char == "\\":
                cursor += 2
                continue
            if char == "(":
                paren_depth += 1
            elif char == ")":
                paren_depth -= 1
            cursor += 1
        if paren_depth:
            index = bracket + 1
            continue
        spans.append((start, cursor))
        index = cursor
    return spans


def _bare_url_spans(text: str) -> List[Tuple[int, int]]:
    spans: List[Tuple[int, int]] = []
    for match in _BARE_URL_START_RE.finditer(text):
        end = match.end()
        while end < len(text) and not text[end].isspace() and text[end] not in '<>"\'`':
            end += 1
        while end > match.end() and text[end - 1] in ".,;:!?":
            end -= 1
        for opening, closing in (("(", ")"), ("[", "]"), ("{", "}")):
            while (
                end > match.end()
                and text[end - 1] == closing
                and text[match.start() : end].count(closing)
                > text[match.start() : end].count(opening)
            ):
                end -= 1
        if end > match.end():
            spans.append((match.start(), end))
    return spans


def _markdown_atomic_spans(text: str) -> List[Tuple[int, int]]:
    matches = [(match.start(), match.end()) for match in _INLINE_CODE_RE.finditer(text)]
    matches.extend(_markdown_link_spans(text))
    matches.extend((match.start(), match.end()) for match in _REFERENCE_LINK_RE.finditer(text))
    matches.extend((match.start(), match.end()) for match in _AUTOLINK_RE.finditer(text))
    matches.extend(_bare_url_spans(text))
    spans: List[Tuple[int, int]] = []
    for span in sorted(matches, key=lambda value: (value[0], -value[1])):
        if spans and span[0] < spans[-1][1]:
            continue
        spans.append(span)
    return spans


def _safe_inline_cut(text: str, max_chars: int) -> int:
    spans = _markdown_atomic_spans(text)

    def is_safe(position: int) -> bool:
        return not any(start < position < end for start, end in spans)

    candidates: List[int] = []
    prefix = text[: max_chars + 1]
    for match in re.finditer(r"\n\n|\n|[。！？!?；;，,]\s*|\s+", prefix):
        position = match.end()
        if 0 < position <= max_chars and is_safe(position):
            candidates.append(position)
    if candidates:
        return max(candidates)
    if is_safe(max_chars):
        return max_chars
    for start, end in spans:
        if start < max_chars < end:
            return start
    return max_chars


def _split_inline(text: str, max_chars: int) -> List[str]:
    pieces: List[str] = []
    remaining = text.strip()
    while len(remaining) > max_chars:
        cut = _safe_inline_cut(remaining, max_chars)
        if cut <= 0:
            spans = _markdown_atomic_spans(remaining)
            oversized_end = spans[0][1] if spans and spans[0][0] == 0 else max_chars
            pieces.append("[单个 Markdown 片段超过 QQ 单条限制，已省略]")
            remaining = remaining[oversized_end:].lstrip()
            continue
        piece = remaining[:cut].rstrip()
        if piece:
            pieces.append(piece)
        remaining = remaining[cut:].lstrip()
    if remaining:
        pieces.append(remaining)
    return pieces


def _split_plain_lines(lines: List[str], max_chars: int) -> List[str]:
    pieces: List[str] = []
    current = ""
    for line in lines:
        if len(line) > max_chars:
            if current:
                pieces.append(current)
                current = ""
            pieces.extend(_split_inline(line, max_chars))
            continue
        candidate = f"{current}\n{line}" if current else line
        if len(candidate) <= max_chars:
            current = candidate
        else:
            if current:
                pieces.append(current)
            current = line
    if current:
        pieces.append(current)
    return pieces


def _split_fenced_block(lines: List[str], max_chars: int) -> List[str]:
    opening = lines[0]
    match = _FENCE_OPEN_RE.match(opening)
    if match is None:
        return _split_plain_lines(lines, max_chars)
    marker = match.group(1)
    has_closing = len(lines) > 1 and re.fullmatch(
        rf"\s*{re.escape(marker[0])}{{{len(marker)},}}\s*", lines[-1]
    )
    closing = lines[-1] if has_closing else marker
    content_lines = lines[1:-1] if has_closing else lines[1:]
    overhead = len(opening) + len(closing) + 2
    available = max_chars - overhead
    if available < 1:
        fallback = ["[代码块围栏过长，已转为分段文本]"]
        info = opening[match.end() :].strip()
        if info:
            fallback.append(f"代码块信息：{info}")
        fallback.extend(content_lines)
        return _split_plain_lines(fallback, max_chars)
    content = "\n".join(content_lines)
    if not content:
        return [f"{opening}\n{closing}"]
    raw_parts: List[str] = []
    remaining = content
    while len(remaining) > available:
        cut = remaining.rfind("\n", 0, available + 1)
        if cut <= 0:
            cut = available
        raw_parts.append(remaining[:cut].rstrip("\n"))
        remaining = remaining[cut:].lstrip("\n")
    if remaining:
        raw_parts.append(remaining)
    return [f"{opening}\n{part}\n{closing}" for part in raw_parts]


def _split_table_block(lines: List[str], max_chars: int) -> List[str]:
    header = "\n".join(lines[:2])
    rows = lines[2:]
    if len(header) > max_chars or any(
        len(header) + 1 + len(row) > max_chars for row in rows
    ):
        fallback = [
            "[表格过宽，已转为分段文本]",
            f"- 表头：{lines[0].strip().strip('|').strip()}",
        ]
        fallback.extend(
            f"- 第 {index} 行：{row.strip().strip('|').strip()}"
            for index, row in enumerate(rows, start=1)
        )
        return _split_plain_lines(fallback, max_chars)
    if not rows:
        return [header]
    pieces: List[str] = []
    current = ""
    for row in rows:
        candidate = f"{current}\n{row}" if current else f"{header}\n{row}"
        if len(candidate) <= max_chars:
            current = candidate
        else:
            pieces.append(current)
            current = f"{header}\n{row}"
    if current:
        pieces.append(current)
    return pieces


def _markdown_blocks(text: str) -> List[Tuple[str, List[str]]]:
    lines = text.splitlines()
    blocks: List[Tuple[str, List[str]]] = []
    index = 0
    while index < len(lines):
        if not lines[index].strip():
            index += 1
            continue
        fence_match = _FENCE_OPEN_RE.match(lines[index])
        if fence_match is not None:
            marker = fence_match.group(1)
            block = [lines[index]]
            index += 1
            while index < len(lines):
                block.append(lines[index])
                index += 1
                if re.fullmatch(
                    rf"\s*{re.escape(marker[0])}{{{len(marker)},}}\s*", block[-1]
                ):
                    break
            blocks.append(("fence", block))
            continue
        if (
            index + 1 < len(lines)
            and "|" in lines[index]
            and _TABLE_DIVIDER_RE.fullmatch(lines[index + 1])
        ):
            block = [lines[index], lines[index + 1]]
            index += 2
            while index < len(lines) and lines[index].strip() and "|" in lines[index]:
                block.append(lines[index])
                index += 1
            blocks.append(("table", block))
            continue
        block = [lines[index]]
        index += 1
        while index < len(lines) and lines[index].strip():
            if _FENCE_OPEN_RE.match(lines[index]):
                break
            if (
                index + 1 < len(lines)
                and "|" in lines[index]
                and _TABLE_DIVIDER_RE.fullmatch(lines[index + 1])
            ):
                break
            block.append(lines[index])
            index += 1
        blocks.append(("plain", block))
    return blocks


def _split_markdown_chunks(text: str, max_chars: int) -> List[str]:
    segments: List[str] = []
    for kind, lines in _markdown_blocks(text):
        if kind == "fence":
            segments.extend(_split_fenced_block(lines, max_chars))
        elif kind == "table":
            segments.extend(_split_table_block(lines, max_chars))
        else:
            segments.extend(_split_plain_lines(lines, max_chars))

    chunks: List[str] = []
    current = ""
    for segment in segments:
        candidate = f"{current}\n\n{segment}" if current else segment
        if len(candidate) <= max_chars:
            current = candidate
        else:
            if current:
                chunks.append(current)
            current = segment
    if current:
        chunks.append(current)
    return chunks


def split_message(text: str, max_chars: int, max_chunks: int) -> List[str]:
    normalized = text.strip() or "(Codex 没有返回文本结果)"
    chunks = _split_markdown_chunks(normalized, max_chars)
    if len(chunks) <= max_chunks:
        return chunks

    suffix = "[内容过长，已截断；回到 Codex Desktop 可查看完整结果]"
    kept = chunks[:max_chunks]
    available = max_chars - len(suffix) - 2
    if available > 0:
        final_parts = _split_markdown_chunks(kept[-1], available)
        prefix = final_parts[0] if final_parts else ""
        kept[-1] = f"{prefix}\n\n{suffix}" if prefix else suffix
    else:
        kept[-1] = suffix[:max_chars]
    return kept


MARKDOWN_IMAGE_RE = re.compile(r"!\[[^\]]*\]\((<[^>]+>|[^)\s]+)(?:\s+[^)]*)?\)")
LOCAL_PATH_RE = re.compile(r"(?<![\w:])/(?:Users|private|var|tmp)/[^\s)\]}>]+")
WINDOWS_ABSOLUTE_PATH_RE = re.compile(r"(?i)^(?:[a-z]:[\\/]|\\\\)")
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


def _local_path_from_target(
    raw_target: str,
    windows: Optional[bool] = None,
) -> Optional[str]:
    use_windows_rules = os.name == "nt" if windows is None else windows
    if use_windows_rules and WINDOWS_ABSOLUTE_PATH_RE.match(raw_target):
        return unquote(raw_target)

    parsed = urlparse(raw_target)
    if parsed.scheme not in {"", "file"}:
        return None
    if parsed.scheme != "file":
        return unquote(raw_target)
    if not use_windows_rules:
        return unquote(parsed.path)

    url_path = parsed.path
    if parsed.netloc and parsed.netloc.lower() != "localhost":
        url_path = f"//{parsed.netloc}{parsed.path}"
    try:
        return nturl2path.url2pathname(url_path)
    except OSError:
        return None


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
        raw_path = _local_path_from_target(raw_target)
        if raw_path is None:
            return match.group(0)
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
