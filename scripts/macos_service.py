#!/usr/bin/env python3
import argparse
import os
import plistlib
import shlex
import shutil
import sqlite3
import subprocess
import sys
import sysconfig
import tempfile
import time
from pathlib import Path
from typing import Dict, List, Optional

try:
    import fcntl
except ImportError:  # pragma: no cover - unavailable on Windows
    fcntl = None  # type: ignore


LABEL = "com.giaozhao.codex-chat-bridge"
RUNTIME_DIRECTORY_NAME = "CodexQQBridge"
TRANSPORTS = ("exec", "auto", "app-server")
RUNTIME_SOURCE_FILES = (
    "bridge.py",
    "bridge_core.py",
    "bridge_store.py",
    "channel_factory.py",
    "chat_channel.py",
    "codex_app_server.py",
    "dingtalk_gateway.py",
    "qq_channel.py",
    "qq_gateway.py",
)


def project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def launch_domain() -> str:
    return f"gui/{os.getuid()}"


def service_target() -> str:
    return f"{launch_domain()}/{LABEL}"


def launch_agent_path(user_home: Optional[Path] = None) -> Path:
    home = (user_home or Path.home()).resolve()
    return home / "Library" / "LaunchAgents" / f"{LABEL}.plist"


def runtime_root(user_home: Optional[Path] = None) -> Path:
    home = (user_home or Path.home()).resolve()
    return home / "Library" / "Application Support" / RUNTIME_DIRECTORY_NAME


def dependency_source() -> Path:
    return Path(sysconfig.get_paths()["purelib"]).resolve()


def dotenv_value(path: Path, key: str) -> Optional[str]:
    if not path.exists():
        return None
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        name, separator, raw_value = line.partition("=")
        if separator and name.strip() == key:
            return raw_value.strip()
    return None


def resolve_codex_binary(base_dir: Path) -> Path:
    raw_command = (
        os.environ.get("CODEX_COMMAND")
        or dotenv_value(base_dir / ".env", "CODEX_COMMAND")
        or "codex"
    )
    command = shlex.split(raw_command, posix=True)
    if not command:
        raise RuntimeError("CODEX_COMMAND 为空")
    executable = Path(command[0]).expanduser()
    if executable.parent != Path("."):
        candidate = executable.absolute()
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return candidate
    discovered = shutil.which(command[0])
    if discovered:
        return Path(discovered).absolute()
    raise RuntimeError(f"未找到 Codex 可执行文件：{command[0]}")


def validate_codex_queue(command: List[str]) -> None:
    try:
        completed = subprocess.run(
            [*command, "queue", "--help"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=10,
            check=False,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise RuntimeError(f"无法检查 `codex queue`：{exc}") from exc
    if completed.returncode != 0:
        detail = (completed.stdout or "").strip()[-500:] or "未知错误"
        raise RuntimeError(f"Codex CLI 不支持 `codex queue`：{detail}")


def launch_runtime(runtime_dir: Path, python_bin: Path) -> int:
    transport = (
        os.environ.get("CODEX_TRANSPORT", "auto").strip().lower()
        or "auto"
    )
    if transport not in TRANSPORTS:
        raise RuntimeError(f"不支持的 Codex 传输方式：{transport}")
    os.environ["CODEX_TRANSPORT"] = transport
    raw_command = os.environ.get("CODEX_COMMAND", "codex")
    command = shlex.split(raw_command, posix=True)
    if not command:
        raise RuntimeError("CODEX_COMMAND 为空")
    validate_codex_queue(command)
    if transport == "app-server":
        launchctl(["setenv", "CODEX_APP_SERVER_USE_LOCAL_DAEMON", "1"])
        subprocess.run(
            [*command, "app-server", "daemon", "start"],
            check=True,
            timeout=30,
        )
    else:
        launchctl(
            ["unsetenv", "CODEX_APP_SERVER_USE_LOCAL_DAEMON"],
            check=False,
        )
    os.execv(
        "/usr/bin/caffeinate",
        [
            "/usr/bin/caffeinate",
            "-i",
            str(python_bin),
            str(runtime_dir / "bridge.py"),
        ],
    )
    return 0  # pragma: no cover - os.execv replaces this process


def build_plist(
    runtime_dir: Path,
    python_bin: Path,
    codex_bin: Path,
    transport: str = "auto",
) -> Dict[str, object]:
    if transport not in TRANSPORTS:
        raise ValueError(f"不支持的 Codex 传输方式：{transport}")
    resolved = runtime_dir.resolve()
    logs_dir = resolved / "data" / "logs"
    return {
        "Label": LABEL,
        "ProgramArguments": [
            str(python_bin.resolve()),
            str(resolved / "macos_service.py"),
            "_launch",
        ],
        "WorkingDirectory": str(resolved),
        "EnvironmentVariables": {
            "CODEX_COMMAND": str(codex_bin),
            "CODEX_TRANSPORT": transport,
            "PYTHONPATH": str(resolved / "vendor"),
            "PYTHONUNBUFFERED": "1",
        },
        "RunAtLoad": True,
        "KeepAlive": True,
        "ThrottleInterval": 10,
        "ProcessType": "Background",
        "StandardOutPath": str(logs_dir / "launchd.stdout.log"),
        "StandardErrorPath": str(logs_dir / "launchd.stderr.log"),
    }


def copy_private_file(source: Path, target: Path) -> None:
    descriptor, raw_path = tempfile.mkstemp(
        prefix=f".{target.name}.",
        dir=str(target.parent),
    )
    temporary = Path(raw_path)
    try:
        with source.open("rb") as input_handle, os.fdopen(descriptor, "wb") as output_handle:
            shutil.copyfileobj(input_handle, output_handle)
        os.chmod(temporary, 0o600)
        os.replace(temporary, target)
    finally:
        if temporary.exists():
            temporary.unlink()


def migrate_data(source_dir: Path, runtime_dir: Path) -> None:
    source_data = source_dir / "data"
    target_data = runtime_dir / "data"
    if target_data.exists():
        return

    stage = Path(tempfile.mkdtemp(prefix=".data.", dir=str(runtime_dir)))
    try:
        if source_data.exists():
            shutil.copytree(source_data, stage, dirs_exist_ok=True)
        for name in (
            "bridge.lock",
            "bridge.sqlite3",
            "bridge.sqlite3-shm",
            "bridge.sqlite3-wal",
        ):
            candidate = stage / name
            if candidate.exists():
                candidate.unlink()
        logs_dir = stage / "logs"
        for name in ("launchd.stdout.log", "launchd.stderr.log"):
            candidate = logs_dir / name
            if candidate.exists():
                candidate.unlink()

        source_db = source_data / "bridge.sqlite3"
        if source_db.exists():
            source_uri = source_db.resolve().as_uri() + "?mode=ro"
            with sqlite3.connect(source_uri, uri=True) as source_connection:
                with sqlite3.connect(stage / "bridge.sqlite3") as target_connection:
                    source_connection.backup(target_connection)
            os.chmod(stage / "bridge.sqlite3", 0o600)
        state_path = stage / "state.json"
        if state_path.exists():
            os.chmod(state_path, 0o600)
        os.replace(stage, target_data)
    finally:
        if stage.exists():
            shutil.rmtree(stage)


def deploy_runtime(source_dir: Path, runtime_dir: Path, dependencies: Path) -> None:
    runtime_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(runtime_dir, 0o700)
    for name in RUNTIME_SOURCE_FILES:
        shutil.copy2(source_dir / name, runtime_dir / name)
    shutil.copy2(source_dir / "scripts" / "macos_service.py", runtime_dir / "macos_service.py")

    copy_private_file(source_dir / ".env", runtime_dir / ".env")
    migrate_data(source_dir, runtime_dir)

    vendor_dir = runtime_dir / "vendor"
    if vendor_dir.exists():
        shutil.rmtree(vendor_dir)
    shutil.copytree(dependencies, vendor_dir)
    (runtime_dir / "data" / "logs").mkdir(parents=True, exist_ok=True)


def write_plist(path: Path, payload: Dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, raw_path = tempfile.mkstemp(
        prefix=f".{LABEL}.",
        suffix=".plist",
        dir=str(path.parent),
    )
    temporary = Path(raw_path)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            plistlib.dump(payload, handle, fmt=plistlib.FMT_XML, sort_keys=False)
        os.chmod(temporary, 0o600)
        subprocess.run(
            ["/usr/bin/plutil", "-lint", str(temporary)],
            check=True,
        )
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def launchctl(arguments: List[str], check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["/bin/launchctl", *arguments],
        check=check,
        text=True,
    )


def service_loaded() -> bool:
    completed = subprocess.run(
        ["/bin/launchctl", "print", service_target()],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    return completed.returncode == 0


def instance_lock_held(lock_path: Path) -> bool:
    if fcntl is None:
        raise RuntimeError("当前平台不支持文件锁")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o600)
    try:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return True
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        return False
    finally:
        os.close(descriptor)


def wait_for_instance_stop(lock_path: Path, timeout: float = 15.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not instance_lock_held(lock_path):
            return True
        time.sleep(0.2)
    return not instance_lock_held(lock_path)


def validate_installation(base_dir: Path) -> None:
    if sys.platform != "darwin":
        raise RuntimeError("macOS LaunchAgent 管理仅支持 macOS")
    required = [
        base_dir / ".venv" / "bin" / "python",
        base_dir / ".env",
        base_dir / "scripts" / "run.sh",
    ]
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        raise RuntimeError("缺少必要文件：" + ", ".join(missing))


def install(
    source_dir: Path,
    runtime_dir: Path,
    plist_path: Path,
    transport: str = "auto",
) -> int:
    validate_installation(source_dir)
    if transport not in TRANSPORTS:
        raise RuntimeError(f"不支持的 Codex 传输方式：{transport}")
    codex_bin = resolve_codex_binary(source_dir)
    source_lock = source_dir / "data" / "bridge.lock"
    runtime_lock = runtime_dir / "data" / "bridge.lock"
    loaded = service_loaded()
    if not loaded and runtime_lock.exists() and instance_lock_held(runtime_lock):
        raise RuntimeError(
            "检测到未由 LaunchAgent 管理的 Bridge 实例，请先停止后再安装"
        )
    if source_lock.exists() and instance_lock_held(source_lock):
        raise RuntimeError(
            "检测到从源码目录运行的 Bridge 实例，请先停止后再安装"
        )

    validate_codex_queue([str(codex_bin)])
    if transport == "app-server":
        launchctl(["setenv", "CODEX_APP_SERVER_USE_LOCAL_DAEMON", "1"])
        subprocess.run(
            [str(codex_bin), "app-server", "daemon", "bootstrap"],
            check=True,
            timeout=30,
        )

    if loaded:
        launchctl(["bootout", service_target()])
        if runtime_lock.exists() and not wait_for_instance_stop(runtime_lock):
            raise RuntimeError("现有 LaunchAgent 未释放 Bridge 锁")

    deploy_runtime(source_dir, runtime_dir, dependency_source())
    write_plist(
        plist_path,
        build_plist(
            runtime_dir,
            Path(sys.executable).resolve(),
            codex_bin,
            transport,
        ),
    )
    launchctl(["bootstrap", launch_domain(), str(plist_path)])
    launchctl(["enable", service_target()])
    launchctl(["kickstart", "-k", service_target()])
    print(f"已安装并启动 {service_target()}")
    print(f"配置文件：{plist_path}")
    print(f"运行目录：{runtime_dir}")
    print(f"传输方式：{transport}")
    return 0


def restart() -> int:
    if not service_loaded():
        raise RuntimeError("LaunchAgent 尚未加载，请先运行 install")
    launchctl(["kickstart", "-k", service_target()])
    print(f"已重启 {service_target()}")
    return 0


def uninstall(runtime_dir: Path, plist_path: Path) -> int:
    lock_path = runtime_dir / "data" / "bridge.lock"
    if service_loaded():
        launchctl(["bootout", service_target()])
        if lock_path.exists() and not wait_for_instance_stop(lock_path):
            raise RuntimeError("LaunchAgent 未释放 Bridge 锁")
    if plist_path.exists():
        plist_path.unlink()
    print(f"已卸载 {service_target()}")
    print(f"运行数据保留在 {runtime_dir}")
    return 0


def status() -> int:
    if not service_loaded():
        print(f"尚未加载：{service_target()}")
        return 1
    return launchctl(["print", service_target()]).returncode


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="管理 macOS QQ Bridge LaunchAgent")
    parser.add_argument("action", choices=("install", "restart", "status", "uninstall"))
    parser.add_argument(
        "--transport",
        choices=TRANSPORTS,
        default="auto",
        help="LaunchAgent 使用的 Codex 传输方式（默认：auto）",
    )
    return parser.parse_args()


def main() -> int:
    if sys.argv[1:] == ["_launch"]:
        try:
            return launch_runtime(
                Path(__file__).resolve().parent,
                Path(sys.executable).resolve(),
            )
        except (OSError, RuntimeError, subprocess.CalledProcessError) as exc:
            print(f"服务操作失败：{exc}", file=sys.stderr)
            return 1

    args = parse_args()
    source_dir = project_root()
    runtime_dir = runtime_root()
    plist_path = launch_agent_path()
    try:
        if args.action == "install":
            return install(source_dir, runtime_dir, plist_path, args.transport)
        if args.action == "restart":
            return restart()
        if args.action == "uninstall":
            return uninstall(runtime_dir, plist_path)
        return status()
    except (OSError, RuntimeError, subprocess.CalledProcessError) as exc:
        print(f"服务操作失败：{exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
