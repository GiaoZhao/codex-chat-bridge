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
    "codex_app_server.py",
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
        raise RuntimeError("CODEX_COMMAND is empty")
    executable = Path(command[0]).expanduser()
    if executable.parent != Path("."):
        candidate = executable.absolute()
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return candidate
    discovered = shutil.which(command[0])
    if discovered:
        return Path(discovered).absolute()
    raise RuntimeError(f"Codex executable not found: {command[0]}")


def launch_runtime(runtime_dir: Path, python_bin: Path) -> int:
    transport = os.environ.get("CODEX_TRANSPORT", "exec").strip().lower() or "exec"
    if transport not in TRANSPORTS:
        raise RuntimeError(f"unsupported Codex transport: {transport}")
    os.environ["CODEX_TRANSPORT"] = transport
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
    transport: str = "exec",
) -> Dict[str, object]:
    if transport not in TRANSPORTS:
        raise ValueError(f"unsupported Codex transport: {transport}")
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
        raise RuntimeError("file locking is unavailable on this platform")
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
        raise RuntimeError("macOS LaunchAgent management is only available on macOS")
    required = [
        base_dir / ".venv" / "bin" / "python",
        base_dir / ".env",
        base_dir / "scripts" / "run.sh",
    ]
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        raise RuntimeError("missing required files: " + ", ".join(missing))


def install(
    source_dir: Path,
    runtime_dir: Path,
    plist_path: Path,
    transport: str = "exec",
) -> int:
    validate_installation(source_dir)
    codex_bin = resolve_codex_binary(source_dir)
    source_lock = source_dir / "data" / "bridge.lock"
    runtime_lock = runtime_dir / "data" / "bridge.lock"
    if service_loaded():
        launchctl(["bootout", service_target()])
        if runtime_lock.exists() and not wait_for_instance_stop(runtime_lock):
            raise RuntimeError("existing LaunchAgent did not release the Bridge lock")
    elif runtime_lock.exists() and instance_lock_held(runtime_lock):
        raise RuntimeError(
            "an unmanaged Bridge instance is running; stop it before installing the LaunchAgent"
        )
    if source_lock.exists() and instance_lock_held(source_lock):
        raise RuntimeError(
            "a Bridge instance is running from the source directory; stop it before installing"
        )

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
    print(f"Installed and started {service_target()}")
    print(f"Plist: {plist_path}")
    print(f"Runtime: {runtime_dir}")
    print(f"Transport: {transport}")
    return 0


def restart() -> int:
    if not service_loaded():
        raise RuntimeError("LaunchAgent is not loaded; run install first")
    launchctl(["kickstart", "-k", service_target()])
    print(f"Restarted {service_target()}")
    return 0


def uninstall(runtime_dir: Path, plist_path: Path) -> int:
    lock_path = runtime_dir / "data" / "bridge.lock"
    if service_loaded():
        launchctl(["bootout", service_target()])
        if lock_path.exists() and not wait_for_instance_stop(lock_path):
            raise RuntimeError("LaunchAgent did not release the Bridge lock")
    if plist_path.exists():
        plist_path.unlink()
    print(f"Uninstalled {service_target()}")
    print(f"Runtime data preserved at {runtime_dir}")
    return 0


def status() -> int:
    if not service_loaded():
        print(f"Not loaded: {service_target()}")
        return 1
    return launchctl(["print", service_target()]).returncode


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Manage the macOS QQ Bridge LaunchAgent")
    parser.add_argument("action", choices=("install", "restart", "status", "uninstall"))
    parser.add_argument(
        "--transport",
        choices=TRANSPORTS,
        default="exec",
        help="Codex transport used by the LaunchAgent (default: exec)",
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
            print(f"Service operation failed: {exc}", file=sys.stderr)
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
        print(f"Service operation failed: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
