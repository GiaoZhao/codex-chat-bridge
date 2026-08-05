#!/usr/bin/env python3
from __future__ import annotations

import argparse
import getpass
import os
import secrets
import shutil
import subprocess
import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
DEFAULT_THREAD_ID = ""
DEFAULT_WORKDIR = ""
DEFAULT_QQ_APP_ID = ""
DEFAULT_CHANNELS = "qq"


def ask(label: str, default: str = "", secret: bool = False) -> str:
    suffix = f" [{default}]" if default else ""
    prompt = f"{label}{suffix}: "
    value = getpass.getpass(prompt) if secret else input(prompt)
    return value.strip() or default


def require_single_line(name: str, value: str) -> str:
    if not value or "\n" in value or "\r" in value:
        raise ValueError(f"{name} 不能为空且必须是单行文本")
    return value


def gui_ask(label: str, default: str = "", secret: bool = False) -> str:
    script = r'''
on run argv
    set promptText to item 1 of argv
    set defaultText to item 2 of argv
    set hiddenFlag to item 3 of argv
    if hiddenFlag is "1" then
        set dialogResult to display dialog promptText default answer defaultText with hidden answer buttons {"取消", "确定"} default button "确定" cancel button "取消"
    else
        set dialogResult to display dialog promptText default answer defaultText buttons {"取消", "确定"} default button "确定" cancel button "取消"
    end if
    return text returned of dialogResult
end run
'''
    result = subprocess.run(
        ["osascript", "-e", script, label, default, "1" if secret else "0"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise KeyboardInterrupt("用户取消配置")
    return result.stdout.rstrip("\n").strip() or default


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Configure chat adapters for Codex Chat Bridge"
    )
    parser.add_argument("--gui", action="store_true", help="use secure macOS dialogs")
    args = parser.parse_args()
    if args.gui and sys.platform != "darwin":
        print("配置错误：--gui 仅支持 macOS；请移除 --gui 使用终端配置。", file=sys.stderr)
        return 2
    env_path = BASE_DIR / ".env"
    if env_path.exists():
        if args.gui:
            answer = gui_ask(".env 已存在。输入 YES 确认覆盖", "")
        else:
            answer = input(".env 已存在，是否覆盖？输入 YES 继续: ").strip()
        if answer != "YES":
            print("已取消。")
            return 1

    ask_value = gui_ask if args.gui else ask
    raw_channels = require_single_line(
        "Bridge channels",
        ask_value("启用渠道，逗号分隔（qq,dingtalk）", DEFAULT_CHANNELS),
    )
    channels = tuple(
        dict.fromkeys(
            value.strip().lower()
            for value in raw_channels.split(",")
            if value.strip()
        )
    )
    unknown = set(channels) - {"qq", "dingtalk"}
    if not channels or unknown:
        detail = ", ".join(sorted(unknown)) if unknown else "空配置"
        raise ValueError(f"启用渠道无效: {detail}")

    app_id = ""
    app_secret = ""
    if "qq" in channels:
        app_id = require_single_line(
            "QQ AppID", ask_value("QQ Bot AppID", DEFAULT_QQ_APP_ID)
        )
        app_secret = require_single_line(
            "QQ AppSecret", ask_value("QQ Bot AppSecret", secret=True)
        )
    dingtalk_client_id = ""
    dingtalk_client_secret = ""
    if "dingtalk" in channels:
        dingtalk_client_id = require_single_line(
            "DingTalk Client ID",
            ask_value("钉钉应用 Client ID（AppKey）"),
        )
        dingtalk_client_secret = require_single_line(
            "DingTalk Client Secret",
            ask_value("钉钉应用 Client Secret（AppSecret）", secret=True),
        )
    thread_id = require_single_line(
        "Codex Thread ID", ask_value("Codex Thread ID", DEFAULT_THREAD_ID)
    )
    workdir = require_single_line(
        "Codex Workdir", ask_value("Codex 工作目录", DEFAULT_WORKDIR)
    )
    command = shutil.which("codex") or "codex"
    qq_bind_code = "".join(secrets.choice("0123456789") for _ in range(8))
    dingtalk_bind_code = "".join(
        secrets.choice("0123456789") for _ in range(8)
    )
    desktop_refresh = "0"

    values = {
        "BRIDGE_CHANNELS": ",".join(channels),
        "QQ_APP_ID": app_id,
        "QQ_APP_SECRET": app_secret,
        "QQ_BIND_CODE": qq_bind_code if "qq" in channels else "",
        "QQ_ALLOWED_OPENID": "",
        "QQ_NOTIFY_ON_READY": "0",
        "DINGTALK_CLIENT_ID": dingtalk_client_id,
        "DINGTALK_CLIENT_SECRET": dingtalk_client_secret,
        "DINGTALK_ROBOT_CODE": dingtalk_client_id,
        "DINGTALK_BIND_CODE": (
            dingtalk_bind_code if "dingtalk" in channels else ""
        ),
        "DINGTALK_ALLOWED_USER_ID": "",
        "DINGTALK_NOTIFY_ON_READY": "0",
        "CODEX_THREAD_ID": thread_id,
        "CODEX_WORKDIR": workdir,
        "CODEX_COMMAND": command,
        "CODEX_TRANSPORT": "auto",
        "CODEX_APP_SERVER_SOCKET": "~/.codex/app-server-control/app-server-control.sock",
        "CODEX_DESKTOP_REFRESH": desktop_refresh,
        "QQ_REPLY_MAX_CHARS": "1500",
        "QQ_REPLY_MAX_CHUNKS": "8",
        "QQ_ATTACHMENT_MAX_BYTES": "20971520",
        "QQ_ATTACHMENT_MAX_IMAGES": "3",
        "DINGTALK_REPLY_MAX_CHARS": "1800",
        "DINGTALK_REPLY_MAX_CHUNKS": "8",
        "DINGTALK_ATTACHMENT_MAX_BYTES": "20971520",
        "DINGTALK_ATTACHMENT_MAX_IMAGES": "3",
        "CODEX_TURN_TIMEOUT": "7200",
        "CODEX_QUEUE_WAIT_TIMEOUT": "21600",
    }
    content = "\n".join(f"{key}={value}" for key, value in values.items()) + "\n"
    env_path.write_text(content, encoding="utf-8")
    os.chmod(env_path, 0o600)
    print(f"配置已写入：{env_path}")
    if "qq" in channels:
        print(f"QQ 首次绑定码：{qq_bind_code}")
        print("启动后，在 QQ 私聊机器人发送：/bind " + qq_bind_code)
    if "dingtalk" in channels:
        print(f"钉钉首次绑定码：{dingtalk_bind_code}")
        print("启动后，在钉钉私聊机器人发送：/bind " + dingtalk_bind_code)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("配置已取消。")
        raise SystemExit(1)
