# Codex Chat Bridge

将本机 Codex Desktop 原生任务桥接到聊天平台。当前版本提供 QQ 官方机器人私聊
适配器：

- 任一可见的 Codex Desktop 根任务产生最终结果后，桥接器都把任务标题和结果主动发送
  到已绑定 QQ；guardian、subagent 和已归档任务不会推送。
- QQ 私聊机器人发送普通文字后，桥接器使用当前所选 Thread UUID 执行
  `codex exec resume`，继续对应的原生 Codex 会话。
- 可从 QQ 列出桌面端任务、切换任务，或在当前项目目录中新建任务。
- QQ 回复优先使用 Markdown 卡片和快捷按钮，失败时自动回退到纯文本。
- QQ 图片可作为图片输入交给 Codex；Codex 明确返回的安全本地图片可上传回 QQ。
- 普通任务开始时会显示 QQ“正在输入”状态。
- 在启用 Desktop 刷新时，QQ 发起的回合结束后通过仅监听本机的 Chrome DevTools
  Protocol 按 Thread UUID 选择目标任务并重载渲染页，使 Desktop 重新读取最新历史；
  不会重启 Codex 主进程或 app-server，也不修改 Codex 数据库。
- 当前任务执行中收到的新消息会排队，避免同时写入同一个会话。
- 首次使用一次性绑定码，绑定后只接受该 QQ `openid`。

> 当前实际可用渠道仅为 QQ。macOS 已完成端到端验证；Windows 已加入核心桥接兼容实现，
> 但尚未经过 Windows 真机端到端验证。项目名称使用 `codex-chat-bridge`，为后续增加
> 其他聊天渠道和管理界面保留空间。

## 演进方向

- 抽离统一渠道接口，逐步接入更多聊天平台。
- 增加 Web 管理界面，用于会话、渠道、权限和通知规则配置。
- 在 Codex 提供稳定的外部控制接口后，替换当前 CLI resume 与 Desktop 刷新方案。

## 当前实现边界

本实现已在 macOS 与 Codex CLI `0.146.0-alpha.3.1` 上验证按 UUID 续接会话，桌面任务
保存在 `~/.codex/sessions` 原生会话文件中。其他系统和版本可能存在兼容性差异。
Windows 的配置、命令路径、PowerShell 启动和核心桥接路径已有兼容处理，但目前没有
Windows 真机验证结论。Codex Desktop 的内部 Socket 不是可直接复用的公开 app-server
control socket，因此本版本使用“监听会话文件 + CLI resume”。

QQ 回复会写入所选任务的原生会话。Codex Desktop 不会订阅外部 CLI 连接的回合事件。
macOS 默认在 QQ 回合结束后连接 `127.0.0.1:9229`，按 UUID 点击侧栏任务并执行一次
renderer reload；Windows 的 Desktop 刷新尚未验证，因此默认关闭。该操作会让页面
闪烁，并可能清除 Desktop 输入框中尚未发送的草稿。可通过
`CODEX_DESKTOP_REFRESH=0` 或 `1` 显式关闭或开启；Windows 开启前需自行确认 Desktop
调试端口可用。找不到目标侧栏行或调试端口不可用时会安全跳过，不会刷新其他任务。
桥接器不会接管 Codex Desktop 原生命令级审批；需要人工批准的操作仍可能在桌面端
等待。不要为方便而启用绕过沙箱的参数。

QQ Gateway 建立 READY 会话时的“Bridge 已上线”消息默认关闭，避免网络重连产生重复
通知。如需启动提醒，将 `.env` 中的 `QQ_NOTIFY_ON_READY` 设为 `1`；同一 Bridge 进程
即使重连多次也只发送一次。

全部任务通知使用 Codex 只读任务索引取得可见根任务，并为每个 rollout 文件保存独立
字节游标。首次启用时只建立当前文件尾部基线，不补发历史；之后桥接短暂重启期间新增
的最终回复会从已保存游标继续读取。通知卡片中的“切换到此任务”按钮只改变 QQ 后续
消息所操作的任务，收到通知本身不会自动切换。

任务列表只读查询 `~/.codex/state_5.sqlite`，排除已归档任务及 guardian/subagent 等内部
任务，不会修改 Codex 数据库。当前选中的任务和工作目录保存在 `data/state.json`，桥接
重启后继续使用。

## 前置条件

1. 运行桥接器的 Mac 或 Windows 电脑保持开机、联网且不进入睡眠。
2. 已安装 Python 3 和 Codex CLI，且 Codex CLI 已登录并可通过 `codex` 命令启动。
3. 已在 [QQ 开放平台](https://bot.q.qq.com/) 创建机器人，取得 AppID 和 AppSecret，
   并开通 C2C 私聊消息事件。

macOS 如需 Desktop 自动同步，完全退出 Codex 后使用以下本机调试参数启动一次：

```bash
/usr/bin/open -a /Applications/ChatGPT.app --args \
  --remote-debugging-port=9229 \
  --remote-allow-origins=http://127.0.0.1:9229
```

## 安装

### macOS

```bash
git clone https://github.com/GiaoZhao/codex-chat-bridge.git
cd codex-chat-bridge
./scripts/setup.sh
./.venv/bin/python configure.py
./scripts/run.sh --check
./scripts/run.sh
```

macOS 可改用安全弹窗输入凭证：

```bash
./.venv/bin/python configure.py --gui
```

AppSecret 输入框会隐藏内容，值不会出现在命令行或程序输出中。

`scripts/run.sh` 默认通过 macOS `caffeinate -i` 在桥接器运行期间阻止空闲睡眠，但允许
正常锁屏。临时关闭这一行为可使用 `CODEX_QQ_KEEP_AWAKE=0 ./scripts/run.sh`。

### Windows PowerShell

```powershell
git clone https://github.com/GiaoZhao/codex-chat-bridge.git
Set-Location codex-chat-bridge
.\scripts\setup.ps1
.\.venv\Scripts\python.exe configure.py
.\scripts\run.ps1 --check
.\scripts\run.ps1
```

Windows 配置器使用终端输入，不支持 `configure.py --gui`。配置器在 macOS 写入
`CODEX_DESKTOP_REFRESH=1`，在 Windows 和其他平台写入 `0`；可随后在 `.env` 中显式
调整。Windows 核心桥接兼容实现尚未经过真机端到端验证，建议先执行 `--check`，确认
Codex 命令、任务索引和会话文件均可读取后再启动。

`configure.py` 会要求输入 QQ AppID、AppSecret、一个初始 Codex Thread UUID，以及该
任务的工作目录。若不知道 Thread UUID，可在终端列出最近的本地根任务。

macOS：

```bash
sqlite3 -header -column ~/.codex/state_5.sqlite \
  "SELECT id, cwd, substr(replace(preview, char(10), ' '), 1, 60) AS preview \
   FROM threads \
   WHERE archived = 0 AND preview <> '' AND source NOT LIKE '%subagent%' \
   ORDER BY updated_at DESC LIMIT 10;"
```

Windows PowerShell（使用 Python 标准库，不要求额外安装 `sqlite3` 命令）：

```powershell
@'
import sqlite3
from pathlib import Path

database = Path.home() / ".codex" / "state_5.sqlite"
query = """
SELECT id, cwd, substr(replace(preview, char(10), ' '), 1, 60) AS preview
FROM threads
WHERE archived = 0 AND preview <> '' AND source NOT LIKE '%subagent%'
ORDER BY updated_at DESC LIMIT 10
"""
with sqlite3.connect(f"{database.as_uri()}?mode=ro", uri=True) as connection:
    for row in connection.execute(query):
        print(" | ".join(str(value) for value in row))
'@ | .\.venv\Scripts\python.exe -
```

复制目标任务的 `id` 和 `cwd`，分别填入 Thread UUID 和工作目录。该查询只读，不会
修改 Codex 数据库。

配置程序会在本机生成八位绑定码。桥接器成功连接 QQ Gateway 后，私聊机器人发送：

```text
/bind 你的八位绑定码
```

绑定信息保存在 `data/state.json`，AppSecret 保存在 `.env`；二者均被 `.gitignore`
排除。配置器在支持 POSIX 权限的平台将 `.env` 设为 `0600`；Windows 还应确保项目目录
只允许当前用户访问。

## QQ 命令

```text
普通文字/图片  继续当前 Codex 任务
/threads    列出最近的 Codex 任务（每页 8 个）
/threads 2  查看第 2 页
/use 3      切换到第 3 个任务，并返回其最新完整一轮原文
/use UUID   按完整 Thread UUID 切换，并返回最新完整一轮原文
/current    查看当前任务、项目和工作目录
/new 内容   在当前项目目录中新建任务，并把“内容”作为第一条消息
/status    查看 Codex、队列和会话状态
/recent    查看最近一次最终结果
/cancel    取消由 QQ 启动的当前任务
/help      查看帮助
```

`/threads` 中 `*` 表示当前任务。切换成功后会直接读取本地 JSONL，返回最后一条已有
Codex 最终回复的用户消息及对应最终回复；这段内容不调用模型、不总结、不改写。之后
的普通 QQ 消息都会进入新选择的任务。
`/new` 必须带第一条消息，因为 Codex CLI 通过首条消息创建任务；新任务完成后会自动
设为当前任务。为防止不同任务串线，执行中或队列非空时不能 `/use` 或 `/new`。

QQ 入站图片仅接受 HTTPS 图片附件，默认每张最大 20MB、每条最多 3 张。临时下载文件
会在本次 Codex 执行结束后删除。Codex 回复中的 Markdown 本地图片只允许来自当前任务
目录或 `~/.codex/visualizations`，其他本地路径不会发送到 QQ。可通过
`QQ_ATTACHMENT_MAX_BYTES` 和 `QQ_ATTACHMENT_MAX_IMAGES` 下调限制。

## 本地验证

macOS：

```bash
./.venv/bin/python -m unittest discover -s tests -v
./scripts/run.sh --check
```

Windows PowerShell：

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
.\scripts\run.ps1 --check
```

`--check` 只检查本机配置、依赖、Codex 命令和会话文件，不连接 QQ。真正启动时才会用
AppID/AppSecret 获取 QQ token 并连接官方 WebSocket Gateway。

## 安全

- 只在 QQ 官方平台创建机器人；不要把 AppSecret、绑定码、`openid` 或 `.env` 发到聊天。
- 首次绑定完成后，可删除 `.env` 中的 `QQ_BIND_CODE`，保留 `data/state.json` 的绑定状态。
- `/cancel` 只终止由 QQ 桥接器启动的 Codex 子进程，不能取消桌面 App 发起的任务。
- 正常停止或升级桥接时会等待 QQ 发起的 Codex 任务完成；再次发送停止信号才会强制
  取消当前子进程，避免升级产生 `-15` 中断。
- 被动回复遵守 QQ 单条消息最多 4 次回复限制；更长内容会截断并提示回桌面端查看。
- 本项目不提供 `--dangerously-bypass-approvals-and-sandbox` 或远程权限切换。
- Desktop 调试端口必须只监听 `127.0.0.1`；配置会拒绝远程 CDP 地址。

QQ Gateway 的请求路径和事件结构参考了 QQ 官方机器人 API，以及公开项目
[G-Photon/codex-remote-bridge](https://github.com/G-Photon/codex-remote-bridge) 的协议实践；
本实现为面向本机 Codex Desktop 任务的独立最小实现。
