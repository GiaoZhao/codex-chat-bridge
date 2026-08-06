# Codex Chat Bridge

将本机 Codex Desktop 原生任务桥接到聊天平台。当前版本提供 QQ 官方机器人和
钉钉企业内部应用机器人的私聊适配器：

- 任一用户可见的 Codex Desktop 任务产生最终结果后，桥接器都把任务标题和结果主动发送
  到所有已绑定渠道；包括由 Codex 拉起的可见委派任务，guardian、内部 subagent 和已归档
  任务不会推送。
- 在 QQ 或钉钉私聊机器人发送普通文字后，macOS 可通过 standalone Codex shared daemon 直接在
  Desktop 对应任务中启动回合；shared daemon 不可用时可回退 `codex exec resume`。
- 可从任一已启用渠道列出桌面端任务、切换任务，或在当前项目目录中新建任务。
- QQ 回复优先使用 Markdown 卡片和快捷按钮，失败时自动回退到纯文本。
- 钉钉回复优先使用内置 AI 互动卡片，快捷操作显示为原生按钮并通过 Stream 回调；
  卡片不可用时自动降级为 Markdown，再降级为纯文本。
- QQ 图片可作为图片输入交给 Codex；Codex 明确返回的安全本地图片可上传回 QQ。
- 钉钉图片也可作为输入；当前不上传 Codex 返回的本地图片，而是提示回 Desktop 查看。
- 普通任务开始时，QQ 会显示“正在输入”状态；钉钉当前无对应状态。
- Bridge 不自动重载、切换或清理 Codex Desktop 页面缓存。使用 shared daemon 时，聊天渠道
  回合和任务标题会由 Codex 实时广播到 Desktop。
- 入站消息会先持久化再排队；当前任务执行中收到的新消息不会同时写入同一个会话。
- 每个渠道有独立绑定码和收件人标识，绑定后只接受该用户的私聊。

> QQ 已在 macOS 完成端到端验证。钉钉已完成适配器和自动化测试，但尚未用真实企业
> 应用做端到端验证；首次上线前需在钉钉开放平台再核对主动单聊权限和发布状态。
> Windows 核心桥接路径也尚未经过真机端到端验证。

## 渠道架构

`bridge.py` 只处理统一的 `InboundMessage`、`Recipient` 和 `ChatChannel`，不直接调用
QQ 或钉钉 SDK。`channel_factory.py` 是唯一的渠道装配点，按 `BRIDGE_CHANNELS`
从注册表创建适配器。QQ 和钉钉分别封装在 `qq_channel.py` 和
`dingtalk_gateway.py`，两者不相互 import。

每个渠道独立管理连接状态、绑定、消息限额、图片处理和 outbox 领取。某个渠道断线时，
另一个已就绪渠道仍会继续发送。各渠道操作的是同一个 Codex 任务索引、“当前任务”和串行
执行队列，因此从任一渠道执行 `/use` 都会改变其他渠道后续消息所进入的任务。

## 演进方向

- 在统一渠道接口上继续接入更多聊天平台。
- 增加 Windows/macOS 轻量控制客户端，用于 Bridge 配置、启停、状态监控和持久日志。
- 在 Codex 提供稳定的外部控制接口后，替换当前 CLI resume 与 Desktop 刷新方案。

已确认的客户端产品边界、电源管理行为和验收标准见
[Desktop Client Requirements](docs/desktop-client-requirements.md)。

## 当前实现边界

本实现已在 macOS、standalone Codex `0.146.0` 和 Codex Desktop shared daemon 上验证。
使用 shared daemon 时，Bridge 与 Desktop 连接同一个 app-server：QQ 发起的用户消息、
执行过程和最终回复会实时出现在 Desktop 对应任务中；`/new` 会把首条消息摘要显式设置为
任务标题。桌面任务仍保存在 `~/.codex/sessions` 原生会话文件中。

shared daemon 目前是 Codex 的实验能力，版本变化和账号策略可能带来兼容性差异。Bridge
手动运行时默认使用 `CODEX_TRANSPORT=auto`：macOS 检测到由官方 Codex 客户端建立的
control socket 时使用 `app-server`，否则回退 `codex exec`。LaunchAgent 默认使用
`exec`，不会自行 bootstrap shared daemon，避免 Bridge 被上游识别为非官方客户端。
`exec` 模式不保证 Desktop 实时显示 QQ 回合。

Windows 的配置、命令路径、PowerShell 启动和核心桥接路径已有兼容处理，但目前没有
Windows 真机端到端验证结论，且当前固定回退 `codex exec`，不能保证 Desktop 实时显示
QQ 新回合。旧路径只写入原生会话文件，Desktop 可能不会立刻刷新。Bridge 在所有传输模式
下都不连接调试端口、不操作页面、不清理缓存、不重载 renderer；`CODEX_DESKTOP_REFRESH`
只是保留配置，即使设为 `1` 也不会执行 Desktop 操作。

桥接器不会远程代替用户批准 Codex 命令或文件操作。shared daemon 请求审批时，QQ 会提示
打开 Desktop 对应任务处理；不要为方便而启用绕过沙箱的参数。

QQ Gateway 建立 READY 会话时的“Bridge 已上线”消息默认关闭，避免网络重连产生重复
通知。如需启动提醒，将 `.env` 中的 `QQ_NOTIFY_ON_READY` 设为 `1`；同一 Bridge 进程
即使重连多次也只发送一次。

QQ 发起任务时会依次区分“已接收并保存”“等待已有任务”和“Codex 回合已启动”；只有
成功创建 shared daemon 回合或 Codex 子进程后才会发送第三种回执。任务队列和待发送通知保存在
`data/bridge.sqlite3`。Bridge 运行期间遇到 QQ 或网络短暂断开时会继续重试；Bridge 重启时
会恢复尚未派发的任务，但会丢弃上个进程没有发出的旧通知，避免上线后集中补发。为避免
重复执行，已进入派发或运行阶段但被异常中断的任务不会自动重跑，而会在 QQ 中提示人工
核对。
`data/bridge.lock` 会阻止同一目录同时启动两个 Bridge 实例。

运行日志写入 `data/logs/bridge.jsonl`，单文件达到 2MB 后轮转，保留 5 份历史文件。
回执语义、重启恢复边界、错误通知能力和取日志步骤见
[Reliability and Troubleshooting](docs/reliability.md)。

全部任务通知使用 Codex 只读任务索引取得用户可见任务，并为每个 rollout 文件保存独立
字节游标。每次启动都在当时最后一条完整 JSONL 记录之后建立新基线，不补发 Bridge 停止
期间积累的历史回复；若启动瞬间最后一条记录仍在写入，则写完后仍会正常处理。通知卡片
会携带本轮问题节选；长 Markdown 按段落、完整链接、表格行和代码围栏安全分片，操作按钮
只出现在最后一片。无法与表头共同装入单片的极宽表格会明确降级为分段文本。通知卡片中
的“切换到此任务”按钮只改变 QQ 后续消息所操作的任务，收到通知本身不会自动切换。

任务列表只读查询 `~/.codex/state_5.sqlite`，保留由 Codex 拉起的用户可见委派任务，排除
已归档任务及 guardian/内部 subagent，不会修改 Codex 数据库。当前选中的任务和工作目录
保存在 `data/state.json`，桥接重启后继续使用。

## 前置条件

1. 运行桥接器的 Mac 或 Windows 电脑保持开机、联网且不进入睡眠。
2. 已安装 Python 3 和 standalone Codex，且 Codex 已登录并可通过 `codex` 命令启动。
3. 启用 QQ 时：已在 [QQ 开放平台](https://bot.q.qq.com/) 创建机器人，取得
   AppID 和 AppSecret，并开通 C2C 私聊消息事件。
4. 启用钉钉时：已在 [钉钉开放平台](https://open-dev.dingtalk.com/) 创建企业内部应用，
   添加机器人能力，并将消息接收模式设为 Stream 模式。

## 安装

### macOS

```bash
git clone https://github.com/GiaoZhao/codex-chat-bridge.git
cd codex-chat-bridge
./scripts/setup.sh
./.venv/bin/python configure.py
./scripts/run.sh --check
./scripts/macos-service.sh install
```

默认安装使用官方 `codex exec`，优先保证账号兼容性和后台可靠性。若账号允许 Bridge
作为 app-server 客户端，且 Desktop 已经建立 shared daemon，可改用
`./scripts/macos-service.sh install --transport auto`。`auto` 只接受 `userAgent` 以
`Codex Desktop/` 或 `codex-cli/` 开头的 daemon；socket 缺失或身份不符合时回退 `exec`。
`--transport app-server` 为严格模式，条件不满足时启动失败。Bridge 不会自行 bootstrap
daemon，也不会伪装成官方客户端。

LaunchAgent 使用 `RunAtLoad` 和 `KeepAlive` 管理 Bridge，登录后自动启动，异常退出后自动
拉起，不依赖终端或 Codex 工具会话。macOS 不允许后台 LaunchAgent 直接读取受保护的
`Documents` 目录，因此安装命令会把运行副本和依赖部署到
`~/Library/Application Support/CodexQQBridge`。常用管理命令：

```bash
./scripts/macos-service.sh status
./scripts/macos-service.sh restart
./scripts/macos-service.sh install --transport auto
./scripts/macos-service.sh uninstall
```

安装时如果检测到手动启动的 Bridge，会拒绝并提示先停止该实例，避免两个进程争用同一
状态库。`.env` 每次安装都会以 `0600` 同步到运行目录；`state.json`、SQLite 队列和历史
日志只在首次安装时迁移，后续重装不会覆盖运行状态。源码更新或源目录配置变化后，重新
执行 `install` 即可部署。LaunchAgent 配置写入
`~/Library/LaunchAgents/com.giaozhao.codex-chat-bridge.plist`，卸载命令会停止服务并删除
该文件，但保留 Application Support 中的 `.env`、绑定状态、任务队列和日志。

macOS 可改用安全弹窗输入凭证：

```bash
./.venv/bin/python configure.py --gui
```

AppSecret 输入框会隐藏内容，值不会出现在命令行或程序输出中。

`scripts/run.sh` 默认通过 macOS `caffeinate -i` 在桥接器运行期间阻止空闲睡眠，但允许
正常锁屏。LaunchAgent 也保留相同的防睡眠行为。临时手动运行时可使用
`CODEX_QQ_KEEP_AWAKE=0 ./scripts/run.sh` 关闭防睡眠。

### Windows PowerShell

```powershell
git clone https://github.com/GiaoZhao/codex-chat-bridge.git
Set-Location codex-chat-bridge
.\scripts\setup.ps1
.\.venv\Scripts\python.exe configure.py
.\scripts\run.ps1 --check
.\scripts\run.ps1
```

Windows 配置器使用终端输入，不支持 `configure.py --gui`。配置器在所有平台都写入
`CODEX_DESKTOP_REFRESH=0`；该预留开关当前不会触发 Desktop 操作。Windows 核心桥接
兼容实现尚未经过真机端到端验证，并会使用 `exec` 传输。建议先执行 `--check`，确认
Codex 命令、任务索引和会话文件均可读取后再启动。

`configure.py` 首先要求输入启用渠道：`qq`、`dingtalk` 或
`qq,dingtalk`，然后只询问已选渠道的凭证。它还会要求输入一个初始 Codex
Thread UUID 和该任务的工作目录。若不知道 Thread UUID，可在终端列出最近的
本地用户可见任务。

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

配置程序会为每个已启用渠道生成独立的八位绑定码。桥接器成功连接后，分别在
对应机器人的私聊中发送：

```text
/bind 你的八位绑定码
```

绑定信息按渠道保存在 `data/state.json`，AppSecret 保存在 `.env`；二者均被 `.gitignore`
排除。配置器在支持 POSIX 权限的平台将 `.env` 设为 `0600`；Windows 还应确保项目目录
只允许当前用户访问。

### 钉钉应用配置

1. 在钉钉开放平台创建企业内部应用，记录 Client ID（AppKey）和 Client Secret（AppSecret）。
2. 为应用添加机器人能力，把机器人消息接收模式设为 Stream 模式。Stream 模式由 Bridge
   主动建立长连接，不需要公网回调地址。
3. 确认机器人在测试用户的应用可见范围内，并按开放平台要求完成版本发布和主动单聊
   权限配置。
4. 在权限管理中开通“互动卡片实例写权限”，用于发送带原生按钮的 AI 互动卡片。
5. 运行 `configure.py`，启用 `dingtalk` 并输入上述凭证。配置器默认将
   `DINGTALK_ROBOT_CODE` 设为 Client ID；如开放平台显示的 Robot Code 不同，在 `.env`
   中修改该值。
6. 运行 `./scripts/run.sh --check`，然后启动 Bridge，在钉钉中私聊机器人并发送配置器
   生成的 `/bind` 命令。

关键环境变量如下：

```dotenv
BRIDGE_CHANNELS=qq,dingtalk
DINGTALK_CLIENT_ID=your-app-key
DINGTALK_CLIENT_SECRET=your-app-secret
DINGTALK_ROBOT_CODE=your-robot-code
DINGTALK_BIND_CODE=your-one-time-bind-code
```

当前钉钉适配器只处理 `conversationType=1` 的单聊消息，群聊事件会被忽略。主动回复使用
`/v1.0/robot/oToMessages/batchSend`；上真实企业环境前必须以当时的钉钉官方文档和
应用权限页为准再核对一次。

## 聊天命令

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
/cancel    取消由聊天渠道启动的当前任务
/help      查看帮助
```

`/threads` 中 `*` 表示当前任务。切换成功后会直接读取本地 JSONL，返回最后一条已有
Codex 最终回复的用户消息及对应最终回复；这段内容不调用模型、不总结、不改写。之后
该渠道后续的普通消息都会进入新选择的任务。
`/new` 必须带第一条消息；新任务标题取该消息的前 80 个字符。新建任务不等待当前或
其他 Desktop 任务结束；如果创建期间当前任务选择没有变化，创建完成后会自动切换到
新任务，否则保留用户后来选择的任务。`/use` 只检查准备切换到的目标任务：只要目标
任务空闲，即使当前任务或其他任务仍在执行，也可以立即切换；已经入队的消息仍保留
入队时的目标任务，不会因之后切换而串线。Bridge 自身仍使用单 worker，聊天渠道任务
之间按入队顺序串行执行。

QQ 入站图片仅接受 HTTPS 图片附件，默认每张最大 20MB、每条最多 3 张。临时下载文件
会在本次 Codex 执行结束后删除。Codex 回复中的 Markdown 本地图片只允许来自当前任务
目录或 `~/.codex/visualizations`，其他本地路径不会发送到 QQ。可通过
`QQ_ATTACHMENT_MAX_BYTES` 和 `QQ_ATTACHMENT_MAX_IMAGES` 下调限制。

钉钉入站图片通过官方 `downloadCode` 换取 HTTPS 地址，默认也是每张最大 20MB、每条
最多 3 张，可通过 `DINGTALK_ATTACHMENT_MAX_BYTES` 和
`DINGTALK_ATTACHMENT_MAX_IMAGES` 下调。钉钉尚不支持发送 Codex 返回的本地图片。

## 本地验证

macOS：

```bash
./.venv/bin/python -m unittest discover -s tests -v
./scripts/run.sh --check
./scripts/macos-service.sh status
```

Windows PowerShell：

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
.\scripts\run.ps1 --check
```

`--check` 只检查本机配置、依赖、Codex 命令和会话文件，不连接 QQ 或钉钉。
真正启动时才会使用各渠道凭证获取 token 并建立 Gateway/Stream 连接。

## 安全

- 只在渠道官方开放平台创建机器人；不要把 AppSecret、绑定码、用户标识或 `.env` 发到聊天。
- 首次绑定完成后，可删除 `.env` 中对应的 `QQ_BIND_CODE` 或
  `DINGTALK_BIND_CODE`，保留 `data/state.json` 的绑定状态。
- `/cancel` 只终止由聊天 Bridge 启动的 Codex 回合或子进程，不能取消 Desktop 发起的任务。
- 正常停止或升级桥接时会等待聊天渠道发起的 Codex 任务完成；再次发送停止信号才会强制
  取消当前回合，避免升级产生意外中断。
- 被动回复遵守 QQ 单条消息最多 4 次回复限制；更长内容会截断并提示回桌面端查看。
- 本项目不提供 `--dangerously-bypass-approvals-and-sandbox` 或远程权限切换。

QQ Gateway 的请求路径和事件结构参考了 QQ 官方机器人 API，以及公开项目
[G-Photon/codex-remote-bridge](https://github.com/G-Photon/codex-remote-bridge) 的协议实践；
本实现为面向本机 Codex Desktop 任务的独立最小实现。
