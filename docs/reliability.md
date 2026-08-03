# 可靠性与故障排查

## 回执含义

QQ 发起的任务会经过以下可区分阶段：

1. `已接收并保存`：消息和附件已写入本机持久队列，但 Codex 尚未启动。
2. `已保存并进入等待队列`：检测到已有 Codex 任务，当前任务继续等待。
3. `Codex 共享回合已启动` 或 `Codex 子进程已启动`：shared daemon 已接受回合，或本机
   已成功创建 Codex CLI 子进程，任务开始执行。

最终还会发送完成、失败、超时或取消结果。“已接收”不再表示 Codex 已经开始执行。

## 持久化与重启恢复

任务队列和待发送通知保存在 `data/bridge.sqlite3`，同一目录由
`data/bridge.lock` 保证只运行一个 Bridge 实例。

- `queued`、`waiting`、`preparing`：Bridge 重启后自动重新排队。
- `dispatching`、`running`：重启后标记为 `interrupted`，不会自动重跑，避免同一远程操作
  被执行两次；用户会收到人工核对提示。
- 当前 Bridge 进程创建但尚未发送的通知：QQ Gateway 在同一进程内恢复 READY 后重试。
- 上个 Bridge 进程遗留的 `pending`、`sending` 通知：启动时标记为 `discarded`，不再补发。
- `sent` 和 `discarded` 通知：保留 30 天用于去重和审计，之后自动清理。

任务启动或终止状态与对应通知在同一个 SQLite 事务中写入，不能出现“状态已经结束、通知
却没有入队”的半完成结果。长消息的多个分片属于同一发送组；前一片未成功时，后一片
不会越过它发送。同一终态即使被 worker 和会话监听器同时发现，也只接受第一组完整通知，
不会把两个版本的分片混在一起。

如果终态和通知连续两次无法写入数据库，Bridge 会受控退出，不会继续运行并吞掉该任务。
任务保留在 `dispatching` 或 `running`；下次启动时按不确定任务转为 `interrupted`，提示人工
核对，不会自动重跑。

通知只在同一 Bridge 进程内采用重试策略。跨进程重启不补发，因此进程退出前尚未发送、
或 QQ 已接收但本地尚未来得及标记成功的通知，重启后都会进入 `discarded`，不会造成上线
后的集中补发；对应代价是这类通知可能丢失。

输入图片保存在 `data/attachments`，直到对应任务进入终态后才清理。Bridge 在尚未派发时
正常退出，会保留任务和附件供下次启动继续执行。

最终回复中需要转发的图片会先复制到 `data/outbox-images`，因此原始任务附件清理后仍可
在当前进程内重试发送。图片发送成功后会删除对应副本；重启时遗留图片通知会被丢弃，
对应副本随后清理。

每次启动时，会话监听器以当时已有 rollout 文件最后一条完整 JSONL 记录之后为新基线，
不读取 Bridge 停止期间新增的最终回复。如果启动瞬间最后一条记录仍未写完，则不会跳过
这条半成品；它在启动后写完时仍会实时处理。启动后创建的新 rollout 或追加的新最终回复
也会实时处理。

最终通知包含同一轮用户问题的节选。纯图片输入使用实际送给 Codex 的默认问题。长
Markdown 优先按段落和完整行切分，不切断内联链接、自动链接、裸 URL、引用式链接或
行内代码；跨片表格重复表头，跨片代码块闭合并重开，操作键盘放在最后一片。单行无法
与表头共同装入 QQ 单片限制的极宽表格会明确降级为带表头、行号的分段文本；围栏信息本身
超过单片限制的代码块也会明确降级为分段文本。达到最大分片数后仍会截断，并提示回
Codex Desktop 查看完整结果。

当前版本不自动操作 Codex Desktop 页面。macOS 的 `app-server` 传输让 Bridge 和 Desktop
连接同一个 standalone Codex shared daemon，QQ 回合可通过官方协议事件实时显示，`/new`
标题通过 `thread/name/set` 显式设置。Bridge 不连接调试端口、不切换页面、不清理缓存、
不重载 renderer；预留开关即使设为 `1` 也不执行 Desktop 操作。

`CODEX_TRANSPORT=auto` 在 control socket 存在时选择 `app-server`，否则选择 `exec`。
socket 存在但握手失败会阻止 Bridge 启动，防止以未同步状态继续运行。强制模式分别为
`app-server` 和 `exec`。Windows 当前固定使用 `exec`，外部 CLI 回合虽然写入原生会话，
Desktop 当前页面仍可能不会立即更新。

shared daemon 是实验能力。当前 macOS 启动方式依赖 `codex app-server daemon start` 和
`launchctl setenv CODEX_APP_SERVER_USE_LOCAL_DAEMON 1`；电脑重启或重新登录后不能假定二者
仍然生效，应重新启动 daemon、完整重启 Desktop，并用 `scripts/run.sh --check` 确认输出
`Transport: app-server`。审批请求不会由 Bridge 自动同意；QQ 只发送提示，实际决定在
Desktop 对应任务完成。

## 故障通知边界

Bridge 会识别 Codex 输出中的网络连接、上游 API、鉴权、限流和 Desktop 审批等待，并按
任务和错误类别去重发送提示；Codex 非零退出、失败回合、外部中断、超时、用户取消以及
会话中的 `turn_aborted` 也会主动通知。

以下情况无法立即通过同一个 QQ 通道告警：

- QQ Gateway 或本机网络已经断开：通知先写入 outbox；只要 Bridge 进程没有退出，连接
  恢复后会继续发送。
- Bridge 进程崩溃、电脑关机或睡眠：进程停止期间无法主动发送；重启后恢复尚未派发的
  任务，但不补发旧通知或停机期间产生的 Desktop 回复。
- 整机失联且没有独立的云端监控：本机 Bridge 无法从失联机器之外发出告警。

锁屏不等于睡眠。macOS 的 `scripts/run.sh` 默认使用 `caffeinate -i` 阻止空闲睡眠；用户
主动睡眠、合盖、关机和断电仍会中断桥接。Windows 需要在系统电源设置中避免运行期间
自动睡眠。

## 日志位置与查看

当前日志：`data/logs/bridge.jsonl`。轮转历史依次为 `bridge.jsonl.1` 到
`bridge.jsonl.5`。每行都是一个 JSON 对象，包含时间、级别、组件和消息。

macOS 实时查看：

```bash
tail -n 200 -f data/logs/bridge.jsonl
```

macOS 打包当前及轮转日志：

```bash
zip -j bridge-logs.zip data/logs/bridge.jsonl*
```

Windows PowerShell 实时查看：

```powershell
Get-Content .\data\logs\bridge.jsonl -Tail 200 -Wait
```

Windows PowerShell 打包当前及轮转日志：

```powershell
Compress-Archive -Path .\data\logs\bridge.jsonl* -DestinationPath .\bridge-logs.zip -Force
```

复现问题后应同时记录大致发生时间、当时发送的 QQ 消息、是否收到各阶段回执、操作系统
和 Codex CLI 版本，再提供 `bridge-logs.zip`。日志不会主动记录 AppSecret、访问令牌、
绑定码或 `openid`，但仍可能包含任务 UUID、本地路径和错误原文，发送前应按需要检查。

不要发送 `.env`、`data/state.json` 或 `data/bridge.sqlite3`；这些文件包含凭证、绑定信息或
任务内容，不是常规日志排查所需材料。

## 状态自查

在 QQ 中发送 `/status` 可查看 Gateway 状态、worker 阶段、最近任务状态、持久任务数、
待发送通知数、最近错误和日志位置。`scripts/run.sh --check` 或 Windows 的
`scripts\run.ps1 --check` 只验证本机配置、依赖、Codex 命令和会话文件，不连接 QQ，也
不能证明 QQ 凭证和公网连接当前可用。
