# 可靠性与故障排查

## 回执含义

QQ 发起的任务会经过以下可区分阶段：

1. `已接收并保存`：消息和附件已写入本机持久队列，但 Codex 尚未启动。
2. `已保存并进入等待队列`：检测到已有 Codex 任务，当前任务继续等待。
3. `Codex 子进程已启动`：本机已成功创建 Codex CLI 子进程，任务开始执行。

最终还会发送完成、失败、超时或取消结果。“已接收”不再表示 Codex 已经开始执行。

## 持久化与重启恢复

任务队列和待发送通知保存在 `data/bridge.sqlite3`，同一目录由
`data/bridge.lock` 保证只运行一个 Bridge 实例。

- `queued`、`waiting`、`preparing`：Bridge 重启后自动重新排队。
- `dispatching`、`running`：重启后标记为 `interrupted`，不会自动重跑，避免同一远程操作
  被执行两次；用户会收到人工核对提示。
- 未发送通知：保留在 outbox，QQ Gateway 恢复 READY 后重试。
- 已发送通知：保留 30 天用于去重，之后自动清理。

任务启动或终止状态与对应通知在同一个 SQLite 事务中写入，不能出现“状态已经结束、通知
却没有入队”的半完成结果。长消息的多个分片属于同一发送组；前一片未成功时，后一片
不会越过它发送。

如果终态和通知连续两次无法写入数据库，Bridge 会受控退出，不会继续运行并吞掉该任务。
任务保留在 `dispatching` 或 `running`；下次启动时按不确定任务转为 `interrupted`，提示人工
核对，不会自动重跑。

通知采用“至少送达一次”策略。如果进程在 QQ 已接收消息、但本地尚未来得及标记成功的
极短窗口内崩溃，恢复后可能重复发送该条通知；这种取舍用于避免静默丢失结果。

输入图片保存在 `data/attachments`，直到对应任务进入终态后才清理。Bridge 在尚未派发时
正常退出，会保留任务和附件供下次启动继续执行。

最终回复中需要转发的图片会先复制到 `data/outbox-images`，因此原始任务附件清理后仍可
继续重试发送。图片发送成功后会删除对应副本；启动时也会清理没有待发通知引用的孤立
副本。

## 故障通知边界

Bridge 会识别 Codex CLI 输出中的网络连接、上游 API、鉴权和限流错误，并按任务和错误
类别去重发送提示；Codex 非零退出、超时、用户取消以及会话中的 `turn_aborted` 也会主动
通知。

以下情况无法立即通过同一个 QQ 通道告警：

- QQ Gateway 或本机网络已经断开：通知只能先写入 outbox，连接恢复后补发。
- Bridge 进程崩溃、电脑关机或睡眠：进程停止期间无法主动发送；系统和 Bridge 恢复后才
  能恢复队列及补发通知。
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
