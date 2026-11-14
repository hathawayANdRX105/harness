# Daemon 运行时接线审计

> 日期：2026-11-14 · 基线：`master` `081cd749` · 方法：静态检索 + `xmllint` + Jev 校验（`compatible` provider，served model `decision-model-preview`）
>
> 结论先行：12 个 crate 全部落地、master CI 全绿，但 **daemon 只接了最小一条运行时链路**。`harness-session` / `harness-context` / `harness-mcp` / `harness-tools::skills` 的能力完整，却大多没有进入实际启动的 daemon。本文逐条记录已校验的缺陷、心智图（`arch/harness-daemon-runtime.svg`）与代码的差异，以及参考三个项目后选出的铺路顺序。

## 1. daemon 实际装配了什么

`crates/harness-daemon/src/main.rs:103-138` 构造的 spec 只有：

- 模型 aliases（`spec.models` + `default_model`）
- `BUILTIN_TOOLS` 七个内建工具（`read/write/edit/glob/grep/terminal/webfetch`）
- 可选 `subagent` 外部工具（配置了 `[[agents.subagents]]` 时）
- policy / budget / event sink / cwd

**没有**：system prompt（`prompt_sections` 为空）、skills、MCP servers、compactor、会话历史。`HarnessSpec::new` 把这些字段都初始化为空（`crates/harness-builder/src/spec.rs:75-86`），daemon 也没有任何赋值（`main.rs` 中检索无结果）。

因此当前 daemon 是一个「裸模型 + 七个文件工具」的 agent；配置里的 `[registry]`、`[mcp.servers]`、压缩语义都只是声明。

## 2. 已校验缺陷

Jev 校验结果：9 条断言全部 `verified`；其中 6 条 `auto`，3 条 confidence 低于 0.8 被标记 `review`（判定方向仍是 verified，但未达自动采信阈值）。**没有 `contradicted`**。

| ID | 缺陷 | 证据 | Jev |
|---|---|---|---|
| D1 | `arch/harness-daemon-runtime.svg` 含提交进仓库的 Git 冲突标记，不是合法 XML，无法渲染 | `arch/harness-daemon-runtime.svg:132-142`；`xmllint` 报 `StartTag: invalid element name` | verified 0.87 · auto |
| D2 | daemon 的 spec 未声明 prompt sections / skills / MCP servers，实际启动无 system prompt、无 `skill` 工具、MCP 池为空 | `main.rs:103-126`、`spec.rs:75-86`、`assemble.rs:493-498`、`assemble.rs:659` | verified 0.92 · auto |
| D3 | 工作区没有 `Compactor` 具体实现，daemon 也从不 `with_compactor`，预算/压缩门永不触发 | `crates/harness-context/src/compactor.rs:63`；`grep 'impl Compactor'` 无结果；`assemble.rs:236-239`；`main.rs:127-137` | verified 0.87 · auto |
| D4 | 会话消息历史既没写入也没重放：`append_message` / `replay_ledger` 在 session crate 外无调用者，`run()` 从空 transcript 开始 | `crates/harness-loop/src/engine.rs:301-302`；全仓检索无外部调用 | verified 0.76 · review |
| D5 | `GraphIndex` 没有运行时消费者，只在 harness-session 测试中使用 | `crates/harness-session/src/lib.rs:48`；`grep GraphIndex` 仅 `tests/session.rs` | verified 0.73 · review |
| D6 | daemon 拒绝 `steer`，尽管 core 与 loop 已实现 steering 邮箱和 turn 边界注入 | `crates/harness-daemon/src/wire.rs:480-482`；`control.rs:75-92`；`engine.rs:107-110` | verified 0.86 · auto |
| D7 | `harness-core` 不含 `SessionStore` seam；trait 定义在 `harness-session`（SVG 归属错误） | `crates/harness-core/src/lib.rs:69-75`；`harness-session/src/store.rs:91` | verified 0.65 · review |
| D8 | 基线事实：scheduler 每会话最多 1 个活跃 run，全局上限默认 4，每会话队列默认 4 | `harness-config/src/config.rs:369-370`；`scheduler.rs:2,102` | verified 0.91 · auto |
| D9 | 事件泵用普通 `tokio::spawn` + `await append_event` 落盘，不是 `spawn_blocking` | `wire.rs:283`、`wire.rs:519-528` | verified 0.88 · auto |
| D10 | `harness-tools` 声明了 `harness-mcp` 依赖但代码从未使用；crate 文档却声称依赖它是为了 MCP 桥类型 | `crates/harness-tools/Cargo.toml:11`；`cargo check` 报 `unused dependency harness-mcp`；`src/` 无 `harness_mcp` 引用 | 编译器证据 |

> Jev 运行备注：`compatible` 端点一次最多接受 8 条 claims（9 条返回 400），所以校验分两批完成；usage 块返回 `0/0`，不代表未调用模型。D4/D5/D7 的 `review` 是阈值问题，不是判定反转。

## 3. 心智图漂移

`arch/harness-daemon-runtime.svg` 除了 D1 的硬缺陷外，内容与代码还有以下差异：

| 图中表述 | 代码实际 | 证据 |
|---|---|---|
| core 冻结层含 `SessionStore seam` | 不在 core，在 `harness-session` | D7 |
| 存储层「重启重放水合」 | 未实现：新 run 不从 ledger 载入历史 | D4 |
| 存储层「pump spawn_blocking 落盘」 | 异步泵 + `await append_event` | D9 |
| loop「steering 转向（turn 边界注入）」 | core+loop 有，daemon 拒绝 `/steer` | D6 |
| loop「budget 超限：compaction 支路」 | 门存在但永不触发（无 compactor） | D3 |
| tools 框含 `skill` | 不在 `BUILTIN_TOOLS`；仅 spec 声明 skills 时安装 | `builder/src/spec.rs:598-600`、`assemble.rs:493-498` |
| tools 框「MCP 桥（mcp__*）」 | 命名空间在 `harness-mcp` 实现，但 daemon spec 无 MCP 工具，池为空 | `mcp/src/transport.rs:206`、D2 |
| 传输层「HTTP · /commands + SSE」 | 实际路由 `POST /command`、`GET /events` | `transport/src/http.rs:69-70` |
| scheduler「每会话 1 活跃 run · 全局 ≤ 4」 | 准确，且两个值都可配置 | D8 |
| `subagent` 只画成 scheduler→loop 虚线 | 实际是 daemon 层工具，且可被 per-session allowlist 关闭 | `daemon/src/subagent.rs:1-24` |

另外：`arch/harness-daemon-model.svg`（旧图）仍写着「Global Session Manager · 多会话并发」，已被 #32 之后的 per-session scheduler 取代；图例已声明保留对照。

## 4. 参考项目对照

三份并行调研的结论（路径均为对应 repo 内文件）。

| 子系统 | harness-rs | pi | oh-my-pi | 我们的差距 |
|---|---|---|---|---|
| 会话持久化 / 恢复 | 会话层 `SessionStore.history` → `run_with_seed` 注入历史；`InMemorySessions` 明确非持久 | 每个会话的 `EntryRecord` 追加日志；`readContext` 按 head→tail 还原上下文；支持 fork/branch | 每会话 JSONL 树（`id,parentId` + leaf 指针）；`buildSessionContext` 从叶路径重建；`--continue` 用 terminal breadcrumb；另有 SQL/Redis 后端 | 表结构已有，但 `append_message`/`replay_ledger` 无人调用 |
| 压缩 | `DefaultCompactor` 具体实现：token 估算 ×校准因子，五级 stage；模型驱动的 Microcompact 非默认 | `CompactionTask` 分 select/summarize/retry 阶段，带 `CompactionCheckpoint` | 纯压缩逻辑与 session 维护分离；`CompactionEntry{firstKeptEntryId,tokensBefore}` 作为检查点 | 只有 trait + 阈值，无实现（D3） |
| MCP | `McpClient` + `McpProxyTool`，远程名**不加前缀** | `mcp__<server>__<tool>`，冲突加 hash；每 server 懒连接，无池 | 进程级 `MCPManager` 单例，user+project 配置，禁用/允许列表，懒重连 + 熔断 | 命名与代理已有；daemon 不装配（D2） |
| prompt / skills | `Context{system,guides,history}`，Guide 在会话开始应用一次 | `buildSystemPromptSections` 生成分段提示；skills 发现后进 `<skills>` 段 | `createAgentSession` 并行发现 skills/rules/prompt 模板/context files，按 turn 重建 | builder 支持 prompt ledger 与 `SkillTool`，但 daemon spec 为空（D2） |
| steering | 无运行中注入 API；`Session.turn` 是新一轮 | `Agent.steer/followUp` 队列，all/one-at-a-time，interrupt 模式，drain 在 loop 边界 | 同上，另有 `interruptMode immediate/default` 与 pause gate | core+loop 已有，缺 scheduler 暴露的 per-run handle（D6） |
| subagent | `SubagentSpec` 显式工具集 + `max_iters`，子 loop 无环境工具 | **不在核心**；示例 extension 用子进程 | 结构化请求：output schema、`maxRuntimeMs`、深度上限 2、spawn allowlist、进度事件 | 已有：profile 工具子集 + max_turns + 父子权限交集；缺深度/超时/结构化输出 |
| 并发 | orchestrator 全局 `max_concurrency=4`，单线程 `FuturesUnordered`；run 级预算 | scheduler 无数字上限，靠 owner edge/restartLimit | 单 Agent 串行（busy 错误 + 队列），provider 级窄信号量，job manager | 每会话 1 + 全局 4 + 有界队列，模型成立 |
| 未来形态铺路 | seed 式恢复、`GoalStore`、`RunStore` 可恢复 | Storage 后端抽象（memory/sqlite/jsonl/indexeddb）、fork/branch、多前端模式 | 多后端 session、collab relay、AgentRegistry、worktree 隔离 | 传输层多前端已就绪；恢复/分支/多后端未接 |

## 5. 铺路修复的选择

选择标准（与仓库既有约束一致）：

1. **杠杆**：解锁未来的多轮会话、重启恢复、多前端/分支形态；
2. **不动 core 契约**：不新增 `RunEvent` 变体、不改冻结 trait；
3. **爆炸半径小**：单 crate 可测，不需要真实模型；
4. **不做投机抽象**：只接现有 seam，不提前造。

`jev_decide`（5 候选、3 条 requirement、`compatible` 端点）选中 **会话历史优先**：`option_session_first` 0.96，`all_sequential` 0.03，其余 0。注意所有候选的 requirement 检查都是 `supported`，因此排序来自杠杆/优先级契合度，不是硬约束淘汰；这是本次判断的置信边界。

建议顺序：

| 阶段 | 内容 | 触碰 | 验收 |
|---|---|---|---|
| P0 | 修 SVG 冲突（D1） | `arch/harness-daemon-runtime.svg` | `xmllint --noout` 通过、图可渲染 |
| P1 | 会话消息落盘 + run 启动注入：明确写入点（loop hook 或 daemon pump），`start_run` 调 `replay_ledger` 填充 transcript | `harness-session`、`harness-loop`（seed 参数）、`daemon/wire.rs` | 两次 run 同一 session，第二次能看到第一次的 user/assistant；测试不依赖真实模型 |
| P2 | config → spec：prompt sections、`[registry].skills`、`[mcp.servers]` 映射进 `HarnessSpec` | `daemon/main.rs`、`builder` spec 构造 | 启动后 `skill` 工具存在、`tools/list` 出现 `mcp__*`、system prompt 非空 |
| P3 | `DefaultCompactor`：token 估算 + 切点 + 现有五级 stage，经 `with_compactor` 注入 | `harness-context`、daemon 一行 | 预算门在超阈值时触发一次压缩，仍超则 `BudgetBlocked` |
| P4 | steering 接线：`Scheduler::submit` 返回 per-run handle，daemon 保存 `run_id → RunControl`，`SlashCommand::Steer` 落到邮箱 | `harness-scheduler`、`daemon/wire.rs` | `/steer` 后下一个 turn 边界出现 `SteeringApplied` |
| 暂缓 | 会话分支/fork、多存储后端、provider 信号量、subagent 深度/超时 | — | 等 P1 的 ledger 语义稳定后再做 |

## 6. 本次 PR 范围

- 记录本审计（本文件）。
- 修复 D1：解决 SVG 中提交进仓库的冲突块（取 HEAD 侧：MCP 桥 + ToolCatalog；另一侧的重复标题与内部箭头是中间版本残留）。

P1-P4 是后续 PR，不在本次改动内。

## 7. 验证

```sh
xmllint --noout arch/harness-daemon-runtime.svg   # XML OK（修复前在 132 行报 parser error）
just verify                                       # fmt-check + clippy + check 通过
```

`just verify` 唯一告警是 D10 的 `unused dependency harness-mcp`（既有问题，非本次引入）；`spec-check` 因本机预编译 dylint 库未就绪而 skip，由 PR CI 补跑。
