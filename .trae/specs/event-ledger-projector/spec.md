# Append-only 事件 Ledger 与 Checkpoint Projector - Product Requirements Document

## Overview
- **Summary**: 在写入任何物化表（chats、messages、FTS、groups/group_participants、starred、polls、call_events、locations、status_messages 等）之前，先将**去重后的协议事件**写入 append-only ledger，记录来源、server/event time、WhatsApp key、parser version、因果引用与原始协议信封哈希（raw_hash）。各物化视图不再由业务路径直接 upsert，而由按 checkpoint 推进的 projector 从 ledger 顺序折叠构建；并提供 shadow rebuild、跨视图 invariant checker、差异报告与原子视图切换。
- **Purpose**:
  - parser 或 LID 规则升级后，可从原始事件**完整重建**视图，而非写一次性数据迁移；
  - 可证明 chats、messages、FTS、group roster、unread 等视图来自**同一因果序列**（统一 seq 空间、统一推进 frontier）；
  - 每条物化数据可溯源到 ledger 事件（来源、时间、key、parser 版本、因果链）。
- **Target Users**: wacli 维护者与需要本地长期同步、审计与重放能力的高级用户/脚本作者。

## Goals
- 单一 append-only 事件账本作为唯一事实源（source of truth），四条现有写入路径（实时、history、app-state、补偿）统一进账本。
- 版本化、可重放、确定性的 projector，以 checkpoint 增量推进全部视图。
- 离线 shadow rebuild；跨视图 invariant 校验；shadow 与 active 的结构化差异报告；事务内原子视图切换与回滚。
- 与现有 payload purge 隐私语义闭环：原始信封字节独立存储，擦除以追加 scrub 事件表达。

## Non-Goals
- 不修改 whatsmeow 依赖、不 fork 其解密路径、不新增第三方依赖。
- 不捕获 WhatsApp 线上加密密文（whatsmeow 在事件回调前已解密且不暴露密文字节）；raw_hash 的口径是“首次观测的原始协议信封”，不是线上 ciphertext。
- 不保证 legacy_snapshot 事件可重建出历史原始形态（只保证投影出捕获时的行形态）。
- 不改动会话数据库 session.db 的既有职责；不做多账本/分布式复制。
- 不重构与物化写入无关的命令输出逻辑。

## Background & Context
- 现状写入路径（直接 upsert，互不经过统一序列）：
  - **实时**：`handleLiveSyncMessage` → `storeParsedMessage` → UpsertChat/UpsertContact/UpsertMessage/...，`incrementLiveUnread` 直接改 unread；
  - **history**：`handleHistorySync`（含 call log、unread count）与 on-demand backfill（`BackfillHistory`），逐消息 upsert；
  - **app-state**：`handleAppStatePersistenceEvent` → star/delete-for-me/archive/pin/mute/mark-read 及 AppState call log；含 full-sync、recovery replay；
  - **补偿**：`migrateHistoricalLIDs` 直接重写多表 JID 并合并行；app-state recovery intents。
- wacli.db 当前由 26 个 schema 版本演进而来；messages_fts 为 trigger 同步的 FTS5 虚拟表；非 FTS 构建需回退 LIKE。
- 已确认的架构决策（用户批准，2026-09-20）：
  1. 存量数据以 **legacy_snapshot 合成事件**纳入 ledger；
  2. 原始字节存独立 **ledger_raw** 表，擦除追加 **scrub 事件**并删除 raw 行；ledger_events 本身保持纯 append-only；
  3. **分阶段双写 + 影子**上线（先双写、projector 只写 shadow，验证后 promote 切换）；
  4. **raw_hash = 原始协议信封的确定性序列化 SHA-256**，预留密文捕获接口但本版不实现。
- append-only 设计原则（历史经验）：状态变化必须以“追加新事件”表达（scrub/supersede/identity-resolution 均为新事件），读取侧计算有效集；禁止 UPDATE 旧行来表达失效。

## Functional Requirements

### Ledger 与摄取
- **FR-1**: 系统提供 append-only 表 `ledger_events`，列至少包含：`seq`（单调递增，本地追加序）、`event_id`（全局唯一）、`source`、`event_type`、`wa_key`、`chat_jid`、`msg_id`、`sender_jid`、`server_ts`、`event_ts`、`received_at`、`raw_hash`、`parser_version`、`rules_version`、`dedup_key`、`causal_refs`（原始 WhatsApp 引用键）、`causes`（解析后的 event_id 列表）、`batch_id`、`flags`。
- **FR-2**: 原始信封字节存于独立表 `ledger_raw(event_id PRIMARY KEY, raw_bytes, encoding)`；ledger_events 不含内容字节。删除 ledger_raw 行只能由对应 scrub 事件触发。
- **FR-3**: `source` 取值限定为受控集合：`live`、`history`、`on_demand_history`、`app_state`、`app_state_recovery`、`receipt`、`identity_resolution`、`scrub`、`legacy_snapshot`。
- **FR-4**: 摄取在写物化表之前发生；同一批次内 ledger 追加与（阶段 C 前的）旧物化写入的顺序保证“ledger 先落盘”。
- **FR-5**: 去重：协议重投递（offline replay、history/live 重叠、on-demand、full-sync 当前态重放）基于 `dedup_key` + `raw_hash` 幂等；同一 WhatsApp key 下不同内容（edit/revoke/不同原始字节）产生不同行，按 seq 保序。dedup 规则由 `rules_version` 标识。
- **FR-6**: 因果引用：reply/quoted、edit、reaction、revoke、poll 创建/投票/加选项等引用以原始 WhatsApp 键记录；linker 在目标事件已存在时解析为 event_id，目标缺失时保留 dangling 状态并可枚举，因果图无环。
- **FR-7**: raw_hash 对“首次观测、未经 parser/LID 规范化”的信封做确定性 protobuf 序列化后 SHA-256（hex）：live=RawMessage、history=WebMessageInfo、app-state=对应 Action/SyncActionValue；无原始 proto 的事件（receipt 等）使用 ledger 包定义的确定性规范编码。
- **FR-8**: parser 与规则版本：ledger 记录摄取时的 parser_version/rules_version；projector 重放 raw 事件时使用**当前二进制的 parser**重新解析，使 parser/LID 升级在 rebuild 后生效；无 raw 字节的事件（legacy_snapshot、已 scrub）使用事件携带的快照载荷投影为快照/tombstone。

### Projector
- **FR-9**: 每个视图有独立 projector 与 `projector_checkpoints(view, last_seq, projector_version, updated_at)`；projector 严格按 seq 升序消费，仅消费 seq > checkpoint 的事件；重启后从 checkpoint 精确续跑。
- **FR-10**: 视图集合覆盖现有全部物化表：chats、contacts、groups、group_participants、messages（含 FTS）、status_messages、call_events、starred、polls、poll_votes、message_locations。
- **FR-11**: 同一批次应用后，全部视图 checkpoint 推进到**同一 seq frontier**（跨视图同因果序列）；projector 对同一事件区间重复应用产出字节级一致的表（幂等）。
- **FR-12**: LID 补偿不再直接改表：resolver 得到的映射以 `identity_resolution` 事件追加，projector 在折叠中完成 JID 规范化；重放该事件序列可复现与旧补偿路径相同的视图状态。
- **FR-13**: 投影输出继续驱动现有副作用（webhook、media enqueue、unread 汇总、poll 处理），副作用以“新推进区间的投影结果 + ledger 溯源信息”为输入；无 WhatsApp 连接时不被需要。

### Rebuild / Verify / Diff / Promote
- **FR-14**: `wacli ledger rebuild` 仅凭 wacli.db（不连接 WhatsApp）从 seq=0 重放全部事件到一组 shadow 表；raw 事件用当前 parser，scrub/legacy 事件退化为 tombstone/快照；输出进度，完成可重入。FTS 与非 FTS 两种构建均支持。
- **FR-15**: `wacli ledger verify` 执行跨视图 invariant 检查（见 NFR 与 AC），健康库通过、故障库以非零退出并输出结构化违例（JSON/table）。
- **FR-16**: `wacli ledger diff` 比较 shadow 与 active：逐视图计数与逐键 missing/extra/changed，并给出导致差异的 seq/event_id 溯源；JSON 与 table 两种输出。
- **FR-17**: `wacli ledger promote` 在单个事务内原子切换全部视图（含 messages 与 FTS、索引、trigger 的协调改名）并更新 checkpoints；active 旧表保留为 backup，`wacli ledger rollback` 可在下次 promote 前回滚；切换前要求 verify 通过（`--force` 仅覆盖 diff 策略，不覆盖硬 invariant）。
- **FR-18**: 提供 `wacli ledger status`：ledger head seq、每视图 checkpoint、模式（shadow/live）、落后事件数、raw/scrub 计数；全部子命令遵守 `--json`、stderr 提示、`--read-only`、store lock 约定。

## Non-Functional Requirements
- **NFR-1 安全/权限**: ledger 相关表与文件保持 0600/目录 0700；ledger_events 有防 UPDATE/DELETE/REPLACE 的触发器作为纵深防御（触发器自身的维护仅允许迁移路径）。
- **NFR-2 确定性**: 同一 ledger 连续两次独立 rebuild 的 shadow 表字节级一致（除显式记录的非确定性字段外不允许差异）。
- **NFR-3 性能**: 摄取与投影的吞吐相对现有直接 upsert 路径回归可控（目标：10k 合成事件场景平均每事件开销增幅 < 20%）；投影与 rebuild 内存有界（流式读取，不整表载入）。
- **NFR-4 可测试性**: 每个修复/能力配回归测试；FTS 敏感测试同时覆盖 `-tags sqlite_fts5` 与无 tag 路径；不触达真实 WhatsApp，使用 fake/table-driven。
- **NFR-5 兼容**: read-only 模式下 status/verify/diff 可用，rebuild/promote 被拒；现有命令行为与输出在切换前保持不变。
- **NFR-6 依赖与构建**: 不新增依赖、不改构建工具；`pnpm format:check && pnpm lint && pnpm lint:deadcode && pnpm test && pnpm build` 全部通过。

## Constraints
- **Technical**: Go + mattn/go-sqlite3 + sqlc 生成；FTS5 需 build tag；单写者锁（LOCK 文件）；SQLite 事务内 `ALTER TABLE RENAME` 实现切换；不改 session.db。
- **Business**: 维护者约定——加依赖/改构建工具需确认；完整 gate 必须通过；输出走 internal/out（数据 stdout、提示 stderr）。
- **Dependencies**: whatsmeow v0.0.0-20260909164725-b25a56d63729：事件层不暴露密文与 RawDebug hook，故密文哈希不可行；现有 protobuf v2 marshal 提供确定性序列化。

## Assumptions
- projector 重放时对 raw 事件统一用当前 parser；parser 版本语义化（常量），版本升级在代码评审中显式登记。
- identity_resolution 事件覆盖投影所需的全部 LID→PN 映射；离线 rebuild 不做实时解析，仅消费账本中的映射事件。
- shadow 表与 active 表同库（前缀/改名方案）；规模处于单文件 SQLite 可承载范围。
- 视图允许保留的“已知非确定性字段”集合为空或在实现中显式登记并在 diff 中排除。

## Acceptance Criteria

### AC-1: Ledger schema 与 append-only 防护
- **Type**: `rule`
- **Given**: 一个迁移到新版本的 wacli.db
- **When**: 检查对象并尝试对 ledger_events 执行 UPDATE/DELETE/REPLACE
- **Then**: ledger_events、ledger_raw、projector_checkpoints 及 ledger 状态表存在；任何改/删既有行的尝试被触发器或权限拒绝并报错；INSERT 成功
- **Pass Condition**: 迁移后对象齐全；全部修改/删除尝试返回错误且行数不变；插入路径正常
- **Evidence**: schema/migration 测试，含 UPDATE/DELETE 拒绝用例（FTS 与非 FTS 各一）

### AC-2: Ledger 事件元数据完整且可溯源
- **Type**: `rule`
- **Given**: 任一路径摄取的一个事件
- **When**: 读取其 ledger 行
- **Then**: event_id 非空且唯一；source 属受控集合；raw_hash 为 64 位十六进制且与原始信封重算一致；parser_version/rules_version/dedup_key 非空；时间列齐全；message/app_state 类事件 wa_key 非空
- **Pass Condition**: 抽样与全量审计均无缺字段/坏哈希行
- **Evidence**: 摄取单测 + 全表审计查询测试

### AC-3: 去重幂等
- **Type**: `rule`
- **Given**: 同一协议事件的重复投递（offline replay、history/live 重叠、on-demand、full-sync 重放）
- **When**: 反复摄取并重跑投影
- **Then**: ledger 恰有一行；重复摄取为 no-op，不推进视图；同一 key 下不同原始字节（edit/revoke）产生不同 seq 行且保序
- **Pass Condition**: 每场景 ledger 行数与视图结果严格符合定义
- **Evidence**: table-driven 去重测试覆盖全部重复来源

### AC-4: 因果引用记录与解析
- **Type**: `rule`
- **Given**: reply/edit/reaction/revoke/poll 类事件，含目标先到与目标后到两种时序
- **When**: 摄取并运行 linker
- **Then**: 原始引用键记录；目标存在时 causes 解析为 event_id；目标缺失标 dangling 且可枚举；无环
- **Pass Condition**: 两时序下解析状态与枚举结果符合预期
- **Evidence**: 乱序到达测试 + 图无环/悬挂枚举测试

### AC-5: Projector checkpoint 与跨视图统一 frontier
- **Type**: `rule`
- **Given**: 含多类型事件的 ledger
- **When**: 运行投影、中途重启、并对同一区间重复应用
- **Then**: 各视图从 checkpoint 续跑；同批后所有视图 last_seq 相同；重复应用表内容一致
- **Pass Condition**: checkpoint 值、frontier 一致性、幂等性三项断言全过
- **Evidence**: projector runner 测试（含重启恢复与重复应用）

### AC-6: 四条写入路径统一进入 ledger
- **Type**: `rule`
- **Given**: live、history/on-demand、app-state(+recovery)、LID 补偿四类输入
- **When**: 在 live 投影模式下执行
- **Then**: 每条输入在物化变更前有对应 ledger 事件；不存在绕过 ledger 直接 upsert 视图表的路径；LID 映射以 identity_resolution 事件表达，重放复现同等视图状态
- **Pass Condition**: 静态调用面审计与行为测试均无 bypass；补偿重放结果一致
- **Evidence**: 路径映射测试 + 写入调用面审计测试

### AC-7: Shadow rebuild 离线完整且与健康 active 一致
- **Type**: `rule`
- **Given**: 一个同步后、未做离线变更的库
- **When**: 断开 WhatsApp 执行 rebuild，再 diff
- **Then**: shadow 覆盖全部视图；raw 事件按当前 parser 投影；shadow 与 active 的 diff 为空
- **Pass Condition**: rebuild 成功且 diff 零差异（退出码 0）
- **Evidence**: 离线 rebuild→diff 集成测试（FTS/非 FTS）

### AC-8: 跨视图 invariant checker 覆盖故障注入
- **Type**: `rule`
- **Given**: 健康库及逐类注入单一故障的库（message 缺 chat、FTS 行不匹配、participant 缺 group、checkpoint 落后/分叉、raw_hash 不符、dangling 引用、raw/scrub 记账违规）
- **When**: 运行 verify
- **Then**: 健康库通过（0）；每个故障库非零退出且报告精确指向被注入的违例
- **Pass Condition**: 全部故障类型可检出且无误报
- **Evidence**: table-driven 故障注入测试

### AC-9: 差异报告可机器消费且可溯源
- **Type**: `rule`
- **Given**: shadow 与 active 存在已知差异的库
- **When**: 运行 diff（JSON/table）
- **Then**: 逐视图计数正确；逐键 missing/extra/changed 分类正确；每条差异附责任 seq/event_id
- **Pass Condition**: 报告内容与预置差异完全一致；无差异退出 0，有差异非 0
- **Evidence**: diff 单测（JSON 结构断言 + table 快照）

### AC-10: 原子视图切换与回滚
- **Type**: `rule`
- **Given**: shadow 已通过 verify 的库
- **When**: promote 成功 / promote 中途强制失败 / rollback
- **Then**: 成功后读者看到全部新视图且 checkpoints 一致；失败时 active 原样不变；rollback 恢复旧视图
- **Pass Condition**: 三种结局下视图完整性与 checkpoints 均正确，无混合视图
- **Evidence**: promote/rollback 测试（含故障注入与并发读者检查）

### AC-11: Raw/scrub 隐私闭环
- **Type**: `rule`
- **Given**: 触发 payload purge 的事件流
- **When**: scrub 追加、投影推进后检查
- **Then**: ledger_raw 对应行被删；视图内容被擦除为 tombstone；不存在“有 scrub 事件却仍有 raw 行”或“非快照事件既无 raw 也无 scrub”
- **Pass Condition**: 两项记账 invariant 恒成立且视图结果正确
- **Evidence**: purge→scrub 回归测试（含 LID 场景）

### AC-12: Legacy snapshot 引导
- **Type**: `rule`
- **Given**: 含既有物化数据的旧库
- **When**: 执行升级迁移
- **Then**: 现有行转为 legacy_snapshot 事件（raw_hash 对行的规范编码计算，无 raw 字节）；rebuild 复现与原行一致的物化结果
- **Pass Condition**: 事件标记正确且 rebuild 后行级一致
- **Evidence**: 旧库迁移测试 + rebuild 比对

### AC-13: Rebuild 确定性
- **Type**: `rule`
- **Given**: 固定 ledger 内容
- **When**: 连续两次独立 rebuild
- **Then**: shadow 全部表与 checkpoint 结果字节级一致
- **Pass Condition**: 两次产物逐表无差异
- **Evidence**: 双跑确定性测试

### AC-14: 摄取与投影性能及资源边界
- **Type**: `rubric`
- **Dimension**: 相对现有路径的吞吐/内存表现
- **Scale**: 1-5
- **Anchors**: 1 = 10k 合成事件下开销增幅 >50% 或内存无界/整表载入；3 = 增幅 20–35%，流式但有明显尖峰；5 = 增幅 <20%、流式读取、内存平稳
- **Pass Threshold**: >= 4
- **Evidence**: 基准/计时测试输出（事件数、耗时、峰值内存的可复现命令）

### AC-15: Parser/LID 升级重放与解释质量
- **Type**: `rubric`
- **Dimension**: 升级后 rebuild 的差异能否被 diff 完整解释并归因
- **Scale**: 1-5
- **Anchors**: 1 = 升级后 rebuild 无法完成或差异无归因；3 = 能完成但部分差异无法映射到事件/版本；5 = 全部变更逐行映射到事件 seq 与 parser/rules 版本，报告可读、无遗漏
- **Pass Threshold**: >= 4
- **Evidence**: 模拟 parser 版本升级的重放对比测试与样例报告

## Open Questions
- 无（4 项关键决策已于 2026-09-20 确认；实现中若出现新的取舍将回到本规格登记）。
