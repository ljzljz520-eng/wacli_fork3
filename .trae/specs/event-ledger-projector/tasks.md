# Append-only 事件 Ledger 与 Checkpoint Projector - Implementation Plan

> AC→任务总览：AC1=T1；AC2=T2,T3；AC3=T3,T5；AC4=T3；AC5=T6；AC6=T5,T8-T14,T20；AC7=T16；AC8=T17；AC9=T18；AC10=T19；AC11=T14,T17；AC12=T4；AC13=T16；AC14=T21；AC15=T18,T21。

## Task 1: Ledger 表结构、迁移 v27 与 append-only 触发器
- **Status**: `completed`
- **Priority**: high
- **Depends On**: None
- **Completion Evidence**: schema.sql 追加 ledger_events/ledger_raw/projector_checkpoints/ledger_view_state/ledger_batches/ledger_causal_links、索引及 6 个触发器；新增 internal/store/ledger_schema.go（迁移 v27，幂等 DDL），migrations.go 注册 `{version:27, event ledger}`；新增 ledger_schema_test.go。发现 SQLite REPLACE 的内部删除不触发 DELETE 触发器，增补 BEFORE INSERT 存在性守卫（ledger_events_no_replace / ledger_links_no_replace）。TR-1.1/TR-1.2 经普通与 `-tags sqlite_fts5` 两次 store 测试全部 PASS。
- **Description**:
  - 在 internal/store 新增迁移版本 27，创建 `ledger_events`、`ledger_raw`、`projector_checkpoints`、`ledger_view_state`（view→shadow/live 模式）、`ledger_batches`；
  - ledger_events 含 spec FR-1 全部列，seq 自增，event_id 唯一，dedup_key 唯一索引，wa_key/chat_jid/causes 等辅助索引；
  - 创建 BEFORE UPDATE/DELETE 触发器（RAISE ABORT）阻止改动既有行；触发器维护只走迁移；
  - 同步更新 schema.sql（新库直建）与 schema 测试。
- **Acceptance Criteria Addressed**: AC1
- **Test Requirements**:
  - `rule` TR-1.1: 迁移后所有对象与索引存在；INSERT 成功；UPDATE/DELETE/REPLACE 均被拒绝且行数不变（证据：store 测试输出，FTS 与非 FTS 构建各跑一次）
  - `rule` TR-1.2: 全新库经 schema.sql 打开与迁移库结构一致（证据：schema 一致性测试）

## Task 2: Ledger 领域原语：编码、raw_hash、dedup 与事件模型
- **Status**: `completed`
- **Priority**: high
- **Depends On**: T1
- **Completion Evidence**: 新增 internal/ledger 包：event.go（9 个 source、31 个 event type、flags、CausalRef、Event、受控校验）；hash.go（ParserVersion/RulesVersion 常量、WAKey、DedupKey、DeriveEventID、确定性 protobuf marshal+SHA-256、canonical-json 哈希、refs 序列化）；canonical.go（CanonicalReceipt，IDs 排序、毫秒时间戳）。event_test.go 含确定性/包装差异测试与冻结 golden 向量（receipt hash a4322b5a…）。TR-2.1/TR-2.2 全部 PASS，gofmt clean。
- **Description**:
  - 新建 internal/ledger：Event/Envelope/CausalRef 模型；source/event_type 受控常量（FR-3）；
  - 各类信封的确定性编码（live RawMessage、history WebMessageInfo、appstate Action、receipt 规范编码）；raw_hash=SHA-256 hex；
  - dedup_key 推导规则与 rules_version 常量；event_id 由（去重要素）确定性派生；parser_version 常量来源（与 app 版本/独立常量的映射）。
- **Acceptance Criteria Addressed**: AC2
- **Test Requirements**:
  - `rule` TR-2.1: 同一输入重复编码哈希一致；字段顺序/包装变化测试确定性；receipt 等无 proto 事件有稳定向量（证据：固定 golden 向量测试）
  - `rule` TR-2.2: raw_hash 输出恒为 64 十六进制；dedup_key/event_id 对等价输入相同、对不同原始字节不同（证据：table-driven 单测）

## Task 3: Ledger append 存储、raw 写入与因果 linker
- **Status**: `completed`
- **Completion Evidence**: 新增 internal/store/ledger.go（AppendEvent 单事务+幂等、ScanEvents、HeadSeq、GetEventByID、GetRaw、StartBatch、checkpoint/view-mode）与 ledger_linker.go（ResolveCausalLinks 递归 CTE 环检测、EnumerateUnresolvedRefs/EnumerateDangling、AuditLedger）；ledger_test.go 覆盖幂等、校验、目标先/后到、环拒绝、审计、checkpoint、scan、batch。过程中修复 json.RawMessage 扫描（TEXT 返回 string）、json_each 别名与 cyclic 第三态分类。TR-3.1/3.2/3.3 普通与 FTS5 构建均 PASS。
- **Priority**: high
- **Depends On**: T2
- **Description**:
  - internal/store 新增 ledger.go：单事务追加事件 + ledger_raw 行；基于 dedup_key 冲突返回“已存在”（no-op）并可回读；
  - head seq / 按 seq 区间流式扫描 / 按 event_id 查询；
  - internal/ledger linker：causal_refs→event_id 解析（含批量解析），未命中标 dangling，图无环校验；
  - raw 读取接口（供 rebuild）与 raw 缺失判定。
- **Acceptance Criteria Addressed**: AC2, AC3, AC4
- **Test Requirements**:
  - `rule` TR-3.1: 重复 append 同一事件返回 no-op、行数不增；同 key 不同 raw_hash 产生不同 seq 行（证据：append 幂等测试）
  - `rule` TR-3.2: 目标先到/后到两时序下 causes 与 dangling 状态正确；构造环被拒；dangling 可枚举（证据：linker 乱序测试）
  - `rule` TR-3.3: 全量审计：所有行必填列齐全、raw_hash 可重算一致（证据：审计查询测试）

## Task 4: Legacy snapshot 存量引导迁移
- **Status**: `completed`
- **Completion Evidence**: 新增 internal/store/ledger_bootstrap.go：snapshotTables 覆盖 11 张协议派生表（排除 operational 表）；BootstrapLegacySnapshots 建 batch，按稳定键 SELECT * 全表扫描，行值规范化（BLOB→base64），snapshot=`{"table","row"}` canonical JSON 的 SHA-256 作为 raw_hash（不留 raw 字节），messages 行推导 WAKey（participant 规则：非 from-me 且 sender≠chat），不写 checkpoint。ledger_bootstrap_test.go：全 11 表计数、raw_hash 重算、wa key、重入 rerun 全部 skipped 不增行。TR-4.1/TR-4.2 PASS，gofmt clean。
- **Priority**: high
- **Depends On**: T3
- **Description**:
  - 迁移（版本 27 后半或 v28）扫描全部既有视图表，逐行追加 source=legacy_snapshot 事件：无 raw 字节，raw_hash 对行的规范编码计算，携带行快照载荷与 wa_key；
  - 批次化执行、可重入（完成标记，中断重跑不重复）；事件顺序按表与稳定键排序，保证确定性；
  - 不写 projector checkpoint。
- **Acceptance Criteria Addressed**: AC12
- **Test Requirements**:
  - `rule` TR-4.1: 旧库迁移后每个旧行恰有一条 legacy_snapshot 事件，raw_hash 与行编码一致（证据：迁移计数/抽样测试）
  - `rule` TR-4.2: 中断重跑不产生重复事件（证据：分段执行测试）

## Task 5: 四条路径摄取接线（阶段 A：ledger + 旧物化双写）
- **Status**: `completed`
- **Priority**: high
- **Depends On**: T3
- **Completion Evidence**: 新增 internal/app/ledger_ingest.go（ledgerEnabled/WACLI_LEDGER、ledgerBatch 进程批次缓存、ingestMessageEvent/ingestReceiptEvent/ingestAppStateEvent）。wa.ParsedMessage 增加 RawEnvelope/IngestSource/FromFullSync；ParseLiveMessage 挂 RawMessage+live，ParseHistoryMessage 挂 WebMessageInfo；handleHistorySync 按 sync type 覆盖 on_demand_history / INITIAL_BOOTSTRAP full-sync flag；storeParsedMessage 将 sender 规范化前置，ledger append 在 UpsertChat 等一切物化写入之前，失败即中止；receipt case 在 handleReceiptPersistenceEvent 之前摄取；persistAppStateEvent 在物化 switch 之前摄取（Star/DeleteForMe/Archive/Pin/Mute/MarkChatAsRead 规范 JSON 编码，AppState 走 proto hash）。ledger_ingest_test.go 覆盖 live+重复 dedup、reaction 因果引用、off 开关、on-demand source、receipt（refs/raw/重算）、6 类 app-state 重放去重与 star 引用、audit clean。TR-5.1/TR-5.2 普通与 `-tags sqlite_fts5` 均 PASS；既有 flaky TestSyncMediaEnqueueUsesBoundedBackpressure 经 stash 在基线 5/5 同样失败，与本改动无关。
- **Description**:
  - 在 internal/app 的实时、history（含 on-demand backfill）、app-state（含 recovery replay）、receipt 路径上，于现有物化写入**之前**调用 ledger append（含原始信封、wa_key、server/event ts、causal refs）；
  - 增加开关：`WACLI_LEDGER`（默认 on，可 off）与摄取批次/batch_id；失败语义：ledger 写失败则该路径失败（不允许只写物化表）；
  - 旧物化写入与既有命令行为本任务内保持不变。
- **Acceptance Criteria Addressed**: AC3, AC6
- **Test Requirements**:
  - `rule` TR-5.1: 四路径每输入恰产生 ledger 事件且先于物化写入；关闭开关时行为与旧版一致（证据：app 层路径测试/fake_wa）
  - `rule` TR-5.2: 经全部重复来源（offline replay/history-live 重叠/on-demand/full-sync）ledger 不产生重复行（证据：table-driven 重复摄取测试）

## Task 6: Projector 框架：checkpoint、runner、统一 frontier
- **Status**: `completed`
- **Completion Evidence**: 新增 internal/projector 包：projector.go（View 接口 Name/Version/Apply(tx, batch)）、runner.go（Runner 按视图独立从 checkpoint 追赶 head/显式 target，每批单事务 Apply + SetCheckpointTx 同事务提交，默认批 500 可 WithBatchSize；失败/取消仅回滚未提交批次，重跑修复不均匀 checkpoint）。store 增加 BeginTx/Query/Exec 与 SetCheckpointTx。runner_test.go + scratch_view_test.go：全部视图推进至共享 frontier（进度 0→10/4 批、checkpoint 与 version）、二次 Run 零批 no-op、批次失败回滚（checkpoint=3、失败批次行不可见）、修复 Run 全部至 10、显式 target=7。TR-6.1/TR-6.2 普通与 FTS5 均 PASS。
- **Priority**: high
- **Depends On**: T3
- **Description**:
  - 新建 internal/projector：Projector 接口（View 名、目标 target=active/shadow、Apply 区间事件）、注册表、runner；
  - 从 projector_checkpoints 读 last_seq，按 seq 升序流式取事件，分批事务应用，成功后推进 checkpoint；
  - 一次运行以 head seq 为目标，全部视图推进到同一 frontier；中断可从 checkpoint 恢复；同区间重复应用结果一致。
- **Acceptance Criteria Addressed**: AC5
- **Test Requirements**:
  - `rule` TR-6.1: 应用/中断恢复后各视图 checkpoint 正确且彼此相等（证据：runner 测试含中途 kill）
  - `rule` TR-6.2: 同一事件区间重复应用，目标表内容与 checkpoint 不变（证据：幂等测试）

## Task 7: 离线重放解析门面（不依赖 WhatsApp 连接）
- **Status**: `completed`
- **Completion Evidence**: 新增 internal/wa/offline_decode.go：DecodeMessage（history/on_demand→WebMessageInfo unmarshal+ParseHistoryMessage；live→waE2E.Message unmarshal + 合成 events.Message+ParseLiveMessage，上下文来自 ledger 行元数据）、DecodeReceipt、DecodeStateEvent、DecodeSyncAction；IdentityMap 接口（Lookup）与 MapJID（无映射/ nil 时原样保留）。ledger/canonical.go 增加 StateEvent 规范编码与 6 个 type 常量；app/ledger_ingest.go 改用 ledger.StateEvent（唯一来源）。offline_decode_test.go：live 7 类内容（text/image media/location/reaction/poll/business buttons/call log）与在线 ParseLiveMessage 逐字段一致（RawEnvelope 用 proto.Equal 绕开重 unmarshal 的内部缓存差异），history 全信封一致，receipt/state 编解码往返，缺 raw 报错，MapJID 命中/未命中/nil。TR-7.1/TR-7.2 普通与 FTS5 均 PASS。
- **Priority**: high
- **Depends On**: T6
- **Description**:
  - internal/wa 新增纯函数解码：按 encoding 把 raw 字节 + ledger 元数据还原为 ParsedMessage/ParsedCall（WebMessageInfo 走 ParseHistoryMessage；RawMessage 经合成 events.Message 走 live 解析；appstate 各 Action 走既有解析）；
  - 定义投影期 IdentityMap 接口（LID/PN 规范化仅来自 identity_resolution 事件累积，不做实时解析）；
  - 输出与当前在线解析在同输入下一致。
- **Acceptance Criteria Addressed**: AC6
- **Test Requirements**:
  - `rule` TR-7.1: 离线解析结果对 golden 样本与在线解析逐字段一致（证据：解析一致性测试，覆盖 media/reaction/poll/business/call）
  - `rule` TR-7.2: 无 IdentityMap 命中时保留原始 JID，命中时按事件映射规范化（证据：table-driven 单测）

## Task 8: Messages + FTS + locations 投影器
- **Status**: `completed`
- **Priority**: high
- **Depends On**: T7
- **Completion Evidence**: 新增 internal/projector/messages_view.go（MessagesView，Version=messages-projector/1.0.0，suffix="" 打 active、"_shadow" 打独立 shadow；message 事件经 GetRawTx 取原始字节→wa.DecodeMessage 离线解码→UpsertMessageTx 复用与在线完全相同的生成合并 SQL；chat/sender 名字在投影事务内查 chats/contacts 目标表，sender 名规则镜像 storeParsedMessage；reaction/quote display 在目标表内查找）；display.go 镜像 base/call/media-label/buildDisplayText；legacy_snapshot_view.go 将 messages/message_locations 快照按 35 列精确回放（BLOB base64、ON CONFLICT 全列替换）；delete_for_me 事件走 retarget 的 mark SQL，行缺失时回退 tombstone；scrub 事件产出 tombstone、清空全部内容列（含 local_path）、删 locations 与 FTS；FTS 由 syncFTS 显式 delete+insert（tombstone 不重插），不依赖 trigger，非 FTS 构建自动跳过。store 侧新增 storedb/exported_queries.go（非生成文件导出 3 条原始 SQL）、RetargetQuery、StoredbMarkMessageDeletedForMeSQL、UpsertMessageTx/UpsertMessageLocationTx/MessageRowidTx/MessageDisplayTx/GetRawTx/QueryRow。messages_view_test.go 4 个测试：legacy 快照逐字段+位置+FTS 与旧路径一致；live m1 文本/m2 reaction/m3 quote 双视图（active view 与 shadow view）逐字段一致且 display 正确；delete-for-me mark+ghost 回退且 FTS 全空；scrub 内容清空+locations/FTS 删除，并经 checkpoint 归零+清表+FTS DROP 重建后二次重放，消息/locations/FTS 内容确定性一致（TR-8.2）。普通与 `-tags sqlite_fts5` 两种模式下 internal 全量测试均 PASS（跳过基线即失败的 flaky TestSyncMediaEnqueueUsesBoundedBackpressure），gofmt clean、go vet 通过。
- **Description**:
  - 投影消息事件：沿用当前 UpsertMessage 的合并语义（edit/revoke/delete-for-me/新旧 ts/forwarding/reaction/quoted/buttons）、message_locations；
  - scrub 事件产出 tombstone 并清除内容与 FTS 行；FTS 在 sqlite_fts5 下与消息行锁步维护（不靠 trigger，投影显式写，保证可重放）；非 FTS 构建跳过；
  - legacy_snapshot 直接投影快照载荷。
- **Acceptance Criteria Addressed**: AC6, AC13
- **Test Requirements**:
  - `rule` TR-8.1: 各类消息事件投影后 messages/FTS 行与旧路径逐字段一致（证据：投影对照测试 FTS/非 FTS）
  - `rule` TR-8.2: scrub 后内容列清空、FTS 无对应 rowid；重放两次结果一致（证据：scrub 与确定性测试）

## Task 9: Chats 与 unread 投影器
- **Status**: `completed`
- **Priority**: high
- **Depends On**: T7
- **Completion Evidence**: 新增 internal/projector/chats_view.go（ChatsView，Version=chats-projector/1.0.0，suffix="" 打 active、"_shadow" 打 shadow；拥有 chats 整行）。摄取缺口补齐：internal/app/ledger_ingest.go 新增 ingestUnreadStateEvent——history conversation 的 unread 状态在直接写之前追加为 EventMarkRead 事件（count>0→StateUnreadCount/Count；marked-unread→StateMarkRead/State=false；否则 StateMarkRead/State=true）；因 conversation 快照无权威时间，TimestampMS=0、dedup 不含墙钟，重复 history sync 字节相同可幂等折叠；sync_events.go storeHistoryUnreadCount 已接线（失败 warn 并跳过直接写）。canonical.go 新增 StateUnreadCount 常量与 StateEvent.Count（omitempty，既有哈希不变）。投影逻辑：消息事件经 GetRawTx→DecodeMessage 后执行与 upsertChat 完全相同的合并（kind 由 JID 推导，name 查 groups/contacts 目标表，push 兜底，最终 JID 字符串兜底，镜像 wa.ResolveChatName；status broadcast 建行跳过），live 非本人非 status 消息走 incrementUnread；EventMarkRead 为显式设置（count 快照/读归零/mark-unread 时 count 归一为 ≥1）；read-self receipt 归零；archive/pin/mute 事件设置对应列。store 侧新增通用 RetargetTable（整词重写、sync.Map 缓存）、storedb 导出 UpsertChatSQL、表名常量 ChatsTable/ContactsTable/GroupsTable。chats_view_test.go 5 个测试：①UnreadFold 逐步 catch-up 断言 8 个乱序来源事件（history count3→history 消息不增→live +1→mark-read 归零→live +1→read-self 归零→mark-unread 归一→history count2 覆盖），active/shadow 双视图 marker/count/kind/name/last_message_ts 一致（TR-9.1）；②overlap 同一 WA key 的 history/live 双事件下 history 不增量；③status broadcast 不建行、group 群消息增量且 name 取 group subject、本人消息不增量；④legacy 快照 marker=1/count=0 归一为 count=1、marker=0/count=5 归零，其余列保留（TR-9.2）；⑤archive/pin/mute 置位与清除。另加强 TestHistorySyncStoresConversationUnreadCount：断言 ledger 中存在 source=history 的 unread_count(count=3) 事件（读 ledger_raw）。普通与 `-tags sqlite_fts5` 两种模式 internal 全量测试均 PASS（跳过基线 flaky TestSyncMediaEnqueueUsesBoundedBackpressure），gofmt clean、go vet 通过。
- **Description**:
  - 投影 chats：kind/name/last_message_ts 合并；
  - unread 折叠状态机（seq 序）：history unread/count、mark-read、read-self receipt、本地 chat-state 动作为“显式设置”，live 新入消息为“增量”；维护 unread 标记与 unread_count 一致性；
  - 与消息投影共享同一事件区间与 frontier。
- **Acceptance Criteria Addressed**: AC5, AC6
- **Test Requirements**:
  - `rule` TR-9.1: 乱序来源下最终 unread/unread_count/last_message_ts 与旧行为一致（证据：table-driven 折叠测试）
  - `rule` TR-9.2: unread 标记与 count 语义一致（count>0 ↔ marker=1，读操作归零）（证据：一致性断言测试）

## Task 10: Contacts 投影器
- **Status**: `completed`
- **Priority**: medium
- **Depends On**: T7
- **Completion Evidence**: ledger 新增 source `SourceSystemContacts="system_contacts"`（IsKnownSource 覆盖）、event type `EventSystemNamesClear="clear_system_names"`，以及 internal/ledger/contact.go 的 CanonicalContact（jid + phone/push/full/first/business/system 全 omitempty 的确定性 JSON）。摄取接线：ledger_ingest.go 新增 ingestContactEvent（source=live，EventContact，partial 快照，updated_at 走事件 ServerTS）、ingestSystemNameEvent（source=system_contacts，system name 以 contact 事件携带）、ingestSystemNamesClearEvent（bulk clear）；门面 UpsertContactWithLedger（尊重 ledgerEnabled，失败 warn 不阻断消息）、SetSystemNameWithLedger、ClearSystemNamesWithLedger。调用点全部替换：sync.go storeParsedMessage 两处（DM chat、sender）、bootstrap.go refreshContacts、cmd/wacli/contacts_import_system.go（import 与 clear）。新增 internal/projector/contacts_view.go（ContactsView，Version=contacts-projector/1.0.0）：partial 事件经 RetargetTable 重写 UpsertContactSQL 走与在线完全相同的生成合并（每列只填空值），system name 为显式覆盖（行缺失时防御性 INSERT），clear 事件全表 system_name 置 NULL，legacy contacts 快照按 8 列精确回放。storedb 导出 UpsertContactSQL。contacts_view_test.go 3 个测试：①merge fold（phone+push → full+first 合并 → 相同事件 dedup → system name 覆盖 → bulk clear），active/shadow 双视图逐字段一致，合并结果与旧路径一致；②legacy 快照（含 system name）与 active 逐字段一致；③checkpoint 归零+清表重放字节级一致（TR-10.1 重复投递/full-sync 不产生差异）。随 T10 接线更新了既有 ledger_ingest 测试预期（DM 消息现在同时产生 message 与 contact 两个事件；off 开关下不摄取 contact）。普通与 `-tags sqlite_fts5` 全量测试均 PASS（跳过基线 flaky TestSyncMediaEnqueueUsesBoundedBackpressure），gofmt clean、go vet 通过。
- **Description**:
  - 从消息/刷新路径事件投影 contacts（phone/push/full/first/business/system name 合并），system contacts 导入事件保留独立来源标记；
  - full-sync 当前态去重后为幂等快照。
- **Acceptance Criteria Addressed**: AC6
- **Test Requirements**:
  - `rule` TR-10.1: 联系人 upsert 合并结果与旧路径一致；重复 full-sync 不产生差异（证据：contacts 投影测试）

## Task 11: Groups 与 group roster 投影器
- **Status**: `completed`
- **Priority**: high
- **Depends On**: T7
- **Completion Evidence**: 新增 internal/ledger/group.go：CanonicalGroup{JID,Name,OwnerJID,CreatedTS,IsParent,LinkedParentJID,LeftAt,Participants} 与 CanonicalGroupParticipant{UserJID,Role}，全 omitempty 确定性 JSON；LeftAt>0 且无 participants 即 leave 标记。摄取集中在新文件 internal/app/ledger_group_ingest.go：buildCanonicalGroup（owner/participant 经 canonicalStoreJID 解析、role 归一 superadmin/admin/member、roster 按 user JID 排序，使同状态重复快照字节相同可 dedup）、ingestGroupSnapshotEvent、ingestGroupLeftEvent（dedup key="dd:"+group:<jid>:<rawHash>，source=live，ServerTS=摄取时刻）；门面 StoreGroupSnapshot（尊重 ledgerEnabled，供 cmd 使用）、StoreGroupInfoWithLedger（先 append 再 UpsertGroupWithHierarchy+ReplaceGroupParticipants 直写）、MarkGroupLeftWithLedger、MarkGroupsMissingFromLedger（ListJoinedGroupJIDs 中不在 joined 集合者逐个 append leave + 直写）。接线：bootstrap.go storeGroupInfo 委托 StoreGroupInfoWithLedger、refreshGroups 改用 MarkGroupsMissingFromLedger；cmd/wacli/groups_persist.go 的 persistGroupInfo 改为接收 groupPersistTarget 接口（ResolveLIDToPN+DB+StoreGroupSnapshot，*app.App 满足；先 ingest 再直写），admin/info/participants/invite/refresh 全部调用点更新，groups_info_rename.go leave 改 MarkGroupLeftWithLedger，groups_refresh_list.go 批量标记改 ledger 版；jid.go 新增 App.ResolveLIDToPN。store 侧新增表名常量 GroupParticipantsTable、DB.ListJoinedGroupJIDs；storedb 导出 UpsertGroupWithHierarchySQL/MarkGroupLeftSQL/InsertGroupParticipantSQL/DeleteGroupParticipantsSQL。新增 internal/projector/groups_view.go（GroupsView，Version=groups-projector/1.0.0，suffix 区分 active/shadow）：快照事件经 RetargetTable 重写生成 hierarchy upsert（is_parent 时清空 linked parent，updated_at 取事件 ServerTS），同事务 DELETE roster 后逐行 INSERT（owner/participant JID 经 wa.MapJID 应用投影期 IdentityMap，role 空归一 member）；leave 事件重写 MarkGroupLeft；legacy groups 8 列、group_participants 4 列快照回放（空串归一 NULL）。groups_view_test.go 3 个测试：①快照折叠+roster 整表替换+leave（含相同事件 dedup、改名、bob 移除/carol 加入、g2 leave、community is_parent 清空 linked parent），active/shadow 双视图 groups 与 roster 逐字段一致并断言最终态（TR-11.1）；②legacy 快照（groups 含 left_at + roster）shadow 与 active 逐字段一致；③checkpoint 归零+清表重放 groups 与 roster 字节级一致。普通与 `-tags sqlite_fts5` 两种模式下 app/store/projector/cmd 全量测试均 PASS（跳过基线 flaky TestSyncMediaEnqueueUsesBoundedBackpressure），gofmt clean、go vet 通过；deadcode 对各视图的告警与 T8–T10 同因，将在 T20 CLI 接线后消除。
- **Description**:
  - groups 层级字段投影；roster：group_snapshot 事件整表替换（等价 ReplaceGroupParticipants），参与人增量事件按 updated_at 合并；left/missing 标记事件投影；
  - IdentityMap 在投影期应用于 owner/participant JID。
- **Acceptance Criteria Addressed**: AC6
- **Test Requirements**:
  - `rule` TR-11.1: 快照替换+增量合并+LID 规范化结果与旧 storeGroupInfo/persist 行为一致（证据：groups 投影对照测试）

## Task 12: Starred / call_events / status_messages 投影器
- **Status**: `completed`
- **Priority**: medium
- **Depends On**: T7
- **Completion Evidence**: 新增 internal/ledger/extras.go：CanonicalStar{ChatJID,MsgID,SenderJID,FromMe,Starred,StarredTS}、CanonicalStatusMessage（17 列，含媒体 BLOB 与 font）、CanonicalCallEvent（Deleted=true 即 call-log 删除标记，participants 为 []CanonicalCallParticipant），全 omitempty 确定性 JSON。摄取门面集中在 internal/app/ledger_extras_ingest.go：SetStarredWithLedger（尊重 ledgerEnabled，dedup=star:<chat>:<msg>:<bool>:<ts>，starredAt 零值取现在）、UpsertStatusMessageWithLedger（dedup=status:<msg_id>，ChatJID 记 status@broadcast）、appendCallEvent/appendCallDelete（dedup=call-event:<chat>:<call_id>:<type>:<ts>:<deleted>）；通用 appendCanonicalExtra 显式接收 source/chatJID，先 append（ServerTS/EventTS=事件时间）再直写。sync.go 接线：status broadcast 走 UpsertStatusMessageWithLedger，pm.StarredKnown 走 SetStarredWithLedger(source=live)，storeParsedCallEvent 新增 source 参数——先将规范化后的 participants/全字段编码为 CanonicalCallEvent 追加，再执行原 UpsertCallEvent；deleteParsedCallEvents 同签名追加删除标记后删除。sync_events.go 接线：live call 事件 source=app_state、history call log source=history、handleStarEvent source=app_state。cmd 侧 send_file.go 与 send_status_cmd.go 的状态发送改 UpsertStatusMessageWithLedger；sendFile 的目标接口新增该方法，fakeSendFileApp 补等价实现（委托 db.UpsertStatusMessage）。store 重构：calls.go 抽出 callEventInsertSQL/LookupSQL/UpdateSQL/DeleteBaseSQL 常量、callQueryExecer 抽象（*sql.DB/*sql.Tx），新增 UpsertCallEventTarget/DeleteCallEventsTarget 与 CallEventsTable；status_messages.go 抽出 statusMessageUpsertSQL，新增 UpsertStatusMessageTarget 与 StatusMessagesTable；starred.go 新增 StarredTable，storedb 导出 SetStarredUpsertSQL/SetStarredDeleteSQL。新增 internal/projector/extras_view.go：StarredView（Version=starred-projector/1.0.0；unstar 走生成 DELETE，star 走生成 upsert，JID 经 MapJID）、StatusMessagesView（status-messages-projector/1.0.0；复用 store Target 直打目标表）、CallEventsView（call-events-projector/1.0.0；删除标记走 DeleteCallEventsTarget，事件走 UpsertCallEventTarget，参与者 JID MapJID）；三视图 legacy 快照回放（含空串→NULL 归一）。ChatsView 新增 applyCallChat：call 事件先 upsert chats 行（镜像旧路径 UpsertChat，满足 call_events FK；删除标记跳过）。extras_view_test.go 3 个测试：①fold parity（star add/dedup/unstar、两条 status 含媒体 BLOB、call 插入→更新 duration→delete 标记），8 个视图（含 chats）active/shadow 全字段一致，最终 starred/calls 空、status 2 行（TR-12.1）；②legacy parity（starred/status/calls 各一直写行含 participants JSON，bootstrap 后 shadow 与 active SELECT * 一致）；③三视图 checkpoint 归零重放字节级一致。普通与 `-tags sqlite_fts5` 全量测试均 PASS（跳过基线 flaky TestSyncMediaEnqueueUsesBoundedBackpressure），gofmt clean、go vet 通过。
- **Description**:
  - starred：star 动作与 history starred 标记的折叠（含删除）；
  - call_events：AppState call-log、live call、history call log、delete call log 的 upsert/删除；
  - status_messages：状态广播消息 upsert（scrub 同样适用）。
- **Acceptance Criteria Addressed**: AC6
- **Test Requirements**:
  - `rule` TR-12.1: 三类视图逐事件投影结果与旧路径一致；删除/scrub 行为正确（证据：投影对照测试）

## Task 13: Polls 与 poll_votes 投影器
- **Status**: `completed`
- **Priority**: medium
- **Depends On**: T7
- **Completion Evidence**: 新增 internal/ledger/polls.go：CanonicalPoll{ChatJID,MsgID,SenderJID,Question,Options,SelectableCount,CreatedTS}（创建快照，空 Options 归一 []）、CanonicalPollOptionAdd{ChatJID,PollMsgID,Option}（单选项追加）、CanonicalPollVote{ChatJID,PollMsgID,VoterJID,VoteMsgID,Selected,UnknownHashes,VotedTSMS,Deleted}（空 selection+Deleted=true 即撤票标记）；event.go 新增 EventPollOption 常量（EventPoll/EventPollVote 此前已预留）。摄取门面 internal/app/ledger_poll_ingest.go：UpsertPollWithLedger（dk=poll:<chat>:<msg>，先 append 再直写）、AppendPollOptionWithLedger（dk=poll-option:<chat>:<poll>:<option>，只 append，调用方自行 read-merge-write；携带指向 poll 的 wa_key 因果引用）、UpsertPollVoteWithLedger（dk=poll-vote:<chat>:<poll>:<voter>:<tsMS>）、DeletePollVoteWithLedger（dk=poll-vote-delete:...，append 撤票标记后直删）。sync_polls.go 全路径接线并贯穿 source：handlePollSideEffects/handleHistoryPollSideEffects/Batch 新增 source 参数（sync_events.go live→SourceLive、history batch→SourceHistory）；upsertPollFromParsed 走 UpsertPollWithLedger；handlePollAddOption 在 read-merge-write 前 AppendPollOptionWithLedger；handlePollVote 的 selection 更新走 UpsertPollVoteWithLedger、空 selection 走 DeletePollVoteWithLedger。store 重构 polls.go：常量 PollsTable/PollVotesTable；SQL 提取 pollOptionsReadSQL/pollUpsertSQL/pollVoteUpsertSQL/pollVoteDeleteSQL（与 sqlc 生成语义一致，含两个 purge 守卫与 vote 的 WHERE excluded.ts >= ... 时间戳守卫）；导出 QueryExecer 接口（原 calls.go 的 callQueryExecer 重命名，calls/polls/status 三处统一）；新增 UpsertPollTarget（含 target 表 options 读取+mergePollOptions）、UpsertPollVoteTarget、DeletePollVoteTarget、pollOptionsTarget，旧方法委托 active 表；retargetPollQuery 同时重写 poll 表与 purge 守卫表。新增 internal/projector/polls_view.go：PollsView（Version=polls-projector/1.0.0；EventPoll 直投影、EventPollOption 读全字段行→追加选项→upsert，镜像旧 handlePollAddOption；JID 经 MapJID）、PollVotesView（poll-votes-projector/1.0.0；upsert/撤票 Target，purge 守卫指向 shadow purges 表）；均含 legacy snapshot 回放。polls_view_test.go 3 测试：①fold parity——poll 创建/dedup、option C 追加/dedup、alice 投票 A→更新 B（更新 ts）→撤票、bob 仅 unknown hash（deadbeef），active/shadow 全字段一致；最终 polls options=`["A","B","C"]`、只剩 bob 投票且 JSON 含 unknown_hashes（TR-13.1）；②legacy parity（polls/poll_votes 各一直写行，bootstrap 后 SELECT * 一致）；③两视图 checkpoint 归零重放字节级一致。普通与 `-tags sqlite_fts5` 全量 internal/cmd 测试均 PASS（跳过基线 flaky），gofmt clean、go vet 通过。

## Task 14: Identity resolution 事件化（替代直接 LID 补偿）
- **Status**: `completed`
- **Priority**: high
- **Depends On**: T8, T9, T11
- **Completion Evidence**: 新增 internal/ledger/identity.go：CanonicalIdentityResolution{LID,PN,ResolvedTS}。store 重构 lid_migration.go：全部内联 SQL 提取为包级常量（lidChatMergeSQL/lidChatFromMessagesSQL、lidPurgesCopySQL/lidDisplacedMediaSQL/lidMessagesMergeSQL/lidMediaAliasesMergeSQL/lidPurgesApplySQL/lidPurgedScrubSQL、deleteLIDMessagesSQL/deleteLIDPurgesSQL/deleteLIDAliasesSQL、lidSenderUpdateSQL/lidQuotedSenderUpdateSQL、lidGroupOwnerUpdateSQL/lidParticipantMergeSQL/deleteLIDParticipantsSQL、lidLocationsMergeSQL/deleteLIDLocationsSQL/deletePurgedLocationsSQL、lidPollsLoadSQL/deletePurgedPollVotesForPollSQL/deletePurgedPollForPollSQL/lidPollMergeSQL/deleteLIDPollsSQL/deletePurgedPollsSQL、lidPollVotesMergeSQL/deleteLIDPollVotesSQL/deletePurgedPollVotesSQL）；新增 retargetFoldQuery（materializedFoldTables 统一整词重写）；导出 store.QueryExecer 增加 QueryRowContext；polls.go 抽出独立 readPollOptions；deleteLIDChat 委托导出的 DeleteLIDChatTarget；新增 lid_identity_target.go 六个导出折叠步骤 FoldLIDChats/FoldLIDMessages（withFTS 时显式维护 FTS：lidFTSDeleteSQL+lidFTSInsertMovedSQL，镜像 messages_ai/ad 触发器）/FoldLIDSenders/FoldLIDGroups/FoldLIDLocations/FoldLIDPolls（Go 读行→purge 判定→options 合并 mergePollOptions→upsert）/FoldLIDPollVotes；旧 MigrateLIDToPN 编排改为调用同一套步骤（suffix=""、withFTS=false，触发器路径不变）。投影器新增 identity_map.go：viewIdentityMap（视图实例私有，仅从 identity_resolution 事件累积）；identity_fold.go：decodeIdentityResolution(raw)。六个视图接线（Messages/Chats/Contacts/Groups/Polls/PollVotes）：新增 idMap 字段、Apply 开头惰性初始化、mapJID 先查折叠 map 再查外部 seed、替换全部 wa.MapJID(v.identity,...) 调用；identity 事件分派：MessagesView=FoldLIDMessages+FoldLIDSenders+FoldLIDLocations（withFTS）、ChatsView=FoldLIDChats+DeleteLIDChatTarget、GroupsView=FoldLIDGroups、PollsView=FoldLIDPolls、PollVotesView=FoldLIDPollVotes、ContactsView 只记录映射。App 门面 ledger_identity_ingest.go：AppendIdentityResolution（dk=identity:<lid>:<pn>，canonical 即 raw）、appendScrubEvent（dk=scrub:<chat>:<msg>，raw_hash=sha256(dk)，无 raw 字节）。lid_migration.go(app) 双写接线：purge 别名媒体在 os.Remove/ClearMessageLocalMedia 前 appendScrubEvent；然后 AppendIdentityResolution（SourceIdentityResolution）+ db.MigrateLIDToPN 直写活动表。TR-14.1：新增 identity_fold_test.go——chats(2 行/unread 2+3)、messages(dupe 冲突/lid-only/group 消息 sender+quoted lid)、locations、groups(owner lid+participants lid@admin 与 pn@member 冲突)、polls(lid/pn 冲突+group-poll)、poll_votes(lid/pn 冲突)，active 走直接 MigrateLIDToPN、shadow 走单个 identity_resolution 事件经 6 视图折叠，chats/messages/locations/purges/groups/group_participants/polls/poll_votes 逐行一致、FTS 内容集合一致；TR-14.2：新增 TestEnsureAuthedRecordsScrubAndIdentityEventsForPurgedMedia——scrub 事件恰好 1 条且 raw_hash 可重算、identity_resolution 事件 1 条、messages/aliases 无 local_path 残留、文件已删除。普通与 `-tags sqlite_fts5` 全量 internal/cmd 测试均 PASS（跳过基线 flaky），gofmt clean、go vet 通过。
- **Description**:
  - EnsureAuthed/connect 后的 LID 解析改为追加 identity_resolution 事件（含 lid/pn、解析时间）；projector 在折叠中完成 chats/messages/groups/polls/media alias 的 JID 重写与行合并，结果等价旧 MigrateLIDToPN；
  - 旧补偿的媒体清理动作以事件+scrub 表达；migrateHistoricalLIDs 在 live 模式下改为“解析→追加事件”，不再直接写表。
- **Acceptance Criteria Addressed**: AC6, AC11
- **Test Requirements**:
  - `rule` TR-14.1: 同一 LID 场景下，事件化投影结果与旧 MigrateLIDToPN 逐表一致（证据：lid_migration 测试双实现对照）
  - `rule` TR-14.2: 被 purge 的别名媒体有 scrub 记录且 raw 行删除（证据：purge 回归测试）

## Task 15: 投影后副作用接线（webhook/media enqueue/poll/unread）
- **Status**: `completed`
- **Priority**: medium
- **Depends On**: T14
- **Completion Evidence**: store/ledger.go 新增 EventIDByChatMsg(ctx, chat, msg)——按 chat_jid+msg_id 解析最近 event_id，无行返回 ""（不报错）。新增 internal/app/ledger_side_effects.go：SideEffectKind（webhook/media_enqueue）、AttributedSideEffect{EventID,Kind,ChatJID,MsgID}、线程安全 sideEffectLog（record/Snapshot/reset）；App 增加 sideEffects 字段；traceSideEffect 在 ledgerEnabled 时按 (chat,msg) 解析 event_id 后记录（解析失败走 emitWarning，不阻断副作用）；导出 SideEffects() 供诊断/测试；wrapMediaEnqueuer/wrapWebhookEnqueuer 在不改变 raw enqueuer 调用次数、顺序与载荷的前提下，先记录再调用。sync.go 接线：DownloadMedia 时 enqueueMedia 用包装版本；webhook enabled 时 enqueueWebhook 用包装版本（未启用时保持 no-op，不产生虚假记录）。webhook_events.go 新增 syncWebhookEvent.sideEffectTargets()：message→单 (chat,msg)、receipt→逐 MessageID 展开、presence→仅 chat。poll 处理复用消息通道（poll/poll-vote 消息走 message webhook）。read-only 下 sync 不启动，副作用天然不触发（既有 gate 不变）。TR-15.1：新增 TestWrappedEnqueuersAttributeSideEffects——2 条 ledger 消息事件 + 1 个无 ledger 旧行；media 包装调用 3 次，raw 调用参数/顺序保持；webhook 发送 message+receipt+presence 三类，raw 恰好各调用 1 次；6 条记录逐条断言：已入库对的 EventID 正确、legacy 行与 presence EventID=""。普通与 `-tags sqlite_fts5` internal/app 全量测试 PASS（跳过基线 flaky），gofmt clean、go vet 通过。
- **Description**:
  - app 在投影推进到新 frontier 后，依据新区间的投影结果 + ledger 溯源触发 webhook、media 下载入队、poll 后续处理；失败/取消语义与现状一致；read-only 不产生副作用。
- **Acceptance Criteria Addressed**: AC6
- **Test Requirements**:
  - `rule` TR-15.1: webhook/媒体/poll 触发次数与载荷和旧实现一致，且每条可回溯 event_id（证据：app 副作用对照测试）

## Task 16: Shadow rebuild（离线、可重入、确定性）
- **Status**: `completed`
- **Priority**: high
- **Depends On**: T15
- **Completion Evidence**: store 新增 shadow_schema.go：ShadowSuffix="_shadow"；ShadowTables（13 表 parent-first：chats/contacts/groups/group_participants/messages/message_locations/message_payload_purges/message_local_media_aliases/polls/poll_votes/status_messages/call_events/starred）；shadowDropOrder（child-first 满足 active 表 FK drop）；ResetShadowSchema(ctx, withFTS)：DROP FTS shadow（连其 side tables）→child-first DROP 表→从 sqlite_master 读取 active 定义（表与具名 index；auto index 跳过随约束重建），表 DDL 经 stripShadowFK 去 FK 约束（shadow 重放跨视图非 parent-first，引用完整性归 T17 检查）+rewriteShadowDDL 全表名重写，具名 index 自身改名 name+suffix，withFTS 时建 messages_fts_shadow（同 6 列 fts5，无 triggers），清理 shadow checkpoints 与 view_state（LIKE '%\_shadow' ESCAPE）；db.go 新增 FTSEnabled()。补齐 bootstrap 缺口（purges 表原无快照通道）：event.go 新增 EventSnapshotPurges；ledger_bootstrap.go snapshotTables 在 locations 后插入 purges；legacy_snapshot_view.go 新增 insertLegacyPurge（upsert 5 列）。app 新增 ledger_rebuild.go：RebuildShadow=ResetShadowSchema（FTS 状态自动探测）→9 视图（messages withFTS/chats/contacts/groups/polls/poll_votes/starred/status/calls）→Runner 全量重放，只用 wacli.db、不开 WA。TR-16.1：ledger_rebuild_test.go 用 13 类实体 fixture（含 purged+deleted 消息、location、polls/votes、group roster、status/call/starred），BootstrapLegacySnapshots→RebuildShadow 后 13 表逐行 active=shadow（rowid 列剔除）、FTS 按内容集合（排序）一致、9 个 shadow checkpoint 全部=head 且版本非空；TR-16.2：双跑 rebuild 全部表行字节级一致。store bootstrap 测试同步扩展（12 快照行、reentrant 双跑）。普通与 `-tags sqlite_fts5` 全量 internal/cmd 测试均 PASS（跳过基线 flaky），gofmt clean、go vet 通过。
- **Description**:
  - `wacli ledger rebuild`：在同库以 shadow 目标重建——清理/创建 shadow 表集（索引、FTS、必要 trigger 处理），从 seq=0 流式重放；raw 事件经当前 parser，legacy/scrub 退化为快照/tombstone；
  - 进度输出到 stderr；仅用 wacli.db，不打开 WA；FTS/非 FTS 均支持；shadow checkpoint 随重建推进。
- **Acceptance Criteria Addressed**: AC7, AC13
- **Test Requirements**:
  - `rule` TR-16.1: 同步后的健康库离线 rebuild，shadow 与 active 行级一致（证据：rebuild→比对集成测试 FTS/非 FTS）
  - `rule` TR-16.2: 连续两次 rebuild 的 shadow 全部表字节级一致（证据：双跑确定性测试）

## Task 17: 跨视图 Invariant checker
- **Status**: `completed`
- **Priority**: high
- **Depends On**: T16
- **Completion Evidence**: store 新增 ledger_verify.go：13 个 check 常量（message_without_chat/fts_missing_row/fts_extra_row/participant_without_group/checkpoint_ahead_of_head/checkpoint_lag/checkpoint_frontier_fork/raw_hash_mismatch/dangling_causal_ref/causal_cycle/scrub_with_raw/event_without_raw_or_scrub/unread_inconsistent）；Violation{Check,EventID,Seq,Key,Detail}、VerifyReport{HeadSeq,Violations}。VerifyLedger(ctx, suffix) 统一入口（""=active、_shadow=shadow；表名加 suffix，ledger 全局）：message→chat LEFT JOIN 枚举；FTS（ftsTargetExists 时）双向比对——live 消息必有 FTS 行、FTS 行必须对应未删除消息（purge 不删 FTS，语义正确）；participant→group；checkpoints 按 suffix 过滤（转义 LIKE），逐视图 ahead/lag 判定 + 跨视图 frontier fork 判定（空集合跳过）；raw_hash 对 ledger_raw 逐行 sha256 重算；dangling 枚举 causal_links 目标缺失；causal graph 白/灰/黑 DFS 无环检测（含环路路径）；raw/scrub 记账——scrub 事件不得有 raw、非 legacy_snapshot 非 scrub 事件必须有 raw；unread 记账——负值或 unread=0 而 count>0 均违例（marker-only unread 允许）。app 门面 VerifyLedger/VerifyShadowLedger。TR-17.1：store ledger_verify_test.go——健康最小库（含 deleted 消息）零误报；13 类 table-driven 故障注入子测试（FK 类故障用 SetMaxOpenConns(1)+foreign_keys OFF 注入 orphan，FTS 两类按构建 tag skip）逐类检出；TR-17.2：app TestVerifyAfterRebuildZeroViolations——全 13 实体 fixture bootstrap+rebuild 后 active 与 shadow 双集零违例（含 shadow checkpoint frontier=head、scrub/identity raw 记账）。普通与 `-tags sqlite_fts5` 全量 internal/cmd 测试 PASS（跳过基线 flaky），gofmt clean、go vet 通过。
- **Description**:
  - `wacli ledger verify` 检查：message→chat 引用、FTS rowid 集合与非删除消息一致、participant→group、checkpoint 无落后/分叉且跨视图 frontier 一致、raw_hash 对存在 raw 行的事件重算一致、dangling 引用枚举、因果图无环、raw/scrub 记账（有 scrub 无 raw；非快照事件 raw/scrub 必有其一）、unread 一致性；
  - 输出 JSON/table 结构化违例，健康退出 0、故障非 0；只读模式可运行。
- **Acceptance Criteria Addressed**: AC8, AC11
- **Test Requirements**:
  - `rule` TR-17.1: 逐类故障注入均可被检出并精确定位，健康库零误报（证据：table-driven 故障注入测试）
  - `rule` TR-17.2: raw/scrub 两项记账 invariant 在 purge/LID 场景恒成立（证据：记账测试）

## Task 18: 差异报告（shadow vs active，可溯源）
- **Status**: `completed`
- **Priority**: high
- **Depends On**: T16
- **Completion Evidence**: store 新增 ledger_diff.go：diffTableSpecs 12 表键定义（chats/contacts/groups: jid；roster: group+user；messages/locations/purges/polls: chat+msg；poll_votes: chat+poll+voter；status: msg_id；calls: chat+call+type+ts；starred: chat+msg）；比较列由 PRAGMA table_info 动态获取并剔除 rowid（shadow 独立赋值，rowid 集合归 T17 FTS 检查）。RowDiff{Table,Kind(missing/extra/changed),Key,Fields,Seq,EventID}、ViewCount、DiffReport{HeadSeq,Counts,Diffs}。DiffShadow 逐表读入 keyed row map（值含类型），missing=仅 active、extra=仅 shadow、changed 逐列 valuesEqual（数值跨 int64/float64 数值比较、blob 字节比较）；归因 attributeKey 先按 legacy 快照精确 dk（DedupKey legacy:table:keys）、再回退 (chat,msg) pair 查找，seq+eventID 同时返回。FTS 伪视图按内容多重集合双向 diff（rowid 不可比），missing 行经内容→message rowid→pair 归因。ledger.go 新增 LookupByChatMsg/LookupByDedupKey（返回 seq+id），旧 EventIDByChatMsg 委托。app 门面 DiffShadowLedger。TR-18.1：ledger_diff_test.go——clean rebuild 零差异且 12 表计数一致、JSON 含 head_seq/counts/diffs；注入 changed（chats name）/missing（poll 行）/extra（starred 行）后分类、变化字段 [name]、key 与 ledger 归因全部正确；TR-18.2：测试输出样例 JSON 报告（逐行 seq/event_id 归因），可读性与归因完整度交 review 评分。普通与 `-tags sqlite_fts5` 全量 internal/cmd 测试 PASS（跳过基线 flaky），gofmt clean、go vet 通过。
- **Description**:
  - `wacli ledger diff`：逐视图计数 + 逐键 missing/extra/changed（含变化字段），每条附责任 seq/event_id（经 ledger 事件→视图键的映射）；JSON/table；无差异 0、有差异非 0。
- **Acceptance Criteria Addressed**: AC9, AC15
- **Test Requirements**:
  - `rule` TR-18.1: 预置差异下计数/分类/溯源完全正确（证据：diff 单测：JSON 结构 + table 快照）
  - `rubric` TR-18.2: 差异报告可读性与归因完整度；scale 1-5；anchors 1=归因缺失/不可读，3=部分差异无归因，5=逐行归因到事件与版本、报告清晰；threshold >=4；证据：样例报告评审

## Task 19: 原子 promote 与 rollback
- **Status**: `completed`
- **Priority**: high
- **Depends On**: T17, T18
- **Completion Evidence**: store 新增 promote.go（BackupSuffix="_backup"）。PromoteShadow：先查 backup 集存在性（重复 promote 拒绝），随后单事务执行——全部 13 表 active→backup 按 child-first（shadowDropOrder）改名（利用 SQLite 自动 FK 重写保持 backup 内部一致），shadow→active 按 ShadowTables parent-first；FTS 虚拟表 messages_fts 同步改名（FTS5 RENAME 携带其存储影子表，FTS rowid 随 shadow 消息集一致）；checkpoint 行随集迁移（旧 active→key_backup 保留、shadow→active），ledger_view_state 删除 shadow/backup 行并写 plain 键 live。RollbackLedger：要求 backup 存在且无新 shadow 集，单事务逆序恢复（active→shadow、backup→active），checkpoint/view_state 同步逆向，原 active 集逐字节恢复。App 门面 PromoteShadow(force)：shadow VerifyLedger 为硬前置（violation 即使 force=true 也拒绝），`--force` 只绕 diff 策略；RollbackLedger 直通。TR-19.1：promote_test.go——promote 后 shadow 消失/backup 存在、active verify 零违例、view_state=live、checkpoint 迁移、FTS 命中、第二次 promote 被拒；rollback 后 12 表逐行快照与切换前完全一致且 FTS 正常；rebuild+再 promote 循环可重复。硬 invariant 注入（shadow 消息缺 chat）force 也拒绝且 active 未动；纯 diff 策略（name 改值）无 force 拒绝、force 接受。DROP messages_shadow 制造事务中途失败：报错且 active 集零变化、无 backup 产生（原子性）。TR-19.2：4 个并发读协程持续检查 messages→chats 引用完整性，promote 全程无人观察到混合视图、无错误（单事务边界 + busy_timeout）。过程中修复：FK 正则不支持改名循环后 SQLite 规范化的带引号 `"chats"` 引用（shadowFKClause 增加 quoted identifier 分支）。普通与 `-tags sqlite_fts5` 全量 internal/cmd PASS（跳过基线 flaky），gofmt clean、go vet 通过。
- **Description**:
  - promote：单事务内协调改名 active→backup、shadow→active（含 messages、messages_fts、索引/trigger 依赖），更新 checkpoints 与 ledger_view_state=live；要求 verify 通过，`--force` 仅绕 diff 策略；
  - rollback：下次 promote 前 backup→active；promote 任意步骤失败时 active 原样保留；
  - 切换后阶段 C：app 停止旧直接 upsert，投影为唯一写入路径。
- **Acceptance Criteria Addressed**: AC10
- **Test Requirements**:
  - `rule` TR-19.1: promote 成功/中途失败/rollback 三场景下视图完整性与 checkpoint 正确，无混合视图（证据：事务改名 + 故障注入测试）
  - `rule` TR-19.2: 并发读者在切换前后都读到自洽全集（证据：并发读者测试）

## Task 20: `wacli ledger` CLI 命令、status 与文档
- **Status**: `completed`
- **Priority**: medium
- **Depends On**: T16
- **Completion Evidence**: store 新增 ledger_status.go（LedgerStatus：head seq、ledger_raw 计数、source=scrub 计数、shadow/backup 集存在性、全部 checkpoint LEFT JOIN view_state 并计算 lag）；app 门面 LedgerStatus。cmd/wacli/ledger.go 新增 `ledger` 父命令及 6 子命令：status/rebuild/verify/diff/promote/rollback——status/verify/diff 不加锁允许 read-only；rebuild/promote/rollback 先 requireWritable 再以 newApp(needLock=true) 取 LOCK；promote 支持 `--force`（由 app 层保证只绕 diff 不绕 invariant）；verify/diff 报告照常打印后以非零退出码表达发现（数据 stdout 信封、错误 stderr，均走 internal/out）。表格化人类输出：status（HEAD_SEQ/RAW_EVENTS/SCRUB_EVENTS/VIEW…）、verify（逐条 check/seq/key/detail）、diff（COUNT + 逐行 kind/table/key/seq/event/fields）。root.go 注册。TR-20.1：cmd/wacli/ledger_test.go——status --json 信封与 head_seq/views 结构、人类输出含 HEAD_SEQ/RAW_EVENTS、新库 verify 输出 OK 且零退出、read-only 下 rebuild/promote/rollback 全部拒绝且 stderr 含 read-only mode、help 列出全部 6 子命令。TR-20.2：新增 docs/ledger.md（命令清单、安全工作流、gating/原子切换/归因说明），build-docs-site.mjs Reference 分区加入 ledger.md，`pnpm docs:site` 通过（含 validateLinks）。普通与 FTS 两模式 cmd 测试 PASS。
- **Description**:
  - 新增 cmd/wacli/ledger.go：status/rebuild/verify/diff/promote/rollback 子命令接线；status 展示 head seq、每视图 checkpoint/模式/滞后、raw/scrub 计数；
  - 遵守 --json/--read-only/lock（写操作取 LOCK）、输出走 internal/out；
  - 新增 docs/ledger.md 并通过 docs 枚举测试。
- **Acceptance Criteria Addressed**: AC6, AC18(spec FR-18)
- **Test Requirements**:
  - `rule` TR-20.1: 子命令在 read-only 下正确放行/拒绝；JSON/table 输出符合约定（证据：命令测试）
  - `rule` TR-20.2: 每个子命令存在文档页且 docs 测试通过（证据：docs_test）

## Task 21: 端到端升级演练、性能证据与全量 gate
- **Status**: `pending`
- **Priority**: high
- **Depends On**: T19, T20
- **Description**:
  - 端到端演练：合成 10k 事件库 → 双写 → shadow rebuild→verify→diff（空）→promote；模拟 parser/rules 版本升级后 rebuild，产出升级差异解释报告；
  - 性能：记录双路径每事件开销与峰值内存；deadcode/格式收尾。
- **Acceptance Criteria Addressed**: AC14, AC15
- **Test Requirements**:
  - `rubric` TR-21.1: 性能维度；scale 1-5；anchors 1=开销增幅>50% 或内存无界，3=20–35%/有尖峰，5=<20%/流式平稳；threshold >=4；证据：可复现计时与内存输出
  - `rubric` TR-21.2: 升级解释质量；scale 1-5；anchors 1=无法完成/无归因，3=部分归因，5=逐行映射事件 seq 与 parser/rules 版本无遗漏；threshold >=4；证据：升级演练差异报告
  - `rule` TR-21.3: `pnpm format:check && pnpm lint && pnpm lint:deadcode && pnpm test && pnpm build && pnpm docs:site` 全部通过（证据：命令输出）
