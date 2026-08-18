# AliquotSeal 生物样本母管分装谱系固化与整批隔离服务

## 项目目标

构建一个无前端、单节点 Go HTTP 后端，为生物样本库执行一支母冻存管的不可逆分装流程。母管依次处于 available、reserved、thawed、aliquoting、depleted，并最终进入 finalized 或 quarantined；会话冻结样本、批次修订、母管、整数微升可分装体积及子管计划。系统以 SQLite WAL 事务维护体积预留账簿和母子谱系图，确保子管创建、母管扣减及谱系边写入原子完成，并在重启后恢复一致占用和终局。高级复杂度仅为并发仲裁与数据库事务恢复。标准规模规划为 26 个左右生产 Go 文件、约 2400 至 2800 行有效生产 Go 代码，覆盖至少 6 个有意义的内部包；交付 go.mod、可运行服务、多阶段 Dockerfile，并支持 linux/amd64 与 linux/arm64。

## 端到端业务流程

1. 目录建档：在空库中事务化登记虚构样本、追加式批次修订及母管；母管绑定唯一样本、批次修订、整数微升体积和 available 状态，已被流程引用的目录事实不可改写。
2. 预约占用：提交母管、样本、批次修订、可分装体积及有序子管计划。系统验证身份一致、母管可用、子管编号全局未占用、每管计划量为正且计划总量不超额，然后原子建立会话和子管编号声明并把母管置为 reserved。
3. 解冻确认：操作员报告实际扫描到的母管、样本和批次修订。完全匹配时进入 thawed；物理身份或批次证据不匹配时，同一事务将母管、会话及全部计划子管声明置入整批 quarantined 终局。
4. 计划分装：每次操作只能创建计划中的一个子管且分配量必须精确等于冻结计划。事务同时插入子管容器、扣减母管剩余量、写体积分录、建立待固化谱系边并推进会话修订；允许登记非负整数损耗，但不得侵占尚未创建子管所需的计划量。
5. 复冻核验：已创建子管逐管提交样本、批次修订、分配量及复冻运行编号。匹配证据被持久化；不匹配证据触发整个分装会话隔离。全部计划子管已创建、母管剩余量归零且每支子管均已核验后，会话才具备固化条件。
6. 终局竞争：固化操作生成按子管编号稳定排序的唯一谱系清单，并把待固化边变为不可变；隔离操作则冻结原因和受影响母管、已创建子管及未执行计划。并发固化与隔离按首个成功提交决定唯一终局，失败竞争者只能取得稳定终局冲突结果。

## 核心组件与职责

1. 样本与容器目录（internal/catalog，约 3 个生产文件）：定义样本、批次修订、母管和子管身份，校验目录引用、容器类型、批次匹配及全局管号可用性，预计约 250 行。
2. 分装会话（internal/aliquot，约 4 个生产文件）：定义预约快照、母管状态机、子管计划、解冻证据、损耗及终局命令，集中产生稳定领域错误，预计约 400 行。
3. 体积预留账簿（internal/volume，约 3 个生产文件）：使用 int64 微升记录冻结起始量、子管分配、损耗和剩余量，并验证每次提交后的守恒式及未执行计划覆盖量，预计约 300 行。
4. 母子谱系图（internal/lineage，约 3 个生产文件）：维护一母多子的待固化边、逐管复冻核验、整批隔离标记和唯一固化清单，预计约 300 行。
5. 会话状态聚合（internal/aggregate，约 4 个生产文件）：装载目录、会话、账簿和谱系状态，执行命令、处理操作号语义幂等、使用按母管或会话键控的同步边界，并生成一致的会话视图，预计约 450 行。
6. 事务仓库（internal/store/sqlite，约 5 个生产文件）：负责迁移、SQLite 事务、条件更新、唯一约束、操作结果、恢复检查及事务中断检查点，预计约 550 行；另以约 4 个薄适配文件实现 internal/httpapi 和 cmd/aliquotseal，使总生产文件自然达到约 26 个。

## 领域规则与不变量

1. 母管状态只允许 available -> reserved -> thawed -> aliquoting -> depleted -> finalized；从 reserved、thawed、aliquoting 或 depleted 可转入 quarantined。finalized 和 quarantined 均不可撤销。
2. 预约时的可分装体积必须等于母管当时的全部剩余体积；样本编号、批次编号、批次修订、母管编号、计划顺序、子管编号和各自分配量作为不可变快照保存。
3. 一个母管最多拥有一个非终局会话；除事务条件检查外，数据库还以唯一开放占用约束兜底。终局母管不能再次预约。
4. 子管编号在预约时即被声明且永久不可复用；计划内编号不得重复，也不得与目录中已有容器或其他会话声明冲突。
5. 所有体积均为非负 int64 微升。每个已提交会话版本必须满足：冻结可分装量 = 已创建子管分配量之和 + 已记录损耗之和 + 母管剩余量，且剩余量不得小于未创建子管的计划量之和。
6. 创建子管只接受计划中的精确分配量。容器插入、母管剩余量更新、账簿分录、谱系边、计划完成标记、会话修订和操作结果必须在同一事务提交。
7. 预约参数与目录不匹配时拒绝且不占用母管；解冻或复冻阶段报告的物理样本、批次修订或容器证据不匹配时，原子形成整批隔离终局。隔离范围是本次会话的母管、全部已创建子管及尚未执行的子管计划。
8. 母管只有在全部计划子管已创建且剩余量为零时进入 depleted；尚有计划量时，损耗登记不得消耗该计划量。
9. 固化要求母管为 depleted，并且每个计划子管都具有匹配冻结快照的复冻核验。缺失项按子管编号稳定排序返回，且固化尝试不得部分改变谱系。
10. 固化清单包含会话、母管、样本、批次修订、冻结起始量、总损耗、每支子管分配量和复冻运行编号；同一会话最多一份，生成后内容不可修改。
11. 每个变更请求携带全局 operation_id、命令类型、目标、期望会话修订及语义字段。仓库先查询操作记录：同一操作号和相同语义指纹返回原结果，不同指纹返回 OPERATION_CONFLICT，均不重新执行业务逻辑。
12. 成功结果及可稳定重放的业务拒绝都与操作指纹一同持久化；事务中断错误不保存操作结果，因此同内容重试可以重新执行完整事务。
13. 固化和隔离使用会话修订条件及唯一终局约束竞争。首个提交者决定终局；后到操作不得覆盖结果，固化清单也不能重复生成。
14. 服务启动时从数据库重新构造开放占用、母管状态、账簿余额和谱系状态，并验证守恒、子管父边及终局唯一性；检测到已提交数据违反不变量时拒绝启动并报告具体会话。

## 数据模型与持久化

1. specimens：sample_id 主键及不可变样本描述。
2. batch_revisions：以 sample_id、batch_id、revision 为复合唯一键，保存追加式批次事实；会话引用精确修订。
3. containers：tube_id 主键，记录 mother 或 child 类型、样本和批次修订、整数微升剩余量、状态及所属会话；子管父关系不在此表中隐式推断。
4. aliquot_sessions：session_id、mother_tube_id、冻结目录字段、locked_volume_uL、状态、revision、终局类型及创建时间；对开放 mother_tube_id 建唯一约束。
5. planned_children：session_id、ordinal、child_tube_id、planned_volume_uL、created 标记；child_tube_id 全局唯一。
6. volume_entries：按 session_id 和递增序号保存 child_allocation 或 recorded_loss、quantity_uL、关联子管及 operation_id，用于重算守恒。
7. lineage_edges：session_id、parent_tube_id、child_tube_id、allocation_uL、pending 或 finalized 状态；child_tube_id 唯一且受外键约束。
8. refreeze_verifications：每个 child_tube_id 最多一条，保存匹配的样本、批次修订、分配量、freeze_run_id、操作号和服务端时间。
9. session_outcomes：每会话最多一条 finalized 或 quarantined 终局，保存提交修订、原因及稳定摘要。
10. lineage_manifests 与 manifest_items：每个 finalized 会话最多一个清单头；明细以稳定序号保存所有子管，禁止终局后更新。
11. operation_results 与 session_events：前者按 operation_id 保存语义指纹、状态码和确定性结果；后者保存状态修订、命令类型及结果，供恢复核对和审计。

## 公开接口

1. 提供 Go 领域服务接口 ReserveMother、ConfirmThaw、CreateChild、RecordLoss、VerifyRefreeze、FinalizeLineage、QuarantineBatch 和 GetSession，公共测试可绕过网络直接调用相同聚合逻辑。
2. POST /v1/catalog/bootstrap：仅在空目录执行一次事务化建档，登记样本、批次修订和 available 母管；重复的相同建档返回原结果，冲突内容被拒绝。
3. POST /v1/sessions：预约母管并提交冻结子管计划。
4. POST /v1/sessions/{id}/thaw、/children、/losses 和 /children/{tube}/refreeze：分别确认解冻、按计划创建子管、登记损耗和登记复冻核验。
5. POST /v1/sessions/{id}/finalize 与 /quarantine：竞争写入唯一终局；响应包含提交后的 revision、终局及稳定错误码。
6. GET /v1/sessions/{id}：返回冻结快照、母管状态、守恒汇总、按计划顺序排列的子管状态、缺失核验及终局；GET /v1/sessions/{id}/manifest 仅在固化后返回不可变清单。
7. HTTP JSON 禁止未知字段并限制请求体、字符串长度和子管计划数量；领域错误映射为固定机器码，operation_id 冲突、修订冲突和终局冲突使用可预测的 409 响应。
8. 仓库提供受控同步屏障及命名事务检查点接口，仅用于确定性并发和恢复测试；生产默认实现为空操作。

## 失败边界

1. 任何目录查询、状态校验或体积校验失败都发生在首次业务写入前；错误返回当前 revision 和稳定错误码。
2. 子管创建事务设置 after_child_insert、after_volume_debit、after_lineage_insert 和 before_commit 四个命名检查点；检查点返回中断错误时必须回滚全部变更。
3. 终局事务设置 after_terminal_claim、after_manifest_insert 和 before_commit 检查点；中断后不得留下终局行、半份清单或已固化的部分谱系边。
4. SQLite 锁等待、提交失败或上下文取消均不更新进程内会话视图；后续查询必须重新读取最后一个已提交版本。
5. 操作号只在事务成功提交或业务拒绝结果成功保存后才算已消费；数据库错误不得产生虚假的幂等成功。
6. 意外重启后由 SQLite 完成未提交事务回滚，启动恢复再核对容器余额、账簿总和、开放占用、父子边和终局清单。
7. 复冻核验缺失只阻止固化，不推测核验成功、不自动补记录；操作者仍可补齐核验或提交整批隔离。
8. 若恢复检查发现无父子管、负剩余量、重复开放占用、重复终局或清单与谱系不一致，服务以明确恢复错误停止接受写请求。

## 验收标准

1. 预约成功后，母管、可分装整数体积、有序子管计划、样本、批次编号和批次修订形成不可变快照；任何错配预约不产生占用。
2. 两个并发预约同一母管时恰好一个成功，另一个得到稳定 MOTHER_ALREADY_RESERVED；数据库中最多存在一个开放分装会话。
3. 每个提交版本均满足整数微升守恒式，超额子管分配或侵占未执行计划量的损耗被原子拒绝，母管剩余量永不为负。
4. 相同 operation_id 与相同语义内容重试返回首次持久化结果；复用该编号提交不同命令、目标或字段时返回 OPERATION_CONFLICT。
5. 建立子管、扣减母管、写入体积账簿和待固化父子边在同一事务完成；任一事务检查点中断后四者均不可部分可见。
6. 存在未创建计划子管、非零剩余量或任一子管缺少匹配复冻核验时，固化被拒绝且不生成清单；条件齐备后只生成一份不可变清单。
7. 通过同步屏障并发提交固化和隔离时，仅先提交者形成终局；另一请求取得稳定冲突，数据库中最多一个终局且最多一份固化清单。
8. 在每个预设事务检查点中断并重启服务后，不存在无父子管、负剩余量或幽灵占用；已提交谱系、幂等结果和开放占用与重启前一致。

## 确定性测试场景

1. catalog_reservation_test.go / TestReservationFreezesSnapshot：固定 1200 微升母管和三个子管计划，验证预约快照不随目录新增修订改变。
2. catalog_reservation_test.go / TestReservationRejectsSampleOrBatchMismatch：分别提交错误样本和错误批次修订，验证无会话、无占用。
3. catalog_reservation_test.go / TestConcurrentReservationHasOneWinner：两个 goroutine 在屏障后预约同一母管，按释放顺序验证一个成功、一个稳定拒绝。
4. catalog_reservation_test.go / TestChildNumberConflictAtReservation：计划使用已有容器或其他会话已声明管号，验证整个预约回滚。
5. volume_lineage_test.go / TestIntegerVolumeConservation：以 500、400、200 微升子管和 100 微升损耗完成 1200 微升母管，逐版本重算守恒。
6. volume_lineage_test.go / TestOverAllocationLeavesStateUnchanged：提交与计划不符或超过剩余量的分装，验证没有子管、账簿分录或谱系边。
7. volume_lineage_test.go / TestLossCannotConsumeOutstandingPlan：尚欠 600 微升计划时登记过量损耗，验证稳定拒绝及余额不变。
8. volume_lineage_test.go / TestChildCreationWritesAllArtifacts：成功分装后同时断言子管、母管扣减、账簿分录、计划标记和父子边。
9. idempotency_test.go / TestIdenticalOperationReturnsOriginalSuccess：提交成功后推进其他状态，再用相同 operation_id 和内容重试仍返回原 revision 与结果。
10. idempotency_test.go / TestOperationContentConflict：复用操作号更换子管、体积或命令类型，验证 OPERATION_CONFLICT 且无业务写入。
11. idempotency_test.go / TestRejectedResultIsStable：缺少复冻核验的固化先被持久化拒绝；补齐后重试原操作号仍返回首次拒绝。
12. terminal_race_test.go / TestFinalizeRequiresEveryVerification：仅核验部分子管时返回稳定排序的缺失列表且没有清单。
13. terminal_race_test.go / TestFinalizeProducesSingleStableManifest：全部核验后使用两个不同操作号连续固化，验证仅一份按管号排序的清单。
14. terminal_race_test.go / TestQuarantineWinsBarrierRace：屏障控制隔离先提交，验证固化失败、无清单且母子容器均隔离。
15. terminal_race_test.go / TestFinalizeWinsBarrierRace：屏障控制固化先提交，验证隔离不能覆盖 finalized 终局。
16. recovery_test.go / TestRestartRestoresCommittedOpenSession：关闭并重开临时数据库，验证预约、剩余量、操作结果和待固化边完整恢复。
17. recovery_test.go / TestInterruptedChildTransactionRollsBackAtEveryCheckpoint：表驱动遍历四个子管事务检查点，每次重启后验证无孤立子管且体积不变。
18. recovery_test.go / TestInterruptedTerminalTransactionRollsBackAtEveryCheckpoint：遍历终局检查点，重启后验证无半份清单、无部分固化边且会话仍可完成。

## 组件追踪关系

1. 验收 1 由样本与容器目录、分装会话及 aliquot_sessions/planned_children 快照约束实现，对应预约快照与错配测试。
2. 验收 2 由会话状态聚合的母管键同步、事务条件更新和开放占用唯一约束共同实现，对应并发预约测试。
3. 验收 3 由体积预留账簿和 containers.remaining_uL 更新实现，对应守恒、超额及损耗覆盖测试。
4. 验收 4 由会话聚合的语义指纹和 operation_results 唯一记录实现，对应三个幂等测试。
5. 验收 5 由事务仓库的单事务子管写入路径及谱系组件实现，对应原子产物和检查点回滚测试。
6. 验收 6 由谱系图的核验集合、depleted 前置条件及唯一 manifest 约束实现，对应缺失核验和唯一清单测试。形验收 7 由会话聚合的终局命令、修订条件更新、session_outcomes 唯一行及同步屏障实现，对应两个终局竞争测试。验收 8 由 SQLite WAL、事务检查点、启动恢复校验和数据库派生视图实现，对应开放会话重启及两组中断恢复测试。

## 独特性

该项目不是样本转移保管流程，也不是通用库存系统；其核心对象是一支母管被不可逆地转换成多支子管时形成的 1:N 谱系图。实现难点集中在同一事务内联动容器诞生、整数体积守恒和待固化父子边，以及复冻证据齐备后的唯一固化与整批隔离竞争。它不处理跨地点交接、凭证授权或可回补库存，业务闭环和数据不变量均与既有项目 materially different。
