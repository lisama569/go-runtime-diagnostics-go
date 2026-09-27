# 可观测性采集与性能诊断链路

面向长时间运行的并发任务，本工程提供四件套并按**同一次任务执行（Execution ID）**稳定关联：

| 包 | 职责 |
|----|------|
| `trace` | 同步调用 / 排队任务 / 异步派生的因果跨度链 |
| `logx` | 结构化日志，按执行 ID 汇总，有界且上报丢弃 |
| `metric` | 计数器 / 仪表 / 事件，幂等去重，有界且可区分降级 |
| `sample` | 确定性、可解释的采样决策（速率 / 头部 / 组合 / 强制保留） |
| `diag` | 按需性能诊断快照：调用阶段、CPU、内存（堆）、阻塞等待、goroutine |
| `obs` | 门面包，为一次执行把上述子系统绑定到同一个 Execution ID |

全程无外部服务、无真实凭据：所有数据在进程内有界缓存，`runtime/pprof` 直接采集。

---

## 1. 上下文关联规则

### 1.1 一次执行 = 一棵因果树

- `trace.StartExecution(ctx, name)` 生成唯一 `ExecutionID`（16 字节）、`TraceID` 与根 span。
- 每个 span 都属于**恰好一个** execution，并携带 `parentID`。同步链、排队链、异步派生最终形成同一棵可回溯的树。
- Execution ID 是日志 `Entry.ExecutionID`、指标 `Record.ExecutionID`、快照 `Snapshot.ExecutionID` 的统一 join key。

### 1.2 三种传播方式

```go
// (a) 同步调用：上下文直接携带当前 span
ctx, s, err := tracer.StartSpan(ctx, "validate", trace.SpanSync)
defer s.End(nil)

// (b) 排队：投递侧生成被立即关闭的 queue.produce 标记 span 与 token
token, err := trace.QueueHandoff(ctx)
// 消费侧可以使用一个全新的、与投递侧无任何关系的 context（如 worker 根上下文）
cctx, cs, err := tracer.QueueConsume(context.Background(), token)

// (c) 异步派生：脱离取消但保留因果父节点
fctx, fs, err := tracer.ForkAsync(reqCtx, "post-process")
```

- `ForkAsync` 内部使用 `context.WithoutCancel`：派生任务**不会**因为发起请求被取消而连带取消，但 `ExecutionID / TraceID / parentID` 完整保留。
- 队列 token 是显式、可序列化的 `SpanContext`（hex 字符串可经 `ParseID`/`String` 跨边界传输），消费时经过完整校验。

### 1.3 失效 / 缺失 / 跨执行上下文：绝不静默挂错链

所有异常都用可 `errors.Is` 区分的哨兵错误表达：

| 错误 | 触发条件 |
|------|----------|
| `trace.ErrNilContext` | 传入 `nil` context |
| `trace.ErrNoExecution` | context 未携带任何 execution |
| `trace.ErrInvalidSpanContext` | token `Valid=false`、零值 ID、引用未知 span、载体损坏 |
| `trace.ErrForeignExecution` | token 引用本 tracer 不认识的 execution；或 context 来自另一个 tracer —— 拒绝跨执行继承 |
| `trace.ErrExecutionFinished` | execution 已 `Finish`，不能再扩展 |
| `trace.ErrSpanFinished` | 试图在已结束的普通 span 下开子 span |
| `trace.ErrTraceCapacity` | 单执行 span 数达到 `MaxSpans`（降级，业务不受影响） |

唯一例外：`queue.consume` 允许挂在已关闭的 `queue.produce` 标记 span 之下（这正是入队→出队的因果语义）。

`logx` / `metric` 在 context 无效时同样**拒绝并计数**，绝不把记录悄悄记到别的 execution 名下：
`DropNoExecution` / `OutcomeNoExecution`、`DropExecutionFinished` / `OutcomeExecutionFinished`。

### 1.4 幂等的结束语义

- `Span.End(err)` 可重复调用：第二次起返回 `ErrSpanFinished`，**不会**覆盖已记录的结束时间与错误原因。
- `Execution.Finish(err)` 幂等：重复返回 `ErrExecutionFinished`，冻结整棵 span 树。
- `Bundle.Finish` 幂等：重复返回 `obs.ErrAlreadyFinished`。

---

## 2. 采样与降级策略

### 2.1 采样决策是确定性的纯函数

```go
rs, _ := sample.NewRate(10)                  // 约 1/10 保留
v := rs.Decide(executionID, seq)             // 同一 (id, 配置) 永远同一结论
```

- 速率采样基于 FNV-1a(executionID) 取桶，**同一 execution 的所有 span 结论一致**；边界用严格 `<`，`rate=1` 全保留。
- `HeadSampler` 保留前 N 个不同 execution，结论对同一 id 稳定，并发安全。
- `Composite` 按规则顺序评估，第一条“丢弃”规则生效并附带原因；`forcedKeep` 谓词为真时以 `ReasonForcedKeep` 覆盖一切规则。
- 每个 `Verdict` 都带 `Decision / Reason / SamplingID / Rate`，失败/丢弃原因可区分、可打印。

### 2.2 容量压力下的降级（不重复计数、不覆盖有效记录）

**日志 `logx`**

- `Config.Capacity`（全局条数）与 `Config.PerExecCap`（单执行条数）任一到达上限 → 新写入返回
  `Result{Accepted:false, Dropped:true, DropReason:DropBufferFull}`，**已有条目永不被覆盖**。
- 每次丢弃都计入 `Report.Reasons[DropBufferFull]`，可与 `DropNoExecution / DropExecutionFinished / DropLoggerClosed` 区分。

**指标 `metric`**

- 计数器/事件携带非空 `dedupKey` 时幂等：相同 `(name, dedupKey)` 只计一次，重复上报返回
  `OutcomeDuplicate`（重复时携带的新值被忽略，防止重试把计数冲大）。
- 空 dedupKey 的计数器按语义累加；gauge 按 last-writer-wins 语义更新。
- 容量耗尽 → `OutcomeDroppedCapacity`，旧值不动；并发下相同幂等键的 N 次竞争上报结果恒为
  “恰好 1 次 recorded + N-1 次 duplicate”（有测试保证）。

**降级不改变业务结果**：以上全部通过返回值/计数表达，不 panic、不返回会中断业务流程的错误；
调用方可以忽略 `Result` 继续执行业务。

---

## 3. 性能诊断快照（diag）

### 3.1 生命周期（单次状态机，语义稳定可验证）

```
Idle --Start--> Running --End--> Finished
```

- 未 `Start` 时 `Snapshot/End` → `ErrNotRunning`。
- `Start` 后再 `Start` → `ErrAlreadyRunning`。
- `End` 精确一次：重复 `End` / 结束后 `Snapshot` → `ErrAlreadyEnded`。
- 并发 `Snapshot` 调用**串行化**（不报错、不丢采集），每次快照都保证各采集器视图一致。
- 单个采集器失败只记录进 `Snapshot.Failures` 与对应 `Profile.Err`，**不会**使整份快照失败。
- CPU 采集器支持取消：窗口内 `ctx.Done()` 时返回已采集数据 + `context.Canceled` 原因。

### 3.2 采集内容

| Collector | 内容 |
|-----------|------|
| `MemStatsCollector` | `HeapAlloc`、`HeapObjects`（廉价，无 pprof IO） |
| `GoroutineCollector` | 当前 goroutine 数 |
| `HeapCollector` | pprof heap profile 字节 |
| `BlockCollector` | pprof block profile；未通过 `runtime.SetBlockProfileRate` 开启时返回可解释的失败而非 panic |
| `NewCPUCollector(window)` | 有界 CPU profile 窗口；运行时全局只允许一个 CPU profile，因此按需显式加入 |

`MarkPhase(name, start, blocking, err)` 记录调用阶段耗时、是否阻塞等待、错误原因，
足以定位慢点与异常链；`Snapshot.Phases` 在按需快照和 `End` 的最终快照中都保留。

### 3.3 典型用法

```go
p, _ := diag.New(diag.Config{
    ExecutionID: execID,
    Collectors:  append(diag.DefaultCollectors(), diag.NewCPUCollector(200*time.Millisecond)),
})
p.Start(ctx)
start := time.Now()
err := doWork()
p.MarkPhase("doWork", start, true, err) // blocking=true 标记阻塞等待
snap, _ := p.Snapshot(ctx)              // 高负载/疑似异常时按需触发
final, _ := p.End(ctx)                  // 结束时拿最终快照
```

---

## 4. 门面包 obs

`obs.New` 一次创建 execution 并绑定 logger / meter / sampler / profiler；`Bundle.Finish`
按固定顺序收尾（诊断 → trace → 关闭 sink），返回 `Summary`，其中包含：

- `ExecutionID`、采样 `Verdict`
- 日志与指标的按执行 `Report`（含 accepted / dropped / duplicates 及分原因计数）
- 最终诊断 `Snapshot` 与业务 `Cause` 错误

---

## 5. 验证方法

不依赖任何外部服务或真实凭据，直接：

```bash
go build ./...
go vet ./...
go test -race ./...        # 并发关联、竞争检测
go test ./... -count=3     # 可重复性：同输入结论稳定
go test ./... -v           # 查看每个用例打印的执行标识、采样决定、降级原因
```

### 覆盖清单

- **并发关联**：50 路 fork+子 span 父图完整（`trace`）；40 worker 高负载全部日志/指标/快照归同一 execution（`obs`）。
- **取消传播**：`ForkAsync` 在 cancel 后仍可用且同 execution；CPU 采集器响应 `context.Canceled`。
- **异常传播**：span 与 execution 的错误原因被记录且重复 End 不覆盖。
- **采样边界**：`rate=1` 全保留、rate=1/4 比例带、同一 id 结论稳定、头部采样并发不超额、强制保留覆盖。
- **重复上报**：相同幂等键并发 N 次 → 值只 +1，duplicate=N-1；事件只计一次。
- **容量压力**：日志/指标容量耗尽 → 可区分的丢弃结果，首条有效记录保留，业务正常 Finish。
- **诊断语义**：重复 Start/End、提前 End、并发 Snapshot、采集器失败、取消采集均有稳定断言。

测试日志统一打印 `execution=<id> decision=... reason=...`，失败原因用哨兵错误区分，
ID 生成器在测试中可注入确定性实现，保证同一输入结果可重复。
