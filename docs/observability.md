# 可观测性与性能诊断链路设计说明

本模块面向长时间运行的并发任务，把**追踪、结构化日志、运行时指标、性能诊断快照**
按同一次任务执行（`ExecutionID`）稳定关联。全部能力仅依赖 Go 标准库，
不依赖任何外部服务或真实凭据。

## 1. 包结构

| 包         | 职责                                                                 |
|------------|----------------------------------------------------------------------|
| `trace`    | 执行/跨度模型、上下文传播（同步/排队/异步）、上下文问题识别           |
| `sampling` | 按执行标识可配置、可复现的采样策略与容量降级                          |
| `sink`     | 并发安全、有界、可去重的通用记录汇聚器（日志与指标复用）              |
| `logx`     | 按执行标识汇总的结构化日志                                            |
| `metrics`  | 按执行标识汇总的幂等指标（counter/gauge/timer）                       |
| `diag`     | 按需诊断会话：调用阶段、CPU、内存、阻塞等待、可选 pprof CPU profile    |
| `runner`   | 把上述组件串联成同步/排队/异步三种执行原语的编排层                    |

## 2. 上下文关联规则（`trace`）

### 2.1 三种传播路径

1. **同步调用**：追踪上下文放在 `context.Context` 中。
   - `tracer.StartExecution(id, name, ctx)` 建立执行根跨度。
   - `tracer.Start(ctx, name)` 建立子跨度，自动读取父跨度。
2. **任务排队**：`context.Context` 在队列边界不再适用，使用显式 `Carrier`。
   - 入队：`carrier, issue := tracer.Enqueue(ctx)`。
   - 出队：`span, issue := tracer.StartFromCarrier(carrier, "queue_wait")`。
   - 因果链形如 `enqueue -> queue_wait -> process`，排队等待是独立可见的跨度。
3. **异步派生**：`ctx, span, issue := tracer.ForkAsync(parent, name, attrs)`，
   生成 `SpanKindAsync` 子跨度，父取消默认仍通过 context 传播，但因果链清晰。

### 2.2 问题识别：绝不静默挂错链路

任何异常上下文都会以 `trace.Issue` 显式标记在跨度上（`Span.Issues()`），
排队/异步入口还会把首要问题作为返回值给出：

| Issue                 | 触发场景                                       | 处理语义                                       |
|-----------------------|------------------------------------------------|------------------------------------------------|
| `missing_context`     | context 中无追踪信息                           | 开独立根跨度并标记，不挂任何父节点             |
| `malformed_carrier`   | 载体字段缺失，或引用了未知执行/未知跨度        | 开独立根跨度，保留 `issue.ref_*` 属性          |
| `cross_execution`     | 执行已结束后继续沿用其载体（跨执行继承）       | 保留问题痕迹；重复 `StartExecution` 同 ID 也触发 |
| `finished_parent`     | 同步 `Start` 时父跨度已结束                    | 保留预期父子关系并标记（排队场景视为正常）     |
| `unknown_parent`      | 父跨度 ID 在 tracer 中不存在                   | 退化为同执行内无父跨度，不连接可疑节点         |

> 排队语义下入队跨度在出队时通常已结束，因此 `StartFromCarrier` 对
> `finished_parent` 不报错；但同步调用链中父跨度提前结束仍是明确的问题。

### 2.3 并发与生命周期

- 所有跨度方法都有互斥保护，`End` 幂等：重复结束不覆盖结束时间与错误。
- `EndExecution(id)` 标记执行结束；之后旧载体出队会被识别为 `cross_execution`。
- `ExecutionSpans(id)` 返回按开始顺序排列的只读快照，可直接验证因果链。

## 3. 采样与降级策略（`sampling` / `sink` / `logx` / `metrics`）

### 3.1 采样策略

- `NewAlways(sample)`：恒定保留/丢弃，reason 为 `always_on`/`always_off`。
- `NewRate(rate, opts...)`：对 `salt + ExecutionID` 做 FNV-1a 哈希归一化后与
  `rate` 比较，因此：
  - **同一执行标识的决策在任何时候、任何独立实例上都可重复**（同 salt）；
  - reason 为 `rate_kept` / `rate_dropped`；`Decision.Hash` 可用于复算；
  - `rate` 越界（<0 或 >1）fail-closed：不采样，reason=`rate_out_of_range`，
    `Degraded=true`，与正常丢弃可区分。
- `WithMaxKept(n)`：保留数量达到上限后，新执行得到
  `sampled=false, reason=cap_dropped, degraded=true` 的**可区分降级结果**。
- 决策按执行标识缓存：容量状态随后变化不会改变已询问过执行的决策（结果可重复）。

### 3.2 汇聚器语义（`sink.Result`）

| 结果            | 含义                             |
|-----------------|----------------------------------|
| `accepted`      | 正常接受                         |
| `duplicate`     | 重复上报被去重，仅计数一次       |
| `capacity`      | 容量上限导致降级丢弃，仅计数一次 |
| `rejected`      | 缺失关联标识等明确拒绝           |

- 并发写入下 **accepted + duplicate + dropped 的记账恒等式**成立，
  永不重复计数；已有记录**绝不被覆盖**。
- 日志去重键为 `执行|跨度|级别|消息`（动态属性不参与，避免重复逻辑日志漏判）。
- 指标通过非空 `ReportID` 幂等：重复上报命中去重，不改变任何聚合值。
- 每个执行的时间序列数有独立上限（默认 256）：超限的是**新序列**，
  已有序列继续上报不受影响；不同执行的上限互不干扰。
- counter 累加；gauge/timer 聚合 count/sum/min/max/last。
- 观测层的任何失败都不改变业务返回值（runner 测试有显式断言）。

### 3.3 背压错误（`runner`）

- 队列满：`Submit` 返回 `runner.ErrQueueFull`，由调用方决定重试或丢弃。
- 异步槽满：`GoAsync` 返回 `runner.ErrAsyncLimit`，且不会启动 goroutine。

## 4. 性能诊断快照（`diag`）

### 4.1 会话生命周期（稳定可验证的语义）

```go
m := diag.NewManager(tracer, diag.WithCPUProfile(100, 20*time.Millisecond))
s, err := m.Start(executionID)          // 重复启动 -> ErrSessionActive
snap, err := s.Snapshot(ctx)            // 按需采集
err = m.Stop(executionID)               // 结束；采集进行中会先等待采集完成
```

| 场景               | 行为                                                        |
|--------------------|-------------------------------------------------------------|
| 重复 `Start`       | 返回 `ErrSessionActive`                                     |
| 提前/重复 `Stop`   | 未知执行 `ErrUnknownExecution`；已停止会话快照 `ErrNotRunning` |
| 并发 `Snapshot`    | 同时只有一个采集，其余立即得到 `ErrConcurrentSnapshot`      |
| 采集阶段失败       | 错误包装链含 `ErrCollectionFailed` + 每个阶段的 `CollectError`；
                      其它阶段数据照常返回（部分成功、可解释）                    |
| `Stop` 时正在采集  | 等待当前快照结束后再停止，不会采集中途截断                  |

### 4.2 快照内容（`diag.Snapshot`）

- `Phases`：调用阶段（名称、跨度 ID、起止时间、耗时、失败标记、错误文本）。
  通过 `manager.WithPhase(ctx, name, fn)` 记录；绑定 tracer 时同时生成
  同因果链子跨度，阶段与跨度通过 `Phase.SpanID` 一一对应。
- `Blocks`：显式阻塞等待（操作、时长、当时 goroutine 数）。
- `CPUStart/CPUEnd/CPUUsed`：基于 `syscall.Getrusage(RUSAGE_SELF)` 的
  用户态+内核态累计 CPU 时间与自愿/非自愿上下文切换次数（Windows 下该阶段
  返回明确采集错误而不是零值）。
- `Memory`：`runtime.ReadMemStats` 的堆分配/Sys/对象数/GC 次数 + goroutine 数。
- `ProfileBytes`：可选的 gzip pprof CPU profile（`WithCPUProfile` 开启）。
- `Failures`：分阶段失败列表，阶段名常量：`phases/blocks/cpu/memory/cpu_profile`。

runner 只对**被采样执行**抓取快照，避免高负载下额外开销；会话始终正常结束。

## 5. 典型用法

```go
r := runner.New(runner.Config{
    Tracer:  trace.New(),
    Sampler: sampling.NewRate(0.1, sampling.WithMaxKept(1000)),
    Logger:  logx.New(logx.WithCapacity(10_000)),
    Metrics: metrics.New(metrics.WithMaxSeriesPerExecution(512)),
    Diag:    diag.NewManager(nil),
    QueueCapacity: 1024,
    MaxAsync: 64,
})

// 同步执行
res := r.Run(ctx, trace.ExecutionID("order-123"), func(ctx context.Context) error {
    return orderService.Handle(ctx) // 内部用 tracer.Start / logger.Log / metrics.Record
})

// 排队执行
carrier, err := r.Submit(ctx, trace.ExecutionID("job-7"), job)
// ... 跨进程/跨队列透传 carrier(JSON) ...
res = r.Process(carrier, job)

// 异步派生
_ = r.GoAsync(ctx, "fanout-worker", func(ctx context.Context) error { ... })
_ = r.Shutdown(ctx)
```

## 6. 验证方法

不依赖外部服务与真实凭据，全部通过标准 `go` 工具链验证：

```bash
go build ./...
go vet ./...
go test -race -count=1 ./...
```

覆盖点：

- **并发关联**：200 并发子跨度 / 100 并发指标上报 / 30 并发执行，
  在 `-race` 下校验因果链、计数恒等式与按执行隔离。
- **取消与异常传播**：父 context 取消时 `Result.Cancelled=true` 且
  业务错误原样保留；阶段错误同时写入跨度、诊断快照与日志。
- **采样边界**：rate=0/1、越界 rate、哈希可复现、salt 差异、MaxKept 降级。
- **重复上报**：同 `ReportID` 幂等；日志同点去重；跨度 `End` 幂等。
- **容量压力**：sink/log 容量丢弃、指标每执行序列上限、队列满与异步槽满。
- **诊断语义**：重复启动、提前结束、并发快照、注入阶段失败
  （`diag.WithFailStages(...)`）、Stop 等待快照、pprof 字节非空。
- **结果可重复**：固定 salt 下两个独立策略实例对 100 个执行给出完全一致决策。

关键测试以 `-v` 运行时会打印**执行标识、采样决定（reason/hash）、
降级原因与上下文问题**，例如：

```
execution=e-3 sampled=false reason=cap_dropped degraded=true hash=0.1803
execution=s-1 sampled=false reason=cap_dropped degraded=true cancelled=false
execution=exec-1 issue=malformed_carrier (correctly refused to attach to wrong chain)
execution=e snapshot partial-failure stage=memory (other stages preserved)
```
