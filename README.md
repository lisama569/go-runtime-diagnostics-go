# go-runtime-diagnostics

面向长时间运行并发任务的可观测性采集与性能诊断链路：追踪、结构化日志、
运行时指标、按需诊断快照按同一次执行（`ExecutionID`）稳定关联，
在取消、异常与高负载下保持可解释。仅依赖 Go 标准库，无外部服务/凭据。

## 快速开始

```go
r := runner.New(runner.Config{
    Tracer:        trace.New(),
    Sampler:       sampling.NewRate(0.1, sampling.WithMaxKept(1000)),
    Logger:        logx.New(),
    Metrics:       metrics.New(),
    Diag:          diag.NewManager(nil),
    QueueCapacity: 1024,
    MaxAsync:      64,
})

res := r.Run(ctx, trace.ExecutionID("order-123"), handler) // 同步
carrier, _ := r.Submit(ctx, id, job)                       // 入队
r.Process(carrier, job)                                    // 出队
_ = r.GoAsync(ctx, "worker", job)                          // 异步派生
_ = r.Shutdown(ctx)
```

## 能力一览

- **追踪传播**：同步 (`context.Context`)、排队 (`trace.Carrier`)、
  异步 (`ForkAsync`) 三条路径形成可靠因果链；缺失、损坏、跨执行继承的
  上下文以 `trace.Issue` 显式标记，绝不静默挂错链路。
- **采样/日志/指标**：按执行标识汇总；策略可配置、决策可复现；
  并发写入不重复计数、不覆盖记录；容量压力产生
  `capacity` / `cap_dropped` 等可区分降级结果，且不改变业务结果。
- **诊断快照**：覆盖调用阶段、CPU（rusage，可选 pprof）、内存与阻塞等待；
  重复启动、提前结束、并发触发、采集失败均有稳定可验证语义。

详细设计、规则与验证方法见 [docs/observability.md](docs/observability.md)。

## 测试

```bash
go test -race -count=1 ./...
```
