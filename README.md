# go-runtime-diagnostics

面向长时间运行并发任务的进程内可观测性采集与性能诊断链路：追踪、结构化日志、
运行时指标、采样与按需诊断快照全部按同一次执行（Execution ID）关联。

- 无外部服务、无真实凭据；`runtime/pprof` 进程内采集。
- 同步 / 排队 / 异步三种上下文传播，失效与跨执行上下文被明确拒绝。
- 采样确定可复现；容量压力下降级结果可区分，重复上报幂等，不影响业务结果。
- 诊断快照生命周期语义稳定，支持并发触发与采集失败隔离。

详见 [docs/observability.md](docs/observability.md)。快速验证：

```bash
go test -race ./...
```
