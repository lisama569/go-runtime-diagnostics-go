// Package runner 把追踪、采样、日志、指标与诊断会话串成一条
// 按执行标识稳定关联的可观测链路,并提供同步、入队/出队与异步派生的执行原语。
//
// 观测行为绝不改变业务结果:容量压力表现为日志/指标的降级结果,
// 队列满与异步槽满通过可区分错误暴露给调用方决策。
package runner

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/lisama569/go-runtime-diagnostics-go/diag"
	"github.com/lisama569/go-runtime-diagnostics-go/logx"
	"github.com/lisama569/go-runtime-diagnostics-go/metrics"
	"github.com/lisama569/go-runtime-diagnostics-go/sampling"
	"github.com/lisama569/go-runtime-diagnostics-go/trace"
)

// 队列/异步槽满的可区分错误。
var (
	// ErrQueueFull 队列达到容量上限,Submit 被降级拒绝。
	ErrQueueFull = errors.New("runner: queue capacity reached")
	// ErrAsyncLimit 同一执行的并发异步派生达到上限。
	ErrAsyncLimit = errors.New("runner: async concurrency limit reached")
)

// Task 是被编排的业务函数。
type Task func(ctx context.Context) error

// Job 是排队执行的工作单元,携带显式透传的追踪载体。
type Job struct {
	Execution trace.ExecutionID
	Carrier   trace.Carrier
	Task      Task
	Submitted time.Time
}

// Result 汇总一次执行的可观测结果(不改变业务结果本身)。
type Result struct {
	Execution trace.ExecutionID
	Err       error
	Cancelled bool
	Sampled   sampling.Decision
	Snapshot  diag.Snapshot
	// Issues 收集本次入口处识别到的上下文问题。
	Issues []trace.Issue
}

// Runner 编排可观测组件。
type Runner struct {
	mu     sync.Mutex
	cfg    Config
	queue  []Job
	wg     sync.WaitGroup
	asyncN map[trace.ExecutionID]int
}

// Config 配置 Runner。
type Config struct {
	Tracer  *trace.Tracer
	Sampler sampling.Policy
	Logger  *logx.Logger
	Metrics *metrics.Registry
	Diag    *diag.Manager
	// QueueCapacity 限制待处理任务数量;<=0 表示不限制。
	QueueCapacity int
	// MaxAsync 限制同一执行并发异步派生数;<=0 表示不限制。
	MaxAsync int
}

// New 创建 Runner。
func New(cfg Config) *Runner {
	if cfg.Tracer == nil {
		cfg.Tracer = trace.New()
	}
	return &Runner{cfg: cfg, asyncN: map[trace.ExecutionID]int{}}
}

// Tracer 暴露底层 tracer,便于调用方查询因果链。
func (r *Runner) Tracer() *trace.Tracer { return r.cfg.Tracer }

// Metrics 暴露指标注册表(可能为 nil)。
func (r *Runner) Metrics() *metrics.Registry { return r.cfg.Metrics }

// Logger 暴露结构化日志器(可能为 nil)。
func (r *Runner) Logger() *logx.Logger { return r.cfg.Logger }

// Sampler 暴露采样策略(可能为 nil)。
func (r *Runner) Sampler() sampling.Policy { return r.cfg.Sampler }

func (r *Runner) recordSample(decision sampling.Decision) {
	if r.cfg.Metrics == nil {
		return
	}
	if decision.Sampled {
		_ = r.cfg.Metrics.Record(metrics.Sample{
			Execution: decision.Execution,
			Name:      "sampling.kept",
			Kind:      metrics.KindCounter,
			Value:     1,
		})
	}
	if decision.Degraded {
		_ = r.cfg.Metrics.Record(metrics.Sample{
			Execution: decision.Execution,
			Name:      "sampling.degraded",
			Kind:      metrics.KindCounter,
			Value:     1,
		})
	}
}

// Run 同步执行一次任务,建立完整的执行链路:
// 开启执行根跨度与诊断会话 -> 采样 -> 执行业务 -> 结束会话并按采样结果出快照。
func (r *Runner) Run(ctx context.Context, id trace.ExecutionID, task Task) Result {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx2, root := r.cfg.Tracer.StartExecution(id, "execution", ctx)
	return r.execute(ctx2, root, nil, task)
}

// execute 是同步执行与出队执行共用的收尾逻辑。
//
// root 为本次执行的主跨度;entryIssues 是在进入阶段(如出队恢复载体)
// 已识别、需要在结果中保留的上下文问题。
func (r *Runner) execute(ctx context.Context, root *trace.Span, entryIssues []trace.Issue, task Task) Result {
	execID := root.Execution()
	result := Result{Execution: execID, Issues: append([]trace.Issue(nil), entryIssues...)}

	var decision sampling.Decision
	if r.cfg.Sampler != nil {
		decision = r.cfg.Sampler.Decide(execID)
	} else {
		decision = sampling.Decision{Execution: execID, Sampled: true, Reason: sampling.ReasonAlwaysOn}
	}
	result.Sampled = decision
	r.recordSample(decision)
	if r.cfg.Logger != nil {
		r.cfg.Logger.LogExecution(execID, root.ID(), logx.LevelInfo,
			"execution started", map[string]string{
				"sampled":       boolStr(decision.Sampled),
				"sample.reason": decision.Reason,
				"degraded":      boolStr(decision.Degraded),
			})
	}

	var session *diag.Session
	if r.cfg.Diag != nil {
		s, err := r.cfg.Diag.Start(execID)
		if err != nil {
			if r.cfg.Logger != nil {
				r.cfg.Logger.LogExecution(execID, root.ID(), logx.LevelWarn,
					"diag session start failed", map[string]string{"error": err.Error()})
			}
		} else {
			session = s
			ctx = diag.WithSession(ctx, s)
		}
	}

	cancelCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	taskErr := make(chan error, 1)
	go func() { taskErr <- task(cancelCtx) }()

	var err error
	select {
	case err = <-taskErr:
	case <-ctx.Done():
		// 父 context 取消:传播取消并等待业务退出,保证取消可解释。
		cancel()
		err = <-taskErr
		result.Cancelled = true
		if err == nil {
			err = ctx.Err()
		}
	}
	result.Err = err

	root.End(err)
	if session != nil {
		session.SetResultError(err)
		// 仅对被采样执行抓取快照,避免高负载下额外开销;会话始终正常结束。
		if snap, snapErr := session.Snapshot(context.Background()); snapErr == nil {
			result.Snapshot = snap
		} else if r.cfg.Logger != nil {
			r.cfg.Logger.LogExecution(execID, root.ID(), logx.LevelWarn,
				"diag snapshot failed", map[string]string{"error": snapErr.Error()})
		}
		_ = r.cfg.Diag.Stop(execID)
	}
	r.cfg.Tracer.EndExecution(execID)

	if r.cfg.Logger != nil {
		lvl := logx.LevelInfo
		if err != nil {
			lvl = logx.LevelError
		}
		r.cfg.Logger.LogExecution(execID, root.ID(), lvl, "execution finished", map[string]string{
			"cancelled": boolStr(result.Cancelled),
			"error":     errStr(err),
		})
	}
	return result
}

// Submit 入队一个任务,返回用于出队关联的载体。
// ctx 缺失追踪信息时会创建新执行并在日志中标记 missing_context;
// 队列满时返回 ErrQueueFull 降级错误,由调用方决定重试或丢弃。
func (r *Runner) Submit(ctx context.Context, id trace.ExecutionID, task Task) (trace.Carrier, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	carrier, issue := r.cfg.Tracer.Enqueue(ctx)
	if issue == trace.IssueMissingContext {
		// 没有上下文时,以新执行根跨度作为入队点,避免无标识任务。
		newCtx, root := r.cfg.Tracer.StartExecution(id, "enqueue", ctx)
		root.MarkIssue(trace.IssueMissingContext)
		carrier = trace.Carrier{Execution: root.Execution(), Span: root.ID()}
		root.End(nil)
		ctx = newCtx
		if r.cfg.Logger != nil {
			r.cfg.Logger.LogExecution(root.Execution(), root.ID(), logx.LevelWarn,
				"submit without trace context",
				map[string]string{"issue": trace.IssueMissingContext.String()})
		}
	}

	r.mu.Lock()
	if r.cfg.QueueCapacity > 0 && len(r.queue) >= r.cfg.QueueCapacity {
		r.mu.Unlock()
		if r.cfg.Metrics != nil {
			_ = r.cfg.Metrics.Record(metrics.Sample{
				Execution: carrier.Execution, Name: "queue.rejected",
				Kind: metrics.KindCounter, Value: 1,
			})
		}
		return carrier, ErrQueueFull
	}
	r.queue = append(r.queue, Job{
		Execution: carrier.Execution,
		Carrier:   carrier,
		Task:      task,
		Submitted: time.Now(),
	})
	r.mu.Unlock()
	return carrier, nil
}

// Process 从载体恢复上下文并执行排队任务。
//
// 因果链:enqueue(已结束) -> queue_wait -> process。
// 载体缺失/损坏/跨执行时问题显式记录在 Result.Issues,且不会挂错链路。
func (r *Runner) Process(carrier trace.Carrier, task Task) Result {
	waitSpan, issue := r.cfg.Tracer.StartFromCarrier(carrier, "queue_wait",
		trace.WithKind(trace.SpanKindQueueWait))
	if waitSpan.Execution() == carrier.Execution {
		waitSpan.AddAttr("queue.submitted", time.Now().Format(time.RFC3339Nano))
	}
	waitSpan.End(nil)

	// process 跨度以 queue_wait 为父(同执行);若载体失效,waitSpan 已位于独立执行。
	ctx, processSpan := r.cfg.Tracer.Start(
		trace.WithContext(context.Background(), waitSpan.Context()),
		"process")

	r.removeJob(carrier)
	return r.execute(ctx, processSpan, issueList(issue), task)
}

func (r *Runner) removeJob(carrier trace.Carrier) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, j := range r.queue {
		if j.Carrier.Execution == carrier.Execution && j.Carrier.Span == carrier.Span {
			r.queue = append(r.queue[:i], r.queue[i+1:]...)
			return
		}
	}
}

func issueList(i trace.Issue) []trace.Issue {
	if i == trace.IssueNone {
		return nil
	}
	return []trace.Issue{i}
}

// GoAsync 从当前执行异步派生任务。
// 父取消默认传播,异步跨度与父跨度形成明确因果链;
// 超过 MaxAsync 时返回 ErrAsyncLimit,不启动 goroutine。
func (r *Runner) GoAsync(ctx context.Context, name string, task Task) error {
	if ctx == nil {
		ctx = context.Background()
	}
	forkCtx, span, issue := r.cfg.Tracer.ForkAsync(ctx, name, nil)
	if issue == trace.IssueMissingContext {
		return errors.New("runner: cannot fork async task without trace context")
	}

	execID := span.Execution()
	r.mu.Lock()
	if r.cfg.MaxAsync > 0 && r.asyncN[execID] >= r.cfg.MaxAsync {
		r.mu.Unlock()
		span.End(ErrAsyncLimit)
		return ErrAsyncLimit
	}
	r.asyncN[execID]++
	r.wg.Add(1)
	r.mu.Unlock()

	go func() {
		defer r.wg.Done()
		defer func() {
			r.mu.Lock()
			r.asyncN[execID]--
			r.mu.Unlock()
		}()
		if r.cfg.Logger != nil {
			r.cfg.Logger.Log(forkCtx, logx.LevelInfo, "async task started",
				map[string]string{"async.name": name})
		}
		err := task(forkCtx)
		span.End(err)
		if r.cfg.Logger != nil {
			r.cfg.Logger.LogExecution(execID, span.ID(), logx.LevelInfo,
				"async task finished", map[string]string{
					"async.name": name, "error": errStr(err),
				})
		}
	}()
	return nil
}

// QueueLen 返回当前排队任务数。
func (r *Runner) QueueLen() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.queue)
}

// Shutdown 等待已接受的异步任务结束;不丢弃任何已接受记录。
func (r *Runner) Shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

func errStr(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
