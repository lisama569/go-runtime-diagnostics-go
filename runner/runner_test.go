package runner

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/go-runtime-diagnostics-go/diag"
	"github.com/lisama569/go-runtime-diagnostics-go/logx"
	"github.com/lisama569/go-runtime-diagnostics-go/metrics"
	"github.com/lisama569/go-runtime-diagnostics-go/sampling"
	"github.com/lisama569/go-runtime-diagnostics-go/trace"
)

func newTestRunner(t *testing.T, sampler sampling.Policy, opts ...func(*Config)) *Runner {
	t.Helper()
	cfg := Config{
		Tracer:  trace.New(),
		Sampler: sampler,
		Logger:  logx.New(logx.WithCapacity(1000)),
		Metrics: metrics.New(metrics.WithMaxSeriesPerExecution(100)),
		Diag:    diag.NewManager(nil),
	}
	for _, o := range opts {
		o(&cfg)
	}
	return New(cfg)
}

func TestRunHappyPathCorrelation(t *testing.T) {
	r := newTestRunner(t, sampling.NewAlways(true))
	res := r.Run(context.Background(), "exec-1", func(ctx context.Context) error {
		ctx, s := r.Tracer().Start(ctx, "work")
		defer s.End(nil)
		r.cfg.Logger.Log(ctx, logx.LevelInfo, "doing work", nil)
		r.cfg.Metrics.Record(metrics.Sample{
			Execution: trace.ExecutionID("exec-1"), Name: "work.done",
			Kind: metrics.KindCounter, Value: 1,
		})
		return nil
	})
	if res.Err != nil || res.Cancelled {
		t.Fatalf("result: %+v", res)
	}
	if !res.Sampled.Sampled {
		t.Fatal("should be sampled")
	}
	if res.Snapshot.Execution != "exec-1" {
		t.Fatalf("snapshot not correlated: %+v", res.Snapshot.Execution)
	}
	spans := r.Tracer().ExecutionSpans("exec-1")
	if len(spans) != 2 {
		t.Fatalf("want execution+work spans, got %d: %+v", len(spans), spans)
	}
	if spans[1].Parent != spans[0].ID {
		t.Fatal("work span not linked to execution root")
	}
	entries := r.cfg.Logger.Entries("exec-1")
	if len(entries) < 2 {
		t.Fatalf("logs not correlated: %d", len(entries))
	}
	views := r.cfg.Metrics.Execution("exec-1")
	if len(views) == 0 {
		t.Fatal("metrics not correlated")
	}
}

func TestRunCancellation(t *testing.T) {
	r := newTestRunner(t, sampling.NewAlways(true))
	ctx, cancel := context.WithCancel(context.Background())
	entered := make(chan struct{})
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()
	res := r.Run(ctx, "exec-cancel", func(ctx context.Context) error {
		close(entered)
		<-ctx.Done()
		return ctx.Err()
	})
	if !res.Cancelled {
		t.Fatal("cancellation not flagged")
	}
	if !errors.Is(res.Err, context.Canceled) {
		t.Fatalf("err=%v, want context.Canceled", res.Err)
	}
	entries := r.cfg.Logger.Entries("exec-cancel")
	last := entries[len(entries)-1]
	if last.Attrs["cancelled"] != "true" {
		t.Fatalf("finish log missing cancelled flag: %+v", last.Attrs)
	}
}

func TestRunErrorKeptAsBusinessResult(t *testing.T) {
	r := newTestRunner(t, sampling.NewAlways(true))
	boom := errors.New("boom")
	res := r.Run(context.Background(), "exec-err", func(ctx context.Context) error {
		return boom
	})
	if !errors.Is(res.Err, boom) {
		t.Fatalf("business error altered: %v", res.Err)
	}
	spans := r.Tracer().ExecutionSpans("exec-err")
	if spans[0].Err != "boom" {
		t.Fatalf("span error not recorded: %q", spans[0].Err)
	}
}

func TestQueueSubmitProcessChain(t *testing.T) {
	r := newTestRunner(t, sampling.NewAlways(true))
	carrier, err := r.Submit(context.Background(), "job-9", func(ctx context.Context) error {
		return nil
	})
	// Submit 在无上下文时会自建执行,这里 carrier 必须有效。
	if err != nil || !carrier.Valid() {
		t.Fatalf("submit: %v %+v", err, carrier)
	}
	if r.QueueLen() != 1 {
		t.Fatalf("queue len=%d", r.QueueLen())
	}

	// 用自建执行的 carrier 出队:链路 enqueue -> queue_wait -> process。
	res := r.Process(carrier, func(ctx context.Context) error {
		sc, ok := trace.FromContext(ctx)
		if !ok || sc.Execution != carrier.Execution {
			return fmt.Errorf("process ctx wrong: %+v", sc)
		}
		return nil
	})
	if res.Err != nil {
		t.Fatalf("process: %v", res.Err)
	}
	if r.QueueLen() != 0 {
		t.Fatal("job not removed from queue")
	}
	spans := r.Tracer().ExecutionSpans(carrier.Execution)
	names := map[string]bool{}
	var process trace.SpanSnapshot
	for _, s := range spans {
		names[s.Name] = true
		if s.Name == "process" {
			process = s
		}
	}
	if !names["enqueue"] || !names["queue_wait"] || !names["process"] {
		t.Fatalf("queue chain incomplete: %+v", names)
	}
	if process.Parent == "" {
		t.Fatal("process span must have queue_wait parent")
	}
}

func TestMalformedCarrierRejectedExplicitly(t *testing.T) {
	r := newTestRunner(t, sampling.NewAlways(true))
	res := r.Process(trace.Carrier{Execution: "ghost", Span: "x"}, func(ctx context.Context) error {
		return nil
	})
	if len(res.Issues) == 0 || res.Issues[0] != trace.IssueMalformedCarrier {
		t.Fatalf("malformed carrier must be surfaced: %+v", res.Issues)
	}
	t.Logf("execution=%s issue=%s (correctly refused to attach to wrong chain)",
		res.Execution, res.Issues[0])
}

func TestQueueFullDegradation(t *testing.T) {
	r := newTestRunner(t, sampling.NewAlways(true), func(c *Config) { c.QueueCapacity = 1 })
	tr := trace.New()
	ctx, root := tr.StartExecution("q", "root", context.Background())
	defer root.End(nil)

	if _, err := r.Submit(ctx, "q", noopTask); err != nil {
		t.Fatal(err)
	}
	_, err := r.Submit(ctx, "q", noopTask)
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("want ErrQueueFull, got %v", err)
	}
}

func TestAsyncForkAndLimit(t *testing.T) {
	r := newTestRunner(t, sampling.NewAlways(true), func(c *Config) { c.MaxAsync = 2 })
	done := make(chan struct{})
	res := r.Run(context.Background(), "async-exec", func(ctx context.Context) error {
		block := make(chan struct{})
		// 占满 2 个槽。
		if err := r.GoAsync(ctx, "a1", func(ctx context.Context) error {
			<-block
			close(done)
			return nil
		}); err != nil {
			return err
		}
		if err := r.GoAsync(ctx, "a2", func(ctx context.Context) error {
			<-block
			return nil
		}); err != nil {
			return err
		}
		if err := r.GoAsync(ctx, "a3", func(ctx context.Context) error { return nil }); !errors.Is(err, ErrAsyncLimit) {
			return fmt.Errorf("want async limit, got %v", err)
		}
		// 无上下文派生必须被拒绝。
		if err := r.GoAsync(context.Background(), "orphan", noopTask); err == nil {
			return errors.New("async without context must fail")
		}
		close(block)
		return nil
	})
	if res.Err != nil {
		t.Fatalf("run: %v", res.Err)
	}
	ctx2, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := r.Shutdown(ctx2); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	default:
		t.Fatal("async task did not run")
	}
}

func TestSamplingDecisionsStableAndLogged(t *testing.T) {
	// 低容量 + 高比率制造降级,确认决策进入日志且同执行稳定。
	r := newTestRunner(t, sampling.NewRate(1, sampling.WithMaxKept(1)))
	for i := 0; i < 3; i++ {
		id := trace.ExecutionID(fmt.Sprintf("s-%d", i))
		res := r.Run(context.Background(), id, noopTask)
		t.Logf("execution=%s sampled=%t reason=%s degraded=%t cancelled=%t",
			id, res.Sampled.Sampled, res.Sampled.Reason, res.Sampled.Degraded, res.Cancelled)
		if i == 0 && (!res.Sampled.Sampled || res.Sampled.Reason != sampling.ReasonRateKept) {
			t.Fatalf("first must be kept: %+v", res.Sampled)
		}
		if i > 0 && (res.Sampled.Sampled || res.Sampled.Reason != sampling.ReasonCapDrop || !res.Sampled.Degraded) {
			t.Fatalf("later must degrade: %+v", res.Sampled)
		}
		entries := r.cfg.Logger.Entries(id)
		for _, e := range entries {
			t.Logf("  log execution=%s span=%s level=%s msg=%q sampled=%s reason=%s degraded=%s",
				e.Execution, e.Span, e.Level, e.Message,
				e.Attrs["sampled"], e.Attrs["sample.reason"], e.Attrs["degraded"])
		}
		if entries[0].Attrs["sample.reason"] != res.Sampled.Reason {
			t.Fatalf("sampling reason not logged: %+v", entries[0].Attrs)
		}
	}
	// 未被采样的执行不应产生快照。
	r2 := r.Run(context.Background(), "unsampled-x", noopTask)
	_ = r2
}

func TestLogCapacityPressureDoesNotChangeResult(t *testing.T) {
	cfg := Config{
		Tracer:  trace.New(),
		Sampler: sampling.NewAlways(false),
		Logger:  logx.New(logx.WithCapacity(1)),
		Metrics: metrics.New(),
	}
	r := New(cfg)
	bizErr := errors.New("biz")
	res := r.Run(context.Background(), "cap", func(ctx context.Context) error { return bizErr })
	if !errors.Is(res.Err, bizErr) {
		t.Fatal("observability pressure must not change business result")
	}
	if res.Snapshot.Execution != "" {
		t.Fatal("unsampled execution must not take snapshot")
	}
}

func TestDeterministicRepeatedInputs(t *testing.T) {
	// 采样决策在两个独立 runner 上对相同输入必须一致。
	p1 := sampling.NewRate(0.5, sampling.WithSalt("fixed"))
	p2 := sampling.NewRate(0.5, sampling.WithSalt("fixed"))
	for i := 0; i < 100; i++ {
		id := trace.ExecutionID(fmt.Sprintf("det-%d", i))
		if p1.Decide(id).Sampled != p2.Decide(id).Sampled {
			t.Fatalf("nondeterministic at %s", id)
		}
	}
}

func TestConcurrentRunsIsolated(t *testing.T) {
	r := newTestRunner(t, sampling.NewAlways(true))
	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := trace.ExecutionID(fmt.Sprintf("par-%d", i))
			res := r.Run(context.Background(), id, func(ctx context.Context) error {
				r.cfg.Metrics.Record(metrics.Sample{
					Execution: id, Name: "n", Kind: metrics.KindCounter, Value: 1,
					ReportID: fmt.Sprintf("rep-%d", i),
				})
				return nil
			})
			if res.Err != nil || res.Execution != id {
				t.Errorf("run %d wrong: %+v", i, res)
			}
		}(i)
	}
	wg.Wait()
	for i := 0; i < 30; i++ {
		id := trace.ExecutionID(fmt.Sprintf("par-%d", i))
		if len(r.cfg.Logger.Entries(id)) < 2 {
			t.Fatalf("logs for %s leaked/missing", id)
		}
		v := r.cfg.Metrics.Execution(id)
		// 每个执行应有业务指标 n,以及框架指标 sampling.kept。
		var n *metrics.SeriesView
		for i := range v {
			if v[i].Name == "n" {
				n = &v[i]
			}
		}
		if n == nil || n.Count != 1 {
			t.Fatalf("metrics for %s mixed/missing: %+v", id, v)
		}
	}
}

var noopTask = func(ctx context.Context) error { return nil }
