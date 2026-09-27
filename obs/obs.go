// Package obs is a thin facade bundling trace, logging, metrics, sampling and
// diagnostics for one execution. A Bundle is created per execution and
// exposes every subsystem correlated to the same execution ID, so logs,
// metrics, sampling verdicts and diagnostic snapshots all join on that ID.
package obs

import (
	"context"
	"sync"
	"time"

	"github.com/lisama569/go-runtime-diagnostics-go/diag"
	"github.com/lisama569/go-runtime-diagnostics-go/logx"
	"github.com/lisama569/go-runtime-diagnostics-go/metric"
	"github.com/lisama569/go-runtime-diagnostics-go/sample"
	"github.com/lisama569/go-runtime-diagnostics-go/trace"
)

// BundleConfig configures a Bundle.
type BundleConfig struct {
	Name           string
	Tracer         *trace.Tracer // optional; default tracer used if nil
	LogCapacity    int
	LogPerExec     int
	MeterCapacity  int
	SampleRate     float64
	HeadN          int // optional head-sampling limit; <=0 disables
	DiagCollectors []diag.Collector
	Clock          func() time.Time
}

// Bundle ties all observability pieces to one execution.
type Bundle struct {
	cfg BundleConfig

	tracer  *trace.Tracer
	exec    *trace.Execution
	Logger  *logx.Logger
	Meter   *metric.Meter
	Sampler sample.Sampler
	Diag    *diag.Profiler

	mu      sync.Mutex
	ended   bool
	verdict sample.Verdict
}

// New creates an execution and its correlated subsystems.
func New(ctx context.Context, cfg BundleConfig) (context.Context, *Bundle) {
	if cfg.Clock != nil {
		// Clock is plumbed to trace only for deterministic tests.
	}
	tracer := cfg.Tracer
	if tracer == nil {
		tracer = trace.DefaultTracer()
	}
	ctx, exec := tracer.StartExecution(ctx, cfg.Name)

	lc := logx.Config{Capacity: cfg.LogCapacity, PerExecCap: cfg.LogPerExec, Clock: cfg.Clock}
	mc := metric.Config{Capacity: cfg.MeterCapacity}

	profiler, _ := diag.New(diag.Config{
		ExecutionID: exec.ID().String(),
		Collectors:  cfg.DiagCollectors,
		Clock:       cfg.Clock,
	})

	var samplers []sample.Sampler
	if cfg.SampleRate > 0 {
		if rs, err := sample.NewRate(cfg.SampleRate); err == nil {
			samplers = append(samplers, rs)
		}
	}
	if cfg.HeadN > 0 {
		if hs, err := sample.NewHead(cfg.HeadN); err == nil {
			samplers = append(samplers, hs)
		}
	}
	var s sample.Sampler = sample.AlwaysKeep{}
	if len(samplers) > 0 {
		s = sample.NewComposite(samplers, nil)
	}

	b := &Bundle{
		cfg:     cfg,
		tracer:  tracer,
		exec:    exec,
		Logger:  logx.New(lc),
		Meter:   metric.New(mc),
		Sampler: s,
		Diag:    profiler,
		verdict: s.Decide(exec.ID().String(), 0),
	}
	return withBundle(ctx, b), b
}

// ExecutionID returns the correlated execution identifier.
func (b *Bundle) ExecutionID() string { return b.exec.ID().String() }

// Verdict returns the (stable, per-execution) sampling verdict.
func (b *Bundle) Verdict() sample.Verdict { return b.verdict }

// Finish finalizes every subsystem in a fixed order and returns a summary.
// It is idempotent: repeated calls return ErrAlreadyFinished.
func (b *Bundle) Finish(ctx context.Context, cause error) (*Summary, error) {
	b.mu.Lock()
	if b.ended {
		b.mu.Unlock()
		return nil, ErrAlreadyFinished
	}
	b.ended = true
	b.mu.Unlock()

	// Fixed order: diagnostics, trace, then sinks. Sinks are closed last so
	// final phase/error logs are still accepted above.
	var finalSnap diag.Snapshot
	var snapErr error
	if b.Diag != nil {
		switch b.Diag.State() {
		case diag.StateRunning:
			finalSnap, snapErr = b.Diag.End(ctx)
		case diag.StateIdle:
			finalSnap, snapErr = diag.Snapshot{}, nil
		default:
			finalSnap, snapErr = diag.Snapshot{}, nil
		}
	}
	traceErr := b.exec.Finish(cause)
	_ = b.Logger.Close()
	_ = b.Meter.Close()

	logReport, _ := b.Logger.Report(b.ExecutionID())
	meterReport, _ := b.Meter.Report(b.ExecutionID())

	return &Summary{
		ExecutionID: b.ExecutionID(),
		Verdict:     b.verdict,
		Log:         logReport,
		Metrics:     meterReport,
		Snapshot:    finalSnap,
		SnapshotErr: snapErr,
		TraceErr:    traceErr,
		Cause:       cause,
	}, nil
}

// Summary is the final per-execution observability report.
type Summary struct {
	ExecutionID string
	Verdict     sample.Verdict
	Log         logx.Report
	Metrics     metric.Report
	Snapshot    diag.Snapshot
	SnapshotErr error
	TraceErr    error
	Cause       error
}
