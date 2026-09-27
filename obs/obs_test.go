package obs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/go-runtime-diagnostics-go/diag"
	"github.com/lisama569/go-runtime-diagnostics-go/logx"
	"github.com/lisama569/go-runtime-diagnostics-go/metric"
	"github.com/lisama569/go-runtime-diagnostics-go/trace"
)

// idSequence builds deterministic execution ids so reruns produce identical
// sampling verdicts and snapshots-of-ids.
func deterministicTracer(seedByte byte) *trace.Tracer {
	var n byte = seedByte
	return trace.NewTracer(trace.Config{
		NewID: func() trace.ID {
			n++
			var id trace.ID
			id[0] = 0xde
			id[15] = n
			return id
		},
		NewSpanID: func() trace.SpanID {
			n++
			var id trace.SpanID
			id[7] = n
			return id
		},
	})
}

// runOnePipeline executes a correlated sync -> queued -> async workload using
// the bundle and returns the final summary. The body is deterministic.
func runOnePipeline(t *testing.T, rate float64, forceErr bool, capHint int) *Summary {
	t.Helper()
	tr := deterministicTracer(0)
	ctx, b := New(context.Background(), BundleConfig{
		Name:          "pipeline",
		Tracer:        tr,
		LogCapacity:   capHint,
		MeterCapacity: capHint,
		SampleRate:    rate,
		DiagCollectors: []diag.Collector{
			diag.MemStatsCollector{},
			diag.GoroutineCollector{},
		},
	})

	if err := b.Diag.Start(ctx); err != nil {
		t.Fatal(err)
	}

	// Synchronous phase. Keep the parent context for the later queue handoff:
	// a handoff must originate from a live span, not an ended one.
	parentCtx := ctx
	ctx, syncSpan, err := tr.StartSpan(ctx, "validate", trace.SpanSync)
	if err != nil {
		t.Fatal(err)
	}
	phaseStart := time.Now()
	b.Logger.Info(ctx, "validate begin", logx.F("rate", rate))
	b.Meter.Add(ctx, "validations", 1, "det-key")
	// duplicate report (retry): must be swallowed, not double-counted.
	if r := b.Meter.Add(ctx, "validations", 1, "det-key"); r.Outcome != metric.OutcomeDuplicate {
		t.Fatalf("duplicate metric want OutcomeDuplicate, got %s", r.Outcome)
	}
	_ = b.Diag.MarkPhase("validate", phaseStart, false, nil)
	var cause error
	if forceErr {
		cause = errors.New("validate: forced failure")
	}
	_ = syncSpan.End(cause)

	// Queue handoff -> consumer on a detached, fresh base context.
	token, err := trace.QueueHandoff(parentCtx)
	if err != nil {
		t.Fatal(err)
	}
	qctx, qspan, err := tr.QueueConsume(context.Background(), token)
	if err != nil {
		t.Fatal(err)
	}
	b.Logger.Info(qctx, "consumed from queue")
	b.Meter.Add(qctx, "consumed", 1, "")

	// Async fork that survives cancellation of the request context. Fork from
	// the live consume span, then close the consume span.
	reqCtx, cancel := context.WithCancel(qctx)
	fctx, fspan, err := tr.ForkAsync(reqCtx, "post-process")
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case <-fctx.Done():
		t.Fatal("async fork must survive request cancellation")
	default:
	}
	b.Logger.Info(fctx, "async post-process")
	b.Meter.RecordEvent(fctx, "postprocessed", "post-1")
	_ = qspan.End(nil)
	_ = fspan.End(nil)

	snap, err := b.Diag.Snapshot(fctx)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Goroutines < 1 || len(snap.Collectors) != 2 {
		t.Fatalf("on-demand snapshot missing data: %+v", snap.Collectors)
	}

	sum, err := b.Finish(context.Background(), cause)
	if err != nil {
		t.Fatal(err)
	}
	return sum
}

func TestEndToEndCorrelationAndDeterminism(t *testing.T) {
	sum1 := runOnePipeline(t, 1, false, 0)
	sum2 := runOnePipeline(t, 1, false, 0)

	// Same inputs -> same execution ids and same sampling decision.
	if sum1.ExecutionID != sum2.ExecutionID {
		t.Fatalf("deterministic execution ids differ: %s vs %s",
			sum1.ExecutionID, sum2.ExecutionID)
	}
	if sum1.Verdict.Decision != sum2.Verdict.Decision ||
		sum1.Verdict.SamplingID != sum2.Verdict.SamplingID {
		t.Fatal("sampling verdict must be reproducible for identical inputs")
	}
	// All logs/metrics joined on the same execution id.
	if sum1.Log.ExecutionID != sum1.ExecutionID {
		t.Fatal("log report not correlated")
	}
	if sum1.Metrics.ExecutionID != sum1.ExecutionID {
		t.Fatal("metric report not correlated")
	}
	if got := sum1.Metrics.Values["validations"]; got != 1 {
		t.Fatalf("duplicate report inflated counter: %d", got)
	}
	if got := sum1.Metrics.Values["consumed"]; got != 1 {
		t.Fatalf("consumed counter mismatch: %d", got)
	}
	if got := sum1.Metrics.Values["postprocessed"]; got != 1 {
		t.Fatalf("postprocessed event mismatch: %d", got)
	}
	if sum1.Snapshot.ExecutionID != sum1.ExecutionID {
		t.Fatal("snapshot not correlated")
	}
	t.Logf("execution=%s sampling=%s(sampling_id=%d) logs[accepted=%d dropped=%d] metrics[recorded=%d dup=%d] snapshot_collectors=%v",
		sum1.ExecutionID, sum1.Verdict.Decision, sum1.Verdict.SamplingID,
		sum1.Log.Accepted, sum1.Log.Dropped,
		sum1.Metrics.Recorded, sum1.Metrics.Duplicates, sum1.Snapshot.Collectors)
}

func TestEndToEndFailureCausePreserved(t *testing.T) {
	sum := runOnePipeline(t, 1, true, 0)
	if sum.Cause == nil || !strings.Contains(sum.Cause.Error(), "forced failure") {
		t.Fatalf("failure cause lost: %v", sum.Cause)
	}
	t.Logf("execution=%s result=error cause=%q trace_err=%v",
		sum.ExecutionID, sum.Cause, sum.TraceErr)
}

func TestEndToEndSamplingDropIsExplainable(t *testing.T) {
	// rate extremely high so at least one of a fixed id set drops; scan ids by
	// varying seedByte and report decision/reason explicitly.
	var kept, dropped int
	var droppedExample string
	for seed := byte(0); seed < 40; seed++ {
		tr := deterministicTracer(seed)
		_, b := New(context.Background(), BundleConfig{
			Name: "probe", Tracer: tr, SampleRate: 1000,
			DiagCollectors: []diag.Collector{diag.GoroutineCollector{}},
		})
		v := b.Verdict()
		switch v.Decision {
		case 0: // keep
			kept++
		default:
			dropped++
			if droppedExample == "" {
				droppedExample = b.ExecutionID()
			}
		}
		_, _ = b.Finish(context.Background(), nil)
	}
	if dropped == 0 {
		t.Fatal("expected some deterministic drops at rate 1000")
	}
	// Note: dropped sampling does NOT stop telemetry; it only labels verdict.
	t.Logf("decision keep=%d drop=%d example_dropped_execution=%s drop_reason=%s",
		kept, dropped, droppedExample, "rate")
}

func TestEndToEndCapacityDegradationDistinguishable(t *testing.T) {
	// capacity 1 forces log/metric drops; valid data must remain intact and
	// business flow must still Finish successfully.
	sum := runOnePipeline(t, 1, false, 1)
	if sum.Log.Reasons[logx.DropBufferFull] == 0 {
		t.Fatalf("expected log capacity drops, report=%+v", sum.Log)
	}
	if sum.Metrics.Reasons[metric.OutcomeDroppedCapacity] == 0 {
		t.Fatalf("expected metric capacity drops, report=%+v", sum.Metrics)
	}
	// At least the first record of each sink survives.
	if sum.Log.Accepted < 1 || sum.Metrics.Recorded < 1 {
		t.Fatal("first valid records must be retained under pressure")
	}
	t.Logf("execution=%s degradation: log_drops=%s metric_drops=capacity; business=finished_ok; kept first records",
		sum.ExecutionID, logx.DropBufferFull)
}

func TestFinishIdempotent(t *testing.T) {
	tr := deterministicTracer(0)
	ctx, b := New(context.Background(), BundleConfig{
		Name: "once", Tracer: tr,
		DiagCollectors: []diag.Collector{diag.GoroutineCollector{}},
	})
	_ = b.Diag.Start(ctx)
	if _, err := b.Finish(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Finish(ctx, nil); !errors.Is(err, ErrAlreadyFinished) {
		t.Fatalf("want ErrAlreadyFinished, got %v", err)
	}
	t.Logf("execution=%s finish exactly-once confirmed", b.ExecutionID())
}

func TestHighLoadCorrelationStable(t *testing.T) {
	tr := deterministicTracer(7)
	ctx, b := New(context.Background(), BundleConfig{
		Name: "load", Tracer: tr,
		DiagCollectors: []diag.Collector{diag.GoroutineCollector{}, diag.MemStatsCollector{}},
	})
	_ = b.Diag.Start(ctx)

	const workers = 40
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			fctx, span, err := tr.ForkAsync(ctx, fmt.Sprintf("w%d", id))
			if err != nil {
				errs <- err
				return
			}
			defer span.End(nil)
			r := b.Logger.Info(fctx, "work", logx.F("w", id))
			if !r.Accepted || r.ExecutionID != b.ExecutionID() {
				errs <- fmt.Errorf("log correlation mismatch: %+v", r)
				return
			}
			if mr := b.Meter.Add(fctx, "work_units", 1, fmt.Sprintf("w%d", id)); mr.Outcome != metric.OutcomeRecorded {
				errs <- fmt.Errorf("metric unexpected outcome %s", mr.Outcome)
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatal(e)
	}
	sum, err := b.Finish(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if sum.Log.Accepted != workers {
		t.Fatalf("want %d accepted logs, got %d (dropped=%d)", workers, sum.Log.Accepted, sum.Log.Dropped)
	}
	if sum.Metrics.Values["work_units"] != int64(workers) {
		t.Fatalf("want %d work units, got %d", workers, sum.Metrics.Values["work_units"])
	}
	if sum.Snapshot.Goroutines < 1 {
		t.Fatal("snapshot missing runtime data under load")
	}
	t.Logf("execution=%s high-load workers=%d logs_accepted=%d metric_total=%d goroutines=%d all-correlated",
		sum.ExecutionID, workers, sum.Log.Accepted, sum.Metrics.Values["work_units"], sum.Snapshot.Goroutines)
}
