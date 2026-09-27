package trace

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

func newTestTracer(t *testing.T) *Tracer {
	t.Helper()
	var n uint64
	tr := NewTracer(Config{
		Clock: func() time.Time { n++; return time.Unix(1000, 0).Add(time.Duration(n) * time.Millisecond) },
		NewID: func() ID {
			n++
			var id ID
			id[15] = byte(n)
			id[0] = 0xab
			return id
		},
		NewSpanID: func() SpanID {
			n++
			var id SpanID
			id[7] = byte(n)
			return id
		},
	})
	return tr
}

func TestStartSpan_MissingContext(t *testing.T) {
	tr := newTestTracer(t)
	_, _, err := tr.StartSpan(context.Background(), "x", SpanSync)
	if !errors.Is(err, ErrNoExecution) {
		t.Fatalf("want ErrNoExecution, got %v", err)
	}
	if _, _, err := tr.StartSpan(nil, "x", SpanSync); !errors.Is(err, ErrNilContext) {
		t.Fatalf("want ErrNilContext, got %v", err)
	}
}

func TestSyncCausalChain(t *testing.T) {
	tr := newTestTracer(t)
	ctx, exec := tr.StartExecution(context.Background(), "root")
	ctx, s1, err := tr.StartSpan(ctx, "sync1", SpanSync)
	if err != nil {
		t.Fatal(err)
	}
	ctx, s2, err := tr.StartSpan(ctx, "sync2", SpanSync)
	if err != nil {
		t.Fatal(err)
	}
	if err := s2.End(nil); err != nil {
		t.Fatal(err)
	}
	if err := s1.End(nil); err != nil {
		t.Fatal(err)
	}
	v := exec.View()
	if !v.Finished {
		if err := exec.Finish(nil); err != nil {
			t.Fatal(err)
		}
	}
	v = exec.View()
	// root -> s1 -> s2
	if len(v.Spans) != 3 {
		t.Fatalf("want 3 spans, got %d", len(v.Spans))
	}
	if s2.sc.parentID != s1.sc.id {
		t.Fatal("s2 parent must be s1")
	}
	if s1.sc.parentID != v.RootID {
		t.Fatal("s1 parent must be root")
	}
	for _, sp := range v.Spans {
		if sp.ID.IsZero() {
			t.Fatalf("span %s has zero id", sp.Name)
		}
		if sp.ID != v.RootID && sp.ParentID == (SpanID{}) {
			t.Fatalf("non-root span %s has zero parent", sp.Name)
		}
		if sp.TraceID != v.TraceID {
			t.Fatal("trace id mismatch")
		}
	}
	t.Logf("execution=%s spans=%d causal chain root->sync1->sync2 OK", exec.ID(), len(v.Spans))
}

func TestEndIdempotent(t *testing.T) {
	tr := newTestTracer(t)
	ctx, exec := tr.StartExecution(context.Background(), "root")
	_, s, _ := tr.StartSpan(ctx, "s", SpanSync)
	_ = ctx
	err1 := errors.New("boom")
	if err := s.End(err1); err != nil {
		t.Fatal(err)
	}
	if err := s.End(errors.New("second")); !errors.Is(err, ErrSpanFinished) {
		t.Fatalf("want ErrSpanFinished, got %v", err)
	}
	sv := s.View()
	if sv.Err != err1 {
		t.Fatal("recorded error must not be overwritten by repeat End")
	}
	if err := exec.Finish(nil); err != nil {
		t.Fatal(err)
	}
	if err := exec.Finish(nil); !errors.Is(err, ErrExecutionFinished) {
		t.Fatalf("want ErrExecutionFinished, got %v", err)
	}
	if _, err := FromContext(ctx); !errors.Is(err, ErrExecutionFinished) {
		t.Fatalf("want ErrExecutionFinished from FromContext, got %v", err)
	}
}

func TestForkAsync_DetachedFromCancellation(t *testing.T) {
	tr := newTestTracer(t)
	base, exec := tr.StartExecution(context.Background(), "root")
	ctx, cancel := context.WithCancel(base)
	fctx, fspan, err := tr.ForkAsync(ctx, "fork")
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	// Forked work must remain usable even though the originating request is done.
	select {
	case <-fctx.Done():
		t.Fatal("forked context must not inherit cancellation")
	default:
	}
	// ...and still belongs to the same execution.
	sc := fspan.Context()
	if sc.ExecutionID != exec.ID() {
		t.Fatal("forked span must keep execution id")
	}
	if fspan.View().Kind != SpanAsyncFork {
		t.Fatal("forked span kind mismatch")
	}
	if err := fspan.End(nil); err != nil {
		t.Fatal(err)
	}
	t.Logf("execution=%s async fork survived cancel, keeps causal parent=%s",
		exec.ID(), fspan.sc.parentID)
	_ = exec.Finish(nil)
}

func TestQueueHandoffAndConsume(t *testing.T) {
	tr := newTestTracer(t)
	ctx, exec := tr.StartExecution(context.Background(), "producer")
	token, err := QueueHandoffOn(tr, ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Consumer uses a brand new unrelated base context (simulating a worker).
	cctx, cspan, err := tr.QueueConsume(context.Background(), token)
	if err != nil {
		t.Fatal(err)
	}
	sc, _ := CurrentSpanContext(cctx)
	if sc.ExecutionID != exec.ID() {
		t.Fatal("queued task must share execution id")
	}
	if cspan.View().Kind != SpanQueueConsume {
		t.Fatal("consume span kind mismatch")
	}
	_ = cspan.End(nil)
	v := exec.View()
	foundProduce, foundConsume := false, false
	for _, sp := range v.Spans {
		if sp.Kind == SpanQueueProduce {
			foundProduce = true
		}
		if sp.Kind == SpanQueueConsume {
			foundConsume = true
			if sp.ParentID == (SpanID{}) {
				t.Fatal("consume span must link to produce span")
			}
		}
	}
	if !foundProduce || !foundConsume {
		t.Fatalf("missing produce=%v consume=%v", foundProduce, foundConsume)
	}
	t.Logf("execution=%s queued handoff correlated across fresh worker context", exec.ID())
	_ = exec.Finish(nil)
}

// QueueHandoffOn is a tracer-bound helper used in tests; mirrors the free fn.
func QueueHandoffOn(tr *Tracer, ctx context.Context) (SpanContext, error) {
	return QueueHandoff(ctx)
}

func TestForeignAndCorruptContextsRejected(t *testing.T) {
	tr := newTestTracer(t)
	ctx1, exec1 := tr.StartExecution(context.Background(), "e1")
	tr2 := newTestTracer(t)

	// Context from default/other tracer must not attach to tr2.
	if _, _, err := tr2.StartSpan(ctx1, "rogue", SpanSync); !errors.Is(err, ErrForeignExecution) {
		t.Fatalf("cross-tracer attach want ErrForeignExecution, got %v", err)
	}

	// Fabricated token (unknown execution id) must be rejected, not mounted.
	fake := SpanContext{Valid: true, ExecutionID: ID{1, 2, 3}, SpanID: SpanID{9}, TraceID: ID{7}}
	if _, _, err := tr.QueueConsume(context.Background(), fake); !errors.Is(err, ErrForeignExecution) {
		t.Fatalf("foreign execution token want ErrForeignExecution, got %v", err)
	}

	// Zero-valued token.
	zero := SpanContext{Valid: true}
	if _, _, err := tr.RestoreContext(context.Background(), zero); !errors.Is(err, ErrInvalidSpanContext) {
		t.Fatalf("zero token want ErrInvalidSpanContext, got %v", err)
	}
	// Invalid flag token.
	if _, _, err := tr.RestoreContext(context.Background(), SpanContext{}); !errors.Is(err, ErrInvalidSpanContext) {
		t.Fatalf("invalid token want ErrInvalidSpanContext, got %v", err)
	}
	t.Logf("execution=%s foreign/corrupt inheritance all rejected distinctly", exec1.ID())
	_ = exec1.Finish(nil)
}

func TestSpanCapacityDegradation(t *testing.T) {
	tr := NewTracer(Config{MaxSpans: 2}) // root + one child
	ctx, exec := tr.StartExecution(context.Background(), "root")
	_, s1, err := tr.StartSpan(ctx, "ok", SpanSync)
	if err != nil {
		t.Fatal(err)
	}
	_ = s1.End(nil)
	_, s2, err := tr.StartSpan(ctx, "over", SpanSync)
	if !errors.Is(err, ErrTraceCapacity) {
		t.Fatalf("want ErrTraceCapacity, got %v (span=%v)", err, s2)
	}
	// Business flow continues; execution still finishable.
	if err := exec.Finish(nil); err != nil {
		t.Fatal(err)
	}
	if len(exec.View().Spans) != 2 {
		t.Fatalf("valid spans must not be overwritten, got %d", len(exec.View().Spans))
	}
	t.Logf("execution=%s capacity degradation ErrTraceCapacity, business unaffected", exec.ID())
}

func TestConcurrentSpans_CausalIntegrity(t *testing.T) {
	tr := NewTracer(Config{})
	ctx, exec := tr.StartExecution(context.Background(), "root")
	const workers = 50
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			fctx, fs, err := tr.ForkAsync(ctx, fmt.Sprintf("fork-%d", id))
			if err != nil {
				errs <- err
				return
			}
			defer fs.End(nil)
			cctx, cs, err := tr.StartSpan(fctx, fmt.Sprintf("child-%d", id), SpanSync)
			if err != nil {
				errs <- err
				return
			}
			defer cs.End(nil)
			sc, err := SpanContextOf(cctx)
			if err != nil {
				errs <- err
				return
			}
			if sc.ExecutionID != exec.ID() {
				errs <- errors.New("execution id drift under concurrency")
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if err := exec.Finish(nil); err != nil {
		t.Fatal(err)
	}
	v := exec.View()
	if len(v.Spans) != 1+2*workers {
		t.Fatalf("want %d spans, got %d", 1+2*workers, len(v.Spans))
	}
	// Every non-root span must resolve to a known parent within the execution.
	known := map[SpanID]bool{v.RootID: true}
	for _, sp := range v.Spans {
		if sp.ID != v.RootID && !known[sp.ParentID] {
			t.Fatalf("span %s dangling parent %s", sp.Name, sp.ParentID)
		}
		known[sp.ID] = true
	}
	t.Logf("execution=%s concurrent fan-out: %d spans, parent graph intact", exec.ID(), len(v.Spans))
}

func TestErrorPropagationRecorded(t *testing.T) {
	tr := newTestTracer(t)
	ctx, exec := tr.StartExecution(context.Background(), "root")
	_, s, _ := tr.StartSpan(ctx, "might-fail", SpanSync)
	boom := errors.New("downstream unavailable")
	_ = s.End(boom)
	sv := s.View()
	if !strings.Contains(sv.Err.Error(), "downstream unavailable") {
		t.Fatalf("error not recorded: %v", sv.Err)
	}
	_ = exec.Finish(boom)
	ev := exec.View()
	if !errors.Is(ev.Err, boom) {
		t.Fatalf("execution cause mismatch: %v", ev.Err)
	}
	t.Logf("execution=%s span/execution error cause preserved: %v", exec.ID(), ev.Err)
}
