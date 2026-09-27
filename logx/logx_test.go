package logx

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/lisama569/go-runtime-diagnostics-go/trace"
)

func newExecCtx(t *testing.T) (context.Context, *trace.Execution, *trace.Tracer) {
	t.Helper()
	var n byte
	tr := trace.NewTracer(trace.Config{
		NewID: func() trace.ID {
			n++
			var id trace.ID
			id[15] = n
			return id
		},
	})
	ctx, exec := tr.StartExecution(context.Background(), "job")
	return ctx, exec, tr
}

func TestLogCorrelation(t *testing.T) {
	ctx, exec, _ := newExecCtx(t)
	l := New(Config{})
	r := l.Info(ctx, "hello", F("k", "v"))
	if !r.Accepted || r.ExecutionID != exec.ID().String() {
		t.Fatalf("bad result %+v", r)
	}
	entries := l.Snapshot()
	if len(entries) != 1 {
		t.Fatalf("want 1 entry, got %d", len(entries))
	}
	if entries[0].ExecutionID != exec.ID().String() || entries[0].SpanID == "" {
		t.Fatal("entry missing correlation ids")
	}
	if entries[0].Fields[0].Key != "k" {
		t.Fatal("field lost")
	}
	t.Logf("execution=%s decision=accepted level=%s msg=%q",
		exec.ID(), entries[0].Level, entries[0].Message)
}

func TestLogMissingExecutionNotSilentlyAttached(t *testing.T) {
	l := New(Config{})
	r := l.Info(context.Background(), "orphan")
	if r.Accepted || r.DropReason != DropNoExecution {
		t.Fatalf("orphan log must be rejected distinctly, got %+v", r)
	}
	if len(l.Snapshot()) != 0 {
		t.Fatal("orphan entry must never be stored under another execution")
	}
	// It must not mutate business flow: callers can ignore Result.
	t.Logf("execution=<missing> decision=dropped reason=%s", r.DropReason)
}

func TestLogCapacityDegradation_DistinguishesAndKeepsValid(t *testing.T) {
	ctx, exec, _ := newExecCtx(t)
	l := New(Config{Capacity: 2, PerExecCap: 2})
	for i := 0; i < 2; i++ {
		if r := l.Info(ctx, "kept"); !r.Accepted {
			t.Fatalf("entry %d should be accepted", i)
		}
	}
	r := l.Info(ctx, "overflow")
	if r.Accepted || r.DropReason != DropBufferFull {
		t.Fatalf("overflow must be DropBufferFull, got %+v", r)
	}
	entries := l.Snapshot()
	if len(entries) != 2 {
		t.Fatal("valid records must not be overwritten")
	}
	rep, err := l.Report(exec.ID().String())
	if err != nil {
		t.Fatal(err)
	}
	if rep.Accepted != 2 || rep.Dropped != 1 || rep.Reasons[DropBufferFull] != 1 {
		t.Fatalf("report mismatch %+v", rep)
	}
	t.Logf("execution=%s decision=accepted=%d dropped=%d degradation_reason=%s",
		exec.ID(), rep.Accepted, rep.Dropped, DropBufferFull)
}

func TestLogAfterFinishRejected(t *testing.T) {
	ctx, exec, _ := newExecCtx(t)
	l := New(Config{})
	if err := exec.Finish(nil); err != nil {
		t.Fatal(err)
	}
	r := l.Info(ctx, "late")
	if r.Accepted || r.DropReason != DropExecutionFinished {
		t.Fatalf("late log want DropExecutionFinished, got %+v", r)
	}
	t.Logf("execution=%s decision=dropped reason=%s", exec.ID(), r.DropReason)
}

func TestLogConcurrentNoLossNoOverwrite(t *testing.T) {
	ctx, exec, _ := newExecCtx(t)
	l := New(Config{}) // unlimited
	const writers = 32
	const perWriter = 100
	var wg sync.WaitGroup
	errs := make(chan error, writers)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				r := l.Info(ctx, "w", F("w", id), F("i", i))
				if !r.Accepted {
					errs <- errors.New("unexpected drop in unlimited logger")
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatal(e)
	}
	entries := l.Snapshot()
	if len(entries) != writers*perWriter {
		t.Fatalf("want %d entries, got %d", writers*perWriter, len(entries))
	}
	for _, e := range entries {
		if e.ExecutionID != exec.ID().String() {
			t.Fatal("correlated id drift")
		}
		if !strings.Contains(e.Message, "w") {
			t.Fatal("entry corrupted")
		}
	}
	rep, _ := l.Report(exec.ID().String())
	if rep.Dropped != 0 {
		t.Fatalf("unexpected drops: %+v", rep)
	}
	t.Logf("execution=%s concurrent writes=%d accepted=%d dropped=%d",
		exec.ID(), writers*perWriter, rep.Accepted, rep.Dropped)
}

func TestLogDrainIsolatesExecutions(t *testing.T) {
	ctx1, exec1, tr := newExecCtx(t)
	ctx2, exec2 := tr.StartExecution(context.Background(), "job2")
	l := New(Config{})
	l.Info(ctx1, "a1")
	l.Info(ctx2, "b1")
	l.Info(ctx1, "a2")

	drained := l.DrainByExecution(exec1.ID().String())
	if len(drained) != 2 {
		t.Fatalf("want 2 drained, got %d", len(drained))
	}
	remaining := l.Snapshot()
	if len(remaining) != 1 || remaining[0].ExecutionID != exec2.ID().String() {
		t.Fatal("drain must remove only the requested execution")
	}
	t.Logf("execution=%s drained=%d other_execution=%s retained=%d",
		exec1.ID(), len(drained), exec2.ID(), len(remaining))
}

func TestLoggerCloseRejects(t *testing.T) {
	ctx, exec, _ := newExecCtx(t)
	l := New(Config{})
	_ = l.Close()
	r := l.Info(ctx, "x")
	if r.DropReason != DropLoggerClosed {
		t.Fatalf("want DropLoggerClosed, got %+v", r)
	}
	t.Logf("execution=%s decision=dropped reason=%s", exec.ID(), r.DropReason)
}
