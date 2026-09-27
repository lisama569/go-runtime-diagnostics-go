package logx

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/lisama569/go-runtime-diagnostics-go/sink"
	"github.com/lisama569/go-runtime-diagnostics-go/trace"
)

func newExecCtx(execution trace.ExecutionID, span trace.SpanID) context.Context {
	return trace.WithContext(context.Background(), trace.SpanContext{
		Execution: execution, Span: span,
	})
}

func TestLogAssociatedByExecution(t *testing.T) {
	l := New()
	ctx := newExecCtx("e1", "s1")
	l.Log(ctx, LevelInfo, "hello", map[string]string{"k": "v"})
	l.Log(ctx, LevelError, "boom", nil)
	l.Log(newExecCtx("e2", "s2"), LevelInfo, "other", nil)

	e1 := l.Entries("e1")
	if len(e1) != 2 {
		t.Fatalf("e1 entries=%d", len(e1))
	}
	if e1[0].Execution != "e1" || e1[0].Span != "s1" || e1[0].Attrs["k"] != "v" {
		t.Fatalf("entry correlation wrong: %+v", e1[0])
	}
	if len(l.Entries("e2")) != 1 || len(l.All()) != 3 {
		t.Fatal("per-execution grouping wrong")
	}
}

func TestLogRejectsMissingContext(t *testing.T) {
	l := New()
	if r := l.Log(context.Background(), LevelInfo, "x", nil); r.Reason != sink.ReasonRejected {
		t.Fatalf("missing context must be rejected, got %+v", r)
	}
	if r := l.LogExecution("", "", LevelInfo, "x", nil); r.Reason != sink.ReasonRejected {
		t.Fatalf("empty execution must be rejected, got %+v", r)
	}
	if len(l.All()) != 0 {
		t.Fatal("rejected logs must not be stored")
	}
}

func TestLogDedupAndCapacity(t *testing.T) {
	l := New(WithCapacity(2))
	ctx := newExecCtx("e", "s")
	r1 := l.Log(ctx, LevelWarn, "same", nil)
	r2 := l.Log(ctx, LevelWarn, "same", map[string]string{"dynamic": "v2"})
	if !r1.Accepted || !r2.Duplicate {
		t.Fatalf("dedup wrong: %+v %+v", r1, r2)
	}
	l.Log(ctx, LevelInfo, "second", nil)
	r3 := l.Log(ctx, LevelInfo, "third", nil)
	if !r3.Dropped || r3.Reason != sink.ReasonCapacity {
		t.Fatalf("capacity degradation wrong: %+v", r3)
	}
	if l.Duplicates() != 1 || l.Dropped() != 1 {
		t.Fatalf("counters wrong dups=%d dropped=%d", l.Duplicates(), l.Dropped())
	}
}

func TestConcurrentWritesStable(t *testing.T) {
	l := New(WithCapacity(100000), WithDedup(false))
	var wg sync.WaitGroup
	for g := 0; g < 50; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			ctx := newExecCtx("e", trace.SpanID(fmt.Sprintf("s%d", g)))
			for i := 0; i < 200; i++ {
				l.Log(ctx, LevelInfo, fmt.Sprintf("msg-%d", i), nil)
			}
		}(g)
	}
	wg.Wait()
	if got := len(l.Entries("e")); got != 10000 {
		t.Fatalf("want 10000 entries, got %d (dropped=%d)", got, l.Dropped())
	}
	if l.Dropped() != 0 {
		t.Fatalf("unexpected drops: %d", l.Dropped())
	}
}
