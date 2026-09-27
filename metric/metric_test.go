package metric

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/lisama569/go-runtime-diagnostics-go/trace"
)

func newCtx(t *testing.T) (context.Context, *trace.Execution) {
	t.Helper()
	var n byte
	tr := trace.NewTracer(trace.Config{NewID: func() trace.ID {
		n++
		var id trace.ID
		id[15] = n
		return id
	}})
	ctx, exec := tr.StartExecution(context.Background(), "job")
	return ctx, exec
}

func TestCounterDedup(t *testing.T) {
	ctx, exec := newCtx(t)
	m := New(Config{})
	r1 := m.Add(ctx, "requests", 1, "evt-1")
	if r1.Outcome != OutcomeRecorded {
		t.Fatalf("first write want recorded, got %s", r1.Outcome)
	}
	r2 := m.Add(ctx, "requests", 1, "evt-1")
	if r2.Outcome != OutcomeDuplicate || !r2.AlreadySeen {
		t.Fatalf("repeat dedup key want duplicate, got %+v", r2)
	}
	// Same logical retries from a client: a third time still no double count.
	r3 := m.Add(ctx, "requests", 500, "evt-1")
	if r3.Outcome != OutcomeDuplicate {
		t.Fatalf("third retry must still dedupe, got %s", r3.Outcome)
	}
	rep, _ := m.Report(exec.ID().String())
	if rep.Values["requests"] != 1 {
		t.Fatalf("duplicate retries must not inflate counter, got %d", rep.Values["requests"])
	}
	if rep.Recorded != 1 || rep.Duplicates != 2 {
		t.Fatalf("counters mismatch: %+v", rep)
	}
	t.Logf("execution=%s counter=requests value=%d recorded=%d duplicates=%d (retry value ignored)",
		exec.ID(), rep.Values["requests"], rep.Recorded, rep.Duplicates)
}

func TestAccumulatingCountersWithoutKey(t *testing.T) {
	ctx, exec := newCtx(t)
	m := New(Config{})
	m.Add(ctx, "bytes", 10, "")
	m.Add(ctx, "bytes", 20, "")
	rep, _ := m.Report(exec.ID().String())
	if rep.Values["bytes"] != 30 {
		t.Fatalf("keyless counter should accumulate, got %d", rep.Values["bytes"])
	}
	t.Logf("execution=%s keyless accumulation bytes=%d", exec.ID(), rep.Values["bytes"])
}

func TestEventDedup(t *testing.T) {
	ctx, exec := newCtx(t)
	m := New(Config{})
	if r := m.RecordEvent(ctx, "job.started", "k1"); r.Outcome != OutcomeRecorded {
		t.Fatal(r.Outcome)
	}
	if r := m.RecordEvent(ctx, "job.started", "k1"); r.Outcome != OutcomeDuplicate {
		t.Fatalf("duplicate event want duplicate, got %s", r.Outcome)
	}
	rep, _ := m.Report(exec.ID().String())
	if rep.Values["job.started"] != 1 {
		t.Fatal("one-shot event must count once")
	}
	// Event without dedup key is rejected (must be distinguishable).
	if r := m.RecordEvent(ctx, "x", ""); r.Outcome != OutcomeNoExecution {
		t.Fatalf("keyless event want rejection, got %s", r.Outcome)
	}
	t.Logf("execution=%s event=job.started count=%d", exec.ID(), rep.Values["job.started"])
}

func TestGaugeLastWriterWins(t *testing.T) {
	ctx, exec := newCtx(t)
	m := New(Config{})
	m.SetGauge(ctx, "queue_depth", 5)
	m.SetGauge(ctx, "queue_depth", 9)
	rep, _ := m.Report(exec.ID().String())
	if rep.Values["queue_depth"] != 9 {
		t.Fatalf("gauge last-writer-wins failed, got %d", rep.Values["queue_depth"])
	}
	t.Logf("execution=%s gauge=queue_depth last=%d", exec.ID(), rep.Values["queue_depth"])
}

func TestCapacityPressureDoesNotOverwrite(t *testing.T) {
	ctx, exec := newCtx(t)
	m := New(Config{Capacity: 2})
	m.Add(ctx, "c1", 1, "a")
	m.Add(ctx, "c2", 1, "b")
	r := m.Add(ctx, "c3", 1, "c")
	if r.Outcome != OutcomeDroppedCapacity {
		t.Fatalf("want dropped_capacity, got %s", r.Outcome)
	}
	rep, _ := m.Report(exec.ID().String())
	if rep.Dropped != 1 || rep.Reasons[OutcomeDroppedCapacity] != 1 {
		t.Fatalf("drop accounting mismatch %+v", rep)
	}
	snap := m.Snapshot()
	if len(snap) != 2 {
		t.Fatal("existing records must survive pressure unchanged")
	}
	if rep.Values["c1"] != 1 || rep.Values["c2"] != 1 {
		t.Fatal("valid values overwritten under pressure")
	}
	t.Logf("execution=%s capacity=2 recorded=%d dropped=%d reason=%s existing-intact",
		exec.ID(), rep.Recorded, rep.Dropped, OutcomeDroppedCapacity)
}

func TestNoExecutionRejected(t *testing.T) {
	m := New(Config{})
	r := m.Add(context.Background(), "c", 1, "k")
	if r.Outcome != OutcomeNoExecution {
		t.Fatalf("want no_execution, got %s", r.Outcome)
	}
	t.Logf("execution=<missing> decision=dropped reason=%s", r.Outcome)
}

func TestConcurrentDedup(t *testing.T) {
	ctx, exec := newCtx(t)
	m := New(Config{})
	const goroutines = 64
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// All goroutines race to report the SAME idempotent key.
			r := m.Add(ctx, "checkout", 1, "same-order-id")
			switch r.Outcome {
			case OutcomeRecorded, OutcomeDuplicate:
			default:
				errs <- errors.New("unexpected outcome: " + r.Outcome.String())
			}
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatal(e)
	}
	rep, _ := m.Report(exec.ID().String())
	if rep.Values["checkout"] != 1 {
		t.Fatalf("exactly one concurrent reporter must count, got %d", rep.Values["checkout"])
	}
	if rep.Recorded != 1 || rep.Recorded+rep.Duplicates != goroutines {
		t.Fatalf("recorded=1 duplicates=%d-%d want total %d", rep.Recorded, rep.Duplicates, goroutines)
	}
	t.Logf("execution=%s concurrent idempotent key: recorded=%d duplicates=%d value=%d",
		exec.ID(), rep.Recorded, rep.Duplicates, rep.Values["checkout"])
}

func TestMeterCloseRejects(t *testing.T) {
	ctx, exec := newCtx(t)
	m := New(Config{})
	_ = m.Close()
	if r := m.Add(ctx, "c", 1, "k"); r.Outcome != OutcomeMeterClosed {
		t.Fatalf("want meter_closed, got %s", r.Outcome)
	}
	t.Logf("execution=%s decision=dropped reason=%s", exec.ID(), OutcomeMeterClosed)
}

func TestExecutionsIsolated(t *testing.T) {
	ctx1, e1 := newCtx(t)
	var n byte
	tr := trace.NewTracer(trace.Config{NewID: func() trace.ID {
		n++
		var id trace.ID
		id[15] = n + 100
		return id
	}})
	ctx2, e2 := tr.StartExecution(context.Background(), "job2")
	m := New(Config{})
	m.Add(ctx1, "x", 1, "k")
	m.Add(ctx2, "x", 2, "k")
	r1, _ := m.Report(e1.ID().String())
	r2, _ := m.Report(e2.ID().String())
	if r1.Values["x"] != 1 || r2.Values["x"] != 2 {
		t.Fatalf("per-execution isolation broken: %d vs %d", r1.Values["x"], r2.Values["x"])
	}
	t.Logf("execution=%s x=%d | execution=%s x=%d isolated", e1.ID(), r1.Values["x"], e2.ID(), r2.Values["x"])
}
