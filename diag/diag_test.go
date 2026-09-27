package diag

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func failingCollector(name string, err error) Collector {
	return &fakeCollector{name: name, err: err}
}

type fakeCollector struct {
	name string
	err  error
	n    int
	mu   sync.Mutex
}

func (f *fakeCollector) Name() string { return f.name }
func (f *fakeCollector) Collect(ctx context.Context) (Profile, error) {
	f.mu.Lock()
	f.n++
	f.mu.Unlock()
	if f.err != nil {
		return Profile{Kind: f.name, Err: f.err.Error()}, f.err
	}
	return Profile{Kind: f.name, Value: 42}, nil
}
func (f *fakeCollector) calls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.n
}

func newTestProfiler(t *testing.T, coll ...Collector) *Profiler {
	t.Helper()
	var clockN int64
	p, err := New(Config{
		ExecutionID: "exec-test",
		Collectors:  coll,
		Clock: func() time.Time {
			clockN++
			return time.Unix(1000, 0).Add(time.Duration(clockN) * time.Millisecond)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLifecycleStableSemantics(t *testing.T) {
	p := newTestProfiler(t, &fakeCollector{name: "memstats"})

	// Snapshot/End before start.
	if _, err := p.Snapshot(context.Background()); !errors.Is(err, ErrNotRunning) {
		t.Fatalf("want ErrNotRunning, got %v", err)
	}
	if _, err := p.End(context.Background()); !errors.Is(err, ErrNotRunning) {
		t.Fatalf("end before start want ErrNotRunning, got %v", err)
	}
	if err := p.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	// Repeated start.
	if err := p.Start(context.Background()); !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("want ErrAlreadyRunning, got %v", err)
	}
	if err := p.MarkPhase("acquire", time.Unix(1000, 0), true, nil); err != nil {
		t.Fatal(err)
	}
	snap, err := p.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Phases) != 1 || !snap.Phases[0].Blocking {
		t.Fatal("phase not captured")
	}
	final, err := p.End(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(final.Phases) != 1 {
		t.Fatal("final snapshot must retain phases")
	}
	// Repeat end: exactly-once.
	if _, err := p.End(context.Background()); !errors.Is(err, ErrAlreadyEnded) {
		t.Fatalf("want ErrAlreadyEnded, got %v", err)
	}
	if _, err := p.Snapshot(context.Background()); !errors.Is(err, ErrAlreadyEnded) {
		t.Fatalf("snapshot after end want ErrAlreadyEnded, got %v", err)
	}
	if err := p.MarkPhase("late", time.Time{}, false, nil); !errors.Is(err, ErrNotRunning) {
		t.Fatalf("phase after end want ErrNotRunning, got %v", err)
	}
	t.Logf("execution=%s lifecycle idle->running->finished, end exactly-once", p.cfg.ExecutionID)
}

func TestNewRequiresExecutionID(t *testing.T) {
	if _, err := New(Config{}); !errors.Is(err, ErrNoExecutionID) {
		t.Fatalf("want ErrNoExecutionID, got %v", err)
	}
}

func TestCollectorFailureIsolated(t *testing.T) {
	boom := errors.New("sampler device busy")
	good := &fakeCollector{name: "memstats"}
	bad := failingCollector("heap", boom)
	p := newTestProfiler(t, good, bad)
	if err := p.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	snap, err := p.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("one failing collector must not abort snapshot: %v", err)
	}
	if len(snap.Failures) != 1 || !strings.Contains(snap.Failures[0], "sampler device busy") {
		t.Fatalf("failure not recorded distinctly: %+v", snap.Failures)
	}
	if good.calls() != 1 {
		t.Fatal("healthy collector must still run")
	}
	if snap.Heap.Err == "" {
		t.Fatal("failed heap profile should carry its error cause")
	}
	t.Logf("execution=%s snapshot OK with degraded collector: failures=%v", p.cfg.ExecutionID, snap.Failures)
	_, _ = p.End(context.Background())
}

func TestConcurrentSnapshotsSerialized(t *testing.T) {
	coll := &fakeCollector{name: "memstats"}
	p := newTestProfiler(t, coll)
	if err := p.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	const n = 20
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			snap, err := p.Snapshot(context.Background())
			if err != nil {
				errs <- err
				return
			}
			if snap.ExecutionID != "exec-test" {
				errs <- errors.New("execution id drift")
			}
		}()
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Fatal(e)
	}
	if got := coll.calls(); got != n {
		t.Fatalf("each snapshot must collect exactly once, got %d calls", got)
	}
	t.Logf("execution=%s %d concurrent snapshots serialized, collector calls=%d",
		p.cfg.ExecutionID, n, coll.calls())
	_, _ = p.End(context.Background())
}

func TestCPUCollectorHonorsCancellation(t *testing.T) {
	c := NewCPUCollector(5 * time.Second) // long window
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(30 * time.Millisecond)
		cancel()
	}()
	prof, err := c.Collect(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if prof.Err == "" || prof.Kind != "cpu" {
		t.Fatal("canceled profile must retain kind and cause")
	}
	t.Logf("execution=exec-test cpu collector canceled, cause=%s duration=%s",
		prof.Err, prof.Duration)
}

func TestBlockUnavailableReported(t *testing.T) {
	b := BlockCollector{}
	prof, err := b.Collect(context.Background())
	// block profiling is off by default in tests; this documents the failure.
	if err == nil {
		// If the test binary enabled it, profile data is fine.
		if prof.Kind != "block" {
			t.Fatal("want block kind")
		}
		return
	}
	if prof.Err == "" {
		t.Fatal("unavailable block profile must explain why")
	}
	t.Logf("execution=exec-test block collector degraded (expected): %s", prof.Err)
}

func TestDefaultCollectorsCaptureMemAndGoroutines(t *testing.T) {
	p, err := New(Config{ExecutionID: "exec-default", Collectors: DefaultCollectors()})
	if err != nil {
		t.Fatal(err)
	}
	_ = p.Start(context.Background())
	snap, err := p.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snap.Goroutines < 1 {
		t.Fatal("goroutine count must be positive")
	}
	if snap.HeapAlloc == 0 {
		t.Fatal("heap alloc should be reported")
	}
	if len(snap.Heap.Bytes) == 0 {
		t.Fatal("heap pprof bytes expected")
	}
	t.Logf("execution=%s goroutines=%d heap_alloc=%d collectors=%v",
		snap.ExecutionID, snap.Goroutines, snap.HeapAlloc, snap.Collectors)
	_, _ = p.End(context.Background())
}
