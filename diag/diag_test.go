package diag

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/go-runtime-diagnostics-go/trace"
)

func TestSnapshotContent(t *testing.T) {
	m := NewManager(nil)
	s, err := m.Start("e1")
	if err != nil {
		t.Fatal(err)
	}
	start := s.StartedAt()
	if start.IsZero() {
		t.Fatal("startedAt zero")
	}

	ok := s.AppendPhase(Phase{Name: "fetch", Duration: 5 * time.Millisecond})
	if !ok {
		t.Fatal("append phase rejected while running")
	}
	s.AppendBlock("chan-recv", 2*time.Millisecond)
	s.SetResultError(errors.New("boom"))

	// 做一点 CPU 工作,确保 CPU 差值为非负。
	busyLoop(5 * time.Millisecond)

	snap, err := s.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	if snap.Execution != "e1" {
		t.Fatalf("execution=%s", snap.Execution)
	}
	if len(snap.Phases) != 1 || snap.Phases[0].Name != "fetch" {
		t.Fatalf("phases: %+v", snap.Phases)
	}
	if len(snap.Blocks) != 1 || snap.Blocks[0].Operation != "chan-recv" {
		t.Fatalf("blocks: %+v", snap.Blocks)
	}
	if snap.CPUUsed < 0 {
		t.Fatalf("cpu used negative: %v", snap.CPUUsed)
	}
	if snap.Memory.AllocBytes == 0 || snap.Memory.Goroutines == 0 {
		t.Fatalf("memory sample empty: %+v", snap.Memory)
	}
	if s.ResultError() != "boom" {
		t.Fatalf("result error not retained: %q", s.ResultError())
	}
	if snap.HasFailures() {
		t.Fatalf("unexpected failures: %+v", snap.Failures)
	}

	if err := m.Stop("e1"); err != nil {
		t.Fatalf("stop: %v", err)
	}
	if s.AppendPhase(Phase{Name: "late"}) {
		t.Fatal("phase append after stop must be rejected")
	}
}

func TestSessionLifecycleErrors(t *testing.T) {
	m := NewManager(nil)
	if _, err := m.Start(""); !errors.Is(err, ErrUnknownExecution) {
		t.Fatalf("empty execution: %v", err)
	}
	if _, err := m.Get("missing"); !errors.Is(err, ErrUnknownExecution) {
		t.Fatalf("get missing: %v", err)
	}
	s, _ := m.Start("e")
	if _, err := m.Start("e"); !errors.Is(err, ErrSessionActive) {
		t.Fatalf("duplicate start: %v", err)
	}
	if err := m.Stop("ghost"); !errors.Is(err, ErrUnknownExecution) {
		t.Fatalf("stop unknown: %v", err)
	}
	if err := m.Stop("e"); err != nil {
		t.Fatalf("first stop: %v", err)
	}
	if err := m.Stop("e"); !errors.Is(err, ErrUnknownExecution) {
		t.Fatalf("second stop must be distinguishable, got %v", err)
	}
	if _, err := s.Snapshot(context.Background()); !errors.Is(err, ErrNotRunning) {
		t.Fatalf("snapshot after stop: %v", err)
	}
}

func TestConcurrentSnapshotOnlyOneRuns(t *testing.T) {
	// 注入内存阶段的短暂耗时:用较慢的 profile 采集窗口制造并发窗口。
	m := NewManager(nil, WithCPUProfile(100, 30*time.Millisecond))
	s, _ := m.Start("e")

	var wg sync.WaitGroup
	errs := make([]error, 5)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = s.Snapshot(context.Background())
		}(i)
	}
	wg.Wait()

	concurrent, succeeded, failed := 0, 0, 0
	for _, e := range errs {
		switch {
		case e == nil:
			succeeded++
		case errors.Is(e, ErrConcurrentSnapshot):
			concurrent++
		case errors.Is(e, ErrCollectionFailed):
			failed++
		}
	}
	// 同时只有一个采集能进行:1 个进行,其余必须拿到 ErrConcurrentSnapshot。
	if concurrent != 4 {
		t.Fatalf("want 4 concurrent rejections, got %d (ok=%d failed=%d) all=%v",
			concurrent, succeeded, failed, errs)
	}
	if err := m.Stop("e"); err != nil {
		t.Fatal(err)
	}
}

func TestCollectionFailureIsPartialAndExplainable(t *testing.T) {
	m := NewManager(nil, WithFailStages(stageMemory))
	s, _ := m.Start("e")
	s.AppendPhase(Phase{Name: "p1"})
	snap, err := s.Snapshot(context.Background())
	if !errors.Is(err, ErrCollectionFailed) {
		t.Fatalf("want wrapped collection failure, got %v", err)
	}
	if !snap.HasFailures() {
		t.Fatal("snapshot must carry failures")
	}
	var ce *CollectError
	if !errors.As(err, &ce) || ce.Stage != stageMemory {
		t.Fatalf("failure chain must locate stage: %v", err)
	}
	t.Logf("execution=e snapshot partial-failure stage=%s (other stages preserved)", ce.Stage)
	// 其余阶段数据仍在,部分成功可解释。
	if len(snap.Phases) != 1 {
		t.Fatalf("phases lost on partial failure: %+v", snap.Phases)
	}
	if snap.Memory.AllocBytes != 0 {
		t.Fatal("failed stage must not present fake data")
	}
	_ = m.Stop("e")
}

func TestCPUProfileOptional(t *testing.T) {
	m := NewManager(nil, WithCPUProfile(100, 25*time.Millisecond))
	s, _ := m.Start("e")
	go func() { busyLoop(30 * time.Millisecond) }()
	snap, err := s.Snapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.ProfileBytes) == 0 {
		t.Fatal("pprof bytes expected when enabled")
	}
	if string(snap.ProfileBytes[:4]) != "\x1f\x8b\x08\x00" &&
		len(snap.ProfileBytes) < 10 {
		t.Fatalf("profile bytes do not look like gzip/pprof: %d bytes", len(snap.ProfileBytes))
	}
	_ = m.Stop("e")
}

func TestStopWaitsForSnapshot(t *testing.T) {
	m := NewManager(nil, WithCPUProfile(100, 40*time.Millisecond))
	s, _ := m.Start("e")
	go func() { _, _ = s.Snapshot(context.Background()) }()
	// 给快照一点时间进入采集态。
	time.Sleep(5 * time.Millisecond)
	done := make(chan error, 1)
	go func() { done <- m.Stop("e") }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
		if s.Running() {
			t.Fatal("session still running after stop")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stop blocked forever while snapshot in progress")
	}
}

func TestStopAllDeterministic(t *testing.T) {
	m := NewManager(nil)
	if _, err := m.Start("b"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Start("a"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Start("c"); err != nil {
		t.Fatal(err)
	}
	errs := m.StopAll()
	if len(errs) != 0 {
		t.Fatalf("unexpected errors: %v", errs)
	}
	if _, err := m.Get("a"); !errors.Is(err, ErrUnknownExecution) {
		t.Fatalf("sessions not removed: %v", err)
	}
}

func TestWithPhaseTraceCorrelation(t *testing.T) {
	tr := trace.New()
	m := NewManager(tr)
	ctx, root := tr.StartExecution("e", "root", context.Background())
	s, _ := m.Start("e")
	ctx = WithSession(ctx, s)

	called := false
	err := m.WithPhase(ctx, "phase-1", func(ctx context.Context) error {
		called = true
		return fmt.Errorf("phase fail")
	})
	if err == nil || !called {
		t.Fatalf("phase fn must run and error propagate: %v", err)
	}

	spans := tr.ExecutionSpans("e")
	// root + phase-1
	var phaseSnap trace.SpanSnapshot
	found := false
	for _, sp := range spans {
		if sp.Name == "phase-1" {
			phaseSnap, found = sp, true
		}
	}
	if !found {
		t.Fatalf("phase span missing: %+v", spans)
	}
	if phaseSnap.Parent != root.ID() {
		t.Fatalf("phase not linked to chain: parent=%s root=%s", phaseSnap.Parent, root.ID())
	}
	if phaseSnap.Err != "phase fail" || !phaseSnap.Ended {
		t.Fatalf("phase error/end not recorded: %+v", phaseSnap)
	}

	snap, _ := s.Snapshot(context.Background())
	if len(snap.Phases) != 1 || !snap.Phases[0].Failed || snap.Phases[0].SpanID != phaseSnap.ID {
		t.Fatalf("phase snapshot wrong: %+v", snap.Phases)
	}
	_ = m.Stop("e")
	root.End(nil)
}

func busyLoop(d time.Duration) {
	deadline := time.Now().Add(d)
	x := 0
	for time.Now().Before(deadline) {
		for i := 0; i < 10000; i++ {
			x++
		}
	}
	_ = x
}
