// Package diag supports on-demand performance diagnostic snapshots covering
// call phases, CPU, memory and blocking waits, with stable lifecycle semantics.
//
// Lifecycle (a Profiler is a single state machine per execution):
//
//		Idle --Start--> Running --End--> Finished
//
//	  - Start while not Idle returns ErrAlreadyRunning.
//	  - End while Idle returns ErrNotRunning; End after Finished returns
//	    ErrAlreadyEnded. End is otherwise exactly-once.
//	  - Snapshot while Running captures a point-in-time Snapshot. Concurrent
//	    Snapshot calls are serialized by a mutex (not rejected), guaranteeing
//	    each capture has consistent collectors; a collector failure is recorded
//	    in Snapshot.Failures but never aborts the whole snapshot.
//	  - Snapshot after Finished returns ErrAlreadyEnded; before Start returns
//	    ErrNotRunning.
package diag

import (
	"context"
	"errors"
	"sync"
	"time"
)

// State is the lifecycle state of a Profiler session.
type State int

const (
	StateIdle State = iota
	StateRunning
	StateFinished
	StateFailed
)

func (s State) String() string {
	switch s {
	case StateIdle:
		return "idle"
	case StateRunning:
		return "running"
	case StateFinished:
		return "finished"
	case StateFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// Phase marks one call stage with timing and an optional error cause.
type Phase struct {
	Name     string
	Start    time.Time
	End      time.Time
	Duration time.Duration
	Err      string
	Blocking bool
}

// Profile holds collected profile data.
type Profile struct {
	Kind        string
	Bytes       []byte
	Err         string
	Duration    time.Duration
	Value       int64  // scalar value, e.g. goroutine count
	HeapAlloc   uint64 // set by memstats collector
	HeapObjects uint64 // set by memstats collector
}

// MemStats returns the embedded memory counters.
func (p Profile) MemStats() (uint64, uint64) { return p.HeapAlloc, p.HeapObjects }

// Snapshot is one complete on-demand diagnostic capture.
type Snapshot struct {
	ExecutionID string
	TakenAt     time.Time
	StartedAt   time.Time
	Phases      []Phase
	Heap        Profile
	CPU         Profile
	Block       Profile
	Goroutines  int
	HeapAlloc   uint64
	HeapObjects uint64
	Collectors  []string
	Failures    []string
}

// Collector is one pluggable capture source.
type Collector interface {
	Name() string
	Collect(ctx context.Context) (Profile, error)
}

// Stable lifecycle sentinels.
var (
	ErrAlreadyRunning = errors.New("diag: profiler already running")
	ErrNotRunning     = errors.New("diag: profiler not running")
	ErrAlreadyEnded   = errors.New("diag: profiler already ended")
	ErrNoExecutionID  = errors.New("diag: empty execution id")
)

// Config configures a Profiler.
type Config struct {
	ExecutionID string
	Collectors  []Collector
	Clock       func() time.Time
}

// Profiler coordinates a single diagnostic session.
type Profiler struct {
	cfg Config

	mu        sync.Mutex
	snapMu    sync.Mutex // serializes concurrent Snapshot captures
	state     State
	startedAt time.Time
	endedAt   time.Time
	phases    []Phase
}

// New builds a Profiler, filling default collectors and clock.
func New(cfg Config) (*Profiler, error) {
	if cfg.ExecutionID == "" {
		return nil, ErrNoExecutionID
	}
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	if cfg.Collectors == nil {
		cfg.Collectors = DefaultCollectors()
	}
	return &Profiler{cfg: cfg, state: StateIdle}, nil
}

// Start transitions Idle -> Running. Repeated/early starts fail stably.
func (p *Profiler) Start(ctx context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	switch p.state {
	case StateRunning:
		return ErrAlreadyRunning
	case StateFinished:
		return ErrAlreadyEnded
	case StateFailed:
		return ErrAlreadyEnded
	}
	p.state = StateRunning
	p.startedAt = p.cfg.Clock()
	return nil
}

// MarkPhase records a call phase. Safe concurrently. Phases recorded outside
// Running are rejected (no silent accumulation onto a stale session).
func (p *Profiler) MarkPhase(name string, start time.Time, blocking bool, err error) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.state != StateRunning {
		return ErrNotRunning
	}
	ph := Phase{Name: name, Start: start, End: p.cfg.Clock(), Blocking: blocking}
	if !start.IsZero() {
		ph.Duration = ph.End.Sub(start)
	}
	if err != nil {
		ph.Err = err.Error()
	}
	p.phases = append(p.phases, ph)
	return nil
}

// Snapshot triggers an on-demand capture. Concurrent calls are serialized;
// collector failures are reported per-collector, never fatal to the snapshot.
func (p *Profiler) Snapshot(ctx context.Context) (Snapshot, error) {
	p.snapMu.Lock()
	defer p.snapMu.Unlock()
	p.mu.Lock()
	state := p.state
	startedAt := p.startedAt
	phases := make([]Phase, len(p.phases))
	copy(phases, p.phases)
	collectors := make([]Collector, len(p.cfg.Collectors))
	copy(collectors, p.cfg.Collectors)
	p.mu.Unlock()

	switch state {
	case StateIdle:
		return Snapshot{}, ErrNotRunning
	case StateFinished, StateFailed:
		return Snapshot{}, ErrAlreadyEnded
	}

	snap := Snapshot{
		ExecutionID: p.cfg.ExecutionID,
		TakenAt:     p.cfg.Clock(),
		StartedAt:   startedAt,
		Phases:      phases,
		Collectors:  make([]string, 0, len(collectors)),
		Failures:    nil,
	}
	for _, c := range collectors {
		if c == nil {
			continue
		}
		name := c.Name()
		snap.Collectors = append(snap.Collectors, name)
		prof, err := c.Collect(ctx)
		if err != nil {
			snap.Failures = append(snap.Failures, name+": "+err.Error())
			prof.Kind = name
			prof.Err = err.Error()
		}
		switch name {
		case "heap":
			snap.Heap = prof
		case "cpu":
			snap.CPU = prof
		case "block":
			snap.Block = prof
		case "goroutine":
			snap.Goroutines = int(prof.Value)
		case "memstats":
			snap.HeapAlloc = prof.HeapAlloc
			snap.HeapObjects = prof.HeapObjects
		}
	}
	return snap, nil
}

// End transitions Running -> Finished and returns a final snapshot.
// It is exactly-once: repeats return ErrAlreadyEnded and no snapshot.
func (p *Profiler) End(ctx context.Context) (Snapshot, error) {
	p.mu.Lock()
	state := p.state
	if state == StateIdle {
		p.mu.Unlock()
		return Snapshot{}, ErrNotRunning
	}
	if state == StateFinished || state == StateFailed {
		p.mu.Unlock()
		return Snapshot{}, ErrAlreadyEnded
	}
	p.state = StateFinished
	p.endedAt = p.cfg.Clock()
	p.mu.Unlock()

	return p.finalSnapshot()
}

// finalSnapshot gathers without the Running guard, used once during End.
func (p *Profiler) finalSnapshot() (Snapshot, error) {
	p.snapMu.Lock()
	defer p.snapMu.Unlock()
	p.mu.Lock()
	startedAt := p.startedAt
	phases := make([]Phase, len(p.phases))
	copy(phases, p.phases)
	collectors := make([]Collector, len(p.cfg.Collectors))
	copy(collectors, p.cfg.Collectors)
	p.mu.Unlock()

	snap := Snapshot{
		ExecutionID: p.cfg.ExecutionID,
		TakenAt:     p.cfg.Clock(),
		StartedAt:   startedAt,
		Phases:      phases,
		Collectors:  make([]string, 0, len(collectors)),
	}
	ctx := context.Background()
	for _, c := range collectors {
		if c == nil {
			continue
		}
		name := c.Name()
		snap.Collectors = append(snap.Collectors, name)
		prof, err := c.Collect(ctx)
		if err != nil {
			snap.Failures = append(snap.Failures, name+": "+err.Error())
			prof.Kind = name
			prof.Err = err.Error()
		}
		switch name {
		case "heap":
			snap.Heap = prof
		case "cpu":
			snap.CPU = prof
		case "block":
			snap.Block = prof
		case "goroutine":
			snap.Goroutines = int(prof.Value)
		case "memstats":
			snap.HeapAlloc = prof.HeapAlloc
			snap.HeapObjects = prof.HeapObjects
		}
	}
	return snap, nil
}

// State returns the current state.
func (p *Profiler) State() State {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.state
}

// EndedAt returns the end time, zero if not ended.
func (p *Profiler) EndedAt() time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.endedAt
}
