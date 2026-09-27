// Package metric provides execution-correlated counters, gauges and events
// with idempotent reporting and bounded, loss-distinguishable storage.
//
// Duplicate reports (same name + same dedup key) are counted once and reported
// as OutcomeDuplicate: they never inflate counters or overwrite valid records.
// Gauges are last-writer-wins (that is their semantic) but repeated equal
// writes are observable. When distinct record capacity is exhausted, writes are
// rejected with OutcomeDroppedCapacity; existing values are never overwritten
// and the business result of the caller is unaffected.
package metric

import (
	"context"
	"errors"
	"sync"

	"github.com/lisama569/go-runtime-diagnostics-go/trace"
)

// Outcome classifies a metric write attempt.
type Outcome int

const (
	OutcomeRecorded Outcome = iota
	OutcomeDuplicate
	OutcomeDroppedCapacity
	OutcomeNoExecution
	OutcomeExecutionFinished
	OutcomeMeterClosed
)

func (o Outcome) String() string {
	switch o {
	case OutcomeRecorded:
		return "recorded"
	case OutcomeDuplicate:
		return "duplicate"
	case OutcomeDroppedCapacity:
		return "dropped_capacity"
	case OutcomeNoExecution:
		return "no_execution"
	case OutcomeExecutionFinished:
		return "execution_finished"
	case OutcomeMeterClosed:
		return "meter_closed"
	default:
		return "unknown"
	}
}

// DropReason is an alias kept for readability in reports.
type DropReason = Outcome

// Kind is the metric kind.
type Kind int

const (
	KindCounter Kind = iota
	KindGauge
	KindEvent
)

// Record is one stored metric observation.
type Record struct {
	ExecutionID string
	Name        string
	Kind        Kind
	Value       int64
	DedupKey    string
}

// Result reports the outcome of one write.
type Result struct {
	Outcome     Outcome
	DropReason  DropReason
	AlreadySeen bool
}

// Report summarizes a metric stream per execution.
type Report struct {
	ExecutionID string
	// Values is name -> aggregated value (counter sum / gauge last / event count).
	Values     map[string]int64
	Recorded   int
	Duplicates int
	Dropped    int
	Reasons    map[DropReason]int
}

// Config configures a Meter.
type Config struct {
	// Capacity bounds the number of distinct (execution,name,dedup) records.
	// 0 means unlimited.
	Capacity int
}

// Meter is a concurrent-safe, deduping, bounded metric sink.
type Meter struct {
	cfg Config

	mu     sync.Mutex
	closed bool
	// records retain insertion order for deterministic snapshots.
	records []*slot
	index   map[string]*slot // key: exec|name|dedup
	counts  map[string]*execCounts
}

type slot struct {
	rec     Record
	updates int // 1 for events/counters first write; >1 means repeated
}

type execCounts struct {
	recorded   int
	duplicates int
	dropped    int
	reasons    map[DropReason]int
}

// New builds a Meter.
func New(cfg Config) *Meter {
	return &Meter{
		cfg:    cfg,
		index:  make(map[string]*slot),
		counts: make(map[string]*execCounts),
	}
}

func recordKey(execID, name, dedup string) string {
	return execID + "\x00" + name + "\x00" + dedup
}

func execIDFromContext(ctx context.Context) (string, Outcome, bool) {
	if ctx == nil {
		return "", OutcomeNoExecution, false
	}
	sc, err := trace.CurrentSpanContext(ctx)
	if err != nil {
		if errors.Is(err, trace.ErrExecutionFinished) {
			return "", OutcomeExecutionFinished, false
		}
		return "", OutcomeNoExecution, false
	}
	return sc.ExecutionID.String(), OutcomeRecorded, true
}

func (m *Meter) fail(execID string, o Outcome) Result {
	m.mu.Lock()
	c := m.getCountsLocked(execID)
	c.dropped++
	c.reasons[o]++
	m.mu.Unlock()
	return Result{Outcome: o, DropReason: o}
}

// Add increments a counter. A non-empty dedupKey makes the write idempotent:
// a repeated (name, dedupKey) is reported OutcomeDuplicate and changes nothing.
// An empty dedupKey allows repeated accumulation (each call is a new record).
func (m *Meter) Add(ctx context.Context, name string, delta int64, dedupKey string) Result {
	execID, outcome, ok := execIDFromContext(ctx)
	if !ok {
		return m.fail(execID, outcome)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return m.lockedFail(execID, OutcomeMeterClosed)
	}
	key := recordKey(execID, name, dedupKey)
	if dedupKey != "" {
		if s, found := m.index[key]; found {
			s.updates++
			c := m.getCountsLocked(execID)
			c.duplicates++
			c.reasons[OutcomeDuplicate]++
			return Result{Outcome: OutcomeDuplicate, AlreadySeen: true}
		}
	}
	if !m.hasCapacityLocked() {
		c := m.getCountsLocked(execID)
		c.dropped++
		c.reasons[OutcomeDroppedCapacity]++
		return Result{Outcome: OutcomeDroppedCapacity, DropReason: OutcomeDroppedCapacity}
	}
	s := &slot{rec: Record{
		ExecutionID: execID, Name: name, Kind: KindCounter,
		Value: delta, DedupKey: dedupKey,
	}, updates: 1}
	m.records = append(m.records, s)
	if dedupKey != "" {
		m.index[key] = s
	}
	m.getCountsLocked(execID).recorded++
	return Result{Outcome: OutcomeRecorded}
}

// SetGauge writes a gauge. Gauges are last-writer-wins within an execution;
// the write is always recorded as one gauge observation.
func (m *Meter) SetGauge(ctx context.Context, name string, value int64) Result {
	execID, outcome, ok := execIDFromContext(ctx)
	if !ok {
		return m.fail(execID, outcome)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return m.lockedFail(execID, OutcomeMeterClosed)
	}
	key := recordKey(execID, name, "__gauge__")
	if s, found := m.index[key]; found {
		s.rec.Value = value
		s.updates++
		return Result{Outcome: OutcomeRecorded}
	}
	if !m.hasCapacityLocked() {
		c := m.getCountsLocked(execID)
		c.dropped++
		c.reasons[OutcomeDroppedCapacity]++
		return Result{Outcome: OutcomeDroppedCapacity, DropReason: OutcomeDroppedCapacity}
	}
	s := &slot{rec: Record{
		ExecutionID: execID, Name: name, Kind: KindGauge, Value: value,
	}, updates: 1}
	m.records = append(m.records, s)
	m.index[key] = s
	m.getCountsLocked(execID).recorded++
	return Result{Outcome: OutcomeRecorded}
}

// RecordEvent records a one-shot event, deduplicated by dedupKey.
// Repeated calls with the same key are OutcomeDuplicate and count once.
func (m *Meter) RecordEvent(ctx context.Context, name, dedupKey string) Result {
	if dedupKey == "" {
		return m.fail("", OutcomeNoExecution) // events require a dedup key
	}
	execID, outcome, ok := execIDFromContext(ctx)
	if !ok {
		return m.fail(execID, outcome)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return m.lockedFail(execID, OutcomeMeterClosed)
	}
	key := recordKey(execID, name, dedupKey)
	if s, found := m.index[key]; found {
		s.updates++
		c := m.getCountsLocked(execID)
		c.duplicates++
		c.reasons[OutcomeDuplicate]++
		return Result{Outcome: OutcomeDuplicate, AlreadySeen: true}
	}
	if !m.hasCapacityLocked() {
		c := m.getCountsLocked(execID)
		c.dropped++
		c.reasons[OutcomeDroppedCapacity]++
		return Result{Outcome: OutcomeDroppedCapacity, DropReason: OutcomeDroppedCapacity}
	}
	s := &slot{rec: Record{
		ExecutionID: execID, Name: name, Kind: KindEvent,
		Value: 1, DedupKey: dedupKey,
	}, updates: 1}
	m.records = append(m.records, s)
	m.index[key] = s
	m.getCountsLocked(execID).recorded++
	return Result{Outcome: OutcomeRecorded}
}

func (m *Meter) hasCapacityLocked() bool {
	return m.cfg.Capacity <= 0 || len(m.records) < m.cfg.Capacity
}

func (m *Meter) lockedFail(execID string, o Outcome) Result {
	c := m.getCountsLocked(execID)
	c.dropped++
	c.reasons[o]++
	return Result{Outcome: o, DropReason: o}
}

func (m *Meter) getCountsLocked(execID string) *execCounts {
	c, ok := m.counts[execID]
	if !ok {
		c = &execCounts{reasons: make(map[DropReason]int)}
		m.counts[execID] = c
	}
	return c
}

// Snapshot returns a defensive, insertion-ordered copy of all records.
func (m *Meter) Snapshot() []Record {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Record, len(m.records))
	for i, s := range m.records {
		out[i] = s.rec
	}
	return out
}

// Report aggregates one execution deterministically (sorted by first-seen
// order is preserved via Snapshot order; values keyed by name).
func (m *Meter) Report(executionID string) (Report, error) {
	if executionID == "" {
		return Report{}, errors.New("metric: empty execution id")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	r := Report{
		ExecutionID: executionID,
		Values:      make(map[string]int64),
		Reasons:     map[DropReason]int{},
	}
	for _, s := range m.records {
		if s.rec.ExecutionID != executionID {
			continue
		}
		switch s.rec.Kind {
		case KindCounter:
			r.Values[s.rec.Name] += s.rec.Value
		case KindGauge:
			r.Values[s.rec.Name] = s.rec.Value
		case KindEvent:
			r.Values[s.rec.Name]++
		}
	}
	if c, ok := m.counts[executionID]; ok {
		r.Recorded = c.recorded
		r.Duplicates = c.duplicates
		r.Dropped = c.dropped
		for k, v := range c.reasons {
			r.Reasons[k] = v
		}
	}
	return r, nil
}

// Close freezes the meter; later writes are OutcomeMeterClosed.
func (m *Meter) Close() error {
	m.mu.Lock()
	m.closed = true
	m.mu.Unlock()
	return nil
}
