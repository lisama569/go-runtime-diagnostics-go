package trace

import (
	"sync"
	"time"
)

// executionState is the internal mutable state of an Execution.
type executionState struct {
	mu       sync.RWMutex
	id       ID
	traceID  ID
	rootID   SpanID
	name     string
	start    time.Time
	end      time.Time
	finished bool
	err      error
	spans    []*spanState
	spanByID map[SpanID]*spanState
	spanN    int
	maxSpans int
	clock    func() time.Time
}

// spanState is the internal mutable state of a Span.
type spanState struct {
	mu       sync.RWMutex
	id       SpanID
	parentID SpanID
	traceID  ID
	execID   ID
	kind     SpanKind
	name     string
	start    time.Time
	end      time.Time
	finished bool
	err      error
}

// SnapshotView is a point-in-time read-only view of a span.
type SnapshotView struct {
	ID       SpanID
	ParentID SpanID
	TraceID  ID
	ExecID   ID
	Kind     SpanKind
	Name     string
	Start    time.Time
	End      time.Time
	Finished bool
	Err      error
}

func (s *spanState) traceIDsLocked() (ID, ID) { return s.traceID, s.execID }

func (s *spanState) view() SnapshotView {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return SnapshotView{
		ID: s.id, ParentID: s.parentID, TraceID: s.traceID, ExecID: s.execID,
		Kind: s.kind, Name: s.name,
		Start: s.start, End: s.end, Finished: s.finished, Err: s.err,
	}
}

// ExecutionView is a point-in-time read-only view of an execution.
type ExecutionView struct {
	ID       ID
	TraceID  ID
	RootID   SpanID
	Name     string
	Start    time.Time
	End      time.Time
	Finished bool
	Err      error
	Spans    []SnapshotView
}

func (e *executionState) view() ExecutionView {
	e.mu.RLock()
	spans := make([]*spanState, len(e.spans))
	copy(spans, e.spans)
	v := ExecutionView{
		ID: e.id, TraceID: e.traceID, RootID: e.rootID, Name: e.name,
		Start: e.start, End: e.end, Finished: e.finished, Err: e.err,
		Spans: make([]SnapshotView, 0, len(spans)),
	}
	e.mu.RUnlock()
	for _, sp := range spans {
		v.Spans = append(v.Spans, sp.view())
	}
	return v
}

// registry tracks live and finished executions so that serialized contexts
// can be resumed and foreign/corrupt contexts can be detected.
type registry struct {
	mu    sync.RWMutex
	execs map[ID]*executionState
}

func newRegistry() *registry {
	return &registry{execs: make(map[ID]*executionState)}
}

func (r *registry) put(es *executionState) {
	r.mu.Lock()
	r.execs[es.id] = es
	r.mu.Unlock()
}

func (r *registry) get(id ID) (*executionState, bool) {
	r.mu.RLock()
	es, ok := r.execs[id]
	r.mu.RUnlock()
	return es, ok
}
