package trace

import (
	"context"
	"time"
)

// ErrTraceCapacity is returned when an execution's span limit is reached.
// It is a distinct degradation: the execution result is unaffected, but the
// span is not recorded.
type capacityError struct{}

func (capacityError) Error() string { return "trace: execution span capacity reached" }

// ErrTraceCapacity is the exported sentinel.
var ErrTraceCapacity error = capacityError{}

// Tracer creates executions and spans.
type Tracer struct {
	cfg Config
	reg *registry
}

// NewTracer builds a Tracer, filling defaults for unset fields.
func NewTracer(cfg Config) *Tracer {
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	if cfg.NewID == nil {
		cfg.NewID = randomID
	}
	if cfg.NewSpanID == nil {
		cfg.NewSpanID = randomSpanID
	}
	if cfg.MaxSpans < 0 {
		cfg.MaxSpans = 0
	}
	return &Tracer{cfg: cfg, reg: newRegistry()}
}

var defaultTracer = NewTracer(Config{})

// DefaultTracer returns the package-level tracer used by free functions.
func DefaultTracer() *Tracer { return defaultTracer }

// SetDefaultTracer replaces the package-level tracer (primarily for tests).
func SetDefaultTracer(t *Tracer) {
	if t != nil {
		defaultTracer = t
	}
}

// StartExecution starts a new root execution bound to t.
func (t *Tracer) StartExecution(ctx context.Context, name string) (context.Context, *Execution) {
	if ctx == nil {
		ctx = context.Background()
	}
	now := t.cfg.Clock()
	es := &executionState{
		id:       t.cfg.NewID(),
		traceID:  t.cfg.NewID(),
		rootID:   t.cfg.NewSpanID(),
		name:     name,
		start:    now,
		spanByID: make(map[SpanID]*spanState),
		maxSpans: t.cfg.MaxSpans,
		clock:    t.cfg.Clock,
	}
	root := &spanState{
		id: es.rootID, traceID: es.traceID, execID: es.id,
		kind: SpanInternal, name: name, start: now,
	}
	es.spans = append(es.spans, root)
	es.spanByID[root.id] = root
	es.spanN = 1
	t.reg.put(es)
	e := &Execution{state: es}
	return withCarrier(ctx, &carrier{tracer: t, exec: es, span: root}), e
}

// StartExecution starts a root execution on the default tracer.
func StartExecution(ctx context.Context, name string) (context.Context, *Execution) {
	return defaultTracer.StartExecution(ctx, name)
}

// FromContext returns the execution carried by ctx.
func FromContext(ctx context.Context) (*Execution, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	c, err := carrierFromContext(ctx)
	if err != nil {
		return nil, err
	}
	c.exec.mu.RLock()
	finished := c.exec.finished
	c.exec.mu.RUnlock()
	if finished {
		return &Execution{state: c.exec}, ErrExecutionFinished
	}
	return &Execution{state: c.exec}, nil
}

// CurrentSpanContext returns the span context carried by ctx.
func CurrentSpanContext(ctx context.Context) (SpanContext, error) {
	c, err := carrierFromContext(ctx)
	if err != nil {
		return SpanContext{}, err
	}
	c.exec.mu.RLock()
	finished := c.exec.finished
	c.exec.mu.RUnlock()
	if finished {
		return SpanContext{}, ErrExecutionFinished
	}
	return SpanContext{
		ExecutionID: c.exec.id,
		SpanID:      c.span.id,
		TraceID:     c.exec.traceID,
		Valid:       true,
	}, nil
}

// StartSpan starts a synchronous child span on t.
func (t *Tracer) StartSpan(ctx context.Context, name string, kind SpanKind) (context.Context, *Span, error) {
	if ctx == nil {
		return ctx, nil, ErrNilContext
	}
	c, err := carrierFromContext(ctx)
	if err != nil {
		return ctx, nil, err
	}
	if c.tracer != t {
		// A context started under a different tracer must not silently attach.
		return ctx, nil, ErrForeignExecution
	}
	return startChild(ctx, c, name, kind)
}

// StartSpan starts a synchronous child span on the default tracer.
func StartSpan(ctx context.Context, name string, kind SpanKind) (context.Context, *Span, error) {
	return defaultTracer.StartSpan(ctx, name, kind)
}

func startChild(ctx context.Context, c *carrier, name string, kind SpanKind) (context.Context, *Span, error) {
	es := c.exec
	es.mu.Lock()
	if es.finished {
		es.mu.Unlock()
		return ctx, nil, ErrExecutionFinished
	}
	c.span.mu.RLock()
	parentFinished := c.span.finished
	parentKind := c.span.kind
	c.span.mu.RUnlock()
	// A QueueConsume is allowed to attach to the immediately-closed
	// QueueProduce marker; all other children require a live parent.
	if parentFinished && !(kind == SpanQueueConsume && parentKind == SpanQueueProduce) {
		es.mu.Unlock()
		return ctx, nil, ErrSpanFinished
	}
	if es.maxSpans > 0 && es.spanN >= es.maxSpans {
		es.mu.Unlock()
		return ctx, nil, ErrTraceCapacity
	}
	sp := &spanState{
		id:       c.tracer.cfg.NewSpanID(),
		parentID: c.span.id,
		traceID:  es.traceID,
		execID:   es.id,
		kind:     kind,
		name:     name,
		start:    es.clock(),
	}
	es.spans = append(es.spans, sp)
	es.spanByID[sp.id] = sp
	es.spanN++
	es.mu.Unlock()
	child := &Span{sc: sp}
	nc := withCarrier(ctx, &carrier{tracer: c.tracer, exec: es, span: sp})
	return nc, child, nil
}

// View returns a read-only snapshot of the execution.
func (e *Execution) View() ExecutionView { return e.state.view() }

// ID returns the execution identifier.
func (e *Execution) ID() ID { return e.state.id }

// Finish closes the execution root and freezes its span tree.
// It is idempotent: repeated calls return ErrExecutionFinished.
func (e *Execution) Finish(err error) error {
	es := e.state
	es.mu.Lock()
	if es.finished {
		es.mu.Unlock()
		return ErrExecutionFinished
	}
	es.finished = true
	es.end = es.clock()
	es.err = err
	root := es.spanByID[es.rootID]
	es.mu.Unlock()
	root.mu.Lock()
	if !root.finished {
		root.finished = true
		root.end = es.end
		root.err = err
	}
	root.mu.Unlock()
	return nil
}

// End closes a span. Repeated calls return ErrSpanFinished and never mutate
// the recorded end time or error.
func (s *Span) End(err error) error {
	s.sc.mu.Lock()
	defer s.sc.mu.Unlock()
	if s.sc.finished {
		return ErrSpanFinished
	}
	s.sc.finished = true
	s.sc.end = time.Now()
	s.sc.err = err
	return nil
}

// View returns the span's read-only snapshot.
func (s *Span) View() SnapshotView { return s.sc.view() }

// Context returns the SpanContext of the span.
func (s *Span) Context() SpanContext {
	s.sc.mu.RLock()
	defer s.sc.mu.RUnlock()
	return SpanContext{
		ExecutionID: s.sc.execID,
		SpanID:      s.sc.id,
		TraceID:     s.sc.traceID,
		Valid:       true,
	}
}

// SpanContextOf extracts the serializable SpanContext from a context.
func SpanContextOf(ctx context.Context) (SpanContext, error) {
	c, err := carrierFromContext(ctx)
	if err != nil {
		return SpanContext{}, err
	}
	c.exec.mu.RLock()
	finished := c.exec.finished
	c.exec.mu.RUnlock()
	if finished {
		return SpanContext{}, ErrExecutionFinished
	}
	return SpanContext{
		ExecutionID: c.exec.id,
		SpanID:      c.span.id,
		TraceID:     c.exec.traceID,
		Valid:       true,
	}, nil
}
