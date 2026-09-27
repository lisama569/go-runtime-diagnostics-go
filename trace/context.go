package trace

import "context"

type ctxKey struct{}

// carrier is what travels inside a context.Context.
type carrier struct {
	tracer *Tracer
	exec   *executionState
	span   *spanState
}

func withCarrier(ctx context.Context, c *carrier) context.Context {
	return context.WithValue(ctx, ctxKey{}, c)
}

func carrierFromContext(ctx context.Context) (*carrier, error) {
	if ctx == nil {
		return nil, ErrNilContext
	}
	v := ctx.Value(ctxKey{})
	if v == nil {
		return nil, ErrNoExecution
	}
	c, ok := v.(*carrier)
	if !ok || c == nil || c.exec == nil || c.span == nil {
		// Corrupted/injected value: never silently attach it anywhere.
		return nil, ErrInvalidSpanContext
	}
	return c, nil
}

// RestoreContext resumes an execution from a serialized SpanContext on t.
// Missing, invalid, unknown (foreign/finished-in-another-process) or
// zero-valued contexts are rejected with a distinguishable error rather than
// silently starting a new chain. A finished parent span is rejected with
// ErrSpanFinished (a completed synchronous span cannot be extended); the
// single exception is a QueueProduce marker, which is by design the parent of
// a later QueueConsume.
func (t *Tracer) RestoreContext(ctx context.Context, sc SpanContext) (context.Context, *Span, error) {
	return t.restore(ctx, sc, false)
}

func (t *Tracer) restore(ctx context.Context, sc SpanContext, allowFinishedParent bool) (context.Context, *Span, error) {
	if ctx == nil {
		return ctx, nil, ErrNilContext
	}
	if !sc.Valid {
		return ctx, nil, ErrInvalidSpanContext
	}
	if sc.ExecutionID.IsZero() || sc.SpanID.IsZero() {
		return ctx, nil, ErrInvalidSpanContext
	}
	es, ok := t.reg.get(sc.ExecutionID)
	if !ok {
		// Unknown execution id: cross-execution inheritance is rejected, never
		// silently mounted under whatever execution ctx happens to carry.
		return ctx, nil, ErrForeignExecution
	}
	es.mu.RLock()
	finished := es.finished
	parent, found := es.spanByID[sc.SpanID]
	traceMatch := sc.TraceID.IsZero() || sc.TraceID == es.traceID
	es.mu.RUnlock()
	if finished {
		return ctx, nil, ErrExecutionFinished
	}
	if !found {
		return ctx, nil, ErrInvalidSpanContext
	}
	if !traceMatch {
		return ctx, nil, ErrForeignExecution
	}
	parent.mu.RLock()
	parentFinished := parent.finished
	parentKind := parent.kind
	parent.mu.RUnlock()
	if parentFinished && !(allowFinishedParent && parentKind == SpanQueueProduce) {
		return ctx, nil, ErrSpanFinished
	}
	c := &carrier{tracer: t, exec: es, span: parent}
	return withCarrier(ctx, c), &Span{sc: parent}, nil
}

// RestoreContext resumes on the default tracer.
func RestoreContext(ctx context.Context, sc SpanContext) (context.Context, *Span, error) {
	return defaultTracer.RestoreContext(ctx, sc)
}

// ForkAsync derives an asynchronous child span. The returned context is
// detached from ctx cancellation so the forked task outlives the request that
// spawned it, but retains full causal and execution correlation.
func (t *Tracer) ForkAsync(ctx context.Context, name string) (context.Context, *Span, error) {
	c, err := carrierFromContext(ctx)
	if err != nil {
		return ctx, nil, err
	}
	if c.tracer != t {
		return ctx, nil, ErrForeignExecution
	}
	// context.WithoutCancel keeps values (the carrier) but drops cancellation
	// and deadline, which is exactly the async-fork semantics.
	detached := context.WithoutCancel(ctx)
	return startChild(detached, c, name, SpanAsyncFork)
}

// ForkAsync forks on the default tracer.
func ForkAsync(ctx context.Context, name string) (context.Context, *Span, error) {
	return defaultTracer.ForkAsync(ctx, name)
}

// QueueHandoff packages the current span context for a queued task.
func QueueHandoff(ctx context.Context) (SpanContext, error) {
	c, err := carrierFromContext(ctx)
	if err != nil {
		return SpanContext{}, err
	}
	c.exec.mu.Lock()
	if c.exec.finished {
		c.exec.mu.Unlock()
		return SpanContext{}, ErrExecutionFinished
	}
	sp := &spanState{
		id:       c.tracer.cfg.NewSpanID(),
		parentID: c.span.id,
		traceID:  c.exec.traceID,
		execID:   c.exec.id,
		kind:     SpanQueueProduce,
		name:     "queue.produce",
		start:    c.exec.clock(),
	}
	c.exec.spans = append(c.exec.spans, sp)
	c.exec.spanByID[sp.id] = sp
	c.exec.spanN++
	c.exec.mu.Unlock()
	// The produce span is closed immediately: it represents the enqueue point.
	sp.mu.Lock()
	sp.finished = true
	sp.end = c.exec.clock()
	sp.mu.Unlock()
	return SpanContext{
		ExecutionID: c.exec.id,
		SpanID:      sp.id,
		TraceID:     c.exec.traceID,
		Valid:       true,
	}, nil
}

// QueueConsume resumes a queued task from a handoff token on t and starts a
// QueueConsume child span. The base ctx may be a fresh background context:
// correlation comes from the token and is fully validated.
func (t *Tracer) QueueConsume(ctx context.Context, sc SpanContext) (context.Context, *Span, error) {
	resumed, _, err := t.restore(ctx, sc, true)
	if err != nil {
		return ctx, nil, err
	}
	c, err := carrierFromContext(resumed)
	if err != nil {
		return ctx, nil, err
	}
	return startChild(resumed, c, "queue.consume", SpanQueueConsume)
}

// QueueConsume consumes on the default tracer.
func QueueConsume(ctx context.Context, sc SpanContext) (context.Context, *Span, error) {
	return defaultTracer.QueueConsume(ctx, sc)
}
