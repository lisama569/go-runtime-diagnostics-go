// Package trace provides per-execution causal tracing across synchronous
// calls, queued tasks and asynchronously forked work.
//
// Correlation rules:
//
//   - Every span belongs to exactly one Execution and carries its ID.
//   - A span can only be started from a context that carries a live execution;
//     missing, nil, corrupted or zero-valued contexts are rejected with a
//     distinguishable sentinel error (ErrNoExecution, ErrNilContext,
//     ErrInvalidSpanContext). Nothing is ever silently attached to a chain.
//   - Serialized SpanContext values are validated against the tracer's
//     registry: unknown execution IDs are treated as cross-execution
//     inheritance and rejected with ErrForeignExecution; finished executions
//     return ErrExecutionFinished.
//   - ForkAsync detaches cancellation but preserves the causal parent.
//   - QueueHandoff/QueueConsume correlate queued work via a validated token.
package trace

import (
	"errors"
	"time"
)

// Errors used to classify invalid context inheritance. They are never
// swallowed: callers can errors.Is against them to distinguish failure modes.
var (
	ErrNoExecution        = errors.New("trace: context carries no execution")
	ErrExecutionFinished  = errors.New("trace: execution already finished")
	ErrForeignExecution   = errors.New("trace: span belongs to a different execution")
	ErrSpanFinished       = errors.New("trace: span already finished")
	ErrNilContext         = errors.New("trace: nil context")
	ErrInvalidSpanContext = errors.New("trace: invalid or corrupted span context")
)

// SpanKind describes the causal role of a span.
type SpanKind int

const (
	SpanInternal SpanKind = iota
	SpanSync
	SpanQueueProduce
	SpanQueueConsume
	SpanAsyncFork
	SpanAsyncJoin
)

// Config configures a Tracer.
type Config struct {
	Clock     func() time.Time
	NewID     func() ID
	NewSpanID func() SpanID
	// MaxSpans caps spans recorded per execution; 0 means unlimited. When the
	// cap is reached StartSpan returns ErrTraceCapacity (degradation, business
	// result unchanged).
	MaxSpans int
}
