package trace

// SpanContext is the serializable correlation portion of a span.
type SpanContext struct {
	ExecutionID ID
	SpanID      SpanID
	TraceID     ID
	Valid       bool
}

// Span is one unit of work in an execution.
type Span struct {
	sc *spanState
}

// Execution is one correlated run: a root and its full causal tree.
type Execution struct {
	state *executionState
}
