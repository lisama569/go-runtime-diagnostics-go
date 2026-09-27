package trace

// ID is a per-execution identifier. The zero value is invalid.
type ID [16]byte

// SpanID identifies a single span within an execution.
type SpanID [8]byte
