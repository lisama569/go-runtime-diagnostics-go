// Package logx provides structured, execution-correlated logging with a
// bounded, loss-reporting in-memory aggregation buffer.
//
// Every accepted Entry carries the trace execution ID (and current span ID).
// Writes without a live execution are NOT silently filed under some other
// execution: they are rejected with a distinguishable DropReason. When total
// or per-execution capacity is exhausted, valid stored records are never
// overwritten; the new write is rejected with DropBufferFull and counted in
// the execution's Report so the degradation is visible.
package logx

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/lisama569/go-runtime-diagnostics-go/trace"
)

// DropReason distinguishes degradation outcomes when capacity is exhausted.
type DropReason int

const (
	DropNone DropReason = iota
	DropBufferFull
	DropNoExecution
	DropExecutionFinished
	DropLoggerClosed
)

func (r DropReason) String() string {
	switch r {
	case DropNone:
		return "none"
	case DropBufferFull:
		return "buffer_full"
	case DropNoExecution:
		return "no_execution"
	case DropExecutionFinished:
		return "execution_finished"
	case DropLoggerClosed:
		return "logger_closed"
	default:
		return "unknown"
	}
}

// Level is a log severity.
type Level int

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
)

func (l Level) String() string {
	switch l {
	case LevelDebug:
		return "DEBUG"
	case LevelInfo:
		return "INFO"
	case LevelWarn:
		return "WARN"
	case LevelError:
		return "ERROR"
	default:
		return "?"
	}
}

// Field is a structured key/value pair.
type Field struct {
	Key   string
	Value any
}

// F is shorthand for a structured field.
func F(key string, value any) Field { return Field{Key: key, Value: value} }

// Entry is one structured log record.
type Entry struct {
	Time        time.Time
	ExecutionID string
	SpanID      string
	Level       Level
	Message     string
	Fields      []Field
}

// Result reports what happened to a write attempt.
type Result struct {
	Accepted    bool
	Dropped     bool
	DropReason  DropReason
	ExecutionID string
}

// Report summarizes buffer behavior per execution.
type Report struct {
	ExecutionID string
	Accepted    int
	Dropped     int
	Reasons     map[DropReason]int
}

// Config configures a Logger.
type Config struct {
	// Capacity is the total number of entries retained. 0 means unlimited.
	Capacity int
	// PerExecCap caps entries retained per execution. 0 means unlimited.
	PerExecCap int
	Clock      func() time.Time
	// MinLevel rejects lower levels (counted neither accepted nor dropped).
	MinLevel Level
}

// Logger is a bounded concurrent-safe structured logger.
type Logger struct {
	cfg     Config
	mu      sync.Mutex
	closed  bool
	entries []Entry // append-only; indices are never overwritten
	counts  map[string]*counters
	perExec map[string]int
}

type counters struct {
	accepted int
	dropped  int
	reasons  map[DropReason]int
}

// New builds a Logger.
func New(cfg Config) *Logger {
	if cfg.Clock == nil {
		cfg.Clock = time.Now
	}
	return &Logger{
		cfg:     cfg,
		counts:  make(map[string]*counters),
		perExec: make(map[string]int),
	}
}

// boundLogger carries extra fields attached via With.
type boundLogger struct {
	*Logger
	extra []Field
}

// With returns a logger that prepends fields to every subsequent entry.
func (l *Logger) With(fields ...Field) *boundLogger {
	cp := make([]Field, len(fields))
	copy(cp, fields)
	return &boundLogger{Logger: l, extra: cp}
}

func (l *boundLogger) With(fields ...Field) *boundLogger {
	merged := make([]Field, 0, len(l.extra)+len(fields))
	merged = append(merged, l.extra...)
	merged = append(merged, fields...)
	return &boundLogger{Logger: l.Logger, extra: merged}
}

func correlate(ctx context.Context) (execID, spanID string, reason DropReason, ok bool) {
	if ctx == nil {
		return "", "", DropNoExecution, false
	}
	sc, err := trace.CurrentSpanContext(ctx)
	if err != nil {
		switch {
		case errors.Is(err, trace.ErrExecutionFinished):
			return "", "", DropExecutionFinished, false
		default:
			return "", "", DropNoExecution, false
		}
	}
	return sc.ExecutionID.String(), sc.SpanID.String(), DropNone, true
}

func (l *Logger) write(ctx context.Context, extra []Field, level Level, msg string, fields []Field) Result {
	execID, spanID, reason, ok := correlate(ctx)
	if !ok {
		l.countDrop(execID, reason)
		return Result{Dropped: true, DropReason: reason, ExecutionID: execID}
	}
	if level < l.cfg.MinLevel {
		// Filtered by configuration: not a capacity degradation.
		return Result{Accepted: false, DropReason: DropNone, ExecutionID: execID}
	}
	all := make([]Field, 0, len(extra)+len(fields))
	all = append(all, extra...)
	all = append(all, fields...)

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		l.countDropLocked(execID, DropLoggerClosed)
		return Result{Dropped: true, DropReason: DropLoggerClosed, ExecutionID: execID}
	}
	if l.cfg.Capacity > 0 && len(l.entries) >= l.cfg.Capacity {
		l.countDropLocked(execID, DropBufferFull)
		return Result{Dropped: true, DropReason: DropBufferFull, ExecutionID: execID}
	}
	if l.cfg.PerExecCap > 0 && l.perExec[execID] >= l.cfg.PerExecCap {
		l.countDropLocked(execID, DropBufferFull)
		return Result{Dropped: true, DropReason: DropBufferFull, ExecutionID: execID}
	}
	l.entries = append(l.entries, Entry{
		Time:        l.cfg.Clock(),
		ExecutionID: execID,
		SpanID:      spanID,
		Level:       level,
		Message:     msg,
		Fields:      all,
	})
	l.perExec[execID]++
	c := l.getCountersLocked(execID)
	c.accepted++
	return Result{Accepted: true, DropReason: DropNone, ExecutionID: execID}
}

// Log writes an entry correlated to the execution in ctx.
func (l *Logger) Log(ctx context.Context, level Level, msg string, fields ...Field) Result {
	return l.write(ctx, nil, level, msg, fields)
}

func (l *boundLogger) Log(ctx context.Context, level Level, msg string, fields ...Field) Result {
	return l.Logger.write(ctx, l.extra, level, msg, fields)
}

// Debug writes at debug level.
func (l *Logger) Debug(ctx context.Context, msg string, fields ...Field) Result {
	return l.write(ctx, nil, LevelDebug, msg, fields)
}

// Info writes at info level.
func (l *Logger) Info(ctx context.Context, msg string, fields ...Field) Result {
	return l.write(ctx, nil, LevelInfo, msg, fields)
}

// Warn writes at warn level.
func (l *Logger) Warn(ctx context.Context, msg string, fields ...Field) Result {
	return l.write(ctx, nil, LevelWarn, msg, fields)
}

// Error writes at error level.
func (l *Logger) Error(ctx context.Context, msg string, fields ...Field) Result {
	return l.write(ctx, nil, LevelError, msg, fields)
}

func (l *boundLogger) Info(ctx context.Context, msg string, fields ...Field) Result {
	return l.Logger.write(ctx, l.extra, LevelInfo, msg, fields)
}
func (l *boundLogger) Error(ctx context.Context, msg string, fields ...Field) Result {
	return l.Logger.write(ctx, l.extra, LevelError, msg, fields)
}

func (l *Logger) countDrop(execID string, reason DropReason) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.countDropLocked(execID, reason)
}

func (l *Logger) countDropLocked(execID string, reason DropReason) {
	c := l.getCountersLocked(execID)
	c.dropped++
	c.reasons[reason]++
}

func (l *Logger) getCountersLocked(execID string) *counters {
	c, ok := l.counts[execID]
	if !ok {
		c = &counters{reasons: make(map[DropReason]int)}
		l.counts[execID] = c
	}
	return c
}

// Snapshot returns a defensive copy of stored entries in insertion order.
func (l *Logger) Snapshot() []Entry {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]Entry, len(l.entries))
	for i, e := range l.entries {
		fe := make([]Field, len(e.Fields))
		copy(fe, e.Fields)
		e.Fields = fe
		out[i] = e
	}
	return out
}

// DrainByExecution returns and removes all entries of one execution atomically.
// Removing does not shift or overwrite any other entry's content.
func (l *Logger) DrainByExecution(executionID string) []Entry {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []Entry
	kept := l.entries[:0]
	for _, e := range l.entries {
		if e.ExecutionID == executionID {
			fe := make([]Field, len(e.Fields))
			copy(fe, e.Fields)
			e.Fields = fe
			out = append(out, e)
		} else {
			kept = append(kept, e)
		}
	}
	// Clear tail references so drained entries can be GC'd.
	for i := len(kept); i < len(l.entries); i++ {
		l.entries[i] = Entry{}
	}
	l.entries = kept
	return out
}

// Report returns accept/drop counters for one execution.
func (l *Logger) Report(executionID string) (Report, error) {
	if executionID == "" {
		return Report{}, errors.New("logx: empty execution id")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	c, ok := l.counts[executionID]
	if !ok {
		return Report{ExecutionID: executionID, Reasons: map[DropReason]int{}}, nil
	}
	r := Report{
		ExecutionID: executionID,
		Accepted:    c.accepted,
		Dropped:     c.dropped,
		Reasons:     make(map[DropReason]int, len(c.reasons)),
	}
	for k, v := range c.reasons {
		r.Reasons[k] = v
	}
	return r, nil
}

// Close freezes the logger; later writes are rejected with DropLoggerClosed.
func (l *Logger) Close() error {
	l.mu.Lock()
	l.closed = true
	l.mu.Unlock()
	return nil
}
