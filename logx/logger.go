// Package logx 提供按执行标识汇总的结构化日志。
//
// 并发写入安全;重复上报(同执行/同跨度/同级/同消息)会被去重并单独计数;
// 容量打满时返回 reason=capacity 的可区分降级结果,且不影响业务返回值。
// 缺失执行标识的写入会被明确拒绝(reason=rejected),不会静默落到错误执行下。
package logx

import (
	"context"
	"sync"
	"time"

	"github.com/lisama569/go-runtime-diagnostics-go/sink"
	"github.com/lisama569/go-runtime-diagnostics-go/trace"
)

// Level 是日志级别。
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
		return "debug"
	case LevelInfo:
		return "info"
	case LevelWarn:
		return "warn"
	case LevelError:
		return "error"
	default:
		return "unknown"
	}
}

// Entry 是一条结构化日志。
type Entry struct {
	Time      time.Time
	Execution trace.ExecutionID
	Span      trace.SpanID
	Level     Level
	Message   string
	Attrs     map[string]string
}

// dedupKey 定义"重复上报"的等价关系。属性不参与,
// 因为同一逻辑点的重复日志即使附带动态值也应被识别为重复。
func dedupKey(e Entry) string {
	return string(e.Execution) + "|" + string(e.Span) + "|" +
		itoa(int(e.Level)) + "|" + e.Message
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// Logger 按执行标识汇聚结构化日志。
type Logger struct {
	mu      sync.Mutex
	sink    *sink.Sink
	now     func() time.Time
	dedup   bool
	byExec  map[trace.ExecutionID][]int // 执行 -> 全局下标
	nextIdx int
}

// Option 配置 Logger。
type Option func(*options)

type options struct {
	capacity int
	dedup    bool
	now      func() time.Time
}

// WithCapacity 设置每个 Logger 的日志容量上限。
func WithCapacity(n int) Option {
	return func(o *options) { o.capacity = n }
}

// WithDedup 开启重复上报去重(默认开启)。
func WithDedup(b bool) Option {
	return func(o *options) { o.dedup = b }
}

// WithClock 注入时钟(主要用于可重复的测试)。
func WithClock(fn func() time.Time) Option {
	return func(o *options) { o.now = fn }
}

// New 创建 Logger。
func New(opts ...Option) *Logger {
	cfg := options{capacity: sink.DefaultCapacity, dedup: true, now: time.Now}
	for _, o := range opts {
		o(&cfg)
	}
	return &Logger{
		sink:   sink.New(cfg.capacity, cfg.dedup),
		now:    cfg.now,
		dedup:  cfg.dedup,
		byExec: map[trace.ExecutionID][]int{},
	}
}

// Log 写入一条日志。无法解析执行标识的 context 会被明确拒绝。
func (l *Logger) Log(ctx context.Context, level Level, message string, attrs map[string]string) sink.Result {
	sc, ok := trace.FromContext(ctx)
	if !ok || !sc.Valid() {
		return sink.Result{Reason: sink.ReasonRejected}
	}
	return l.LogExecution(sc.Execution, sc.Span, level, message, attrs)
}

// LogExecution 按显式执行标识写入日志(用于队列消费侧没有 context 的场景)。
func (l *Logger) LogExecution(execution trace.ExecutionID, span trace.SpanID, level Level, message string, attrs map[string]string) sink.Result {
	if execution == "" {
		return sink.Result{Reason: sink.ReasonRejected}
	}
	entry := Entry{
		Time:      l.now(),
		Execution: execution,
		Span:      span,
		Level:     level,
		Message:   message,
		Attrs:     cloneAttrs(attrs),
	}

	l.mu.Lock()
	key := ""
	if l.dedup {
		key = dedupKey(entry)
	}
	// 先在 sink 判定去重/容量;接受后才更新索引,避免丢弃记录污染索引。
	res := l.sink.Add(key, entry)
	if res.Accepted {
		idx := l.nextIdx
		l.nextIdx++
		l.byExec[execution] = append(l.byExec[execution], idx)
	}
	l.mu.Unlock()
	return res
}

func cloneAttrs(in map[string]string) map[string]string {
	if len(in) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func cloneEntry(e Entry) Entry {
	e.Attrs = cloneAttrs(e.Attrs)
	return e
}

// Entries 返回某次执行的全部日志副本(按写入顺序)。
func (l *Logger) Entries(execution trace.ExecutionID) []Entry {
	l.mu.Lock()
	idxs := l.byExec[execution]
	records := l.sink.Records()
	out := make([]Entry, 0, len(idxs))
	for _, idx := range idxs {
		if idx >= 0 && idx < len(records) {
			if e, ok := records[idx].(Entry); ok {
				out = append(out, cloneEntry(e))
			}
		}
	}
	l.mu.Unlock()
	return out
}

// All 返回全部日志副本(按全局接受顺序)。
func (l *Logger) All() []Entry {
	records := l.sink.Records()
	out := make([]Entry, 0, len(records))
	for _, r := range records {
		if e, ok := r.(Entry); ok {
			out = append(out, cloneEntry(e))
		}
	}
	return out
}

// Dropped 返回容量丢弃总数。
func (l *Logger) Dropped() int64 { return l.sink.Dropped() }

// Duplicates 返回去重拒绝总数。
func (l *Logger) Duplicates() int64 { return l.sink.Duplicates() }

// Reset 清空状态,主要用于测试。
func (l *Logger) Reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.sink.Reset()
	l.byExec = map[trace.ExecutionID][]int{}
	l.nextIdx = 0
}
