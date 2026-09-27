// Package metrics 提供按执行标识汇总的运行时业务指标。
//
// 通过 ReportID 实现幂等上报:同一 ReportID 重复上报不会重复计数。
// 每个执行的时间序列数量有界,超限时返回 reason=capacity 的降级结果。
// 指标类型不同,聚合语义不同:
//   - Counter: 值累加;
//   - Gauge:   保留最后一次值,同时记录 min/max;
//   - Timer:   按分布聚合(count/sum/min/max/last)。
package metrics

import (
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/lisama569/go-runtime-diagnostics-go/sink"
	"github.com/lisama569/go-runtime-diagnostics-go/trace"
)

// Kind 是指标类型。
type Kind int

const (
	KindCounter Kind = iota
	KindGauge
	KindTimer
)

func (k Kind) String() string {
	switch k {
	case KindCounter:
		return "counter"
	case KindGauge:
		return "gauge"
	case KindTimer:
		return "timer"
	default:
		return "unknown"
	}
}

// Sample 是一次指标上报。
type Sample struct {
	At        time.Time
	Execution trace.ExecutionID
	Name      string
	Kind      Kind
	Value     float64
	// Labels 参与时间序列划分。
	Labels map[string]string
	// ReportID 非空时用于幂等去重;为空则不去重。
	ReportID string
}

// SeriesView 是单个时间序列的聚合视图。
type SeriesView struct {
	Name   string
	Kind   Kind
	Labels map[string]string
	Count  int64
	Sum    float64
	Min    float64
	Max    float64
	Last   float64
}

type series struct {
	view  SeriesView
	first bool
}

// Registry 按执行标识汇聚指标。
type Registry struct {
	mu      sync.Mutex
	maxSrs  int
	seenID  map[string]struct{}
	data    map[trace.ExecutionID]map[string]*series
	order   map[trace.ExecutionID][]string
	dropped int64
	dups    int64
}

// Option 配置 Registry。
type Option func(*options)

type options struct {
	maxSeriesPerExecution int
}

// WithMaxSeriesPerExecution 限制每个执行的时间序列数量;默认 256。
func WithMaxSeriesPerExecution(n int) Option {
	return func(o *options) { o.maxSeriesPerExecution = n }
}

// New 创建 Registry。
func New(opts ...Option) *Registry {
	cfg := options{maxSeriesPerExecution: 256}
	for _, o := range opts {
		o(&cfg)
	}
	return &Registry{
		maxSrs: cfg.maxSeriesPerExecution,
		seenID: map[string]struct{}{},
		data:   map[trace.ExecutionID]map[string]*series{},
		order:  map[trace.ExecutionID][]string{},
	}
}

// seriesKey 由名称与稳定排序后的标签构成。
func seriesKey(name string, labels map[string]string) string {
	if len(labels) == 0 {
		return name
	}
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(name)
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(labels[k])
	}
	b.WriteByte('}')
	return b.String()
}

// Record 上报一个样本。
//
//   - Execution 为空:reason=rejected,不计数;
//   - ReportID 重复:reason=duplicate,只增加一次重复计数,不改变聚合;
//   - 新序列且已达每执行序列上限:reason=capacity 降级丢弃;
//   - 其余:按类型聚合并发安全。
func (r *Registry) Record(s Sample) sink.Result {
	if s.Execution == "" || s.Name == "" {
		return sink.Result{Reason: sink.ReasonRejected}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if s.ReportID != "" {
		if _, ok := r.seenID[s.ReportID]; ok {
			r.dups++
			return sink.Result{Duplicate: true, Reason: sink.ReasonDedup}
		}
	}

	key := seriesKey(s.Name, s.Labels)
	execMap := r.data[s.Execution]
	if execMap == nil {
		execMap = map[string]*series{}
		r.data[s.Execution] = execMap
	}
	sr, exists := execMap[key]
	if !exists {
		if len(execMap) >= r.maxSrs {
			r.dropped++
			return sink.Result{Dropped: true, Reason: sink.ReasonCapacity}
		}
		sr = &series{
			view: SeriesView{
				Name:   s.Name,
				Kind:   s.Kind,
				Labels: cloneLabels(s.Labels),
			},
			first: true,
		}
		execMap[key] = sr
		r.order[s.Execution] = append(r.order[s.Execution], key)
	}

	if s.Kind == KindCounter {
		// 计数器语义:值直接累加,min/max 对计数器无诊断意义,保留累计值。
		sr.view.Count++
		sr.view.Sum += s.Value
		sr.view.Last = sr.view.Sum
	} else {
		if sr.first {
			sr.view.Min = s.Value
			sr.view.Max = s.Value
			sr.first = false
		} else {
			if s.Value < sr.view.Min {
				sr.view.Min = s.Value
			}
			if s.Value > sr.view.Max {
				sr.view.Max = s.Value
			}
		}
		sr.view.Count++
		sr.view.Sum += s.Value
		sr.view.Last = s.Value
	}

	if s.ReportID != "" {
		r.seenID[s.ReportID] = struct{}{}
	}
	return sink.Result{Accepted: true, Reason: sink.ReasonAccepted}
}

func cloneLabels(in map[string]string) map[string]string {
	if len(in) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// Execution 返回某次执行的全部时间序列聚合视图(按首次出现顺序)。
func (r *Registry) Execution(id trace.ExecutionID) []SeriesView {
	r.mu.Lock()
	defer r.mu.Unlock()
	keys := r.order[id]
	execMap := r.data[id]
	out := make([]SeriesView, 0, len(keys))
	for _, k := range keys {
		if sr, ok := execMap[k]; ok {
			out = append(out, sr.view)
			out[len(out)-1].Labels = cloneLabels(sr.view.Labels)
		}
	}
	return out
}

// Snapshot 返回全部执行的聚合视图副本。
func (r *Registry) Snapshot() map[trace.ExecutionID][]SeriesView {
	r.mu.Lock()
	ids := make([]trace.ExecutionID, 0, len(r.order))
	for id := range r.order {
		ids = append(ids, id)
	}
	r.mu.Unlock()

	out := make(map[trace.ExecutionID][]SeriesView, len(ids))
	for _, id := range ids {
		out[id] = r.Execution(id)
	}
	return out
}

// Duplicates 返回幂等去重命中次数。
func (r *Registry) Duplicates() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.dups
}

// Dropped 返回容量丢弃次数。
func (r *Registry) Dropped() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.dropped
}

// Reset 清空状态,主要用于测试。
func (r *Registry) Reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seenID = map[string]struct{}{}
	r.data = map[trace.ExecutionID]map[string]*series{}
	r.order = map[trace.ExecutionID][]string{}
	r.dropped = 0
	r.dups = 0
}
