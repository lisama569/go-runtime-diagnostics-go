// Package sink 提供并发安全、容量有界的记录汇聚能力,供日志与指标复用。
//
// 写入绝不覆盖已有记录;达到容量上限时返回可区分的降级结果
// (Dropped=true, Reason=capacity),并只累计一次丢弃计数。
// 开启去重后,相同 key 的重复写入返回 Duplicate=true,同样不覆盖原记录。
package sink

import "sync"

// Result 描述一次写入的结果。
type Result struct {
	// Accepted 表示记录被接受。
	Accepted bool
	// Dropped 表示因容量压力被丢弃(降级)。
	Dropped bool
	// Duplicate 表示记录被去重逻辑拒绝。
	Duplicate bool
	// Reason 是 machine-readable 的原因。
	Reason string
}

const (
	// ReasonAccepted 记录被接受。
	ReasonAccepted = "accepted"
	// ReasonCapacity 容量不足导致降级丢弃。
	ReasonCapacity = "capacity"
	// ReasonDedup 重复上报被去重拒绝。
	ReasonDedup = "duplicate"
	// ReasonRejected 其他明确拒绝(如缺失关联标识)。
	ReasonRejected = "rejected"
)

// DefaultCapacity 是未显式指定容量时的上限。
const DefaultCapacity = 1024

// Sink 是一个并发安全的有界记录汇聚器。
type Sink struct {
	mu         sync.Mutex
	capacity   int
	dedup      bool
	records    []any
	seen       map[string]struct{}
	dropped    int64
	duplicates int64
}

// New 创建容量为 capacity(<=0 使用 DefaultCapacity)的汇聚器。
// dedup=true 时,Add 传入的相同 key 只会被接受一次。
func New(capacity int, dedup bool) *Sink {
	if capacity <= 0 {
		capacity = DefaultCapacity
	}
	return &Sink{
		capacity: capacity,
		dedup:    dedup,
		seen:     map[string]struct{}{},
	}
}

// Add 尝试加入一条记录。
//
//   - dedup 开启且 key 非空、key 已出现:返回 Duplicate 结果,只计一次重复。
//   - 容量已满:返回 Dropped+ReasonCapacity 的可区分降级结果。
//   - 其余情况:接受并按顺序保存,绝不覆盖已有记录。
//
// 该函数不 panic、不修改入参,因此不会改变调用方的业务结果。
func (s *Sink) Add(key string, record any) Result {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.dedup && key != "" {
		if _, ok := s.seen[key]; ok {
			s.duplicates++
			return Result{Duplicate: true, Reason: ReasonDedup}
		}
	}

	if len(s.records) >= s.capacity {
		s.dropped++
		return Result{Dropped: true, Reason: ReasonCapacity}
	}

	s.records = append(s.records, record)
	if s.dedup && key != "" {
		s.seen[key] = struct{}{}
	}
	return Result{Accepted: true, Reason: ReasonAccepted}
}

// Len 返回当前已接受的记录数。
func (s *Sink) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.records)
}

// Cap 返回容量。
func (s *Sink) Cap() int { return s.capacity }

// Dropped 返回因容量不足而丢弃的累计次数(并发安全)。
func (s *Sink) Dropped() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dropped
}

// Duplicates 返回被去重拒绝的累计次数。
func (s *Sink) Duplicates() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.duplicates
}

// Records 返回当前记录的快照副本(顺序为接受顺序)。
func (s *Sink) Records() []any {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]any, len(s.records))
	copy(out, s.records)
	return out
}

// Reset 清空全部记录与计数,主要用于测试。
func (s *Sink) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records = nil
	s.seen = map[string]struct{}{}
	s.dropped = 0
	s.duplicates = 0
}
