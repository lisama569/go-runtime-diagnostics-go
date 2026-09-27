// Package sampling 提供按执行标识可配置、可复现的采样决策。
//
// 决策只依赖执行标识与可配置策略,因此同一执行的决策稳定且可重复;
// 当保留数量达到上限时,决策会带上明确的降级原因(cap_dropped,Degraded=true)。
package sampling

import (
	"hash/fnv"
	"sync"
	"sync/atomic"

	"github.com/lisama569/go-runtime-diagnostics-go/trace"
)

// 决策原因常量,测试可据此断言且日志可直接打印。
const (
	ReasonAlwaysOn  = "always_on"
	ReasonAlwaysOff = "always_off"
	ReasonRateKept  = "rate_kept"
	ReasonRateDrop  = "rate_dropped"
	ReasonCapDrop   = "cap_dropped"
	ReasonRateError = "rate_out_of_range"
)

// Decision 是一次采样决策的结果。
type Decision struct {
	Execution trace.ExecutionID
	Sampled   bool
	// Reason 解释为何采样或丢弃。
	Reason string
	// Degraded 表示该决策因容量压力而产生,与正常采样结果可区分。
	Degraded bool
	// Hash 是决策所依据的归一化哈希 [0,1),便于测试复算与日志解释。
	Hash float64
}

// Policy 根据执行标识给出采样决策。
type Policy interface {
	Decide(execution trace.ExecutionID) Decision
	Stats() Stats
}

// Stats 描述策略的累计计数。
type Stats struct {
	Asked    int64
	Kept     int64
	Dropped  int64
	Degraded int64
}

// AlwaysPolicy 恒定返回同一决策。
type AlwaysPolicy struct {
	sample bool
	asked  int64
	kept   int64
	drop   int64
}

// NewAlways 创建恒定策略。
func NewAlways(sample bool) *AlwaysPolicy { return &AlwaysPolicy{sample: sample} }

// Decide 实现 Policy。
func (p *AlwaysPolicy) Decide(execution trace.ExecutionID) Decision {
	atomic.AddInt64(&p.asked, 1)
	if p.sample {
		atomic.AddInt64(&p.kept, 1)
		return Decision{Execution: execution, Sampled: true, Reason: ReasonAlwaysOn}
	}
	atomic.AddInt64(&p.drop, 1)
	return Decision{Execution: execution, Sampled: false, Reason: ReasonAlwaysOff}
}

// Stats 实现 Policy。
func (p *AlwaysPolicy) Stats() Stats {
	return Stats{
		Asked:   atomic.LoadInt64(&p.asked),
		Kept:    atomic.LoadInt64(&p.kept),
		Dropped: atomic.LoadInt64(&p.drop),
	}
}

// RatePolicy 以可复现的 FNV-1a 哈希按比率采样,并可限制保留数量上限。
type RatePolicy struct {
	mu      sync.Mutex
	rate    float64
	maxKept int64
	salt    string
	asked   int64
	kept    int64
	dropped int64
	degrade int64
	// seen 保证同一执行标识永远得到同一决策(即使容量状态变化)。
	seen map[trace.ExecutionID]Decision
}

// RateOption 配置 RatePolicy。
type RateOption func(*rateConfig)

type rateConfig struct {
	maxKept int64
	salt    string
}

// WithMaxKept 设置保留数量上限。超出上限后新执行以 cap_dropped 降级丢弃。
func WithMaxKept(n int64) RateOption {
	return func(c *rateConfig) { c.maxKept = n }
}

// WithSalt 设置哈希盐值,可让不同部署实例的采样集合相互错开。
func WithSalt(salt string) RateOption {
	return func(c *rateConfig) { c.salt = salt }
}

// NewRate 创建按比率采样策略。rate 取值 [0,1]。
// rate<0 或 >1 时所有决策返回 ReasonRateError 且不采样(fail-closed 但可区分)。
func NewRate(rate float64, opts ...RateOption) *RatePolicy {
	cfg := rateConfig{}
	for _, o := range opts {
		o(&cfg)
	}
	return &RatePolicy{
		rate:    rate,
		maxKept: cfg.maxKept,
		salt:    cfg.salt,
		seen:    map[trace.ExecutionID]Decision{},
	}
}

// hash 对 salt+execution 做 FNV-1a 32 位哈希并归一化到 [0,1)。
func (p *RatePolicy) hash(execution trace.ExecutionID) float64 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(p.salt))
	_, _ = h.Write([]byte(execution))
	return float64(h.Sum32()) / float64(1<<32)
}

// Decide 实现 Policy。
//
// 同一执行标识第一次询问后的决策会被缓存:之后容量状态如何变化,
// 该执行都得到同一 Sampled/Reason,保证"同一输入结果可重复"。
// Stats.Asked 统计调用次数(含缓存命中),Kept/Dropped 统计首次决策数。
func (p *RatePolicy) Decide(execution trace.ExecutionID) Decision {
	p.mu.Lock()
	p.asked++
	if d, ok := p.seen[execution]; ok {
		p.mu.Unlock()
		return d
	}

	d := Decision{Execution: execution}

	switch {
	case p.rate < 0 || p.rate > 1:
		d.Sampled = false
		d.Reason = ReasonRateError
		d.Degraded = true
		p.dropped++
		p.degrade++
	case p.rate == 0:
		d.Sampled = false
		d.Reason = ReasonRateDrop
		p.dropped++
	default:
		hv := p.hash(execution)
		d.Hash = hv
		if hv >= p.rate {
			d.Sampled = false
			d.Reason = ReasonRateDrop
			p.dropped++
		} else if p.maxKept > 0 && p.kept >= p.maxKept {
			// 容量压力:产生可区分的降级结果。
			d.Sampled = false
			d.Reason = ReasonCapDrop
			d.Degraded = true
			p.dropped++
			p.degrade++
		} else {
			d.Sampled = true
			d.Reason = ReasonRateKept
			p.kept++
		}
	}

	p.seen[execution] = d
	p.mu.Unlock()
	return d
}

// Stats 实现 Policy。
func (p *RatePolicy) Stats() Stats {
	p.mu.Lock()
	defer p.mu.Unlock()
	return Stats{Asked: p.asked, Kept: p.kept, Dropped: p.dropped, Degraded: p.degrade}
}

// Reset 清空缓存与计数,主要用于测试。
func (p *RatePolicy) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.asked, p.kept, p.dropped, p.degrade = 0, 0, 0, 0
	p.seen = map[trace.ExecutionID]Decision{}
}
