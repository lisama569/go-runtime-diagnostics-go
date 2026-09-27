// Package sample provides deterministic, per-execution sampling decisions.
//
// Decisions are pure functions of (executionID, seq) for a given
// configuration, so the same input always yields the same verdict across
// reruns; tests never depend on randomness, time or external services.
package sample

import (
	"errors"
	"hash/fnv"
)

// Decision is the sampling verdict.
type Decision int

const (
	DecisionKeep Decision = iota
	DecisionDrop
)

func (d Decision) String() string {
	if d == DecisionKeep {
		return "keep"
	}
	return "drop"
}

// DropReason explains a verdict.
type DropReason int

const (
	ReasonNone DropReason = iota
	ReasonRate
	ReasonCapacity
	ReasonHead
	ReasonForcedKeep
	ReasonNoRule
)

func (r DropReason) String() string {
	switch r {
	case ReasonNone:
		return "none"
	case ReasonRate:
		return "rate"
	case ReasonCapacity:
		return "capacity"
	case ReasonHead:
		return "head"
	case ReasonForcedKeep:
		return "forced_keep"
	case ReasonNoRule:
		return "no_rule"
	default:
		return "unknown"
	}
}

// Verdict is one fully explained sampling decision.
type Verdict struct {
	Decision   Decision
	Reason     DropReason
	SamplingID uint64
	Rate       float64
}

// Kept reports whether the verdict is keep.
func (v Verdict) Kept() bool { return v.Decision == DecisionKeep }

// Sampler answers sampling questions deterministically.
type Sampler interface {
	Decide(executionID string, seq uint64) Verdict
}

// hashBucket maps an execution id to a stable bucket in [0,1).
func hashBucket(executionID string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(executionID))
	return h.Sum64()
}

// RateSampler keeps approximately 1/rate executions deterministically.
// A given execution id is always placed in the same bucket, so all its spans
// agree; boundary values (bucket == rate*2^64) are handled by strict <.
type RateSampler struct {
	rate   float64
	cutoff uint64 // keep when bucket < cutoff
	scaled float64
}

// NewRate builds a rate sampler. rate must be >= 1; rate 1 keeps everything.
func NewRate(rate float64) (*RateSampler, error) {
	if rate < 1 {
		return nil, errors.New("sample: rate must be >= 1")
	}
	if rate != rate { // NaN
		return nil, errors.New("sample: rate must not be NaN")
	}
	cut := uint64(float64(^uint64(0)) / rate)
	// rate == 1 => cutoff == max; strict < keeps all but the max bucket, which
	// is the documented inclusive boundary.
	if rate == 1 {
		cut = ^uint64(0)
	}
	return &RateSampler{rate: rate, cutoff: cut, scaled: 1 / rate}, nil
}

// Decide implements Sampler.
func (s *RateSampler) Decide(executionID string, seq uint64) Verdict {
	bucket := hashBucket(executionID)
	v := Verdict{SamplingID: bucket, Rate: s.rate}
	if bucket < s.cutoff {
		v.Decision = DecisionKeep
		v.Reason = ReasonNone
	} else {
		v.Decision = DecisionDrop
		v.Reason = ReasonRate
	}
	return v
}

// AlwaysKeep always keeps.
type AlwaysKeep struct{}

// Decide implements Sampler.
func (AlwaysKeep) Decide(executionID string, seq uint64) Verdict {
	return Verdict{Decision: DecisionKeep, Reason: ReasonNone, Rate: 1, SamplingID: hashBucket(executionID)}
}

// AlwaysDrop always drops with ReasonRate.
type AlwaysDrop struct{}

// Decide implements Sampler.
func (AlwaysDrop) Decide(executionID string, seq uint64) Verdict {
	return Verdict{Decision: DecisionDrop, Reason: ReasonRate, Rate: 0, SamplingID: hashBucket(executionID)}
}
