package sample

import "sync"

// HeadSampler keeps only the first n distinct execution ids it observes.
// The verdict for an execution id is stable: once kept it always keeps; once
// dropped it always drops. It is safe for concurrent use.
type HeadSampler struct {
	n int

	mu    sync.Mutex
	kept  map[string]bool
	count int
}

// NewHead builds a head sampler that keeps the first n unique executions.
func NewHead(n int) (*HeadSampler, error) {
	if n < 0 {
		return nil, errHeadN
	}
	return &HeadSampler{n: n, kept: make(map[string]bool)}, nil
}

// Decide implements Sampler.
func (h *HeadSampler) Decide(executionID string, seq uint64) Verdict {
	h.mu.Lock()
	defer h.mu.Unlock()
	v := Verdict{SamplingID: hashBucket(executionID), Rate: float64(h.n)}
	if wasKept, known := h.kept[executionID]; known {
		if wasKept {
			v.Decision = DecisionKeep
			return v
		}
		v.Decision = DecisionDrop
		v.Reason = ReasonHead
		return v
	}
	if h.count < h.n {
		h.count++
		h.kept[executionID] = true
		v.Decision = DecisionKeep
		return v
	}
	h.kept[executionID] = false
	v.Decision = DecisionDrop
	v.Reason = ReasonHead
	return v
}
