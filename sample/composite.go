package sample

// Composite evaluates rules in priority order. A forcedKeep predicate, if
// non-nil and true for the execution id, overrides every rule and forces a
// keep with ReasonForcedKeep. Otherwise the first rule that drops wins; if no
// rule drops, the result is keep. This makes the chain explainable: exactly
// one reason is attached to each verdict.
type Composite struct {
	rules      []Sampler
	forcedKeep func(string) bool
}

// NewComposite builds a composite sampler.
func NewComposite(rules []Sampler, forcedKeep func(string) bool) *Composite {
	return &Composite{rules: rules, forcedKeep: forcedKeep}
}

// Decide implements Sampler.
func (c *Composite) Decide(executionID string, seq uint64) Verdict {
	if c.forcedKeep != nil && c.forcedKeep(executionID) {
		v := AlwaysKeep{}.Decide(executionID, seq)
		v.Reason = ReasonForcedKeep
		return v
	}
	for _, r := range c.rules {
		v := r.Decide(executionID, seq)
		if v.Decision == DecisionDrop {
			return v
		}
	}
	return Verdict{Decision: DecisionKeep, Reason: ReasonNone,
		SamplingID: hashBucket(executionID), Rate: 1}
}
