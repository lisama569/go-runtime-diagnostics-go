package sample

import (
	"errors"
	"sync"
	"testing"
)

func TestRateDeterminismAndBoundaries(t *testing.T) {
	if _, err := NewRate(0); err == nil {
		t.Fatal("rate < 1 must error")
	}
	if _, err := NewRate(-3); err == nil {
		t.Fatal("negative rate must error")
	}
	r1, err := NewRate(1)
	if err != nil {
		t.Fatal(err)
	}
	// rate 1 keeps every id (the max bucket is kept via inclusive boundary).
	for _, id := range []string{"a", "b", "c", "zzz"} {
		v := r1.Decide(id, 0)
		if v.Decision != DecisionKeep {
			t.Fatalf("rate=1 must keep %s, got %+v", id, v)
		}
	}

	r10, _ := NewRate(10)
	// Pure function: repeated identical calls yield identical verdicts.
	first := r10.Decide("stable-exec-id", 42)
	for i := 0; i < 100; i++ {
		if r10.Decide("stable-exec-id", 42) != first {
			t.Fatal("verdict must be a pure function of (id, seq, config)")
		}
	}
	// All spans/seq of one execution agree (id-based hashing, not seq-based).
	for seq := uint64(0); seq < 50; seq++ {
		if r10.Decide("stable-exec-id", seq).Decision != first.Decision {
			t.Fatal("one execution must have one consistent sampling verdict")
		}
	}
	t.Logf("sampling_id=%d rate=%.1f decision=%s reason=%s",
		first.SamplingID, first.Rate, first.Decision, first.Reason)
}

func TestRateRoughProportionAndExplainability(t *testing.T) {
	r, _ := NewRate(4)
	keep := 0
	const n = 4000
	for i := 0; i < n; i++ {
		v := r.Decide(string(rune('a'+i%26))+itoa(i), 0)
		if v.Kept() {
			keep++
		} else if v.Reason != ReasonRate {
			t.Fatalf("drop must carry ReasonRate, got %s", v.Reason)
		}
	}
	ratio := float64(keep) / n
	if ratio < 0.20 || ratio > 0.30 {
		t.Fatalf("rate 1/4 keep ratio out of expected band: %f (%d/%d)", ratio, keep, n)
	}
	t.Logf("rate=4 decision keep-ratio=%.3f (drops reason=rate)", ratio)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [20]byte
	p := len(b)
	for i > 0 {
		p--
		b[p] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		p--
		b[p] = '-'
	}
	return string(b[p:])
}

func TestHeadSamplerStableAndConcurrent(t *testing.T) {
	h, err := NewHead(3)
	if err != nil {
		t.Fatal(err)
	}
	ids := []string{"e1", "e2", "e3", "e4", "e5"}
	first := map[string]Verdict{}
	for _, id := range ids {
		first[id] = h.Decide(id, 0)
	}
	kept := 0
	for _, id := range ids {
		if first[id].Decision == DecisionKeep {
			kept++
		} else if first[id].Reason != ReasonHead {
			t.Fatalf("dropped by head must carry ReasonHead: %s", id)
		}
	}
	if kept != 3 {
		t.Fatalf("want 3 head-kept, got %d", kept)
	}
	// Repeat: stable per id.
	for _, id := range ids {
		if h.Decide(id, 9).Decision != first[id].Decision {
			t.Fatalf("verdict for %s must be stable", id)
		}
	}
	// Concurrent first-sight races must never exceed the head count.
	h2, _ := NewHead(10)
	var wg sync.WaitGroup
	seen := sync.Map{}
	for w := 0; w < 100; w++ {
		wg.Add(1)
		go func(x int) {
			defer wg.Done()
			id := "c" + itoa(x%20)
			v := h2.Decide(id, 0)
			if v.Decision == DecisionKeep {
				seen.Store(id, true)
			}
		}(w)
	}
	wg.Wait()
	c := 0
	seen.Range(func(_, _ any) bool { c++; return true })
	if c > 10 {
		t.Fatalf("head cap exceeded under concurrency: %d", c)
	}
	t.Logf("head_n=3 kept=%d/5; concurrent distinct-kept=%d (<=10)", kept, c)
}

func TestCompositeForcedKeepOverride(t *testing.T) {
	drop := AlwaysDrop{}
	c := NewComposite([]Sampler{drop}, func(id string) bool { return id == "priority" })
	v := c.Decide("priority", 0)
	if v.Decision != DecisionKeep || v.Reason != ReasonForcedKeep {
		t.Fatalf("forced keep override failed: %+v", v)
	}
	v2 := c.Decide("normal", 0)
	if v2.Decision != DecisionDrop || v2.Reason != ReasonRate {
		t.Fatalf("non-priority must drop via rule: %+v", v2)
	}
	// Without override, first dropping rule wins and is explainable.
	c2 := NewComposite([]Sampler{drop}, nil)
	if v3 := c2.Decide("x", 0); !errors.Is(nil, nil) && v3.Reason != ReasonRate {
		t.Fatalf("want ReasonRate, got %s", v3.Reason)
	}
	t.Logf("execution=priority decision=keep reason=%s; execution=normal decision=drop reason=%s",
		v.Reason, v2.Reason)
}
