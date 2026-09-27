package sampling

import (
	"sync"
	"testing"

	"github.com/lisama569/go-runtime-diagnostics-go/trace"
)

func TestAlwaysPolicy(t *testing.T) {
	on := NewAlways(true)
	d := on.Decide("e1")
	if !d.Sampled || d.Reason != ReasonAlwaysOn {
		t.Fatalf("always-on decision wrong: %+v", d)
	}
	off := NewAlways(false)
	if off.Decide("e2").Sampled {
		t.Fatal("always-off must never sample")
	}
	if got := on.Stats(); got.Kept != 1 || got.Asked != 1 {
		t.Fatalf("stats wrong: %+v", got)
	}
}

func TestRateDeterministic(t *testing.T) {
	p := NewRate(0.5)
	first := p.Decide("exec-42")
	for i := 0; i < 50; i++ {
		if d := p.Decide("exec-42"); d != first {
			t.Fatalf("decision changed: %+v vs %+v", d, first)
		}
	}
	// 独立实例同盐值必须复算出相同哈希与决策。
	q := NewRate(0.5)
	if d := q.Decide("exec-42"); d.Hash != first.Hash || d.Sampled != first.Sampled {
		t.Fatalf("not reproducible across instances: %+v vs %+v", d, first)
	}
}

func TestRateBoundaries(t *testing.T) {
	if NewRate(0).Decide("a").Sampled {
		t.Fatal("rate=0 must drop all")
	}
	if !NewRate(1).Decide("a").Sampled {
		t.Fatal("rate=1 must keep all")
	}
	bad := NewRate(1.5).Decide("a")
	if bad.Sampled || !bad.Degraded || bad.Reason != ReasonRateError {
		t.Fatalf("out-of-range rate must fail closed distinguishably: %+v", bad)
	}
}

func TestRateCapDegradation(t *testing.T) {
	p := NewRate(1, WithMaxKept(3))
	kept, dropped := 0, 0
	for i := 0; i < 10; i++ {
		id := trace.ExecutionID("e-" + itoaTest(i))
		d := p.Decide(id)
		t.Logf("execution=%s sampled=%t reason=%s degraded=%t hash=%.4f",
			id, d.Sampled, d.Reason, d.Degraded, d.Hash)
		if d.Sampled {
			kept++
			if d.Degraded {
				t.Fatal("kept decisions are not degraded")
			}
		} else {
			dropped++
			if !d.Degraded || d.Reason != ReasonCapDrop {
				t.Fatalf("cap drop must be distinguishable: %+v", d)
			}
		}
	}
	t.Logf("sampling summary: kept=%d dropped(degraded)=%d", kept, dropped)
	if kept != 3 || dropped != 7 {
		t.Fatalf("want 3 kept / 7 dropped, got %d/%d", kept, dropped)
	}
	if kept != 3 || dropped != 7 {
		t.Fatalf("want 3 kept / 7 dropped, got %d/%d", kept, dropped)
	}
	// 早期被保留的执行,在容量耗尽后询问仍得到原决策(可重复)。
	d := p.Decide("e-0")
	if !d.Sampled || d.Reason != ReasonRateKept {
		t.Fatalf("cached decision changed: %+v", d)
	}
	st := p.Stats()
	if st.Kept != 3 || st.Degraded != 7 {
		t.Fatalf("stats wrong: %+v", st)
	}
}

func TestSaltChangesPopulation(t *testing.T) {
	a := NewRate(0.5, WithSalt("a"))
	b := NewRate(0.5, WithSalt("b"))
	diff := 0
	for i := 0; i < 100; i++ {
		id := trace.ExecutionID("exec-" + itoaTest(i))
		if a.Decide(id).Sampled != b.Decide(id).Sampled {
			diff++
		}
	}
	if diff == 0 {
		t.Fatal("different salts should produce different sample sets")
	}
}

func TestConcurrentDecisionsConsistent(t *testing.T) {
	p := NewRate(0.7)
	var wg sync.WaitGroup
	res := make(chan bool, 100)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := trace.ExecutionID("c-" + itoaTest(i%20))
			d1 := p.Decide(id)
			d2 := p.Decide(id)
			if d1 != d2 {
				t.Errorf("decision race inconsistency for %s", id)
			}
			res <- d1.Sampled
		}(i)
	}
	wg.Wait()
	close(res)
	// 只校验统计自洽,不依赖具体调度。
	st := p.Stats()
	if st.Asked != 200 {
		t.Fatalf("asked=%d want 200", st.Asked)
	}
	if st.Kept+st.Dropped != 20 {
		t.Fatalf("unique first decisions=%d want 20", st.Kept+st.Dropped)
	}
}

func itoaTest(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}
