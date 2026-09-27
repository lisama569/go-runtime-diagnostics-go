package metrics

import (
	"fmt"
	"sync"
	"testing"

	"github.com/lisama569/go-runtime-diagnostics-go/sink"
)

func TestCounterIdempotentReport(t *testing.T) {
	r := New()
	s1 := Sample{Execution: "e", Name: "req", Kind: KindCounter, Value: 1, ReportID: "r1"}
	s2 := Sample{Execution: "e", Name: "req", Kind: KindCounter, Value: 1, ReportID: "r1"}
	s3 := Sample{Execution: "e", Name: "req", Kind: KindCounter, Value: 2, ReportID: "r2"}

	if res := r.Record(s1); !res.Accepted {
		t.Fatalf("s1: %+v", res)
	}
	if res := r.Record(s2); !res.Duplicate || res.Reason != sink.ReasonDedup {
		t.Fatalf("duplicate report must be idempotent: %+v", res)
	}
	if res := r.Record(s3); !res.Accepted {
		t.Fatalf("s3: %+v", res)
	}
	v := r.Execution("e")[0]
	if v.Count != 2 || v.Sum != 3 || v.Last != 3 {
		t.Fatalf("counter aggregation wrong: %+v", v)
	}
	if r.Duplicates() != 1 {
		t.Fatalf("dups=%d", r.Duplicates())
	}
}

func TestGaugeAndTimerAggregation(t *testing.T) {
	r := New()
	for _, v := range []float64{3, 7, 2} {
		r.Record(Sample{Execution: "e", Name: "temp", Kind: KindGauge, Value: v})
		r.Record(Sample{Execution: "e", Name: "lat", Kind: KindTimer, Value: v})
	}
	views := r.Execution("e")
	if len(views) != 2 {
		t.Fatalf("want 2 series, got %d", len(views))
	}
	for _, v := range views {
		if v.Min != 2 || v.Max != 7 || v.Count != 3 || v.Last != 2 {
			t.Fatalf("%s aggregation wrong: %+v", v.Name, v)
		}
	}
}

func TestSeriesCapacityDegradation(t *testing.T) {
	r := New(WithMaxSeriesPerExecution(2))
	for i, name := range []string{"a", "b", "c"} {
		res := r.Record(Sample{Execution: "e", Name: name, Kind: KindGauge, Value: float64(i)})
		if i < 2 && !res.Accepted {
			t.Fatalf("series %s: %+v", name, res)
		}
		if i == 2 && (!res.Dropped || res.Reason != sink.ReasonCapacity) {
			t.Fatalf("third series must degrade: %+v", res)
		}
	}
	// 已有序列继续上报不受上限影响。
	if res := r.Record(Sample{Execution: "e", Name: "a", Kind: KindGauge, Value: 9}); !res.Accepted {
		t.Fatalf("existing series update: %+v", res)
	}
	// 其它执行独立计数上限。
	if res := r.Record(Sample{Execution: "e2", Name: "a", Kind: KindGauge, Value: 1}); !res.Accepted {
		t.Fatalf("per-execution cap must be isolated: %+v", res)
	}
	if len(r.Execution("e")) != 2 || r.Dropped() != 1 {
		t.Fatalf("capacity accounting wrong")
	}
}

func TestRejectsInvalidSample(t *testing.T) {
	r := New()
	if res := r.Record(Sample{Name: "x", Value: 1}); res.Reason != sink.ReasonRejected {
		t.Fatalf("missing execution: %+v", res)
	}
	if res := r.Record(Sample{Execution: "e", Value: 1}); res.Reason != sink.ReasonRejected {
		t.Fatalf("missing name: %+v", res)
	}
}

func TestLabelsPartitionSeries(t *testing.T) {
	r := New()
	r.Record(Sample{Execution: "e", Name: "rpc", Kind: KindCounter, Value: 1, Labels: map[string]string{"code": "200"}})
	r.Record(Sample{Execution: "e", Name: "rpc", Kind: KindCounter, Value: 1, Labels: map[string]string{"code": "500"}})
	r.Record(Sample{Execution: "e", Name: "rpc", Kind: KindCounter, Value: 1, Labels: map[string]string{"code": "200"}})
	views := r.Execution("e")
	if len(views) != 2 {
		t.Fatalf("want 2 label series, got %d", len(views))
	}
	tot := int64(0)
	for _, v := range views {
		tot += v.Count
		if len(v.Labels) != 1 {
			t.Fatalf("labels not copied: %+v", v.Labels)
		}
	}
	if tot != 3 {
		t.Fatalf("counts=%d", tot)
	}
}

func TestConcurrentRecordConsistency(t *testing.T) {
	r := New(WithMaxSeriesPerExecution(100000))
	var wg sync.WaitGroup
	for g := 0; g < 100; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			// 所有 goroutine 复用同一批 10 个 ReportID,验证幂等计数。
			for i := 0; i < 10; i++ {
				r.Record(Sample{
					Execution: "e",
					Name:      fmt.Sprintf("m%d", i),
					Kind:      KindCounter,
					Value:     1,
					ReportID:  fmt.Sprintf("id-%d", i),
				})
			}
		}(g)
	}
	wg.Wait()
	views := r.Execution("e")
	if len(views) != 10 {
		t.Fatalf("want 10 series, got %d", len(views))
	}
	var sum int64
	for _, v := range views {
		sum += v.Count
	}
	if sum != 10 {
		t.Fatalf("each series counted once, got %d", sum)
	}
	if r.Duplicates() != 990 {
		t.Fatalf("want 990 duplicate hits, got %d", r.Duplicates())
	}
}
