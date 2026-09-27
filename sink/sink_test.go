package sink

import (
	"sync"
	"testing"
)

func TestCapacityAndDedup(t *testing.T) {
	s := New(2, true)

	if r := s.Add("k1", "a"); !r.Accepted || r.Reason != ReasonAccepted {
		t.Fatalf("first add: %+v", r)
	}
	if r := s.Add("k1", "a2"); !r.Duplicate || r.Reason != ReasonDedup {
		t.Fatalf("duplicate add: %+v", r)
	}
	if r := s.Add("k2", "b"); !r.Accepted {
		t.Fatalf("second add: %+v", r)
	}
	// 容量已达 2,即使 key 是新的也必须降级丢弃。
	r := s.Add("k3", "c")
	if !r.Dropped || r.Reason != ReasonCapacity {
		t.Fatalf("capacity result: %+v", r)
	}
	// 已有记录绝不能被覆盖。
	recs := s.Records()
	if len(recs) != 2 || recs[0] != "a" || recs[1] != "b" {
		t.Fatalf("records corrupted: %v", recs)
	}
	if s.Dropped() != 1 || s.Duplicates() != 1 {
		t.Fatalf("counters wrong: dropped=%d dups=%d", s.Dropped(), s.Duplicates())
	}
}

func TestDedupDisabled(t *testing.T) {
	s := New(10, false)
	s.Add("k", 1)
	r := s.Add("k", 2)
	if !r.Accepted {
		t.Fatalf("same key without dedup must be accepted: %+v", r)
	}
	if s.Len() != 2 {
		t.Fatalf("len=%d", s.Len())
	}
}

func TestRejectedEmptyKeyDedup(t *testing.T) {
	s := New(1, true)
	s.Add("", "x") // 空 key 不参与去重
	r := s.Add("", "y")
	// 第二个因容量被丢弃,而不是被当作重复。
	if !r.Dropped || r.Duplicate {
		t.Fatalf("empty key must not dedup: %+v", r)
	}
}

func TestConcurrentAddNoDoubleCount(t *testing.T) {
	s := New(1000, true)
	var wg sync.WaitGroup
	for i := 0; i < 500; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// 500 个 goroutine 各用同一组 50 个 key 写,共 2500 次写。
			s.Add(keyOf(i%50), i)
			s.Add(keyOf(i%50), i) // 每 goroutine 自身重复一次
		}(i)
	}
	wg.Wait()

	accepted := int64(s.Len())
	dups := s.Duplicates()
	dropped := s.Dropped()
	if accepted+dups+dropped != 1000 {
		t.Fatalf("accounting broken: accepted=%d dups=%d dropped=%d sum=%d",
			accepted, dups, dropped, accepted+dups+dropped)
	}
	if accepted != 50 {
		t.Fatalf("want exactly 50 unique accepted, got %d", accepted)
	}
	if dups != 950 {
		t.Fatalf("want 950 duplicates, got %d", dups)
	}
}

func keyOf(n int) string {
	if n == 0 {
		return "k0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return "k" + string(b)
}

func TestReset(t *testing.T) {
	s := New(1, false)
	s.Add("x", 1)
	s.Reset()
	if s.Len() != 0 || s.Dropped() != 0 {
		t.Fatal("reset failed")
	}
	if r := s.Add("x", 2); !r.Accepted {
		t.Fatalf("after reset: %+v", r)
	}
}
