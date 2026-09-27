package diag

import (
	"bytes"
	"context"
	"runtime"
	"runtime/pprof"
	"time"
)

// DefaultCollectors returns the built-in collector set: memstats, goroutine,
// heap and block. CPU profiling is opt-in (see NewCPUCollector) because the Go
// runtime only permits one active CPU profile process-wide.
func DefaultCollectors() []Collector {
	return []Collector{
		MemStatsCollector{},
		GoroutineCollector{},
		HeapCollector{},
		BlockCollector{},
	}
}

// MemStatsCollector captures runtime.MemStats counters (cheap, no pprof I/O).
type MemStatsCollector struct{}

// Name implements Collector.
func (MemStatsCollector) Name() string { return "memstats" }

// Collect implements Collector.
func (MemStatsCollector) Collect(ctx context.Context) (Profile, error) {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return Profile{
		Kind:        "memstats",
		HeapAlloc:   ms.HeapAlloc,
		HeapObjects: ms.HeapObjects,
	}, nil
}

// GoroutineCollector captures the current goroutine count.
type GoroutineCollector struct{}

// Name implements Collector.
func (GoroutineCollector) Name() string { return "goroutine" }

// Collect implements Collector.
func (GoroutineCollector) Collect(ctx context.Context) (Profile, error) {
	return Profile{Kind: "goroutine", Value: int64(runtime.NumGoroutine())}, nil
}

// HeapCollector captures a pprof heap profile snapshot.
type HeapCollector struct{}

// Name implements Collector.
func (HeapCollector) Name() string { return "heap" }

// Collect implements Collector.
func (HeapCollector) Collect(ctx context.Context) (Profile, error) {
	var buf bytes.Buffer
	start := time.Now()
	if err := pprof.Lookup("heap").WriteTo(&buf, 0); err != nil {
		return Profile{Kind: "heap", Err: err.Error(), Duration: time.Since(start)}, err
	}
	return Profile{Kind: "heap", Bytes: buf.Bytes(), Duration: time.Since(start)}, nil
}

// BlockCollector captures a pprof blocking profile snapshot. Block profiling
// must have been enabled at the desired rate; if unavailable the collector
// reports a descriptive failure rather than panicking.
type BlockCollector struct{}

// Name implements Collector.
func (BlockCollector) Name() string { return "block" }

// Collect implements Collector.
func (BlockCollector) Collect(ctx context.Context) (Profile, error) {
	var buf bytes.Buffer
	start := time.Now()
	p := pprof.Lookup("block")
	if p == nil {
		err := errBlockUnavailable
		return Profile{Kind: "block", Err: err.Error(), Duration: time.Since(start)}, err
	}
	if err := p.WriteTo(&buf, 0); err != nil {
		return Profile{Kind: "block", Err: err.Error(), Duration: time.Since(start)}, err
	}
	return Profile{Kind: "block", Bytes: buf.Bytes(), Duration: time.Since(start)}, nil
}

// CPUCollector runs a bounded CPU profile window per Collect call. It honors
// context cancellation: if ctx ends before the window elapses, the profile
// captured so far is returned with that cause recorded.
type CPUCollector struct {
	Window time.Duration
}

// NewCPUCollector builds a CPU collector with the given profile window.
func NewCPUCollector(window time.Duration) CPUCollector {
	if window <= 0 {
		window = 100 * time.Millisecond
	}
	return CPUCollector{Window: window}
}

// Name implements Collector.
func (CPUCollector) Name() string { return "cpu" }

// Collect implements Collector.
func (c CPUCollector) Collect(ctx context.Context) (Profile, error) {
	var buf bytes.Buffer
	start := time.Now()
	if err := pprof.StartCPUProfile(&buf); err != nil {
		// Most commonly a second concurrent CPU profile: report distinguishably.
		return Profile{Kind: "cpu", Err: err.Error(), Duration: time.Since(start)}, err
	}
	timer := time.NewTimer(c.Window)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		pprof.StopCPUProfile()
		err := ctx.Err()
		return Profile{Kind: "cpu", Bytes: buf.Bytes(), Err: err.Error(), Duration: time.Since(start)}, err
	case <-timer.C:
	}
	pprof.StopCPUProfile()
	return Profile{Kind: "cpu", Bytes: buf.Bytes(), Duration: time.Since(start)}, nil
}
