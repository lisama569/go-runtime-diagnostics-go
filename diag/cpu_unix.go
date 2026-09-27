//go:build !windows

package diag

import (
	"syscall"
	"time"
)

// cpuUsage 采集当前进程的累计 CPU 时间与上下文切换次数。
// 仅依赖标准库 syscall.Getrusage,不需要外部服务,结果在同机上稳定可复现。
func cpuUsage() (CPUSample, error) {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return CPUSample{}, err
	}
	return CPUSample{
		At:            time.Now(),
		UserNanos:     timevalNanos(ru.Utime),
		SystemNanos:   timevalNanos(ru.Stime),
		VoluntaryCS:   int64(ru.Nvcsw),
		InvoluntaryCS: int64(ru.Nivcsw),
	}, nil
}

func timevalNanos(tv syscall.Timeval) int64 {
	return int64(tv.Sec)*1_000_000_000 + int64(tv.Usec)*1_000
}
