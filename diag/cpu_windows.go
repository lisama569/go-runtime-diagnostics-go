//go:build windows

package diag

import (
	"errors"
	"time"
)

// Windows 平台没有等价的 syscall.Getrusage,返回可区分的采集错误,
// 由快照的 Failures 体现,而不是静默给出零值。
func cpuUsage() (CPUSample, error) {
	return CPUSample{}, errors.New("cpu usage not supported on windows")
}

var _ = time.Now
