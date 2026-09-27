package diag

import "errors"

var errBlockUnavailable = errors.New("diag: block profiling is not enabled; call runtime.SetBlockProfileRate")
