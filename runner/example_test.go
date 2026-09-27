package runner_test

import (
	"context"
	"errors"
	"fmt"

	"github.com/lisama569/go-runtime-diagnostics-go/diag"
	"github.com/lisama569/go-runtime-diagnostics-go/logx"
	"github.com/lisama569/go-runtime-diagnostics-go/metrics"
	"github.com/lisama569/go-runtime-diagnostics-go/runner"
	"github.com/lisama569/go-runtime-diagnostics-go/sampling"
	"github.com/lisama569/go-runtime-diagnostics-go/trace"
)

// Example 演示同步执行、入队/出队与异步派生三种场景下,
// 追踪、采样、日志、指标与诊断快照如何按执行标识关联。
func Example() {
	r := runner.New(runner.Config{
		Tracer:        trace.New(),
		Sampler:       sampling.NewAlways(true),
		Logger:        logx.New(),
		Metrics:       metrics.New(),
		Diag:          diag.NewManager(nil),
		QueueCapacity: 4,
		MaxAsync:      2,
	})

	// 1) 同步执行:业务错误原样透传,观测数据按执行标识汇总。
	res := r.Run(context.Background(), "order-1", func(ctx context.Context) error {
		r.Metrics().Record(metrics.Sample{
			Execution: "order-1", Name: "orders", Kind: metrics.KindCounter, Value: 1,
		})
		return errors.New("payment declined")
	})
	fmt.Printf("sync: err=%q spans=%d\n", res.Err.Error(),
		len(r.Tracer().ExecutionSpans("order-1")))

	// 2) 入队/出队:载体显式透传,链路 enqueue -> queue_wait -> process。
	carrier, err := r.Submit(context.Background(), "job-1", func(ctx context.Context) error {
		return nil
	})
	fmt.Printf("submit: carrier-valid=%v err=%v\n", carrier.Valid(), err)
	proc := r.Process(carrier, func(ctx context.Context) error { return nil })
	fmt.Printf("process: err=%v issues=%d\n", proc.Err, len(proc.Issues))

	// 3) 损坏载体不会静默挂错链路,问题显式可见。
	bad := r.Process(trace.Carrier{Execution: "ghost", Span: "x"},
		func(ctx context.Context) error { return nil })
	fmt.Printf("bad carrier: issue=%s\n", bad.Issues[0])

	// Output:
	// sync: err="payment declined" spans=1
	// submit: carrier-valid=true err=<nil>
	// process: err=<nil> issues=0
	// bad carrier: issue=malformed_carrier
}

// 避免未使用导入(Metrics 辅助方法在主测试文件之外被示例引用时保证编译)。
var _ = logx.LevelInfo
