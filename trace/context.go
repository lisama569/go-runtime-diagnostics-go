// Package trace 提供基于执行标识(ExecutionID)的轻量级追踪能力。
//
// 追踪上下文在同步调用(context.Context)、任务排队(Carrier 透传)与
// 异步派生(ForkAsync)三种场景下传播。任何缺失、失效或跨执行继承的
// 上下文都会以 Issue 形式被显式标记，而不是静默挂到错误链路上。
package trace

import "context"

// ExecutionID 标识一次完整的任务执行。同一次执行内的所有跨度、
// 日志、指标与诊断快照都按它汇总。
type ExecutionID string

// SpanID 标识执行内的单个跨度。
type SpanID string

// Issue 描述上下文关联过程中发现的问题类型。
type Issue int

const (
	// IssueNone 没有问题。
	IssueNone Issue = iota
	// IssueMissingContext context 中完全不携带追踪信息。
	IssueMissingContext
	// IssueMalformedCarrier 排队透传的载体缺失或包含无效字段。
	IssueMalformedCarrier
	// IssueCrossExecution 携带的执行标识属于另一次执行(跨执行继承)。
	IssueCrossExecution
	// IssueFinishedParent 父跨度已经结束。
	IssueFinishedParent
	// IssueUnknownParent 引用了不存在的父跨度。
	IssueUnknownParent
)

// String 返回问题的可读名称。
func (i Issue) String() string {
	switch i {
	case IssueNone:
		return "none"
	case IssueMissingContext:
		return "missing_context"
	case IssueMalformedCarrier:
		return "malformed_carrier"
	case IssueCrossExecution:
		return "cross_execution"
	case IssueFinishedParent:
		return "finished_parent"
	case IssueUnknownParent:
		return "unknown_parent"
	default:
		return "unknown_issue"
	}
}

// SpanKind 描述跨度的语义类型。
type SpanKind int

const (
	SpanKindInternal SpanKind = iota
	// SpanKindQueueWait 描述任务在队列中等待的阶段。
	SpanKindQueueWait
	// SpanKindAsync 描述异步派生出的阶段。
	SpanKindAsync
)

func (k SpanKind) String() string {
	switch k {
	case SpanKindQueueWait:
		return "queue_wait"
	case SpanKindAsync:
		return "async"
	default:
		return "internal"
	}
}

// SpanContext 携带建立因果关系所需的最小信息。
type SpanContext struct {
	Execution ExecutionID
	Span      SpanID
}

// Valid 报告上下文是否可用于建立父子关系。
func (sc SpanContext) Valid() bool {
	return sc.Execution != "" && sc.Span != ""
}

// Carrier 用于在任务排队(脱离 context.Context 的场景)时显式透传追踪信息。
type Carrier struct {
	Execution ExecutionID
	Span      SpanID
}

// Valid 报告载体是否完整。
func (c Carrier) Valid() bool {
	return c.Execution != "" && c.Span != ""
}

type contextKey struct{}

// WithContext 将跨度上下文放入 context。
// 已有的追踪上下文会被覆盖——调用方应只在建立新执行/新跨度后使用它。
func WithContext(ctx context.Context, sc SpanContext) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, contextKey{}, sc)
}

// FromContext 从 context 提取跨度上下文，返回其是否存在。
// 返回 false 或 !Valid() 都意味着调用链缺失追踪信息,
// 上层必须以 IssueMissingContext 显式标记,不能静默挂链。
func FromContext(ctx context.Context) (SpanContext, bool) {
	if ctx == nil {
		return SpanContext{}, false
	}
	sc, ok := ctx.Value(contextKey{}).(SpanContext)
	if !ok {
		return SpanContext{}, false
	}
	return sc, true
}
