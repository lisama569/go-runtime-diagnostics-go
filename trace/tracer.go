package trace

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// Event 是跨度上的带时间戳的日志点。
type Event struct {
	At      time.Time
	Name    string
	Message string
	Attrs   map[string]string
}

// Span 表示一次执行中的一个跨度。
type Span struct {
	mu        sync.Mutex
	tracer    *Tracer
	id        SpanID
	parent    SpanID
	execution ExecutionID
	name      string
	kind      SpanKind
	startTime time.Time
	endTime   time.Time
	attrs     map[string]string
	issues    []Issue
	events    []Event
	ended     bool
	err       string
}

// SpanOption 在创建跨度时配置它。
type SpanOption func(*spanConfig)

type spanConfig struct {
	kind  SpanKind
	attrs map[string]string
}

// WithKind 设置跨度类型。
func WithKind(kind SpanKind) SpanOption {
	return func(c *spanConfig) { c.kind = kind }
}

// WithSpanAttrs 设置创建时的初始属性。
func WithSpanAttrs(attrs map[string]string) SpanOption {
	return func(c *spanConfig) {
		c.attrs = make(map[string]string, len(attrs))
		for k, v := range attrs {
			c.attrs[k] = v
		}
	}
}

// ID 返回跨度标识。
func (s *Span) ID() SpanID { return s.id }

// Name 返回跨度名。
func (s *Span) Name() string { return s.name }

// Parent 返回父跨度标识。
func (s *Span) Parent() SpanID { return s.parent }

// Kind 返回跨度类型。
func (s *Span) Kind() SpanKind { return s.kind }

// Context 返回跨度上下文。
func (s *Span) Context() SpanContext {
	return SpanContext{Execution: s.execution, Span: s.id}
}

// Execution 返回跨度所属的执行标识。
func (s *Span) Execution() ExecutionID { return s.execution }

// StartTime 返回开始时间。
func (s *Span) StartTime() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.startTime
}

// EndTime 返回结束时间;未结束返回零值。
func (s *Span) EndTime() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.endTime
}

// Ended 报告跨度是否已结束。
func (s *Span) Ended() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ended
}

// Err 返回结束时记录的错误文本(无错误返回 "")。
func (s *Span) Err() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// AddAttr 给跨度增加一个属性。
func (s *Span) AddAttr(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.attrs == nil {
		s.attrs = map[string]string{}
	}
	s.attrs[key] = value
}

// MarkIssue 显式记录一个上下文关联问题。同一问题只记录一次。
func (s *Span) MarkIssue(issue Issue) {
	if issue == IssueNone {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, ex := range s.issues {
		if ex == issue {
			return
		}
	}
	s.issues = append(s.issues, issue)
}

// Issues 返回问题副本。
func (s *Span) Issues() []Issue {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Issue, len(s.issues))
	copy(out, s.issues)
	return out
}

// AddEvent 在跨度上追加一个事件。
func (s *Span) AddEvent(name, message string, attrs map[string]string) Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	ev := Event{
		At:      s.tracer.now(),
		Name:    name,
		Message: message,
		Attrs:   cloneAttrs(attrs),
	}
	s.events = append(s.events, ev)
	return ev
}

// End 结束跨度。重复结束是幂等的:只有第一次调用会记录结束时间与错误。
func (s *Span) End(err error) {
	s.mu.Lock()
	if s.ended {
		s.mu.Unlock()
		return
	}
	s.ended = true
	s.endTime = s.tracer.now()
	if err != nil {
		s.err = err.Error()
		s.attrs["error"] = "true"
	}
	s.mu.Unlock()
	s.tracer.markFinished(s.id)
}

// Snapshot 返回跨度的不可变快照,用于诊断输出。
func (s *Span) Snapshot() SpanSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := SpanSnapshot{
		ID:        s.id,
		Parent:    s.parent,
		Execution: s.execution,
		Name:      s.name,
		Kind:      s.kind,
		StartTime: s.startTime,
		EndTime:   s.endTime,
		Ended:     s.ended,
		Err:       s.err,
		Attrs:     cloneAttrs(s.attrs),
		Issues:    make([]Issue, len(s.issues)),
	}
	copy(out.Issues, s.issues)
	out.Events = make([]Event, len(s.events))
	for i, e := range s.events {
		e.Attrs = cloneAttrs(e.Attrs)
		out.Events[i] = e
	}
	return out
}

func cloneAttrs(in map[string]string) map[string]string {
	if len(in) == 0 {
		return map[string]string{}
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

// SpanSnapshot 是 Span 的只读视图。
type SpanSnapshot struct {
	ID        SpanID
	Parent    SpanID
	Execution ExecutionID
	Name      string
	Kind      SpanKind
	StartTime time.Time
	EndTime   time.Time
	Ended     bool
	Err       string
	Attrs     map[string]string
	Issues    []Issue
	Events    []Event
}

type execInfo struct {
	spans    []SpanID
	finished bool
}

// Tracer 记录跨度并负责上下文校验。
type Tracer struct {
	mu    sync.Mutex
	cfg   tracerConfig
	seq   uint64
	spans map[SpanID]*Span
	execs map[ExecutionID]*execInfo
	order []ExecutionID
}

// Option 配置 Tracer。
type Option func(*tracerConfig)

type tracerConfig struct {
	idgen func() string
	now   func() time.Time
}

// WithIDGen 注入跨度 ID 生成器(主要用于可重复的测试)。
func WithIDGen(fn func() string) Option {
	return func(c *tracerConfig) { c.idgen = fn }
}

// WithClock 注入时钟(主要用于可重复的测试)。
func WithClock(fn func() time.Time) Option {
	return func(c *tracerConfig) { c.now = fn }
}

// New 创建 Tracer。
func New(opts ...Option) *Tracer {
	cfg := tracerConfig{now: time.Now}
	for _, o := range opts {
		o(&cfg)
	}
	return &Tracer{
		cfg:   cfg,
		spans: map[SpanID]*Span{},
		execs: map[ExecutionID]*execInfo{},
	}
}

func (t *Tracer) now() time.Time {
	if t.cfg.now != nil {
		return t.cfg.now()
	}
	return time.Now()
}

func (t *Tracer) nextSpanID() SpanID {
	n := atomic.AddUint64(&t.seq, 1)
	if t.cfg.idgen != nil {
		return SpanID(t.cfg.idgen())
	}
	return SpanID(itoa(n))
}

func itoa(n uint64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

// StartExecution 开启一次新的任务执行,返回携带根跨度上下文的 context。
// 重复使用同一执行标识时,新根跨度会被标记 IssueCrossExecution,
// 防止不同执行因标识复用而静默合并。
func (t *Tracer) StartExecution(id ExecutionID, name string, ctx context.Context) (context.Context, *Span) {
	if ctx == nil {
		ctx = context.Background()
	}
	if id == "" {
		id = ExecutionID("exec-" + itoa(atomic.AddUint64(&t.seq, 1)))
	}
	span := t.newSpan(id, "", name, SpanKindInternal, nil)

	t.mu.Lock()
	_, exists := t.execs[id]
	duplicate := exists
	t.mu.Unlock()

	// register 会同时写入 spans 表与执行索引,保证后续父引用可查。
	t.register(span)

	if duplicate {
		span.MarkIssue(IssueCrossExecution)
		span.AddAttr("issue.detail", "duplicate execution start; treated as a new root")
	}
	return WithContext(ctx, span.Context()), span
}

func (t *Tracer) newSpan(exec ExecutionID, parent SpanID, name string, kind SpanKind, attrs map[string]string) *Span {
	return &Span{
		tracer:    t,
		id:        t.nextSpanID(),
		parent:    parent,
		execution: exec,
		name:      name,
		kind:      kind,
		startTime: t.now(),
		attrs:     cloneAttrs(attrs),
	}
}

// Start 在已有执行上开启一个子跨度。
//
// 上下文缺失、引用未知/已结束父跨度等情况不会静默挂到错误链路:
// 跨度仍可用(避免影响业务),但带有明确 Issue,并脱离可疑父节点:
//   - IssueMissingContext: ctx 中没有追踪信息 → 开一条新执行的根跨度。
//   - IssueUnknownParent:  父跨度不存在 → 同执行内的无父跨度。
//   - IssueFinishedParent: 父跨度已结束 → 仍按调用意图保留父子关系并标记。
func (t *Tracer) Start(ctx context.Context, name string, opts ...SpanOption) (context.Context, *Span) {
	cfg := spanConfig{kind: SpanKindInternal}
	for _, o := range opts {
		o(&cfg)
	}
	if ctx == nil {
		ctx = context.Background()
	}

	sc, ok := FromContext(ctx)
	if !ok || !sc.Valid() {
		id := ExecutionID("exec-" + itoa(atomic.AddUint64(&t.seq, 1)))
		span := t.newSpan(id, "", name, cfg.kind, cfg.attrs)
		span.MarkIssue(IssueMissingContext)
		t.register(span)
		return WithContext(ctx, span.Context()), span
	}

	issue, parentKnown := t.classifyParent(sc)
	span := t.newSpan(sc.Execution, sc.Span, name, cfg.kind, cfg.attrs)
	if issue == IssueUnknownParent {
		// 不挂到未知跨度上,退化为同执行的无父跨度,但保留问题痕迹。
		span.parent = ""
		span.AddAttr("issue.ref_parent", string(sc.Span))
	}
	if issue != IssueNone {
		span.MarkIssue(issue)
	}
	_ = parentKnown
	t.register(span)
	return WithContext(ctx, span.Context()), span
}

// classifyParent 校验上下文中的父引用。调用方持有/不持锁均可(内部加锁)。
func (t *Tracer) classifyParent(sc SpanContext) (Issue, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	info, ok := t.execs[sc.Execution]
	if !ok {
		return IssueUnknownParent, false
	}
	parent, ok := t.spans[sc.Span]
	if !ok {
		return IssueUnknownParent, false
	}
	if parent.Execution() != sc.Execution {
		return IssueCrossExecution, false
	}
	if info.finished {
		return IssueCrossExecution, true
	}
	if parent.Ended() {
		return IssueFinishedParent, true
	}
	return IssueNone, true
}

func (t *Tracer) register(s *Span) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.spans[s.id] = s
	info := t.execs[s.execution]
	if info == nil {
		info = &execInfo{}
		t.execs[s.execution] = info
		t.order = append(t.order, s.execution)
	}
	info.spans = append(info.spans, s.id)
}

func (t *Tracer) markFinished(id SpanID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if s, ok := t.spans[id]; ok {
		_ = s
	}
}

// StartFromCarrier 从排队载体恢复上下文并开启一个排队阶段的跨度。
//
// 返回的 Issue 明确区分恢复结果:
//   - IssueMalformedCarrier: 载体不完整或引用了未知执行/未知跨度。
//   - IssueCrossExecution:   目标执行已结束(过期/跨执行继承的载体)。
//   - IssueFinishedParent:   载体中的入队跨度已结束。
//
// 任何异常情况下返回的跨度都不会静默挂错链路:失效载体生成独立根跨度。
func (t *Tracer) StartFromCarrier(c Carrier, name string, opts ...SpanOption) (*Span, Issue) {
	cfg := spanConfig{kind: SpanKindQueueWait}
	for _, o := range opts {
		o(&cfg)
	}
	if !c.Valid() {
		id := ExecutionID("exec-" + itoa(atomic.AddUint64(&t.seq, 1)))
		span := t.newSpan(id, "", name, SpanKindQueueWait, cfg.attrs)
		span.MarkIssue(IssueMalformedCarrier)
		t.register(span)
		return span, IssueMalformedCarrier
	}

	sc := SpanContext{Execution: c.Execution, Span: c.Span}
	issue, known := t.classifyParent(sc)
	switch issue {
	case IssueUnknownParent:
		if !known {
			// 引用了 tracer 完全不认识的执行/跨度,视为损坏载体,
			// 开独立根跨度而不是接到错误链路上。
			id := ExecutionID("exec-" + itoa(atomic.AddUint64(&t.seq, 1)))
			span := t.newSpan(id, "", name, SpanKindQueueWait, cfg.attrs)
			span.MarkIssue(IssueMalformedCarrier)
			span.AddAttr("issue.ref_execution", string(c.Execution))
			t.register(span)
			return span, IssueMalformedCarrier
		}
	case IssueFinishedParent:
		// 排队语义下,入队跨度在出队时已结束是正常现象:
		// 载体仍然有效,保留父子关系,不作为问题上报。
		issue = IssueNone
	}

	span := t.newSpan(c.Execution, c.Span, name, SpanKindQueueWait, cfg.attrs)
	if issue != IssueNone {
		span.MarkIssue(issue)
	}
	t.register(span)
	return span, issue
}

// Enqueue 为"任务入队"生成可跨队列透传的载体。
// ctx 缺失追踪信息时返回带 IssueMissingContext 的空载体,调用方不得静默继续。
func (t *Tracer) Enqueue(ctx context.Context) (Carrier, Issue) {
	sc, ok := FromContext(ctx)
	if !ok || !sc.Valid() {
		return Carrier{}, IssueMissingContext
	}
	return Carrier{Execution: sc.Execution, Span: sc.Span}, IssueNone
}

// ForkAsync 为异步派生创建跨度上下文,与父跨度形成因果链。
// 父上下文缺失时返回 IssueMissingContext 且派生跨度位于独立执行,
// 不会与当前任何执行错误关联。
func (t *Tracer) ForkAsync(parent context.Context, name string, attrs map[string]string) (context.Context, *Span, Issue) {
	if parent == nil {
		parent = context.Background()
	}
	ctx, span := t.Start(parent, name, WithKind(SpanKindAsync), WithSpanAttrs(attrs))
	issues := span.Issues()
	issue := IssueNone
	if len(issues) > 0 {
		issue = issues[0]
	}
	return ctx, span, issue
}

// EndExecution 标记一次执行结束。结束后再用其载体出队会被识别为
// IssueCrossExecution(跨执行继承)。
func (t *Tracer) EndExecution(id ExecutionID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if info, ok := t.execs[id]; ok {
		info.finished = true
	}
}

// IsExecutionFinished 报告执行是否已显式结束。
func (t *Tracer) IsExecutionFinished(id ExecutionID) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	info, ok := t.execs[id]
	return ok && info.finished
}

// Span 按 ID 查找跨度快照(第二个返回值表示是否存在)。
func (t *Tracer) Span(id SpanID) (SpanSnapshot, bool) {
	t.mu.Lock()
	s, ok := t.spans[id]
	t.mu.Unlock()
	if !ok {
		return SpanSnapshot{}, false
	}
	return s.Snapshot(), true
}

// ExecutionSpans 返回某次执行所有跨度的快照(按开始顺序)。
func (t *Tracer) ExecutionSpans(id ExecutionID) []SpanSnapshot {
	t.mu.Lock()
	info := t.execs[id]
	t.mu.Unlock()
	if info == nil {
		return nil
	}
	t.mu.Lock()
	ids := make([]SpanID, len(info.spans))
	copy(ids, info.spans)
	t.mu.Unlock()
	out := make([]SpanSnapshot, 0, len(ids))
	t.mu.Lock()
	for _, id := range ids {
		if s, ok := t.spans[id]; ok {
			out = append(out, s.Snapshot())
		}
	}
	t.mu.Unlock()
	return out
}

// Executions 返回当前记录的全部执行标识(按首次出现顺序)。
func (t *Tracer) Executions() []ExecutionID {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]ExecutionID, len(t.order))
	copy(out, t.order)
	return out
}
