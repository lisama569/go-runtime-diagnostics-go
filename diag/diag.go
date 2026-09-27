// Package diag 支持按需生成覆盖调用阶段、CPU、内存与阻塞等待的
// 性能诊断快照。
//
// 会话按执行标识与追踪链路关联。重复启动、提前结束、并发触发或
// 采集失败都有稳定、可验证的语义(见各错误哨兵与 CollectError)。
package diag

import (
	"bytes"
	"context"
	"errors"
	"runtime"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/lisama569/go-runtime-diagnostics-go/trace"
)

// 稳定的会话级错误哨兵,调用方可 errors.Is 区分失败原因。
var (
	// ErrSessionActive 同一执行重复启动诊断会话。
	ErrSessionActive = errors.New("diag: session already active for execution")
	// ErrNotRunning 对未运行的会话执行结束/快照等操作。
	ErrNotRunning = errors.New("diag: session is not running")
	// ErrConcurrentSnapshot 同一会话上已有快照采集在进行。
	ErrConcurrentSnapshot = errors.New("diag: concurrent snapshot in progress")
	// ErrCollectionFailed 某个采集阶段失败,细节见 Snapshot.Failures。
	ErrCollectionFailed = errors.New("diag: collection failed")
	// ErrUnknownExecution 操作的执行标识没有活动会话。
	ErrUnknownExecution = errors.New("diag: unknown execution")
	// ErrNoTracer 在依赖 Tracer 的操作未绑定 tracer 时返回。
	ErrNoTracer = errors.New("diag: tracer not bound")
)

// 采集阶段名,用于注入失败与错误归因。
const (
	stagePhase   = "phases"
	stageBlock   = "blocks"
	stageCPU     = "cpu"
	stageMemory  = "memory"
	stageProfile = "cpu_profile"
)

// CollectError 记录单个采集阶段的失败,便于定位异常链。
type CollectError struct {
	Stage string
	Err   error
}

func (e *CollectError) Error() string {
	if e == nil {
		return ""
	}
	if e.Err != nil {
		return "diag: stage " + e.Stage + ": " + e.Err.Error()
	}
	return "diag: stage " + e.Stage
}

func (e *CollectError) Unwrap() error { return e.Err }

// Phase 描述一个调用阶段(用于定位慢点)。
type Phase struct {
	Name      string
	SpanID    trace.SpanID
	Start     time.Time
	End       time.Time
	Duration  time.Duration
	Failed    bool
	ErrString string
}

// BlockEvent 描述一次阻塞等待。
type BlockEvent struct {
	At         time.Time
	Duration   time.Duration
	Operation  string
	Goroutines int
}

// CPUSample 是进程级 CPU 使用样本(基于资源用量,跨平台可测)。
type CPUSample struct {
	At            time.Time
	UserNanos     int64
	SystemNanos   int64
	VoluntaryCS   int64
	InvoluntaryCS int64
}

// TotalNanos 返回用户态+内核态 CPU 时间。
func (c CPUSample) TotalNanos() int64 { return c.UserNanos + c.SystemNanos }

// MemSample 是 Go 堆/运行时内存样本。
type MemSample struct {
	At          time.Time
	AllocBytes  uint64
	SysBytes    uint64
	HeapObjects uint64
	NumGC       uint32
	Goroutines  int
}

// Snapshot 是一次诊断采集的完整结果。
type Snapshot struct {
	Execution trace.ExecutionID
	TakenAt   time.Time
	StartedAt time.Time
	Duration  time.Duration
	Phases    []Phase
	Blocks    []BlockEvent
	CPUStart  CPUSample
	CPUEnd    CPUSample
	CPUUsed   time.Duration
	Memory    MemSample
	// ProfileBytes 在启用 CPU profile 时携带 pprof 字节流。
	ProfileBytes []byte
	// Failures 记录各采集阶段失败;非空时 Snapshot 仍尽量返回其余数据。
	Failures []CollectError
}

// HasFailures 报告快照是否存在采集失败。
func (s Snapshot) HasFailures() bool { return len(s.Failures) > 0 }

// Config 是诊断配置。
type Config struct {
	// EnableCPUProfile 是否同时采集 runtime/pprof CPU profile。
	EnableCPUProfile bool
	// CPUProfileHz 传递给 pprof 的采样率,<=0 使用运行时默认值。
	CPUProfileHz int
	// ProfileDuration 为 Snapshot 内 pprof 采集时长,<=0 使用默认 20ms。
	ProfileDuration time.Duration
	// failStages 配置注入失败的采集阶段(仅测试使用)。
	failStages map[string]bool
}

// Option 配置 Manager。
type Option func(*Config)

// WithCPUProfile 启用 pprof CPU profile 采集。
func WithCPUProfile(hz int, d time.Duration) Option {
	return func(c *Config) {
		c.EnableCPUProfile = true
		c.CPUProfileHz = hz
		c.ProfileDuration = d
	}
}

// WithFailStages 注入指定阶段的采集失败(仅测试用,验证失败语义)。
func WithFailStages(stages ...string) Option {
	return func(c *Config) {
		if c.failStages == nil {
			c.failStages = map[string]bool{}
		}
		for _, s := range stages {
			c.failStages[s] = true
		}
	}
}

// Session 是单次执行的诊断会话。
type Session struct {
	mu           sync.Mutex
	manager      *Manager
	execution    trace.ExecutionID
	startedAt    time.Time
	now          func() time.Time
	phases       []Phase
	blocks       []BlockEvent
	resultErr    string
	running      bool
	snapshotting bool
	cfg          Config
	cpuStart     CPUSample
	cpuOK        bool
}

// Execution 返回会话关联的执行标识。
func (s *Session) Execution() trace.ExecutionID { return s.execution }

// StartedAt 返回会话启动时间。
func (s *Session) StartedAt() time.Time { return s.startedAt }

// Running 报告会话是否活动。
func (s *Session) Running() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.running
}

// AppendPhase 追加一个已完成的调用阶段。会话结束后的追加被忽略并返回 false。
func (s *Session) AppendPhase(p Phase) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.running {
		return false
	}
	if p.End.IsZero() {
		p.End = s.now()
	}
	if p.Duration <= 0 {
		p.Duration = p.End.Sub(p.Start)
	}
	s.phases = append(s.phases, p)
	return true
}

// AppendBlock 记录一次阻塞等待。会话结束后的追加被忽略并返回 false。
func (s *Session) AppendBlock(op string, d time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.running {
		return false
	}
	s.blocks = append(s.blocks, BlockEvent{
		At:         s.now(),
		Duration:   d,
		Operation:  op,
		Goroutines: runtime.NumGoroutine(),
	})
	return true
}

// SetResultError 记录整个执行的结果错误(异常链定位)。
func (s *Session) SetResultError(err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.resultErr = err.Error()
}

// ResultError 返回记录的执行结果错误文本。
func (s *Session) ResultError() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.resultErr
}

// Snapshot 执行一次采集,语义如下:
//
//   - 会话未运行:ErrNotRunning;
//   - 同一快照并发触发:第二个起返回 ErrConcurrentSnapshot,不重复采集;
//   - 单个阶段失败:记录到 Failures 并返回包装了 ErrCollectionFailed 的错误,
//     其余阶段数据仍然返回(部分成功可解释);
//   - 成功:Snapshot.Failures 为空。
func (s *Session) Snapshot(ctx context.Context) (Snapshot, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return Snapshot{Execution: s.execution}, ErrNotRunning
	}
	if s.snapshotting {
		s.mu.Unlock()
		return Snapshot{Execution: s.execution}, ErrConcurrentSnapshot
	}
	s.snapshotting = true
	cfg := s.cfg
	now := s.now
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		s.snapshotting = false
		s.mu.Unlock()
	}()

	snap := Snapshot{
		Execution: s.execution,
		TakenAt:   now(),
		StartedAt: s.startedAt,
	}

	// 阶段与阻塞事件
	func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		snap.Phases = append([]Phase(nil), s.phases...)
		snap.Blocks = append([]BlockEvent(nil), s.blocks...)
	}()
	if cfg.failStages[stagePhase] {
		snap.Failures = append(snap.Failures, CollectError{Stage: stagePhase, Err: errInjected(stagePhase)})
	}
	if cfg.failStages[stageBlock] {
		snap.Failures = append(snap.Failures, CollectError{Stage: stageBlock, Err: errInjected(stageBlock)})
	}

	// CPU 起止用量(起始值在会话启动时已采样)
	s.mu.Lock()
	snap.CPUStart = s.cpuStart
	cpuOK := s.cpuOK
	s.mu.Unlock()
	endCPU, err := cpuUsage()
	if err != nil {
		snap.Failures = append(snap.Failures, CollectError{Stage: stageCPU, Err: err})
	} else if cfg.failStages[stageCPU] {
		snap.Failures = append(snap.Failures, CollectError{Stage: stageCPU, Err: errInjected(stageCPU)})
	} else {
		snap.CPUEnd = endCPU
		if cpuOK {
			snap.CPUUsed = time.Duration(endCPU.TotalNanos() - snap.CPUStart.TotalNanos())
		}
	}

	// 内存与 goroutine
	if cfg.failStages[stageMemory] {
		snap.Failures = append(snap.Failures, CollectError{Stage: stageMemory, Err: errInjected(stageMemory)})
	} else {
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		snap.Memory = MemSample{
			At:          now(),
			AllocBytes:  ms.Alloc,
			SysBytes:    ms.Sys,
			HeapObjects: ms.HeapObjects,
			NumGC:       ms.NumGC,
			Goroutines:  runtime.NumGoroutine(),
		}
	}

	// 可选 pprof CPU profile
	if cfg.EnableCPUProfile {
		d := cfg.ProfileDuration
		if d <= 0 {
			d = 20 * time.Millisecond
		}
		if cfg.failStages[stageProfile] {
			snap.Failures = append(snap.Failures, CollectError{Stage: stageProfile, Err: errInjected(stageProfile)})
		} else if err := captureCPUProfile(ctx, &snap, d, cfg.CPUProfileHz); err != nil {
			snap.Failures = append(snap.Failures, CollectError{Stage: stageProfile, Err: err})
		}
	}

	snap.Duration = now().Sub(s.startedAt)

	if len(snap.Failures) > 0 {
		return snap, failWithStages(snap.Failures)
	}
	return snap, nil
}

type injectedError struct{ stage string }

func (e *injectedError) Error() string { return "diag: injected failure at stage " + e.stage }

func errInjected(stage string) error { return &injectedError{stage: stage} }

// failWithStages 把阶段失败包装为 ErrCollectionFailed,保留完整失败链。
func failWithStages(failures []CollectError) error {
	all := make([]error, 0, len(failures)+1)
	all = append(all, ErrCollectionFailed)
	for i := range failures {
		f := failures[i]
		all = append(all, &f)
	}
	return errors.Join(all...)
}

func captureCPUProfile(ctx context.Context, snap *Snapshot, d time.Duration, hz int) error {
	var buf bytes.Buffer
	if hz > 0 {
		// pprof 文档要求在 StartCPUProfile 之前设置采样率。
		runtime.SetCPUProfileRate(hz)
	}
	if err := pprof.StartCPUProfile(&buf); err != nil {
		return err
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-ctx.Done():
		pprof.StopCPUProfile()
		snap.ProfileBytes = append([]byte(nil), buf.Bytes()...)
		return ctx.Err()
	}
	pprof.StopCPUProfile()
	snap.ProfileBytes = append([]byte(nil), buf.Bytes()...)
	return nil
}

// Manager 管理按执行标识索引的诊断会话。
type Manager struct {
	mu     sync.Mutex
	tracer *trace.Tracer
	cfg    Config
	now    func() time.Time
	active map[trace.ExecutionID]*Session
}

// NewManager 创建 Manager,可绑定 Tracer 使调用阶段同时生成跨度。
func NewManager(tr *trace.Tracer, opts ...Option) *Manager {
	cfg := Config{}
	for _, o := range opts {
		o(&cfg)
	}
	return &Manager{
		tracer: tr,
		cfg:    cfg,
		now:    time.Now,
		active: map[trace.ExecutionID]*Session{},
	}
}

// Start 为一次执行启动诊断会话;重复启动返回 ErrSessionActive。
func (m *Manager) Start(execution trace.ExecutionID) (*Session, error) {
	if execution == "" {
		return nil, ErrUnknownExecution
	}
	startCPU, err := cpuUsage()

	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.active[execution]; ok {
		return nil, ErrSessionActive
	}
	s := &Session{
		manager:   m,
		execution: execution,
		startedAt: m.now(),
		now:       m.now,
		running:   true,
		cfg:       m.cfg,
	}
	if err != nil {
		// 起始 CPU 不可用时不阻塞会话:快照阶段会把 CPU 阶段标记失败。
		s.cpuOK = false
	} else {
		s.cpuStart = startCPU
		s.cpuOK = true
	}
	m.active[execution] = s
	return s, nil
}

// Get 返回执行的活动会话,不存在返回 ErrUnknownExecution。
func (m *Manager) Get(execution trace.ExecutionID) (*Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.active[execution]
	if !ok {
		return nil, ErrUnknownExecution
	}
	return s, nil
}

// Stop 结束会话。
//
//   - 不存在/已结束:ErrNotRunning 或 ErrUnknownExecution(可区分);
//   - 快照正在进行:等待其完成后再结束,不会把采集中途截断;
//   - 正常结束:会话从活动表移除,并在绑定 tracer 时标记执行结束。
func (m *Manager) Stop(execution trace.ExecutionID) error {
	m.mu.Lock()
	s, ok := m.active[execution]
	m.mu.Unlock()
	if !ok {
		return ErrUnknownExecution
	}

	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return ErrNotRunning
	}
	snapshotting := s.snapshotting
	s.mu.Unlock()
	_ = snapshotting // 快照自身通过 s.snapshotting 串行化,这里轮询等待其退出。

	for {
		s.mu.Lock()
		busy := s.snapshotting
		if !busy {
			s.running = false
			s.mu.Unlock()
			break
		}
		s.mu.Unlock()
		time.Sleep(time.Millisecond)
	}

	m.mu.Lock()
	delete(m.active, execution)
	m.mu.Unlock()
	if m.tracer != nil {
		m.tracer.EndExecution(execution)
	}
	return nil
}

// Snapshot 直接对某次执行采集快照;无活动会话返回 ErrUnknownExecution。
func (m *Manager) Snapshot(ctx context.Context, execution trace.ExecutionID) (Snapshot, error) {
	s, err := m.Get(execution)
	if err != nil {
		return Snapshot{Execution: execution}, err
	}
	return s.Snapshot(ctx)
}

// StopAll 结束全部活动会话,返回各会话结束时遇到的错误(按执行标识)。
// 停止顺序按执行标识排序,保证结果稳定可重复。
func (m *Manager) StopAll() map[trace.ExecutionID]error {
	m.mu.Lock()
	ids := make([]trace.ExecutionID, 0, len(m.active))
	for id := range m.active {
		ids = append(ids, id)
	}
	m.mu.Unlock()
	// 简单稳定排序。
	for i := 1; i < len(ids); i++ {
		for j := i; j > 0 && ids[j-1] > ids[j]; j-- {
			ids[j-1], ids[j] = ids[j], ids[j-1]
		}
	}
	errs := map[trace.ExecutionID]error{}
	for _, id := range ids {
		if err := m.Stop(id); err != nil {
			errs[id] = err
		}
	}
	return errs
}

type sessionContextKey struct{}

func withSession(ctx context.Context, s *Session) context.Context {
	return context.WithValue(ctx, sessionContextKey{}, s)
}

func sessionFromContext(ctx context.Context) *Session {
	if ctx == nil {
		return nil
	}
	s, _ := ctx.Value(sessionContextKey{}).(*Session)
	return s
}

// WithSession 把会话放入 context,供 WithPhase 使用。
func WithSession(ctx context.Context, s *Session) context.Context {
	if s == nil {
		return ctx
	}
	return withSession(ctx, s)
}

// WithPhase 在上下文中的活动会话上记录一个调用阶段;
// 若 Manager 绑定了 Tracer,同时生成同因果链的子跨度。
//
// 阶段包含开始/结束时间、耗时、跨度 ID 与错误信息,fn 的返回值原样透传,
// 观测逻辑绝不改变业务结果。
func (m *Manager) WithPhase(ctx context.Context, name string, fn func(context.Context) error) error {
	return m.withPhase(ctx, name, fn)
}

func (m *Manager) withPhase(ctx context.Context, name string, fn func(context.Context) error) error {
	s := sessionFromContext(ctx)
	if s == nil {
		// 没有活动会话时不创建隐式会话;fn 照常执行,观测降级为 no-op。
		return fn(ctx)
	}

	var span *trace.Span
	start := s.now()
	sc, _ := trace.FromContext(ctx)
	if m.tracer != nil && sc.Valid() {
		var inner *trace.Span
		ctx, inner = m.tracer.Start(ctx, name)
		span = inner
	}

	err := fn(ctx)

	end := s.now()
	p := Phase{
		Name:     name,
		Start:    start,
		End:      end,
		Duration: end.Sub(start),
	}
	if span != nil {
		p.SpanID = span.ID()
		span.End(err)
	}
	if err != nil {
		p.Failed = true
		p.ErrString = err.Error()
	}
	s.AppendPhase(p)
	return err
}

// WithPhase 是包级便捷函数,使用 ctx 中的活动会话;
// 需要 tracer 联动时请使用 Manager.WithPhase。
func WithPhase(ctx context.Context, name string, fn func(context.Context) error) error {
	s := sessionFromContext(ctx)
	if s == nil {
		return fn(ctx)
	}
	return s.manager.withPhase(ctx, name, fn)
}
