package trace

import (
	"context"
	"sync"
	"testing"
)

func TestSyncCausalChain(t *testing.T) {
	tr := New()
	ctx, root := tr.StartExecution("exec-1", "root", context.Background())
	ctx, child := tr.Start(ctx, "child")
	_, grand := tr.Start(ctx, "grand")
	child.End(nil)
	grand.End(nil)
	root.End(nil)

	spans := tr.ExecutionSpans("exec-1")
	if len(spans) != 3 {
		t.Fatalf("want 3 spans, got %d", len(spans))
	}
	if spans[0].ID != root.ID() || spans[0].Parent != "" {
		t.Fatalf("root chain wrong: %+v", spans[0])
	}
	if spans[1].Parent != root.ID() {
		t.Fatalf("child parent = %s, want %s", spans[1].Parent, root.ID())
	}
	if spans[2].Parent != child.ID() {
		t.Fatalf("grand parent = %s, want %s", spans[2].Parent, child.ID())
	}
	if len(spans[1].Issues) != 0 {
		t.Fatalf("unexpected issues: %v", spans[1].Issues)
	}
}

func TestMissingContextMarked(t *testing.T) {
	tr := New()
	_, s := tr.Start(context.Background(), "orphan")
	if got := s.Issues(); len(got) != 1 || got[0] != IssueMissingContext {
		t.Fatalf("want [missing_context], got %v", got)
	}
	if tr.ExecutionSpans(s.Execution())[0].Parent != "" {
		t.Fatal("orphan span must not attach to any parent")
	}
}

func TestFinishedAndUnknownParent(t *testing.T) {
	tr := New()
	ctx, root := tr.StartExecution("e", "root", context.Background())
	root.End(nil)
	// 同步场景:父跨度已结束必须被标记,而不是静默挂链。
	_, after := tr.Start(ctx, "after")
	if !hasIssue(after.Issues(), IssueFinishedParent) {
		t.Fatalf("want finished_parent, got %v", after.Issues())
	}
	if after.Parent() != root.ID() {
		t.Fatal("finished-parent span keeps the intended link")
	}

	// 未知父跨度:脱离可疑父节点。
	bogus := SpanContext{Execution: "e", Span: "nope"}
	_, unknown := tr.Start(WithContext(context.Background(), bogus), "x")
	if !hasIssue(unknown.Issues(), IssueUnknownParent) {
		t.Fatalf("want unknown_parent, got %v", unknown.Issues())
	}
	if unknown.Parent() != "" {
		t.Fatalf("unknown parent must not be linked, got %s", unknown.Parent())
	}
}

func TestEnqueueDequeueAndCrossExecution(t *testing.T) {
	tr := New()
	ctx, root := tr.StartExecution("job-1", "root", context.Background())

	// 缺失上下文入队必须被识别。
	if _, issue := tr.Enqueue(context.Background()); issue != IssueMissingContext {
		t.Fatalf("want missing context, got %v", issue)
	}

	carrier, issue := tr.Enqueue(ctx)
	if issue != IssueNone || !carrier.Valid() {
		t.Fatalf("enqueue failed: %v %+v", issue, carrier)
	}
	root.End(nil)

	// 执行已结束后再用旧载体:跨执行继承必须被识别。
	tr.EndExecution("job-1")
	_, stale := tr.StartFromCarrier(carrier, "stale")
	if stale != IssueCrossExecution {
		t.Fatalf("want cross_execution on stale carrier, got %v", stale)
	}

	// 损坏载体
	_, mal := tr.StartFromCarrier(Carrier{Execution: "x"}, "bad")
	if mal != IssueMalformedCarrier {
		t.Fatalf("want malformed_carrier, got %v", mal)
	}
	// 引用未知执行的"完整"载体也属于损坏,不得挂错链路。
	_, unknown := tr.StartFromCarrier(Carrier{Execution: "ghost", Span: "9"}, "bad2")
	if unknown != IssueMalformedCarrier {
		t.Fatalf("want malformed_carrier for unknown exec, got %v", unknown)
	}
}

func TestAsyncForkChain(t *testing.T) {
	tr := New()
	ctx, root := tr.StartExecution("a", "root", context.Background())
	forkCtx, async, issue := tr.ForkAsync(ctx, "worker", map[string]string{"k": "v"})
	if issue != IssueNone {
		t.Fatalf("unexpected issue: %v", issue)
	}
	if async.Kind() != SpanKindAsync {
		t.Fatalf("want async kind, got %v", async.Kind())
	}
	sc, ok := FromContext(forkCtx)
	if !ok || sc.Span != async.ID() {
		t.Fatal("fork context does not carry async span")
	}
	if async.Parent() != root.ID() {
		t.Fatalf("async parent = %s, want %s", async.Parent(), root.ID())
	}
	if async.Snapshot().Attrs["k"] != "v" {
		t.Fatal("fork attrs lost")
	}
}

func TestDuplicateExecutionStartMarked(t *testing.T) {
	tr := New()
	_, first := tr.StartExecution("dup", "root", context.Background())
	first.End(nil)
	_, second := tr.StartExecution("dup", "root2", context.Background())
	if !hasIssue(second.Issues(), IssueCrossExecution) {
		t.Fatalf("duplicate exec start must be marked, got %v", second.Issues())
	}
}

func TestConcurrentSpansNoCorruption(t *testing.T) {
	tr := New()
	ctx, root := tr.StartExecution("conc", "root", context.Background())

	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, s := tr.Start(ctx, "parallel")
			s.AddAttr("g", "1")
			s.MarkIssue(IssueFinishedParent)
			s.MarkIssue(IssueFinishedParent) // 去重
			s.AddEvent("tick", "", nil)
			s.End(nil)
		}()
	}
	wg.Wait()
	root.End(nil)

	spans := tr.ExecutionSpans("conc")
	if len(spans) != 201 {
		t.Fatalf("want 201 spans, got %d", len(spans))
	}
	for _, sp := range spans[1:] {
		if sp.Parent != root.ID() {
			t.Fatalf("span %s parent %s", sp.ID, sp.Parent)
		}
		if len(sp.Issues) != 1 || sp.Issues[0] != IssueFinishedParent {
			t.Fatalf("dedup/issue wrong for %s: %v", sp.ID, sp.Issues)
		}
	}
}

func TestEndIdempotent(t *testing.T) {
	tr := New()
	_, s := tr.StartExecution("e", "root", context.Background())
	s.End(nil)
	first := s.EndTime()
	s.End(nil)
	if s.EndTime() != first || !s.Ended() {
		t.Fatal("End must be idempotent")
	}
}

func hasIssue(list []Issue, want Issue) bool {
	for _, i := range list {
		if i == want {
			return true
		}
	}
	return false
}
