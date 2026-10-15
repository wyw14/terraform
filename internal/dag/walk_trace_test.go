// Copyright IBM Corp. 2014, 2026
// SPDX-License-Identifier: BUSL-1.1

package dag

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/terraform/internal/tfdiags"
	"github.com/hashicorp/terraform/internal/tracing"
)

// TestWalkWithContext_vertexOperationIDs proves that, even though every
// vertex runs in its own goroutine, all vertex visits are attributed to the
// single operation ID of the walk (rubric 2), and finish without leaking
// open spans (rubric 8).
func TestWalkWithContext_vertexOperationIDs(t *testing.T) {
	tracing.SetSampleRate(1)
	tracing.Reset()
	t.Cleanup(tracing.Reset)

	var g AcyclicGraph
	// A fan-out: a root feeding eight independent leaves, so the leaves run
	// concurrently.
	const leaves = 8
	root := testNamedString("root")
	g.Add(root)
	for i := range leaves {
		leaf := testNamedString(leafName(i))
		g.Add(leaf)
		g.Connect(root, leaf)
	}

	ctx, id, endOp := tracing.StartOperation(context.Background(), "test.dag.walk")

	var mu sync.Mutex
	seen := make(map[string]int)
	cb := func(v Vertex) tfdiags.Diagnostics {
		// Simulate a little concurrent work.
		time.Sleep(2 * time.Millisecond)
		mu.Lock()
		seen[v.Name()]++
		mu.Unlock()
		// From inside the vertex goroutine the walk's operation must still
		// be identifiable.
		if got, ok := tracing.OperationIDFromContext(ctx); !ok || got != id {
			t.Errorf("vertex %q sees operation ID %q; want %q", v.Name(), got, id)
		}
		tracing.IncrCounter(ctx, "vertex.visit", tracing.StringAttr("vertex", v.Name()))
		return nil
	}

	if diags := g.WalkWithContext(ctx, cb); diags.HasErrors() {
		t.Fatalf("unexpected diagnostics: %s", diags.Err())
	}
	endOp(nil)

	if len(seen) != leaves+1 {
		t.Fatalf("visited %d unique vertices; want %d", len(seen), leaves+1)
	}
	for name, n := range seen {
		if n != 1 {
			t.Errorf("vertex %q visited %d times; want 1", name, n)
		}
	}

	if got := tracing.CounterSum(id, "dag.vertex.started"); got != leaves+1 {
		t.Fatalf("dag.vertex.started = %v; want %d", got, leaves+1)
	}
	if got := tracing.CounterSum(id, "vertex.visit"); got != leaves+1 {
		t.Fatalf("vertex.visit = %v; want %d", got, leaves+1)
	}
	for _, e := range tracing.MetricEventsForOperation(id) {
		if e.OperationID != id {
			t.Fatalf("event attributed to %q; want %q", e.OperationID, id)
		}
	}
	if got := tracing.OpenSpanCount(id); got != 0 {
		t.Fatalf("open spans after walk = %d; want 0", got)
	}
}

// TestWalkWithContext_concurrentWalksIsolated walks two graphs under two
// different operations at once and proves neither aggregate is contaminated
// by the other (rubric 7).
func TestWalkWithContext_concurrentWalksIsolated(t *testing.T) {
	tracing.SetSampleRate(1)
	tracing.Reset()
	t.Cleanup(tracing.Reset)

	buildGraph := func(prefix string) *AcyclicGraph {
		var g AcyclicGraph
		for i := range 6 {
			g.Add(testNamedString(prefix + "-" + itoa(i)))
		}
		return &g
	}

	ctxA, idA, endA := tracing.StartOperation(context.Background(), "walk-A")
	ctxB, idB, endB := tracing.StartOperation(context.Background(), "walk-B")
	gA := buildGraph("a")
	gB := buildGraph("b")

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		gA.WalkWithContext(ctxA, func(v Vertex) tfdiags.Diagnostics {
			tracing.IncrCounter(ctxA, "walk.a", tracing.StringAttr("vertex", v.Name()))
			time.Sleep(time.Millisecond)
			return nil
		})
	}()
	go func() {
		defer wg.Done()
		gB.WalkWithContext(ctxB, func(v Vertex) tfdiags.Diagnostics {
			tracing.IncrCounter(ctxB, "walk.b", tracing.StringAttr("vertex", v.Name()))
			time.Sleep(time.Millisecond)
			return nil
		})
	}()
	wg.Wait()
	endA(nil)
	endB(nil)

	if got := tracing.CounterSum(idA, "walk.a"); got != 6 {
		t.Errorf("walk.a counter for op A = %v; want 6", got)
	}
	if got := tracing.CounterSum(idA, "walk.b"); got != 0 {
		t.Errorf("op A contains %v walk.b events", got)
	}
	if got := tracing.CounterSum(idB, "walk.b"); got != 6 {
		t.Errorf("walk.b counter for op B = %v; want 6", got)
	}
	if got := tracing.CounterSum(idB, "walk.a"); got != 0 {
		t.Errorf("op B contains %v walk.a events", got)
	}
}

// TestWalk_withoutContextUnchanged ensures the context-free Walk path keeps
// behaving exactly as before: no operation is synthesized and no aggregate is
// written (rubric 9).
func TestWalk_withoutContextUnchanged(t *testing.T) {
	tracing.SetSampleRate(1)
	tracing.Reset()
	t.Cleanup(tracing.Reset)

	var g AcyclicGraph
	g.Add(testNamedString("lonely"))

	var visited int
	if diags := g.Walk(func(v Vertex) tfdiags.Diagnostics {
		visited++
		return nil
	}); diags.HasErrors() {
		t.Fatalf("unexpected diagnostics: %s", diags.Err())
	}
	if visited != 1 {
		t.Fatalf("visited = %d; want 1", visited)
	}
	if ids := tracing.OperationIDs(); len(ids) != 0 {
		t.Fatalf("context-free walk unexpectedly created operations: %v", ids)
	}
}

func leafName(i int) string { return "leaf-" + itoa(i) }

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}
