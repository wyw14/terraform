// Copyright IBM Corp. 2014, 2026
// SPDX-License-Identifier: BUSL-1.1

package promising_test

import (
	"context"
	"testing"
	"time"

	"github.com/hashicorp/terraform/internal/promising"
	"github.com/hashicorp/terraform/internal/tracing"
)

// TestAsyncTask_inheritsOperationID checks that the detached-root span used
// for asynchronous promise tasks does not detach the operation identity: the
// task and all of its records stay attributable to the caller's operation
// (rubric 2).
func TestAsyncTask_inheritsOperationID(t *testing.T) {
	tracing.SetSampleRate(1)
	tracing.Reset()
	t.Cleanup(tracing.Reset)

	rootCtx, id, endOp := tracing.StartOperation(context.Background(), "test.promising")

	const wantVal = "hello"
	gotVal, err := promising.MainTask(rootCtx, func(ctx context.Context) (string, error) {
		resolver, get := promising.NewPromise[string](ctx, "test")

		promising.AsyncTask(
			ctx, resolver,
			func(ctx context.Context, resolver promising.PromiseResolver[string]) {
				// The span parent is deliberately cut, but the operation ID
				// is an ordinary context value and must survive.
				gotID, ok := tracing.OperationIDFromContext(ctx)
				if !ok || gotID != id {
					t.Errorf("async task sees operation ID %q; want %q", gotID, id)
				}
				tracing.IncrCounter(ctx, "promising.task.work")
				tracing.RecordLog(ctx, "INFO", "resolving from async task")
				resolver.Resolve(ctx, wantVal, nil)
			},
		)
		return get(ctx)
	})
	endOp(nil)

	if err != nil {
		t.Fatal(err)
	}
	if gotVal != wantVal {
		t.Fatalf("got %q; want %q", gotVal, wantVal)
	}

	if got := tracing.CounterSum(id, "promising.async_task"); got != 1 {
		t.Errorf("promising.async_task = %v; want 1", got)
	}
	if got := tracing.CounterSum(id, "promising.task.work"); got != 1 {
		t.Errorf("promising.task.work = %v; want 1", got)
	}
	if len(tracing.LogEventsForOperation(id)) == 0 {
		t.Error("async task produced no attributable log records")
	}

	// The detached span must still end once the task completes; poll because
	// the task goroutine closes the span shortly after MainTask returns.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if tracing.OpenSpanCount(id) == 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("open spans for operation %q never reached zero", id)
}
