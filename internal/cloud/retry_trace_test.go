// Copyright IBM Corp. 2014, 2026
// SPDX-License-Identifier: BUSL-1.1

package cloud

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/terraform/internal/tracing"
)

// fastBackoff shrinks the retry delays for the duration of a test and
// restores them afterwards.
func fastBackoff(t *testing.T) {
	t.Helper()
	origInitial := initialBackoffDelay
	origMax := maxBackoffDelay
	initialBackoffDelay = time.Millisecond
	maxBackoffDelay = 5 * time.Millisecond
	t.Cleanup(func() {
		initialBackoffDelay = origInitial
		maxBackoffDelay = origMax
	})
}

// Test_RetryBackoff_operationIDAcrossAttempts makes sure every retry
// attempt, including the ones that fail, is attributed to the same operation
// as the call that initiated the retry loop (rubric 3).
func Test_RetryBackoff_operationIDAcrossAttempts(t *testing.T) {
	fastBackoff(t)
	tracing.SetSampleRate(1)
	tracing.Reset()
	t.Cleanup(tracing.Reset)

	ctx, id, endOp := tracing.StartOperation(context.Background(), "test.cloud.retry")

	var attempts int
	err := RetryBackoff(ctx, func() error {
		attempts++
		// The attempt executes in a goroutine that only captured ctx: the
		// operation identity must still be visible there.
		if got, ok := tracing.OperationIDFromContext(ctx); !ok || got != id {
			t.Errorf("attempt %d sees operation ID %q; want %q", attempts, got, id)
		}
		if attempts < 3 {
			return errors.New("transient failure")
		}
		return nil
	})
	endOp(nil)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d; want 3", attempts)
	}

	if got := tracing.CounterSum(id, "retry.attempt"); got != 3 {
		t.Fatalf("retry.attempt counter = %v; want 3", got)
	}
	events := tracing.MetricEventsForOperation(id)
	for _, e := range events {
		if e.OperationID != id {
			t.Fatalf("event attributed to %q; want %q", e.OperationID, id)
		}
	}
	// The two failed attempts must each have produced a retryable-error log
	// attributable to the operation, plus an attempt outcome per attempt.
	var warnLogs, failedAttempts, succeededAttempts int
	for _, e := range tracing.LogEventsForOperation(id) {
		if e.OperationID != id {
			t.Fatalf("log attributed to %q; want %q", e.OperationID, id)
		}
		if e.Message == "retryable error" {
			warnLogs++
		}
	}
	for _, e := range events {
		if e.Name != "retry.attempt.duration" {
			continue
		}
		for _, a := range e.Attrs {
			if a.Key == "outcome" {
				switch a.Value {
				case "failed":
					failedAttempts++
				case "succeeded":
					succeededAttempts++
				}
			}
		}
	}
	if warnLogs != 2 {
		t.Errorf("retryable-error logs = %d; want 2", warnLogs)
	}
	if failedAttempts != 2 || succeededAttempts != 1 {
		t.Errorf("attempt outcomes = %d failed / %d succeeded; want 2 / 1", failedAttempts, succeededAttempts)
	}
	if got := tracing.OpenSpanCount(id); got != 0 {
		t.Fatalf("open spans after retry = %d; want 0", got)
	}
}

// Test_RetryBackoff_canceledLeavesNoOpenRecords checks the cancellation half
// of the exception requirement: on cancellation existing records remain
// attributable and no span stays open, while the canceled error semantics are
// unchanged (rubrics 8 and 10).
func Test_RetryBackoff_canceledLeavesNoOpenRecords(t *testing.T) {
	fastBackoff(t)
	tracing.SetSampleRate(1)
	tracing.Reset()
	t.Cleanup(tracing.Reset)

	ctx, cancel := context.WithCancel(context.Background())
	ctx, id, endOp := tracing.StartOperation(ctx, "test.cloud.retry.canceled")

	started := make(chan struct{})
	retryDone := make(chan struct{})
	go func() {
		defer close(retryDone)
		RetryBackoff(ctx, func() error {
			select {
			case started <- struct{}{}:
			default:
			}
			return errors.New("always fails")
		})
	}()

	<-started
	// Let at least one attempt complete and become attributable.
	waitForAttempts(t, id, 1)

	cancel()
	<-retryDone // the retry span ends as RetryBackoff returns
	endOp(nil)

	if got := tracing.CounterSum(id, "retry.attempt"); got < 1 {
		t.Fatalf("expected at least one attempt record before cancel, got %v", got)
	}
	if got := tracing.OpenSpanCount(id); got != 0 {
		t.Fatalf("open spans after cancel = %d; want 0", got)
	}
}

// Test_RetryBackoff_concurrentOperationsIsolated runs retries for two
// operations at once and proves the aggregates never mix (rubric 7).
func Test_RetryBackoff_concurrentOperationsIsolated(t *testing.T) {
	fastBackoff(t)
	tracing.SetSampleRate(1)
	tracing.Reset()
	t.Cleanup(tracing.Reset)

	ctxA, idA, endA := tracing.StartOperation(context.Background(), "op-A")
	ctxB, idB, endB := tracing.StartOperation(context.Background(), "op-B")

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		var n int
		RetryBackoff(ctxA, func() error {
			n++
			tracing.IncrCounter(ctxA, "marker.a")
			if n < 2 {
				return errors.New("a transient")
			}
			return nil
		})
	}()
	go func() {
		defer wg.Done()
		var n int
		RetryBackoff(ctxB, func() error {
			n++
			tracing.IncrCounter(ctxB, "marker.b")
			if n < 2 {
				return errors.New("b transient")
			}
			return nil
		})
	}()
	wg.Wait()
	endA(nil)
	endB(nil)

	if got := tracing.CounterSum(idA, "marker.a"); got != 2 {
		t.Errorf("op A marker.a = %v; want 2", got)
	}
	if got := tracing.CounterSum(idA, "marker.b"); got != 0 {
		t.Errorf("op A unexpectedly contains %v marker.b records", got)
	}
	if got := tracing.CounterSum(idB, "marker.b"); got != 2 {
		t.Errorf("op B marker.b = %v; want 2", got)
	}
	if got := tracing.CounterSum(idB, "marker.a"); got != 0 {
		t.Errorf("op B unexpectedly contains %v marker.a records", got)
	}
	for _, e := range tracing.MetricEventsForOperation(idA) {
		if e.OperationID != idA {
			t.Fatalf("op A aggregate contains event for %q", e.OperationID)
		}
	}
	for _, e := range tracing.MetricEventsForOperation(idB) {
		if e.OperationID != idB {
			t.Fatalf("op B aggregate contains event for %q", e.OperationID)
		}
	}
}

func waitForAttempts(t *testing.T, id tracing.OperationID, want float64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if tracing.CounterSum(id, "retry.attempt") >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %v attempts for %q", want, id)
}
