// Copyright IBM Corp. 2014, 2026
// SPDX-License-Identifier: BUSL-1.1

package communicator

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/hashicorp/terraform/internal/tracing"
)

// TestRetry_operationIDAcrossAttempts verifies that connection retries keep
// the operation ID of the operation performing them, across failing and
// successful attempts (rubric 3).
func TestRetry_operationIDAcrossAttempts(t *testing.T) {
	origInitial := initialBackoffDelay
	origMax := maxBackoffDelay
	initialBackoffDelay = time.Millisecond
	maxBackoffDelay = 5 * time.Millisecond
	t.Cleanup(func() {
		initialBackoffDelay = origInitial
		maxBackoffDelay = origMax
	})

	tracing.SetSampleRate(1)
	tracing.Reset()
	t.Cleanup(tracing.Reset)

	ctx, id, endOp := tracing.StartOperation(context.Background(), "test.communicator.retry")

	var attempts int
	err := Retry(ctx, func() error {
		attempts++
		if got, ok := tracing.OperationIDFromContext(ctx); !ok || got != id {
			t.Errorf("attempt %d sees operation ID %q; want %q", attempts, got, id)
		}
		if attempts < 3 {
			return errors.New("connection reset")
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
	for _, e := range tracing.MetricEventsForOperation(id) {
		if e.OperationID != id {
			t.Fatalf("event attributed to %q; want %q", e.OperationID, id)
		}
	}
	// Two retryable warnings and two backoff sleeps, one per failed attempt.
	var warnings, sleeps int
	for _, e := range tracing.LogEventsForOperation(id) {
		switch e.Message {
		case "retryable error":
			warnings++
		case "sleeping between retries":
			sleeps++
		}
	}
	if warnings != 2 {
		t.Errorf("retryable logs = %d; want 2", warnings)
	}
	if sleeps != 2 {
		t.Errorf("backoff logs = %d; want 2", sleeps)
	}
	if got := tracing.OpenSpanCount(id); got != 0 {
		t.Fatalf("open spans after retry = %d; want 0", got)
	}
}

// TestRetry_interruptedStillClosesSpans checks the existing interruption
// semantics are preserved ("interrupted - last error: ...") and no span is
// left open when the context expires (rubrics 8 and 10).
func TestRetry_interruptedStillClosesSpans(t *testing.T) {
	origInitial := initialBackoffDelay
	origMax := maxBackoffDelay
	initialBackoffDelay = time.Millisecond
	maxBackoffDelay = 5 * time.Millisecond
	t.Cleanup(func() {
		initialBackoffDelay = origInitial
		maxBackoffDelay = origMax
	})

	tracing.SetSampleRate(1)
	tracing.Reset()
	t.Cleanup(tracing.Reset)

	ctx, cancel := context.WithCancel(context.Background())
	ctx, id, endOp := tracing.StartOperation(ctx, "test.communicator.retry.canceled")

	started := make(chan struct{})
	done := make(chan struct{})
	var retErr error
	go func() {
		defer close(done)
		retErr = Retry(ctx, func() error {
			select {
			case started <- struct{}{}:
			default:
			}
			return errors.New("never connects")
		})
	}()

	<-started
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && tracing.CounterSum(id, "retry.attempt") < 1 {
		time.Sleep(time.Millisecond)
	}

	cancel()
	<-done
	endOp(nil)

	if retErr == nil {
		t.Fatal("expected an interruption error, got nil")
	}
	if got := tracing.OpenSpanCount(id); got != 0 {
		t.Fatalf("open spans after interruption = %d; want 0", got)
	}
	if len(tracing.MetricEventsForOperation(id)) == 0 {
		t.Fatal("interrupted retry kept no attributable records")
	}
}
