// Copyright IBM Corp. 2014, 2026
// SPDX-License-Identifier: BUSL-1.1

package remote

import (
	"context"
	"log"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/terraform/internal/tracing"
)

// Fatal implements a RetryBackoff func return value that, if encountered,
// signals that the func should not be retried. In that case, the error
// returned by the interface method will be returned by RetryBackoff
type Fatal interface {
	FatalError() error
}

// NonRetryableError is a simple implementation of Fatal that wraps an error
type NonRetryableError struct {
	InnerError error
}

// FatalError returns the inner error, but also implements Fatal, which
// signals to RetryBackoff that a non-retryable error occurred.
func (e NonRetryableError) FatalError() error {
	return e.InnerError
}

// Error returns the inner error string
func (e NonRetryableError) Error() string {
	return e.InnerError.Error()
}

var (
	initialBackoffDelay = time.Second
	maxBackoffDelay     = 3 * time.Second
)

// RetryBackoff retries function f until nil or a FatalError is returned.
// RetryBackoff only returns an error if the context is in error or if a
// FatalError was encountered.
//
// The whole loop and each attempt share the operation ID carried by ctx, so
// retried work stays attributable to the operation that started it. The
// retry/backoff/cancellation semantics are unchanged.
func RetryBackoff(ctx context.Context, f func() error) (retErr error) {
	retryCtx, endRetry := tracing.StartRetry(ctx, "remote.RetryBackoff")
	defer endRetry(&retErr)

	// doneCh signals that the routine is done and sends the last error
	var doneCh = make(chan struct{})
	var errVal atomic.Value
	type errWrap struct {
		E error
	}

	go func() {
		// the retry delay between each attempt
		var delay time.Duration = 0
		defer close(doneCh)

		attempt := 0
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}

			attempt++
			var err error
			func() {
				_, endAttempt := tracing.StartAttempt(retryCtx, attempt)
				defer endAttempt(&err)
				err = f()
			}()

			switch e := err.(type) {
			case nil:
				return
			case Fatal:
				errVal.Store(errWrap{e.FatalError()})
				return
			}

			delay *= 2
			if delay == 0 {
				delay = initialBackoffDelay
			}

			delay = min(delay, maxBackoffDelay)

			log.Printf("[WARN] retryable error: %q, delaying for %s", err, delay)
			tracing.RecordLog(retryCtx, hclog.Warn.String(), "retryable error",
				tracing.StringAttr("retry.kind", "remote.RetryBackoff"),
				tracing.StringAttr("retry.attempt", strconv.Itoa(attempt)),
				tracing.StringAttr("delay", delay.String()),
				tracing.StringAttr("error", err.Error()),
			)
		}
	}()

	// Wait until done or deadline
	select {
	case <-doneCh:
	case <-ctx.Done():
	}

	err, hadErr := errVal.Load().(errWrap)
	var lastErr error
	if hadErr {
		lastErr = err.E
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}

	return lastErr
}
