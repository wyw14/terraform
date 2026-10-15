// Copyright IBM Corp. 2014, 2026
// SPDX-License-Identifier: BUSL-1.1

package batch

import (
	"context"
	"sync"
	"time"
)

// Limiter gates step execution. The processor calls Wait once immediately
// before running every step, so the limiter bounds the rate at which side
// effects are produced across all workers of a processor.
//
// Wait must honor context cancellation so that a shutting-down processor is
// never stuck waiting for a permit: it should return the context error when
// ctx is done before a permit becomes available.
//
// Implementations must be safe for concurrent use.
type Limiter interface {
	Wait(ctx context.Context) error
}

// NopLimiter never blocks. It is the default when no limiter is configured.
type NopLimiter struct{}

// Wait implements [Limiter].
func (NopLimiter) Wait(context.Context) error { return nil }

// minTimerGranularity keeps token-bucket wake-ups from busy looping when
// clock math produces a vanishingly short duration.
const minTimerGranularity = 500 * time.Microsecond

// TokenBucket is a thread-safe token bucket rate limiter.
//
// Tokens accrue continuously at Rate tokens per second up to Burst, and each
// Wait consumes one token. This permits short bursts of up to Burst steps
// while sustaining the configured average rate across all workers.
type TokenBucket struct {
	mu     sync.Mutex
	rate   float64 // tokens accrued per second
	burst  float64
	tokens float64
	last   time.Time
}

// NewTokenBucket returns a limiter allowing rate tokens per second with up
// to burst accumulated tokens. rate must be positive and burst must be at
// least one.
func NewTokenBucket(rate float64, burst int) *TokenBucket {
	if rate <= 0 {
		panic("batch: NewTokenBucket requires a positive rate")
	}
	if burst < 1 {
		panic("batch: NewTokenBucket requires burst >= 1")
	}
	return &TokenBucket{
		rate:   rate,
		burst:  float64(burst),
		tokens: float64(burst),
		last:   time.Now(),
	}
}

// Wait implements [Limiter], blocking until a token is available or ctx is
// canceled.
func (l *TokenBucket) Wait(ctx context.Context) error {
	for {
		l.mu.Lock()
		now := time.Now()
		elapsed := now.Sub(l.last)
		l.last = now
		l.tokens += elapsed.Seconds() * l.rate
		if l.tokens > l.burst {
			l.tokens = l.burst
		}
		if l.tokens >= 1 {
			l.tokens--
			l.mu.Unlock()
			return nil
		}
		// Time until one whole token has accrued.
		wait := time.Duration((1 - l.tokens) / l.rate * float64(time.Second))
		l.mu.Unlock()

		if wait < minTimerGranularity {
			wait = minTimerGranularity
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
	}
}
