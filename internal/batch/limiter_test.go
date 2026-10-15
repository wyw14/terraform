// Copyright IBM Corp. 2014, 2026
// SPDX-License-Identifier: BUSL-1.1

package batch

import (
	"context"
	"testing"
	"time"
)

func TestTokenBucket_AllowsConfiguredBurstImmediately(t *testing.T) {
	l := NewTokenBucket(1, 3)
	start := time.Now()
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		if err := l.Wait(ctx); err != nil {
			t.Fatalf("Wait %d: %v", i, err)
		}
	}
	if d := time.Since(start); d > 25*time.Millisecond {
		t.Fatalf("burst of 3 took %v, want near-immediate", d)
	}
}

func TestTokenBucket_PacesSustainedRate(t *testing.T) {
	const (
		rate  = 50.0 // 50 tokens/sec => one permit every 20ms
		burst = 1
		n     = 4
	)
	l := NewTokenBucket(rate, burst)
	startTimes := make(chan time.Time, n)
	var wg wgHelper
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.add(1)
		go func() {
			defer wg.done()
			<-start
			if err := l.Wait(context.Background()); err != nil {
				t.Errorf("Wait: %v", err)
				return
			}
			startTimes <- time.Now()
		}()
	}
	close(start)
	wg.wait(t)
	close(startTimes)

	var times []time.Time
	for ts := range startTimes {
		times = append(times, ts)
	}
	if len(times) != n {
		t.Fatalf("got %d acquisitions, want %d", len(times), n)
	}
	// Goroutines race for the single burst token, but acquisitions must
	// still be spaced by the refill interval.
	for i := 1; i < n; i++ {
		if d := times[i].Sub(times[i-1]); d < 14*time.Millisecond {
			t.Fatalf("acquisition %d started only %v after the previous, want >= ~20ms pacing", i, d)
		}
	}
	if span := times[n-1].Sub(times[0]); span < 45*time.Millisecond {
		t.Fatalf("4 acquisitions at 50/s burst 1 spanned %v, want >= 60ms of pacing", span)
	}
}

func TestTokenBucket_WaitHonorsContextCancel(t *testing.T) {
	l := NewTokenBucket(1, 1)
	if err := l.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	canceled := make(chan error, 1)
	go func() {
		// The bucket is empty; this would block for roughly a second.
		canceled <- l.Wait(ctx)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-canceled:
		if err != context.Canceled {
			t.Fatalf("Wait after cancel = %v, want context.Canceled", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("Wait did not return shortly after context cancellation")
	}
}

// wgHelper is a tiny adapter so test failures inside goroutines still join
// the goroutines deterministically.
type wgHelper struct {
	ch chan struct{}
	n  int
}

func (w *wgHelper) add(n int) {
	if w.ch == nil {
		w.ch = make(chan struct{})
	}
	w.n += n
}

func (w *wgHelper) done() { w.ch <- struct{}{} }

func (w *wgHelper) wait(t *testing.T) {
	t.Helper()
	for i := 0; i < w.n; i++ {
		select {
		case <-w.ch:
		case <-time.After(5 * time.Second):
			t.Fatalf("wgHelper: only %d of %d goroutines finished", i, w.n)
		}
	}
}
