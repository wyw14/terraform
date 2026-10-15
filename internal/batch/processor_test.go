// Copyright IBM Corp. 2014, 2026
// SPDX-License-Identifier: BUSL-1.1

package batch

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

func eventually(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}

func waitRecord(t *testing.T, store Store, key string, timeout time.Duration, cond func(*Record) bool) *Record {
	t.Helper()
	var rec *Record
	ok := eventually(timeout, func() bool {
		r, err := store.Get(context.Background(), key)
		if err != nil {
			return false
		}
		rec = r
		return cond(r)
	})
	if !ok {
		t.Fatalf("record %q never satisfied condition; last state: %+v", key, rec)
	}
	return rec
}

func waitHandle(t *testing.T, h *Handle) *Result {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := h.Wait(ctx)
	if err != nil {
		t.Fatalf("handle did not finish: %v", err)
	}
	return res
}

// gate is a thread-safe blocker: a step or limiter signals reached when it is
// parked, and the test releases open to let it continue.
type gate struct {
	open    chan struct{}
	reached chan struct{}
	once    sync.Once
}

func newGate() *gate {
	return &gate{open: make(chan struct{}), reached: make(chan struct{})}
}

func (g *gate) signal() { g.once.Do(func() { close(g.reached) }) }
func (g *gate) release() {
	select {
	case <-g.open:
	default:
		close(g.open)
	}
}

// effects records step side effects, payloads and cancellation observations.
type effects struct {
	mu       sync.Mutex
	hits     map[string]int
	payloads map[string][]byte
	canceled map[string]bool
}

func newEffects() *effects {
	return &effects{
		hits:     make(map[string]int),
		payloads: make(map[string][]byte),
		canceled: make(map[string]bool),
	}
}

func (e *effects) hit(name string, payload []byte) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.hits[name]++
	e.payloads[name] = append([]byte(nil), payload...)
}

func (e *effects) markCanceled(name string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.canceled[name] = true
}

func (e *effects) count(name string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.hits[name]
}

func (e *effects) payload(name string) []byte {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.payloads[name]
}

func (e *effects) wasCanceled(name string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.canceled[name]
}

func (e *effects) step(name string) Step {
	return Step{Name: name, Run: func(_ context.Context, payload []byte) error {
		e.hit(name, payload)
		return nil
	}}
}

// gateStep performs its side effect and then blocks until the gate opens or
// its context is canceled.
func (e *effects) gateStep(name string, g *gate) Step {
	return Step{Name: name, Run: func(ctx context.Context, payload []byte) error {
		e.hit(name, payload)
		g.signal()
		select {
		case <-g.open:
			return nil
		case <-ctx.Done():
			e.markCanceled(name)
			return ctx.Err()
		}
	}}
}

// gatedLimiter blocks every Wait until the gate opens or ctx cancels.
type gatedLimiter struct{ g *gate }

func (l *gatedLimiter) Wait(ctx context.Context) error {
	l.g.signal()
	select {
	case <-l.g.open:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// countedGateLimiter lets the first pass Wait calls through immediately and
// parks any later call on the gate.
type countedGateLimiter struct {
	g    *gate
	pass int

	mu sync.Mutex
	n  int
}

func (l *countedGateLimiter) waits() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.n
}

func (l *countedGateLimiter) Wait(ctx context.Context) error {
	l.mu.Lock()
	l.n++
	n := l.n
	l.mu.Unlock()
	if n <= l.pass {
		return nil
	}
	l.g.signal()
	select {
	case <-l.g.open:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type procOption func(*Config)

func withLeaseTTL(d time.Duration) procOption { return func(c *Config) { c.LeaseTTL = d } }
func withPoll(d time.Duration) procOption     { return func(c *Config) { c.PollInterval = d } }
func withWorkers(n int) procOption            { return func(c *Config) { c.Workers = n } }
func withLimiter(l Limiter) procOption        { return func(c *Config) { c.Limiter = l } }
func withDefaultTTL(d time.Duration) procOption {
	return func(c *Config) { c.DefaultTTL = d }
}

func startTestProcessor(t *testing.T, store Store, reg *Registry, opts ...procOption) *Processor {
	t.Helper()
	cfg := Config{
		Store:        store,
		Registry:     reg,
		Workers:      1,
		LeaseTTL:     500 * time.Millisecond,
		PollInterval: 20 * time.Millisecond,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	p, err := NewProcessor(cfg)
	if err != nil {
		t.Fatalf("NewProcessor: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := p.Start(ctx); err != nil {
		cancel()
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		cancel()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer shutdownCancel()
		_ = p.Shutdown(shutdownCtx)
	})
	return p
}

func shutdownProcessor(t *testing.T, p *Processor) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := p.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

func TestProcessor_HappyPath_AndCheckpointsBetweenSteps(t *testing.T) {
	store := NewMemoryStore()
	reg := NewRegistry()
	eff := newEffects()
	midGate := newGate()
	reg.Register("pipeline",
		eff.step("s1"),
		eff.gateStep("s2", midGate),
		eff.step("s3"),
	)
	p := startTestProcessor(t, store, reg)

	h, err := p.Submit(context.Background(), Task{
		Key:     "job-1",
		Type:    "pipeline",
		Payload: []byte("abc"),
	})
	if err != nil {
		t.Fatalf("Submit: %v", err)
	}

	// While s2 is blocked, s1 must be durably checkpointed.
	<-midGate.reached
	rec := waitRecord(t, store, "job-1", time.Second, func(r *Record) bool {
		return r.NextStep == 1 && r.Status == StatusRunning
	})
	if rec.LeaseOwner == "" {
		t.Fatal("running record has no lease owner")
	}
	if len(rec.Steps) != 3 || rec.Steps[1] != "s2" {
		t.Fatalf("checkpoint step names = %v", rec.Steps)
	}

	midGate.release()
	res := waitHandle(t, h)
	if res.Status != StatusCompleted || res.Err != nil {
		t.Fatalf("result = (%s, %v)", res.Status, res.Err)
	}

	final := waitRecord(t, store, "job-1", time.Second, func(r *Record) bool {
		return r.Status == StatusCompleted
	})
	if final.NextStep != 3 || final.LeaseOwner != "" {
		t.Fatalf("final record = %+v", final)
	}
	for _, name := range []string{"s1", "s2", "s3"} {
		if eff.count(name) != 1 {
			t.Fatalf("step %q side effects = %d, want 1", name, eff.count(name))
		}
	}
	if got := eff.payload("s3"); string(got) != "abc" {
		t.Fatalf("s3 payload = %q, want %q", got, "abc")
	}
}

func TestProcessor_ConcurrentDuplicateSubmits_ExecuteOnce(t *testing.T) {
	store := NewMemoryStore()
	reg := NewRegistry()
	eff := newEffects()
	reg.Register("dedup",
		Step{Name: "s1", Run: func(context.Context, []byte) error {
			time.Sleep(2 * time.Millisecond)
			eff.hit("s1", nil)
			return nil
		}},
		eff.step("s2"),
	)
	p := startTestProcessor(t, store, reg)

	const n = 16
	handles := make([]*Handle, n)
	start := make(chan struct{})
	var submitWG wgHelper
	submitWG.add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer submitWG.done()
			<-start
			h, err := p.Submit(context.Background(), Task{Key: "same-key", Type: "dedup"})
			if err != nil {
				t.Errorf("Submit %d: %v", i, err)
				return
			}
			handles[i] = h
		}(i)
	}
	close(start)
	submitWG.wait(t)

	for i, h := range handles {
		if h == nil {
			t.Fatalf("submit %d returned no handle", i)
		}
		if h != handles[0] {
			t.Fatalf("submit %d returned a different handle; duplicates must share one outcome", i)
		}
		if res := waitHandle(t, h); res.Status != StatusCompleted {
			t.Fatalf("submit %d result = %s", i, res.Status)
		}
	}
	if eff.count("s1") != 1 || eff.count("s2") != 1 {
		t.Fatalf("side effects = s1:%d s2:%d, want 1/1", eff.count("s1"), eff.count("s2"))
	}
	recs, err := store.List(context.Background())
	if err != nil || len(recs) != 1 {
		t.Fatalf("store has %d records (err=%v), want exactly 1", len(recs), err)
	}
}

func TestProcessor_ResubmitAfterCompletion_HasNoNewEffects(t *testing.T) {
	store := NewMemoryStore()
	reg := NewRegistry()
	eff := newEffects()
	reg.Register("once", eff.step("s1"))
	p := startTestProcessor(t, store, reg)

	h1, err := p.Submit(context.Background(), Task{Key: "k", Type: "once", Payload: []byte("first")})
	if err != nil {
		t.Fatal(err)
	}
	if res := waitHandle(t, h1); res.Status != StatusCompleted {
		t.Fatalf("first result = %s", res.Status)
	}
	before := waitRecord(t, store, "k", time.Second, func(r *Record) bool { return true })

	// A redelivery with a different payload is still the same idempotent
	// request and must be answered from the record.
	h2, err := p.Submit(context.Background(), Task{Key: "k", Type: "once", Payload: []byte("second")})
	if err != nil {
		t.Fatal(err)
	}
	if h2 != h1 {
		t.Fatal("resubmit did not return the original handle")
	}
	if res, ok := h2.Result(); !ok || res.Status != StatusCompleted {
		t.Fatalf("resubmit result = (%+v, %v), want completed immediately", res, ok)
	}
	if eff.count("s1") != 1 {
		t.Fatalf("side effects after resubmit = %d, want 1", eff.count("s1"))
	}
	after := waitRecord(t, store, "k", time.Second, func(r *Record) bool { return true })
	if after.Version != before.Version {
		t.Fatalf("resubmit bumped record version %d -> %d; it must not write", before.Version, after.Version)
	}
}

func TestProcessor_ResumeAfterRestart_AtCheckpoint(t *testing.T) {
	store := NewMemoryStore()
	reg := NewRegistry()
	eff := newEffects()
	// s3 parks so the replacement worker's lease ownership is observable
	// before it finishes.
	finishGate := newGate()
	reg.Register("resume", eff.step("s1"), eff.step("s2"), eff.gateStep("s3", finishGate))

	// p1 may run two steps; the third permit never arrives, so when it is
	// shut down the task rests at the s2 checkpoint.
	limiterGate := newGate()
	p1 := startTestProcessor(t, store, reg,
		withLeaseTTL(400*time.Millisecond),
		withPoll(10*time.Millisecond),
		withLimiter(&countedGateLimiter{g: limiterGate, pass: 2}),
	)
	h1, err := p1.Submit(context.Background(), Task{
		Key:     "resume-key",
		Type:    "resume",
		Payload: []byte("hello"),
	})
	if err != nil {
		t.Fatal(err)
	}
	<-limiterGate.reached
	waitRecord(t, store, "resume-key", time.Second, func(r *Record) bool {
		return r.NextStep == 2 && r.Status == StatusRunning && r.LeaseOwner == p1.ID()
	})
	shutdownProcessor(t, p1)

	// Nothing marked the record terminal on the way down.
	abandoned, err := store.Get(context.Background(), "resume-key")
	if err != nil {
		t.Fatal(err)
	}
	if abandoned.Status != StatusRunning || abandoned.NextStep != 2 || abandoned.LeaseOwner != p1.ID() {
		t.Fatalf("abandoned record = %+v, want running at checkpoint 2 owned by p1", abandoned)
	}

	// A replacement process re-resolves steps from the registry and resumes.
	p2 := startTestProcessor(t, store, reg,
		withLeaseTTL(80*time.Millisecond),
		withPoll(10*time.Millisecond),
	)

	// p2 must fence p1: it claims the lapsed lease and reaches s3, which is
	// exactly the first step after the persisted checkpoint.
	waitRecord(t, store, "resume-key", 3*time.Second, func(r *Record) bool {
		return r.Status == StatusRunning && r.LeaseOwner == p2.ID() && r.NextStep == 2
	})
	<-finishGate.reached
	finishGate.release()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	res, err := p2.Result(ctx, "resume-key")
	if err != nil {
		t.Fatalf("p2 Result: %v", err)
	}
	if res.Status != StatusCompleted {
		t.Fatalf("resumed result = %s, want completed", res.Status)
	}

	final := waitRecord(t, store, "resume-key", time.Second, func(r *Record) bool {
		return r.Status == StatusCompleted
	})
	if final.NextStep != 3 {
		t.Fatalf("final record = %+v, want NextStep 3", final)
	}
	for _, name := range []string{"s1", "s2", "s3"} {
		if eff.count(name) != 1 {
			t.Fatalf("step %q ran %d times across restart, want exactly 1", name, eff.count(name))
		}
	}
	if got := eff.payload("s3"); string(got) != "hello" {
		t.Fatalf("resumed step payload = %q, want hello", got)
	}
	// The abandoned handle from p1 never fabricated a terminal outcome.
	if _, done := h1.Result(); done {
		t.Fatal("abandoned p1 handle should not have synthesized a terminal result")
	}
}

func TestProcessor_DuplicateDelivery_TwoProcessors_ExactlyOnce(t *testing.T) {
	store := NewMemoryStore()
	reg := NewRegistry()
	eff := newEffects()
	stepGate := newGate()
	reg.Register("redeliver", eff.gateStep("s1", stepGate), eff.step("s2"))

	// Simulate one durable message that got delivered to both processors:
	// seed the record directly, let both poll loops discover it.
	seed := &Record{
		Key:    "dup-delivery",
		Type:   "redeliver",
		Steps:  []string{"s1", "s2"},
		Status: StatusPending,
	}
	if _, _, err := store.CreateIfAbsent(context.Background(), seed); err != nil {
		t.Fatal(err)
	}

	p1 := startTestProcessor(t, store, reg, withPoll(10*time.Millisecond), withLeaseTTL(2*time.Second))
	p2 := startTestProcessor(t, store, reg, withPoll(10*time.Millisecond), withLeaseTTL(2*time.Second))

	<-stepGate.reached
	rec := waitRecord(t, store, "dup-delivery", time.Second, func(r *Record) bool {
		return r.Status == StatusRunning && (r.LeaseOwner == p1.ID() || r.LeaseOwner == p2.ID())
	})
	owner := rec.LeaseOwner

	// Let several poll/claim cycles elapse on both processors while s1 is
	// still parked; the non-owner must be unable to run anything.
	time.Sleep(80 * time.Millisecond)
	if eff.count("s1") != 1 {
		t.Fatalf("s1 ran %d times while claimed, want 1", eff.count("s1"))
	}
	still, err := store.Get(context.Background(), "dup-delivery")
	if err != nil || still.LeaseOwner != owner {
		t.Fatalf("lease changed to %+v (%v); want stable owner %q", still, err, owner)
	}

	stepGate.release()

	res1, err := p1.Result(context.Background(), "dup-delivery")
	if err != nil {
		t.Fatalf("p1 Result: %v", err)
	}
	res2, err := p2.Result(context.Background(), "dup-delivery")
	if err != nil {
		t.Fatalf("p2 Result: %v", err)
	}
	if res1.Status != StatusCompleted || res2.Status != StatusCompleted {
		t.Fatalf("results = %s / %s, want completed for both deliveries", res1.Status, res2.Status)
	}
	if eff.count("s1") != 1 || eff.count("s2") != 1 {
		t.Fatalf("side effects after duplicate delivery = s1:%d s2:%d, want 1/1",
			eff.count("s1"), eff.count("s2"))
	}
}

func TestProcessor_FailedStep_IsTerminalAndIdempotent(t *testing.T) {
	store := NewMemoryStore()
	reg := NewRegistry()
	eff := newEffects()
	reg.Register("failable",
		eff.step("s1"),
		Step{Name: "s2", Run: func(context.Context, []byte) error { return errors.New("boom") }},
		eff.step("s3"),
	)
	p := startTestProcessor(t, store, reg)

	h, err := p.Submit(context.Background(), Task{Key: "fail-key", Type: "failable"})
	if err != nil {
		t.Fatal(err)
	}
	res := waitHandle(t, h)
	if res.Status != StatusFailed || res.Err == nil || res.Err.Error() != "boom" {
		t.Fatalf("result = (%s, %v), want failed/boom", res.Status, res.Err)
	}
	rec := waitRecord(t, store, "fail-key", time.Second, func(r *Record) bool {
		return r.Status == StatusFailed
	})
	if rec.NextStep != 1 || rec.FailErr != "boom" {
		t.Fatalf("failure record = %+v", rec)
	}
	if eff.count("s1") != 1 || eff.count("s3") != 0 {
		t.Fatalf("effects = s1:%d s3:%d, want 1/0", eff.count("s1"), eff.count("s3"))
	}

	// Redelivery of a failed request returns the recorded failure without
	// re-running steps.
	h2, err := p.Submit(context.Background(), Task{Key: "fail-key", Type: "failable"})
	if err != nil {
		t.Fatal(err)
	}
	res2 := waitHandle(t, h2)
	if res2.Status != StatusFailed || res2.Err.Error() != "boom" {
		t.Fatalf("redelivered failure = (%s, %v)", res2.Status, res2.Err)
	}
	if eff.count("s1") != 1 {
		t.Fatalf("s1 re-ran on redelivery: %d", eff.count("s1"))
	}
}

func TestProcessor_ExpiredPendingRecord_IsSweptWithoutEffects(t *testing.T) {
	store := NewMemoryStore()
	reg := NewRegistry()
	eff := newEffects()
	reg.Register("sweep", eff.step("s1"))
	p := startTestProcessor(t, store, reg, withPoll(10*time.Millisecond))

	rec := &Record{
		Key:       "old",
		Type:      "sweep",
		Steps:     []string{"s1"},
		Status:    StatusPending,
		ExpiresAt: time.Now().Add(-time.Minute),
	}
	if _, _, err := store.CreateIfAbsent(context.Background(), rec); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	res, err := p.Result(ctx, "old")
	if err != nil {
		t.Fatalf("Result: %v", err)
	}
	if res.Status != StatusExpired {
		t.Fatalf("result = %s, want expired", res.Status)
	}
	if eff.count("s1") != 0 {
		t.Fatalf("expired task produced %d side effects, want 0", eff.count("s1"))
	}
}

func TestProcessor_ExpiresWhileWaitingForPermit(t *testing.T) {
	store := NewMemoryStore()
	reg := NewRegistry()
	eff := newEffects()
	reg.Register("slow-rate", eff.step("s1"))

	permitGate := newGate()
	p := startTestProcessor(t, store, reg,
		withLimiter(&gatedLimiter{g: permitGate}),
		withDefaultTTL(120*time.Millisecond),
	)
	h, err := p.Submit(context.Background(), Task{Key: "late", Type: "slow-rate"})
	if err != nil {
		t.Fatal(err)
	}
	<-permitGate.reached // claim succeeded, parked behind the limiter

	time.Sleep(250 * time.Millisecond)
	permitGate.release()

	res := waitHandle(t, h)
	if res.Status != StatusExpired {
		t.Fatalf("result = %s, want expired", res.Status)
	}
	if eff.count("s1") != 0 {
		t.Fatalf("expired task produced %d side effects, want 0", eff.count("s1"))
	}
}

func TestProcessor_CancelInFlightStep_DoesNotFailRecord(t *testing.T) {
	store := NewMemoryStore()
	reg := NewRegistry()
	eff := newEffects()
	stepGate := newGate()
	reg.Register("cancelable", eff.gateStep("s1", stepGate), eff.step("s2"))
	p := startTestProcessor(t, store, reg)

	h, err := p.Submit(context.Background(), Task{Key: "cancel-key", Type: "cancelable"})
	if err != nil {
		t.Fatal(err)
	}
	<-stepGate.reached

	// Do not release the gate: shutdown must propagate context
	// cancellation into the parked step.
	shutdownProcessor(t, p)

	if !eff.wasCanceled("s1") {
		t.Fatal("step did not observe context cancellation")
	}
	if eff.count("s2") != 0 {
		t.Fatalf("s2 ran %d times after cancellation, want 0", eff.count("s2"))
	}
	rec, err := store.Get(context.Background(), "cancel-key")
	if err != nil {
		t.Fatal(err)
	}
	if rec.Status != StatusRunning || rec.NextStep != 0 {
		t.Fatalf("record after cancel = %+v, want non-terminal running at checkpoint 0", rec)
	}
	if _, done := h.Result(); done {
		t.Fatal("canceled in-flight handle must not synthesize a terminal result")
	}
}

func TestProcessor_CancelBeforeSideEffect_ResumesOnReplacement(t *testing.T) {
	store := NewMemoryStore()
	reg := NewRegistry()
	eff := newEffects()
	reg.Register("queued", eff.step("s1"))

	permitGate := newGate()
	p1 := startTestProcessor(t, store, reg, withLimiter(&gatedLimiter{g: permitGate}))
	if _, err := p1.Submit(context.Background(), Task{Key: "queued-key", Type: "queued"}); err != nil {
		t.Fatal(err)
	}
	<-permitGate.reached
	shutdownProcessor(t, p1)
	if eff.count("s1") != 0 {
		t.Fatal("side effect happened while waiting for a permit")
	}

	// The gate stays closed; the replacement processor has its own
	// unlimited limiter and finishes the work once the lease lapses.
	p2 := startTestProcessor(t, store, reg,
		withLeaseTTL(80*time.Millisecond),
		withPoll(10*time.Millisecond),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	res, err := p2.Result(ctx, "queued-key")
	if err != nil {
		t.Fatalf("p2 Result: %v", err)
	}
	if res.Status != StatusCompleted {
		t.Fatalf("result after resume = %s, want completed", res.Status)
	}
	if eff.count("s1") != 1 {
		t.Fatalf("s1 ran %d times, want exactly 1", eff.count("s1"))
	}
}

func TestProcessor_RateLimit_GatesStepStartsAcrossWorkers(t *testing.T) {
	store := NewMemoryStore()
	reg := NewRegistry()

	var mu sync.Mutex
	var starts []time.Time
	reg.Register("rated", Step{Name: "s1", Run: func(context.Context, []byte) error {
		mu.Lock()
		starts = append(starts, time.Now())
		mu.Unlock()
		return nil
	}})

	// 25 permits/sec, no burst: with 8 hungry workers the step starts must
	// be spaced by ~40ms.
	p := startTestProcessor(t, store, reg,
		withWorkers(8),
		withLimiter(NewTokenBucket(25, 1)),
	)

	const tasks = 5
	var handles []*Handle
	for i := 0; i < tasks; i++ {
		h, err := p.Submit(context.Background(), Task{Key: fmt.Sprintf("rated-%d", i), Type: "rated"})
		if err != nil {
			t.Fatal(err)
		}
		handles = append(handles, h)
	}
	for _, h := range handles {
		if res := waitHandle(t, h); res.Status != StatusCompleted {
			t.Fatalf("result = %s", res.Status)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if len(starts) != tasks {
		t.Fatalf("recorded %d step starts, want %d", len(starts), tasks)
	}
	for i := 1; i < len(starts); i++ {
		if d := starts[i].Sub(starts[i-1]); d < 25*time.Millisecond {
			t.Fatalf("start %d came only %v after start %d; limiter failed to pace", i, d, i-1)
		}
	}
	if span := starts[len(starts)-1].Sub(starts[0]); span < 140*time.Millisecond {
		t.Fatalf("5 step starts at 25/s spanned %v, want >= ~160ms of pacing", span)
	}
}

func TestProcessor_SubmitValidation(t *testing.T) {
	store := NewMemoryStore()
	reg := NewRegistry()
	reg.Register("known", Step{Name: "s1", Run: func(context.Context, []byte) error { return nil }})
	p := startTestProcessor(t, store, reg)

	if _, err := p.Submit(context.Background(), Task{Key: "", Type: "known"}); !errors.Is(err, ErrInvalidTask) {
		t.Fatalf("empty key = %v, want ErrInvalidTask", err)
	}
	if _, err := p.Submit(context.Background(), Task{Key: "k", Type: "unknown"}); !errors.Is(err, ErrUnknownTaskType) {
		t.Fatalf("unknown type = %v, want ErrUnknownTaskType", err)
	}

	shutdownProcessor(t, p)
	if _, err := p.Submit(context.Background(), Task{Key: "k2", Type: "known"}); !errors.Is(err, ErrProcessorStopped) {
		t.Fatalf("submit after shutdown = %v, want ErrProcessorStopped", err)
	}
	if err := p.Start(context.Background()); !errors.Is(err, ErrProcessorRunning) {
		t.Fatalf("second Start = %v, want ErrProcessorRunning", err)
	}
}
