// Copyright IBM Corp. 2014, 2026
// SPDX-License-Identifier: BUSL-1.1

package batch

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
)

// Processor-level sentinel errors.
var (
	// ErrProcessorStopped is returned by Submit after the processor has
	// started shutting down or has never been started.
	ErrProcessorStopped = errors.New("batch: processor is not running")

	// ErrProcessorRunning is returned when Run or Start is called on a
	// processor that has already been started.
	ErrProcessorRunning = errors.New("batch: processor has already been started")

	// ErrInvalidTask is returned for submissions missing an idempotency
	// key.
	ErrInvalidTask = errors.New("batch: invalid task")

	// ErrUnknownTaskType is returned for submissions whose Type has no
	// registered steps.
	ErrUnknownTaskType = errors.New("batch: unknown task type")
)

const (
	defaultWorkers      = 1
	defaultLeaseTTL     = 30 * time.Second
	defaultPollInterval = 250 * time.Millisecond
	defaultQueueSize    = 1024
	minHeartbeatGap     = 10 * time.Millisecond
)

// Config configures a [Processor].
type Config struct {
	// Store is the checkpoint store. Required.
	Store Store

	// Registry resolves step implementations for persisted task types.
	// Required.
	Registry *Registry

	// Workers is the number of goroutines executing steps concurrently.
	// Defaults to 1 when zero or negative.
	Workers int

	// Limiter gates step starts across all workers. Defaults to
	// NopLimiter.
	Limiter Limiter

	// LeaseTTL is how long a claim stays valid without a heartbeat. When a
	// process stops, another processor resumes its tasks once their leases
	// lapse. Defaults to 30s.
	LeaseTTL time.Duration

	// PollInterval is how often the processor scans the store for tasks
	// delivered elsewhere or abandoned by a previous process, refreshes
	// externally observed handles, and sweeps expiration. Submissions are
	// dispatched immediately; polling only covers recovery and duplicate
	// delivery. Defaults to 250ms.
	PollInterval time.Duration

	// DefaultTTL is applied to submissions without an explicit WithTTL.
	// Zero means tasks never expire by default.
	DefaultTTL time.Duration

	// QueueSize is the buffer of the internal dispatch channel. Defaults
	// to 1024.
	QueueSize int
}

// Processor runs submitted tasks: it deduplicates by idempotency key,
// checkpoints step progress in the [Store], renews an exclusive lease while
// working, resumes tasks left behind by stopped processes once their leases
// lapse, enforces expiration and rate limiting, and cooperates with context
// cancellation.
//
// A Processor models one worker process. Replacement processes are simply
// new processors sharing the same Store and Registry. A processor is started
// once (via Start or Run) and shut down once.
type Processor struct {
	cfg Config
	id  string

	startMu  sync.Mutex
	started  bool
	runCtx   context.Context
	cancel   context.CancelFunc
	done     chan struct{}
	doneOnce sync.Once

	jobs      chan string
	scheduled sync.Map // key -> struct{}: locally queued/in-flight dedup

	handlesMu sync.Mutex
	handles   map[string]*Handle
}

// NewProcessor validates cfg and constructs a processor.
func NewProcessor(cfg Config) (*Processor, error) {
	if cfg.Store == nil {
		return nil, errors.New("batch: Config.Store is required")
	}
	if cfg.Registry == nil {
		return nil, errors.New("batch: Config.Registry is required")
	}
	if cfg.Workers < 1 {
		cfg.Workers = defaultWorkers
	}
	if cfg.Limiter == nil {
		cfg.Limiter = NopLimiter{}
	}
	if cfg.LeaseTTL <= 0 {
		cfg.LeaseTTL = defaultLeaseTTL
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = defaultPollInterval
	}
	if cfg.QueueSize < 1 {
		cfg.QueueSize = defaultQueueSize
	}

	return &Processor{
		cfg:     cfg,
		id:      uuid.NewString(),
		jobs:    make(chan string, cfg.QueueSize),
		handles: make(map[string]*Handle),
	}, nil
}

// ID returns the unique worker identity used as lease owner by this
// processor. It is primarily useful in diagnostics and tests.
func (p *Processor) ID() string {
	return p.id
}

// Start launches the processor in the background and returns immediately.
// It fails with [ErrProcessorRunning] on a second start. The processor ties
// its lifetime to ctx: canceling ctx is the crash/stop signal, after which
// in-flight records keep their lease until it lapses and become eligible for
// resume rather than being marked failed.
func (p *Processor) Start(ctx context.Context) error {
	if err := p.markStarted(ctx); err != nil {
		return err
	}
	p.done = make(chan struct{})
	go func() {
		defer close(p.done)
		_ = p.run(p.runCtx)
	}()
	return nil
}

// Run starts the processor and blocks until ctx is canceled and all workers
// have released. It is an alternative to Start/Shutdown for callers that
// already own a goroutine.
func (p *Processor) Run(ctx context.Context) error {
	if err := p.markStarted(ctx); err != nil {
		return err
	}
	p.doneOnce.Do(func() { p.done = make(chan struct{}); close(p.done) })
	return p.run(ctx)
}

// Shutdown cancels the processor and waits for its workers to stop, or until
// waitCtx is done. In-flight steps are expected to honor the canceled step
// context; records are not marked failed and are resumed by the next
// processor after the lease lapses.
func (p *Processor) Shutdown(waitCtx context.Context) error {
	if p.cancel == nil {
		return ErrProcessorStopped
	}
	p.cancel()
	if p.done == nil {
		return nil
	}
	select {
	case <-p.done:
		return nil
	case <-waitCtx.Done():
		return waitCtx.Err()
	}
}

// Done returns a channel closed when a Start-ed processor has fully stopped.
func (p *Processor) Done() <-chan struct{} {
	if p.done == nil {
		return nil
	}
	return p.done
}

func (p *Processor) markStarted(ctx context.Context) error {
	p.startMu.Lock()
	defer p.startMu.Unlock()
	if p.started {
		return ErrProcessorRunning
	}
	runCtx, cancel := context.WithCancel(ctx)
	p.runCtx = runCtx
	p.cancel = cancel
	p.started = true
	return nil
}

// Submit accepts a task using its idempotency key.
//
// The first submission for a key creates the durable record; every
// subsequent submission with the same key returns a handle to the same
// outcome without executing any step, whether the original submission is
// still running, has already finished, or was left behind by a stopped
// process.
func (p *Processor) Submit(ctx context.Context, task Task, opts ...SubmitOption) (*Handle, error) {
	if task.Key == "" {
		return nil, fmt.Errorf("%w: missing idempotency key", ErrInvalidTask)
	}
	steps, ok := p.cfg.Registry.resolve(task.Type)
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrUnknownTaskType, task.Type)
	}
	if p.runCtx == nil || p.runCtx.Err() != nil {
		return nil, ErrProcessorStopped
	}

	options := submitOptions{}
	for _, opt := range opts {
		opt(&options)
	}
	var expiresAt time.Time
	switch {
	case options.ttlSet && options.ttl > 0:
		expiresAt = time.Now().Add(options.ttl)
	case !options.ttlSet && p.cfg.DefaultTTL > 0:
		expiresAt = time.Now().Add(p.cfg.DefaultTTL)
	}

	stepNames := make([]string, len(steps))
	for i, st := range steps {
		stepNames[i] = st.Name
	}
	rec := &Record{
		Key:       task.Key,
		Type:      task.Type,
		Payload:   append([]byte(nil), task.Payload...),
		Steps:     stepNames,
		Status:    StatusPending,
		ExpiresAt: expiresAt,
	}

	stored, created, err := p.cfg.Store.CreateIfAbsent(ctx, rec)
	if err != nil {
		return nil, err
	}
	handle := p.obtainHandle(stored)
	if created {
		p.enqueue(stored.Key)
	}
	return handle, nil
}

// Result waits until the task identified by key reaches a terminal state and
// returns its outcome. The key does not need to have been submitted through
// this processor: Result also observes tasks completed by another processor
// sharing the store, which is how a duplicated delivery to a non-owner is
// answered.
func (p *Processor) Result(ctx context.Context, key string) (*Result, error) {
	p.handlesMu.Lock()
	h := p.handles[key]
	p.handlesMu.Unlock()
	if h != nil {
		return h.Wait(ctx)
	}
	for {
		rec, err := p.cfg.Store.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		if rec.Status.Terminal() {
			return resultFromRecord(rec), nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(p.cfg.PollInterval):
		}
	}
}

// run is the processor main loop.
func (p *Processor) run(ctx context.Context) error {
	var wg sync.WaitGroup
	for i := 0; i < p.cfg.Workers; i++ {
		wg.Add(1)
		go p.worker(ctx, &wg)
	}

	// Immediate pass so startup recovery and the first submission do not
	// wait for the poll tick.
	p.poll(ctx)

	ticker := time.NewTicker(p.cfg.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return ctx.Err()
		case <-ticker.C:
			p.poll(ctx)
		}
	}
}

func (p *Processor) worker(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case key := <-p.jobs:
			p.runTask(ctx, key)
			p.scheduled.Delete(key)
		}
	}
}

// poll performs one recovery/dedup scan, an expiration sweep, and refreshes
// handles whose tasks are owned by another processor.
func (p *Processor) poll(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	now := time.Now()

	if swept, err := p.cfg.Store.SweepExpired(ctx, now); err == nil {
		for _, rec := range swept {
			p.resolveHandle(rec)
		}
	}

	recs, err := p.cfg.Store.List(ctx)
	if err != nil {
		return
	}
	for _, rec := range recs {
		switch rec.Status {
		case StatusPending:
			p.enqueue(rec.Key)
		case StatusRunning:
			// Reclaim only leases that have lapsed; a live lease (ours or
			// another processor's) is left alone.
			if !now.Before(rec.LeaseUntil) {
				p.enqueue(rec.Key)
			}
		default:
			if rec.Status.Terminal() {
				p.resolveHandle(rec)
			}
		}
	}
}

// enqueue schedules a key locally exactly once. A full queue is safe: the
// key remains recoverable via the poll scan.
func (p *Processor) enqueue(key string) {
	if _, loaded := p.scheduled.LoadOrStore(key, struct{}{}); loaded {
		return
	}
	select {
	case p.jobs <- key:
	default:
		p.scheduled.Delete(key)
	}
}

func (p *Processor) runTask(ctx context.Context, key string) {
	rec, err := p.cfg.Store.Claim(ctx, key, p.id, time.Now(), p.cfg.LeaseTTL)
	if err != nil {
		// ErrTerminal and ErrExpired carry the decisive state; surface it
		// to any local waiter. ErrLeaseHeld means another processor owns
		// the work; the poll watcher observes the eventual outcome.
		if errors.Is(err, ErrTerminal) || errors.Is(err, ErrExpired) {
			p.resolveHandle(rec)
		}
		return
	}

	steps, ok := p.cfg.Registry.resolve(rec.Type)
	if !ok {
		// A record this process cannot execute (for example an older
		// binary after a rolling restart). It fails durably rather than
		// pinning a lease forever.
		l := newLease(p, rec)
		p.fail(ctx, l, fmt.Errorf("%w: %q", ErrUnknownTaskType, rec.Type))
		return
	}

	lease := newLease(p, rec)
	hbCtx, stopHeartbeat := context.WithCancel(ctx)
	heartbeatsDone := lease.startHeartbeat(hbCtx)
	defer func() {
		stopHeartbeat()
		<-heartbeatsDone
	}()

	for i := rec.NextStep; i < len(steps); i++ {
		if ctx.Err() != nil {
			// Processor is stopping: abandon without a terminal write so
			// the lease-lapse recovery path resumes this task.
			return
		}
		if lease.snapshot().expired(time.Now()) {
			p.markExpired(ctx, lease)
			return
		}
		if err := p.cfg.Limiter.Wait(ctx); err != nil {
			if ctx.Err() != nil {
				return // shutting down while queued for a permit: abandon
			}
			p.fail(ctx, lease, fmt.Errorf("rate limiter: %w", err))
			return
		}
		if ctx.Err() != nil {
			return
		}
		if lease.snapshot().expired(time.Now()) {
			p.markExpired(ctx, lease)
			return
		}

		err := steps[i].Run(ctx, append([]byte(nil), lease.snapshot().Payload...))
		if err != nil {
			if ctx.Err() != nil {
				// The stop signal interrupted the step; the record stays
				// non-terminal and the next processor retries from the
				// last checkpoint.
				return
			}
			p.fail(ctx, lease, err)
			return
		}

		// Persist the checkpoint even if shutdown raced the step: the
		// observed side effect completed, so skipping it on resume is
		// correct.
		updated, err := lease.checkpoint(ctx, i+1)
		if err != nil {
			// Fenced out by a newer lease owner (or a terminal write came
			// from elsewhere): stop touching this record.
			return
		}
		rec = updated
	}

	p.complete(ctx, lease)
}

func (p *Processor) fail(_ context.Context, lease *taskLease, cause error) {
	msg := cause.Error()
	if len(msg) > maxErrLen {
		msg = msg[:maxErrLen]
	}
	rec, err := lease.commit(StatusFailed, lease.snapshot().NextStep, msg)
	if err == nil {
		p.resolveHandle(rec)
	}
}

func (p *Processor) complete(_ context.Context, lease *taskLease) {
	rec, err := lease.commit(StatusCompleted, lease.snapshot().NextStep, "")
	if err == nil {
		p.resolveHandle(rec)
	}
}

func (p *Processor) markExpired(_ context.Context, lease *taskLease) {
	rec, err := lease.commit(StatusExpired, lease.snapshot().NextStep, "")
	if err == nil {
		p.resolveHandle(rec)
	}
}

// obtainHandle returns the local handle for a key, creating one and possibly
// resolving it immediately for records that are already terminal (for
// example a duplicate submission long after completion, or a record owned by
// a different processor).
func (p *Processor) obtainHandle(rec *Record) *Handle {
	p.handlesMu.Lock()
	defer p.handlesMu.Unlock()
	if h, ok := p.handles[rec.Key]; ok {
		return h
	}
	h := &Handle{
		key:  rec.Key,
		done: make(chan struct{}),
	}
	p.handles[rec.Key] = h
	if rec.Status.Terminal() {
		h.resolve(rec)
	}
	return h
}

func (p *Processor) resolveHandle(rec *Record) {
	if rec == nil || !rec.Status.Terminal() {
		return
	}
	p.handlesMu.Lock()
	h, ok := p.handles[rec.Key]
	p.handlesMu.Unlock()
	if !ok {
		return
	}
	h.resolve(rec)
}

// taskLease serializes the heartbeat/checkpoint updates to one claimed
// record so heartbeats and checkpoints never race each other's versions.
type taskLease struct {
	p   *Processor
	key string

	mu  sync.Mutex
	rec *Record
}

func newLease(p *Processor, rec *Record) *taskLease {
	return &taskLease{p: p, key: rec.Key, rec: rec}
}

func (l *taskLease) snapshot() *Record {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.rec.clone()
}

func (l *taskLease) startHeartbeat(ctx context.Context) <-chan struct{} {
	done := make(chan struct{})
	interval := l.p.cfg.LeaseTTL / 2
	if interval < minHeartbeatGap {
		interval = minHeartbeatGap
	}
	go func() {
		defer close(done)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				l.heartbeat(ctx)
			}
		}
	}()
	return done
}

func (l *taskLease) heartbeat(ctx context.Context) {
	l.mu.Lock()
	defer l.mu.Unlock()

	cur := l.rec
	if cur.Status != StatusRunning || cur.LeaseOwner != l.p.id {
		return
	}
	upd, err := l.p.cfg.Store.Heartbeat(ctx, l.key, l.p.id, cur.Version, time.Now(), l.p.cfg.LeaseTTL)
	if errors.Is(err, ErrConflict) {
		// The heartbeat lost a race with our own checkpoint or with
		// fencing: re-read and adopt the current version when we are still
		// the owner.
		fresh, getErr := l.p.cfg.Store.Get(ctx, l.key)
		if getErr != nil || fresh.Status != StatusRunning || fresh.LeaseOwner != l.p.id {
			return
		}
		upd, err = fresh, nil
	}
	if err == nil {
		l.rec = upd
	}
}

// checkpoint stores progress. Commits deliberately do not bind to the
// processor's run context: a checkpoint must land even when shutdown has
// just raced a successfully completed step.
func (l *taskLease) checkpoint(_ context.Context, nextStep int) (*Record, error) {
	return l.commit(StatusRunning, nextStep, "")
}

func (l *taskLease) commit(status Status, nextStep int, failErr string) (*Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	next := l.rec.clone()
	next.Status = status
	next.NextStep = nextStep
	next.FailErr = failErr

	upd, err := l.p.cfg.Store.Commit(context.Background(), next)
	if err != nil {
		return nil, err
	}
	l.rec = upd
	return upd.clone(), nil
}

// Handle represents a submitted task's eventual outcome.
type Handle struct {
	key string

	done chan struct{}

	mu       sync.Mutex
	resolved bool
	rec      *Record
}

// Done returns a channel closed once the task reaches a terminal state.
func (h *Handle) Done() <-chan struct{} {
	return h.done
}

// Result returns the outcome without blocking and whether it is available.
func (h *Handle) Result() (*Result, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if !h.resolved {
		return nil, false
	}
	return resultFromRecord(h.rec), true
}

// Wait blocks until the task reaches a terminal state or ctx is done.
func (h *Handle) Wait(ctx context.Context) (*Result, error) {
	select {
	case <-h.done:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	h.mu.Lock()
	rec := h.rec.clone()
	h.mu.Unlock()
	return resultFromRecord(rec), nil
}

func (h *Handle) resolve(rec *Record) {
	h.mu.Lock()
	if h.resolved {
		h.mu.Unlock()
		return
	}
	h.resolved = true
	h.rec = rec.clone()
	h.mu.Unlock()
	close(h.done)
}

// Result is the terminal outcome of a task.
type Result struct {
	// Key is the task's idempotency key.
	Key string

	// Status is the terminal status: completed, failed or expired.
	Status Status

	// Err carries the step error when Status is failed.
	Err error
}

func resultFromRecord(rec *Record) *Result {
	res := &Result{Key: rec.Key, Status: rec.Status}
	if rec.Status == StatusFailed && rec.FailErr != "" {
		res.Err = errors.New(rec.FailErr)
	}
	return res
}
