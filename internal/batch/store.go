// Copyright IBM Corp. 2014, 2026
// SPDX-License-Identifier: BUSL-1.1

package batch

import (
	"context"
	"sync"
	"time"
)

// Store is the durable checkpoint store for task records.
//
// It is the coordination point that makes processing safe under duplicate
// delivery and worker restarts: all of its operations are atomic with
// respect to one another, and execution ownership is taken exclusively
// through [Store.Claim]. A production implementation would back this with a
// database row, a document, or a compare-and-swap capable KV entry; the
// bundled [MemoryStore] is suitable for embedding and tests.
//
// Implementations must be safe for concurrent use by multiple goroutines and
// by multiple processor processes sharing the same backing data.
type Store interface {
	// CreateIfAbsent atomically stores rec when no record with rec.Key
	// exists yet. On success the stored record is returned with created
	// true. When the key already exists the existing record is returned
	// with created false and rec is ignored.
	//
	// The returned record has the Version assigned by the store. rec.Key
	// must be non-empty and rec.Status must be StatusPending.
	CreateIfAbsent(ctx context.Context, rec *Record) (stored *Record, created bool, err error)

	// Get returns a snapshot of the record for key, or [ErrNotFound].
	Get(ctx context.Context, key string) (*Record, error)

	// List returns snapshots of every record, in an unspecified order.
	// Processors use this on startup and on a poll to discover tasks left
	// behind by a previous process or delivered to a different processor.
	List(ctx context.Context) ([]*Record, error)

	// Claim atomically acquires an exclusive execution lease on the task.
	//
	// A claim succeeds for Pending records, or for Running records whose
	// lease has expired at now (lease fencing: the new owner replaces the
	// old one and the old owner's subsequent Commits fail with
	// [ErrConflict]). It fails with [ErrLeaseHeld] when another owner's
	// lease is still valid, with [ErrTerminal] for terminal records, and
	// with [ErrExpired] (after committing StatusExpired) for records past
	// their deadline.
	Claim(ctx context.Context, key, owner string, now time.Time, leaseTTL time.Duration) (*Record, error)

	// Heartbeat extends the lease on a record the caller currently owns.
	// The caller must present its last known version; [ErrConflict] is
	// returned if it has been fenced out and [ErrTerminal] if the record
	// finished meanwhile. On success the returned record carries the new
	// version and deadline.
	Heartbeat(ctx context.Context, key, owner string, version int64, now time.Time, leaseTTL time.Duration) (*Record, error)

	// Commit applies a checkpoint or a terminal transition using optimistic
	// concurrency: rec.Version must match the current version or the call
	// fails with [ErrConflict].
	//
	// Only mutable progress fields are applied: Status, NextStep, FailErr,
	// LeaseOwner and LeaseUntil. Key, Type, Payload, Steps, CreatedAt and
	// ExpiresAt are immutable after creation. Non-terminal commits are only
	// accepted when LeaseOwner matches the caller's owner in rec. Committing
	// a terminal status clears the lease. The returned record is the new
	// store state.
	Commit(ctx context.Context, rec *Record) (*Record, error)

	// SweepExpired transitions every non-terminal record whose deadline has
	// passed at now to StatusExpired and returns the affected records. It
	// lets queued tasks expire even when no worker gets around to claiming
	// them. Running records with a live lease are left untouched; running
	// records whose leases have expired are swept as well.
	SweepExpired(ctx context.Context, now time.Time) ([]*Record, error)
}

// MemoryStore is an in-process [Store] implementation.
//
// Its state is held in memory rather than on disk, but it lives independently
// of any single [Processor]: multiple processors (including "replacement"
// processes started in tests) can share one and exercise the exact same
// claim, checkpoint and recovery paths a durable backend would provide.
type MemoryStore struct {
	mu sync.Mutex
	m  map[string]*Record
}

// NewMemoryStore returns an empty in-memory store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{m: make(map[string]*Record)}
}

// CreateIfAbsent implements [Store].
func (s *MemoryStore) CreateIfAbsent(_ context.Context, rec *Record) (*Record, bool, error) {
	if rec == nil || rec.Key == "" {
		return nil, false, ErrNotFound
	}
	if rec.Status != StatusPending {
		return nil, false, ErrTerminal
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if existing, ok := s.m[rec.Key]; ok {
		return existing.clone(), false, nil
	}

	stored := rec.clone()
	now := time.Now()
	stored.CreatedAt = now
	stored.UpdatedAt = now
	stored.Version = 1
	s.m[rec.Key] = stored
	return stored.clone(), true, nil
}

// Get implements [Store].
func (s *MemoryStore) Get(_ context.Context, key string) (*Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.m[key]
	if !ok {
		return nil, ErrNotFound
	}
	return rec.clone(), nil
}

// List implements [Store].
func (s *MemoryStore) List(_ context.Context) ([]*Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*Record, 0, len(s.m))
	for _, rec := range s.m {
		out = append(out, rec.clone())
	}
	return out, nil
}

// Claim implements [Store].
func (s *MemoryStore) Claim(_ context.Context, key, owner string, now time.Time, leaseTTL time.Duration) (*Record, error) {
	if owner == "" {
		return nil, ErrLeaseHeld
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	rec, ok := s.m[key]
	if !ok {
		return nil, ErrNotFound
	}

	switch {
	case rec.Status.Terminal():
		return rec.clone(), ErrTerminal
	case rec.expired(now):
		rec.Status = StatusExpired
		rec.LeaseOwner = ""
		rec.LeaseUntil = time.Time{}
		rec.UpdatedAt = now
		rec.Version++
		return rec.clone(), ErrExpired
	case rec.Status == StatusPending:
		// First claim: grant.
	case rec.Status == StatusRunning && !now.Before(rec.LeaseUntil):
		// The previous owner's lease has run out; fence it.
	default:
		return rec.clone(), ErrLeaseHeld
	}

	rec.Status = StatusRunning
	rec.LeaseOwner = owner
	rec.LeaseUntil = now.Add(leaseTTL)
	rec.UpdatedAt = now
	rec.Version++
	return rec.clone(), nil
}

// Heartbeat implements [Store].
func (s *MemoryStore) Heartbeat(_ context.Context, key, owner string, version int64, now time.Time, leaseTTL time.Duration) (*Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	rec, ok := s.m[key]
	if !ok {
		return nil, ErrNotFound
	}
	if rec.Status.Terminal() {
		return rec.clone(), ErrTerminal
	}
	if rec.Version != version {
		return rec.clone(), ErrConflict
	}
	if rec.LeaseOwner != owner {
		return rec.clone(), ErrConflict
	}

	rec.LeaseUntil = now.Add(leaseTTL)
	rec.UpdatedAt = now
	rec.Version++
	return rec.clone(), nil
}

// Commit implements [Store].
func (s *MemoryStore) Commit(_ context.Context, rec *Record) (*Record, error) {
	if rec == nil || rec.Key == "" {
		return nil, ErrNotFound
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	cur, ok := s.m[rec.Key]
	if !ok {
		return nil, ErrNotFound
	}
	if cur.Version != rec.Version {
		return cur.clone(), ErrConflict
	}

	if !rec.Status.Terminal() && cur.LeaseOwner != rec.LeaseOwner {
		// Only the current lease holder may advance a non-terminal record.
		return cur.clone(), ErrConflict
	}

	switch rec.Status {
	case StatusRunning:
		cur.LeaseOwner = rec.LeaseOwner
		cur.LeaseUntil = rec.LeaseUntil
	case StatusCompleted, StatusFailed, StatusExpired:
		// Terminal records are not leased. The NextStep checkpoint is
		// preserved for observability.
		cur.LeaseOwner = ""
		cur.LeaseUntil = time.Time{}
	default:
		// Checkpoints must keep the task running; only Claim creates
		// pending records.
		return cur.clone(), ErrConflict
	}

	cur.Status = rec.Status
	cur.NextStep = rec.NextStep
	cur.FailErr = rec.FailErr
	cur.UpdatedAt = time.Now()
	cur.Version++
	return cur.clone(), nil
}

// SweepExpired implements [Store].
func (s *MemoryStore) SweepExpired(_ context.Context, now time.Time) ([]*Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var swept []*Record
	for _, rec := range s.m {
		if rec.Status.Terminal() || !rec.expired(now) {
			continue
		}
		if rec.Status == StatusRunning && now.Before(rec.LeaseUntil) {
			// A live worker heartbeat is still within its lease; let the
			// worker observe the deadline itself.
			continue
		}
		rec.Status = StatusExpired
		rec.LeaseOwner = ""
		rec.LeaseUntil = time.Time{}
		rec.UpdatedAt = now
		rec.Version++
		swept = append(swept, rec.clone())
	}
	return swept, nil
}
