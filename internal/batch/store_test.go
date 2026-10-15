// Copyright IBM Corp. 2014, 2026
// SPDX-License-Identifier: BUSL-1.1

package batch

import (
	"context"
	"errors"
	"testing"
	"time"
)

func newPendingRecord(key string) *Record {
	return &Record{
		Key:    key,
		Type:   "test",
		Steps:  []string{"one", "two"},
		Status: StatusPending,
	}
}

func TestMemoryStore_CreateIfAbsent_Deduplicates(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()

	first, created, err := store.CreateIfAbsent(ctx, newPendingRecord("k"))
	if err != nil || !created || first.Version != 1 {
		t.Fatalf("first CreateIfAbsent = (v%d, %v, %v), want (v1, true, nil)", first.Version, created, err)
	}

	second, created, err := store.CreateIfAbsent(ctx, newPendingRecord("k"))
	if err != nil || created {
		t.Fatalf("duplicate CreateIfAbsent = (created=%v, err=%v), want (false, nil)", created, err)
	}
	if second.Version != 1 || second.Type != "test" {
		t.Fatalf("duplicate returned unexpected record: %+v", second)
	}

	got, err := store.Get(ctx, "k")
	if err != nil || got.Version != 1 {
		t.Fatalf("Get = (%+v, %v)", got, err)
	}

	if _, err := store.Get(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get missing = %v, want ErrNotFound", err)
	}
}

func TestMemoryStore_Claim_LeaseAndFencing(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	now := time.Now()
	const ttl = time.Minute

	if _, _, err := store.CreateIfAbsent(ctx, newPendingRecord("k")); err != nil {
		t.Fatal(err)
	}

	// First claim wins.
	rec, err := store.Claim(ctx, "k", "worker-A", now, ttl)
	if err != nil || rec.Status != StatusRunning || rec.LeaseOwner != "worker-A" || rec.Version != 2 {
		t.Fatalf("first claim = (%+v, %v)", rec, err)
	}

	// Neither another worker nor the same worker can double-claim while
	// the lease is live.
	if _, err := store.Claim(ctx, "k", "worker-B", now.Add(ttl/2), ttl); !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("live lease claim = %v, want ErrLeaseHeld", err)
	}
	if _, err := store.Claim(ctx, "k", "worker-A", now.Add(ttl/2), ttl); !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("owner re-claim = %v, want ErrLeaseHeld", err)
	}

	// Heartbeat extends the lease and bumps the version.
	beat, err := store.Heartbeat(ctx, "k", "worker-A", rec.Version, now.Add(ttl/2), ttl)
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if got := beat.LeaseUntil.Sub(now.Add(ttl / 2)); got != ttl {
		t.Fatalf("heartbeat extended lease by %v, want %v", got, ttl)
	}

	// A stale-version heartbeat is rejected.
	if _, err := store.Heartbeat(ctx, "k", "worker-A", rec.Version, now.Add(ttl), ttl); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale heartbeat = %v, want ErrConflict", err)
	}

	// Once the lease lapses another worker can fence the old owner.
	reclaimed, err := store.Claim(ctx, "k", "worker-B", beat.LeaseUntil.Add(time.Second), ttl)
	if err != nil || reclaimed.LeaseOwner != "worker-B" {
		t.Fatalf("fencing claim = (%+v, %v)", reclaimed, err)
	}

	// The fenced-out owner must no longer be able to heartbeat or commit.
	if _, err := store.Heartbeat(ctx, "k", "worker-A", reclaimed.Version, reclaimed.LeaseUntil, ttl); !errors.Is(err, ErrConflict) {
		t.Fatalf("fenced heartbeat = %v, want ErrConflict", err)
	}
	stale := reclaimed.clone()
	stale.Version = reclaimed.Version - 1
	stale.Status = StatusRunning
	stale.NextStep = 1
	if _, err := store.Commit(ctx, stale); !errors.Is(err, ErrConflict) {
		t.Fatalf("fenced commit = %v, want ErrConflict", err)
	}
}

func TestMemoryStore_Checkpoint_AndTerminalCommit(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	if _, _, err := store.CreateIfAbsent(ctx, newPendingRecord("k")); err != nil {
		t.Fatal(err)
	}
	rec, err := store.Claim(ctx, "k", "owner", time.Now(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	rec.NextStep = 1
	cp, err := store.Commit(ctx, rec)
	if err != nil {
		t.Fatalf("checkpoint commit: %v", err)
	}
	if cp.Status != StatusRunning || cp.NextStep != 1 || cp.Version != rec.Version+1 {
		t.Fatalf("checkpoint = %+v", cp)
	}

	// A stranger cannot advance someone else's checkpoint even with the
	// right version.
	intruder := cp.clone()
	intruder.LeaseOwner = "intruder"
	intruder.NextStep = 2
	if _, err := store.Commit(ctx, intruder); !errors.Is(err, ErrConflict) {
		t.Fatalf("intruder commit = %v, want ErrConflict", err)
	}

	// Terminal commit releases the lease.
	cp.NextStep = 2
	cp.Status = StatusCompleted
	done, err := store.Commit(ctx, cp)
	if err != nil {
		t.Fatalf("completion commit: %v", err)
	}
	if done.Status != StatusCompleted || done.LeaseOwner != "" || done.LeaseUntil != (time.Time{}) {
		t.Fatalf("terminal record = %+v", done)
	}
	if _, err := store.Claim(ctx, "k", "anyone", time.Now(), time.Minute); !errors.Is(err, ErrTerminal) {
		t.Fatalf("claim terminal = %v, want ErrTerminal", err)
	}
}

func TestMemoryStore_Claim_ExpiredRecord(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	rec := newPendingRecord("k")
	rec.ExpiresAt = time.Now().Add(-time.Second)
	if _, _, err := store.CreateIfAbsent(ctx, rec); err != nil {
		t.Fatal(err)
	}

	claimed, err := store.Claim(ctx, "k", "owner", time.Now(), time.Minute)
	if !errors.Is(err, ErrExpired) || claimed.Status != StatusExpired {
		t.Fatalf("expired claim = (%+v, %v)", claimed, err)
	}
	if claimed.LeaseOwner != "" {
		t.Fatalf("expired record retains lease owner %q", claimed.LeaseOwner)
	}
}

func TestMemoryStore_SweepExpired(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	now := time.Now()

	pendingFresh := newPendingRecord("fresh")
	pendingFresh.ExpiresAt = now.Add(time.Hour)
	pendingStale := newPendingRecord("stale")
	pendingStale.ExpiresAt = now.Add(-time.Second)
	runningLive := newPendingRecord("running-live")
	runningLive.ExpiresAt = now.Add(-time.Second)
	runningAbandoned := newPendingRecord("running-abandoned")
	runningAbandoned.ExpiresAt = now.Add(-time.Second)
	for _, rec := range []*Record{pendingFresh, pendingStale, runningLive, runningAbandoned} {
		if _, _, err := store.CreateIfAbsent(ctx, rec); err != nil {
			t.Fatal(err)
		}
	}

	// One running record keeps a live (long) lease; the other's lease was
	// granted two seconds ago for only one second and has lapsed.
	if _, err := store.Claim(ctx, "running-live", "a", now.Add(-2*time.Second), time.Hour); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Claim(ctx, "running-abandoned", "a", now.Add(-2*time.Second), time.Second); err != nil {
		t.Fatal(err)
	}

	swept, err := store.SweepExpired(ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, rec := range swept {
		got[rec.Key] = rec.Status == StatusExpired
	}
	if !got["stale"] || !got["running-abandoned"] {
		t.Fatalf("swept = %v, want stale and running-abandoned expired", got)
	}
	if got["running-live"] || got["fresh"] {
		t.Fatalf("swept = %v, live-lease/fresh records must be kept", got)
	}

	live, err := store.Get(ctx, "running-live")
	if err != nil || live.Status != StatusRunning {
		t.Fatalf("live lease record = (%+v, %v), want running", live, err)
	}
}
