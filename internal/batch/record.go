// Copyright IBM Corp. 2014, 2026
// SPDX-License-Identifier: BUSL-1.1

package batch

import (
	"errors"
	"time"
)

// Sentinel errors returned by [Store] implementations and surfaced by the
// processor. They use errors.Is matching so callers can wrap them.
var (
	// ErrNotFound is returned when no record exists for the given key.
	ErrNotFound = errors.New("batch: task record not found")

	// ErrConflict is returned by an optimistic-concurrency update when the
	// record version held by the caller no longer matches the store. The
	// caller has been fenced out (for example its lease was lost) and must
	// stop touching the task.
	ErrConflict = errors.New("batch: record version conflict")

	// ErrLeaseHeld is returned by Claim when another owner still holds a
	// non-expired lease on the task.
	ErrLeaseHeld = errors.New("batch: task lease is held by another worker")

	// ErrTerminal is returned by Claim/Heartbeat/Commit when the record has
	// already reached a terminal state. The returned record (when available)
	// describes that state.
	ErrTerminal = errors.New("batch: task is already in a terminal state")

	// ErrExpired is returned by Claim when the record was found to be past
	// its expiration time. The store has already transitioned the record to
	// StatusExpired before returning this error.
	ErrExpired = errors.New("batch: task has expired")
)

// maxErrLen bounds how much of a step error is retained in a record so a
// noisy failure cannot grow the checkpoint store without limit.
const maxErrLen = 4096

// Record is the persisted, checkpointed state of a single task.
//
// Records are the unit of crash recovery and idempotent deduplication. A
// worker resumes by loading a record and starting at NextStep. Records are
// value snapshots returned by the [Store]; callers never mutate records owned
// by the store directly.
type Record struct {
	// Key is the caller-provided idempotency key. It uniquely identifies
	// the logical request for as long as records are retained.
	Key string

	// Type selects the registered step list used to execute the task. The
	// type is persisted so a freshly started worker process can resolve the
	// step implementations after a restart.
	Type string

	// Payload is the opaque request payload captured when the task was
	// submitted. It is handed unchanged to every step.
	Payload []byte

	// Steps lists the registered step names in execution order, captured at
	// submission time for observability and audit of the checkpoint.
	Steps []string

	// Status is the current lifecycle state.
	Status Status

	// NextStep is the index of the first step that has not completed and
	// been checkpointed. It is the resume cursor.
	NextStep int

	// CreatedAt and UpdatedAt track the record lifetime.
	CreatedAt time.Time
	UpdatedAt time.Time

	// ExpiresAt is the absolute expiration deadline. The zero value means
	// the task never expires.
	ExpiresAt time.Time

	// LeaseOwner identifies the worker currently allowed to execute the
	// task, and LeaseUntil is when that lease expires. Both are zero for
	// records that are not currently leased.
	LeaseOwner string
	LeaseUntil time.Time

	// FailErr holds the error message from the failing step when Status is
	// StatusFailed.
	FailErr string

	// Version is an optimistic-concurrency token, incremented on every
	// mutation. Updates must present the version they based their change
	// on; mismatches fail with [ErrConflict].
	Version int64
}

// clone returns a deep enough copy of the record that callers may mutate the
// returned value (including its Payload and Steps slices) without aliasing
// store state.
func (r *Record) clone() *Record {
	if r == nil {
		return nil
	}
	cp := *r
	if r.Payload != nil {
		cp.Payload = append([]byte(nil), r.Payload...)
	}
	if r.Steps != nil {
		cp.Steps = append([]string(nil), r.Steps...)
	}
	return &cp
}

// expired reports whether the record is past its expiration deadline at now.
func (r *Record) expired(now time.Time) bool {
	return !r.ExpiresAt.IsZero() && !now.Before(r.ExpiresAt)
}
