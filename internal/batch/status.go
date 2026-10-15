// Copyright IBM Corp. 2014, 2026
// SPDX-License-Identifier: BUSL-1.1

package batch

// Status is the lifecycle state of a task record.
type Status int

const (
	// StatusUnknown is the zero value and never persisted for a real task.
	StatusUnknown Status = iota

	// StatusPending means the task was accepted but no worker has ever
	// acquired a lease on it yet.
	StatusPending

	// StatusRunning means a worker holds (or previously held) an active
	// lease and is executing, or was executing when its process stopped.
	// A Running record with an expired lease is eligible to be claimed by
	// another worker and resumed from its last checkpoint.
	StatusRunning

	// StatusCompleted means every step finished successfully. Records are
	// retained in this state so that duplicate submissions with the same
	// idempotency key return the original result without re-running any
	// side effect.
	StatusCompleted

	// StatusFailed means a step returned a non-cancellation error. The
	// record records the failure and is not retried automatically;
	// duplicate submissions return the recorded failure.
	StatusFailed

	// StatusExpired means the task passed its expiration policy before it
	// could be completed, and no further steps will be executed for it.
	StatusExpired
)

// String returns a stable human readable name for the status.
func (s Status) String() string {
	switch s {
	case StatusPending:
		return "pending"
	case StatusRunning:
		return "running"
	case StatusCompleted:
		return "completed"
	case StatusFailed:
		return "failed"
	case StatusExpired:
		return "expired"
	default:
		return "unknown"
	}
}

// Terminal reports whether the status is a final state.
func (s Status) Terminal() bool {
	switch s {
	case StatusCompleted, StatusFailed, StatusExpired:
		return true
	default:
		return false
	}
}
