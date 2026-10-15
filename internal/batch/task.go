// Copyright IBM Corp. 2014, 2026
// SPDX-License-Identifier: BUSL-1.1

package batch

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// StepFunc executes one step of a task.
//
// The context is canceled when the processor shuts down or loses its lease;
// implementations should observe ctx.Done for any blocking work so a stopping
// worker can release in-flight work for recovery.
//
// payload is the opaque payload supplied at submission time.
//
// Delivery semantics: a step is executed at most once between two
// checkpoints while its lease is healthy. If a worker process stops after a
// step's side effects but before the step checkpoint durably lands, another
// worker may execute that same step again after reclaiming the lease. Step
// implementations therefore ought to be safe to retry, or must confine their
// non-idempotent effects to steps that are followed by a durable checkpoint
// before any interruption is possible.
type StepFunc func(ctx context.Context, payload []byte) error

// Step is a named unit of work within a task type. Names are recorded in the
// checkpoint and must be unique within a task type.
type Step struct {
	// Name identifies the step in checkpoints and diagnostics.
	Name string

	// Run performs the step's work.
	Run StepFunc
}

// Registry maps task type names to their ordered step implementations.
//
// The registry is what makes restart recovery possible: the persistent
// record stores only the task type, and a replacement worker process uses
// the registry to resolve the step functions before resuming from the
// checkpoint.
//
// A Registry is safe for concurrent Register calls only before processors
// using it have started; concurrent lookups after that are safe.
type Registry struct {
	mu    sync.RWMutex
	steps map[string][]Step
}

// NewRegistry returns an empty task registry.
func NewRegistry() *Registry {
	return &Registry{steps: make(map[string][]Step)}
}

// Register associates an ordered list of steps with a task type.
// Registering the same type twice, registering no steps, registering a step
// without a name, or registering duplicate step names panics, as these are
// programming-time configuration errors.
func (r *Registry) Register(taskType string, steps ...Step) {
	if taskType == "" {
		panic("batch: Register called with empty task type")
	}
	if len(steps) == 0 {
		panic(fmt.Sprintf("batch: task type %q registered with no steps", taskType))
	}
	seen := make(map[string]bool, len(steps))
	for _, st := range steps {
		if st.Name == "" {
			panic(fmt.Sprintf("batch: task type %q has a step with an empty name", taskType))
		}
		if st.Run == nil {
			panic(fmt.Sprintf("batch: task type %q step %q has no Run function", taskType, st.Name))
		}
		if seen[st.Name] {
			panic(fmt.Sprintf("batch: task type %q registers step %q more than once", taskType, st.Name))
		}
		seen[st.Name] = true
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.steps[taskType]; exists {
		panic(fmt.Sprintf("batch: task type %q registered more than once", taskType))
	}
	r.steps[taskType] = append([]Step(nil), steps...)
}

// resolve returns a copy of the steps registered for taskType and true, or
// nil and false when the type is unknown.
func (r *Registry) resolve(taskType string) ([]Step, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	steps, ok := r.steps[taskType]
	if !ok {
		return nil, false
	}
	return append([]Step(nil), steps...), true
}

// Task is a submission request.
type Task struct {
	// Key is the mandatory idempotency key. Submitting the same key more
	// than once (whether concurrently, after completion, or after a worker
	// restart) never causes the task's steps to be executed more than once;
	// repeated submissions observe the original outcome.
	Key string

	// Type selects a registered list of steps.
	Type string

	// Payload is an opaque blob passed to each step.
	Payload []byte
}

// submitOptions holds optional settings for a submission.
type submitOptions struct {
	// ttl, when ttlSet is true, overrides the processor default.
	ttlSet bool
	ttl    time.Duration
}

// SubmitOption customizes a [Processor.Submit] call.
type SubmitOption func(*submitOptions)

// WithTTL overrides the processor's default expiration for this task.
//
// A positive duration sets an absolute deadline of now+ttl. A zero or
// negative duration means the task never expires even when the processor has
// a default TTL.
func WithTTL(ttl time.Duration) SubmitOption {
	return func(o *submitOptions) {
		o.ttlSet = true
		o.ttl = ttl
	}
}
