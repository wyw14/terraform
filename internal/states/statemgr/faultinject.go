// Copyright IBM Corp. 2026
// SPDX-License-Identifier: BUSL-1.1

package statemgr

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/hashicorp/terraform/internal/schemarepo"
	"github.com/hashicorp/terraform/internal/states"
)

// This file contains fault-injection helpers intended for use in tests that
// need to exercise Terraform's state checkpointing and recovery behavior under
// simulated infrastructure failures, such as an RPC timeout, a local disk
// I/O error, or a remote state backend becoming temporarily unreachable.
//
// The helpers are safe for concurrent use, because the apply graph walk calls
// state hooks concurrently.

var (
	// ErrInjectedTimeout is returned by [Faulty] when a simulated operation
	// timeout expires. It wraps context.DeadlineExceeded so that callers
	// which already special-case deadline errors keep working.
	ErrInjectedTimeout = fmt.Errorf("injected fault: simulated operation timeout: %w", context.DeadlineExceeded)

	// ErrInjectedDisk is returned by [Faulty] when a simulated local disk
	// I/O failure occurs.
	ErrInjectedDisk = errors.New("injected fault: simulated disk I/O error")

	// ErrInjectedNetwork is returned by [Faulty] when a simulated network
	// failure occurs. It represents a transient condition that is expected
	// to clear after a reconnect (see FaultController.Reconnect).
	ErrInjectedNetwork = errors.New("injected fault: simulated network connection failure")
)

// FaultOp identifies a state manager operation that faults can be injected
// into.
type FaultOp int

const (
	// FaultWriteState targets the transient Writer.WriteState method, which
	// for filesystem-based managers also performs the durable write.
	FaultWriteState FaultOp = iota

	// FaultPersistState targets Persister.PersistState, the durable
	// checkpoint operation (for example a remote state "Put").
	FaultPersistState

	// FaultRefreshState targets Refresher.RefreshState, used when a new
	// Terraform process loads the last persisted checkpoint.
	FaultRefreshState
)

// FaultKind classifies an injected fault.
type FaultKind int

const (
	// FaultTimeout reports a deadline-exceeded style error.
	FaultTimeout FaultKind = iota

	// FaultDisk reports a hard local disk error; recovery requires the
	// fault to be explicitly cleared (see FaultController.Recover).
	FaultDisk

	// FaultNetwork reports a transient network error that clears once the
	// connection has been re-established (see FaultController.Reconnect or
	// an auto-healing FailNext schedule).
	FaultNetwork
)

// faultSchedule describes the pending faults for a single operation.
type faultSchedule struct {
	// delay is waited out before every call to the operation. It simulates
	// slow disks or slow remote RPCs that may outlive a caller's deadline.
	delay time.Duration

	// remaining is the number of upcoming calls that fail, and the kind of
	// fault they fail with. Counts down automatically, so a value of one
	// models a single transient blip and larger values model an outage that
	// needs a reconnect.
	remaining int
	kind      FaultKind

	// failWhen, if non-nil, is consulted with the state the operation is
	// about to handle and forces a fault of kind when it returns true.
	// This lets tests select a deterministic checkpoint (for example
	// "once two resources have been recorded") even when the apply walk is
	// running resources concurrently.
	failWhen func(*states.State) bool
	whenKind FaultKind
}

// FaultController decides which faults a [Faulty] manager should inject. A
// controller is created by [NewFaulty] and may be reconfigured at any time,
// including from another goroutine while an apply is in progress.
type FaultController struct {
	mu        sync.Mutex
	connected bool
	schedules [3]faultSchedule
	attempts  [3]int
	failures  [3]int
}

// NewFaultController creates a controller with no injected faults and with
// the connection to persistent storage considered healthy.
func NewFaultController() *FaultController {
	return &FaultController{connected: true}
}

// Connected reports whether the connection to persistent storage is
// currently considered up.
func (c *FaultController) Connected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.connected
}

// InjectDelay makes every future call to the given operation wait for d
// before proceeding. Pass zero to remove the delay.
//
// A delay on its own never fails; combine it with a caller-side deadline to
// simulate an operation timing out, or use FailNext with FaultTimeout to
// return an explicit timeout error.
func (c *FaultController) InjectDelay(op FaultOp, d time.Duration) {
	c.mu.Lock()
	c.schedules[op].delay = d
	c.mu.Unlock()
}

// FailNext arranges for the next n calls to the given operation to fail with
// the given kind of fault.
//
// When kind is FaultNetwork the calls fail while the simulated connection is
// down, and the connection is automatically considered restored once all n
// failures have been consumed. For FaultDisk and FaultTimeout the failures
// are reported regardless of connection state.
func (c *FaultController) FailNext(op FaultOp, kind FaultKind, n int) {
	if n <= 0 {
		return
	}
	c.mu.Lock()
	c.schedules[op].remaining = n
	c.schedules[op].kind = kind
	if kind == FaultNetwork {
		c.connected = false
	}
	c.mu.Unlock()
}

// FailPersistWhenState arranges for PersistState calls whose about-to-be
// persisted state causes fn to return true to fail with the given kind of
// fault.
//
// The predicate must not modify the state it is given.
func (c *FaultController) FailPersistWhenState(fn func(*states.State) bool, kind FaultKind) {
	c.mu.Lock()
	c.schedules[FaultPersistState].failWhen = fn
	c.schedules[FaultPersistState].whenKind = kind
	c.mu.Unlock()
}

// Disconnect simulates loss of the network connection to persistent
// storage. While disconnected, PersistState and RefreshState calls fail
// with ErrInjectedNetwork; transient WriteState calls keep working because
// they only touch the local in-memory snapshot, mirroring the behavior of
// the remote state manager.
func (c *FaultController) Disconnect() {
	c.mu.Lock()
	c.connected = false
	c.mu.Unlock()
}

// Reconnect simulates restoring the network connection to persistent
// storage. Any transient network faults scheduled with FailNext may still
// report their remaining failures; use Recover to clear those too.
func (c *FaultController) Reconnect() {
	c.mu.Lock()
	c.connected = true
	c.mu.Unlock()
}

// Recover clears every injected fault, restores the connection and leaves
// the controller as if NewFaultController had just been called. Usage
// counters are preserved.
func (c *FaultController) Recover() {
	c.mu.Lock()
	c.connected = true
	for i := range c.schedules {
		c.schedules[i] = faultSchedule{}
	}
	c.mu.Unlock()
}

// Attempts returns how many calls to the given operation have been
// observed, including calls that failed due to an injected fault.
func (c *FaultController) Attempts(op FaultOp) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.attempts[op]
}

// Failures returns how many calls to the given operation were failed by an
// injected fault.
func (c *FaultController) Failures(op FaultOp) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.failures[op]
}

// before is consulted by Faulty before delegating to the wrapped manager. It
// applies any configured delay and then returns the fault (if any) that the
// call should fail with.
func (c *FaultController) before(op FaultOp, state *states.State) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.attempts[op]++
	sched := c.schedules[op]

	if sched.delay > 0 {
		// Sleeping while holding the mutex serializes delayed calls, which
		// mirrors the real managers (both the filesystem manager and the
		// StateHook that drives it are mutex-protected) and keeps attempt
		// accounting deterministic.
		time.Sleep(sched.delay)
	}

	if sched.remaining > 0 {
		sched.remaining--
		c.schedules[op].remaining = sched.remaining
		if sched.kind == FaultNetwork && sched.remaining == 0 {
			// The transient outage window has elapsed.
			c.connected = true
		}
		c.failures[op]++
		return faultError(sched.kind)
	}

	// Once any scheduled failures are consumed, an explicit disconnection
	// (see Disconnect) still fails calls that would leave the process:
	// persisting or refreshing durable storage.
	if !c.connected && (op == FaultPersistState || op == FaultRefreshState) {
		c.failures[op]++
		return ErrInjectedNetwork
	}

	if sched.failWhen != nil && state != nil && sched.failWhen(state) {
		// A state-triggered fault stays armed until explicitly recovered,
		// because the same checkpoint would otherwise be retried forever.
		c.failures[op]++
		return faultError(sched.whenKind)
	}

	return nil
}

func faultError(kind FaultKind) error {
	switch kind {
	case FaultTimeout:
		return ErrInjectedTimeout
	case FaultDisk:
		return ErrInjectedDisk
	case FaultNetwork:
		return ErrInjectedNetwork
	default:
		return ErrInjectedDisk
	}
}

// Faulty wraps a [Full] state manager and injects the faults described by its
// [FaultController], while otherwise delegating every call to the wrapped
// manager. It additionally implements PersistentMeta and
// IntermediateStateConditionalPersister by delegation when the wrapped
// manager does, so it can stand in for a filesystem or remote state manager
// in checkpoint/recovery tests without changing Terraform's persist
// decisions.
type Faulty struct {
	// Inner is the wrapped manager. It must not be replaced while calls are
	// in flight.
	Inner Full

	// C controls the injected faults.
	C *FaultController
}

var (
	_ Full                                  = (*Faulty)(nil)
	_ PersistentMeta                        = (*Faulty)(nil)
	_ IntermediateStateConditionalPersister = (*Faulty)(nil)
)

// NewFaulty wraps inner, returning both the manager and the controller used
// to drive its injected faults.
func NewFaulty(inner Full) (*Faulty, *FaultController) {
	c := NewFaultController()
	return &Faulty{Inner: inner, C: c}, c
}

// State delegates to the wrapped manager.
func (m *Faulty) State() *states.State {
	return m.Inner.State()
}

// GetRootOutputValues delegates to the wrapped manager.
func (m *Faulty) GetRootOutputValues(ctx context.Context) (map[string]*states.OutputValue, error) {
	return m.Inner.GetRootOutputValues(ctx)
}

// WriteState injects the configured faults and then delegates to the
// wrapped manager.
func (m *Faulty) WriteState(state *states.State) error {
	if err := m.C.before(FaultWriteState, state); err != nil {
		return err
	}
	return m.Inner.WriteState(state)
}

// PersistState injects the configured faults and then delegates to the
// wrapped manager.
func (m *Faulty) PersistState(schemas *schemarepo.Schemas) error {
	if err := m.C.before(FaultPersistState, m.Inner.State()); err != nil {
		return err
	}
	return m.Inner.PersistState(schemas)
}

// RefreshState injects the configured faults and then delegates to the
// wrapped manager.
func (m *Faulty) RefreshState() error {
	if err := m.C.before(FaultRefreshState, nil); err != nil {
		return err
	}
	return m.Inner.RefreshState()
}

// Lock delegates to the wrapped manager.
func (m *Faulty) Lock(info *LockInfo) (string, error) {
	return m.Inner.Lock(info)
}

// Unlock delegates to the wrapped manager.
func (m *Faulty) Unlock(id string) error {
	return m.Inner.Unlock(id)
}

// StateSnapshotMeta delegates to the wrapped manager when it supports
// persistent snapshot metadata.
func (m *Faulty) StateSnapshotMeta() SnapshotMeta {
	if meta, ok := m.Inner.(PersistentMeta); ok {
		return meta.StateSnapshotMeta()
	}
	return SnapshotMeta{}
}

// ShouldPersistIntermediateState delegates to the wrapped manager when it
// implements IntermediateStateConditionalPersister, and otherwise applies
// the default persist rule so that Terraform's intermediate checkpoint
// behavior is unaffected by the wrapper.
func (m *Faulty) ShouldPersistIntermediateState(info *IntermediateStatePersistInfo) bool {
	if p, ok := m.Inner.(IntermediateStateConditionalPersister); ok {
		return p.ShouldPersistIntermediateState(info)
	}
	return DefaultIntermediateStatePersistRule(info)
}
