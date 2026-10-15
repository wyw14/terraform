// Copyright IBM Corp. 2026
// SPDX-License-Identifier: BUSL-1.1

package statemgr

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/zclconf/go-cty/cty"

	"github.com/hashicorp/terraform/internal/addrs"
	"github.com/hashicorp/terraform/internal/states"
)

func faultyTestState(v string) *states.State {
	s := states.NewState()
	s.SetOutputValue(
		addrs.OutputValue{Name: "v"}.Absolute(addrs.RootModuleInstance),
		cty.StringVal(v), false,
	)
	return s
}

// A failed WriteState must not replace the previous transient snapshot, so
// that callers can keep using the last consistent in-memory state.
func TestFaulty_DiskError(t *testing.T) {
	m, c := NewFaulty(NewFullFake(nil, nil))

	good := faultyTestState("good")
	if err := m.WriteState(good); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	c.FailNext(FaultWriteState, FaultDisk, 1)
	if err := m.WriteState(faultyTestState("lost")); !errors.Is(err, ErrInjectedDisk) {
		t.Fatalf("expected ErrInjectedDisk, got %v", err)
	}
	if got, want := c.Attempts(FaultWriteState), 2; got != want {
		t.Errorf("wrong attempt count: got %d, want %d", got, want)
	}
	if got, want := c.Failures(FaultWriteState), 1; got != want {
		t.Errorf("wrong failure count: got %d, want %d", got, want)
	}

	if got := m.State(); !got.Equal(good) {
		t.Fatalf("transient state was corrupted by the failed write\n%#v", got)
	}

	// The fault was scheduled for a single call, so the next write succeeds.
	if err := m.WriteState(faultyTestState("better")); err != nil {
		t.Fatalf("write after recovery failed: %s", err)
	}
}

// While "disconnected" durable calls fail with the network error but local
// writes keep working; after Reconnect checkpoints can be persisted and
// reloaded again.
func TestFaulty_NetworkDisconnectReconnect(t *testing.T) {
	m, c := NewFaulty(NewFullFake(nil, nil))
	s := faultyTestState("remote")

	if err := m.WriteState(s); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	c.Disconnect()
	if c.Connected() {
		t.Fatal("controller should report as disconnected")
	}
	if err := m.WriteState(faultyTestState("local-only")); err != nil {
		t.Fatalf("transient writes must keep working while disconnected: %s", err)
	}
	if err := m.PersistState(nil); !errors.Is(err, ErrInjectedNetwork) {
		t.Fatalf("expected ErrInjectedNetwork from PersistState, got %v", err)
	}
	if err := m.RefreshState(); !errors.Is(err, ErrInjectedNetwork) {
		t.Fatalf("expected ErrInjectedNetwork from RefreshState, got %v", err)
	}

	// The failed persist must not have reached durable storage.
	c.Reconnect()
	if err := m.RefreshState(); err != nil {
		t.Fatalf("refresh after reconnect failed: %s", err)
	}
	if got := m.State(); got != nil && got.RootOutputValues["v"] != nil {
		t.Fatalf("durable storage unexpectedly contained the unpersisted state: %#v", got.RootOutputValues)
	}

	// After reconnect the checkpoint lands durably and survives a refresh.
	if err := m.WriteState(s); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if err := m.PersistState(nil); err != nil {
		t.Fatalf("persist after reconnect failed: %s", err)
	}
	m.WriteState(faultyTestState("discarded"))
	if err := m.RefreshState(); err != nil {
		t.Fatalf("refresh failed: %s", err)
	}
	if got := m.State(); !got.Equal(s) {
		t.Fatalf("did not recover the last consistent checkpoint\n%#v", got)
	}
}

// FailNext with a network fault models a short outage: the scheduled calls
// fail and the connection is considered restored automatically once the
// outage window has elapsed.
func TestFaulty_NetworkAutoHeal(t *testing.T) {
	m, c := NewFaulty(NewFullFake(nil, nil))
	s := faultyTestState("blip")
	c.FailNext(FaultPersistState, FaultNetwork, 3)

	if err := m.WriteState(s); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if err := m.PersistState(nil); !errors.Is(err, ErrInjectedNetwork) {
		t.Fatalf("first persist should have failed, got %v", err)
	}
	if err := m.PersistState(nil); !errors.Is(err, ErrInjectedNetwork) {
		t.Fatalf("second persist should have failed, got %v", err)
	}
	if c.Connected() {
		t.Fatal("connection should still be down until the last scheduled failure")
	}
	if err := m.PersistState(nil); !errors.Is(err, ErrInjectedNetwork) {
		t.Fatalf("third persist should have failed, got %v", err)
	}
	if !c.Connected() {
		t.Fatal("connection should be restored once failures were consumed")
	}
	if err := m.PersistState(nil); err != nil {
		t.Fatalf("persist after reconnect should have succeeded, got %s", err)
	}

	m.WriteState(faultyTestState("other"))
	if err := m.RefreshState(); err != nil {
		t.Fatalf("refresh failed: %s", err)
	}
	if got := m.State(); !got.Equal(s) {
		t.Fatalf("last consistent checkpoint was not durable\n%#v", got)
	}
}

// A timeout fault is recognizable both as the injected fault and as a
// deadline exceeded error, so existing timeout handling paths apply.
func TestFaulty_TimeoutError(t *testing.T) {
	m, c := NewFaulty(NewFullFake(nil, nil))
	c.FailNext(FaultRefreshState, FaultTimeout, 1)

	err := m.RefreshState()
	if !errors.Is(err, ErrInjectedTimeout) {
		t.Fatalf("expected ErrInjectedTimeout, got %v", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("injected timeout should wrap context.DeadlineExceeded, got %v", err)
	}
}

// A pure delay never fails by itself but takes at least the requested time,
// allowing callers with their own deadline to observe a timeout.
func TestFaulty_Delay(t *testing.T) {
	m, c := NewFaulty(NewFullFake(nil, nil))
	c.InjectDelay(FaultPersistState, 50*time.Millisecond)
	if err := m.WriteState(faultyTestState("slow")); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}

	start := time.Now()
	if err := m.PersistState(nil); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if elapsed := time.Since(start); elapsed < 40*time.Millisecond {
		t.Fatalf("expected injected delay to elapse, returned after %s", elapsed)
	}
}

// State predicates let tests pick deterministic checkpoint boundaries even
// under concurrent walking: the fault only fires for matching snapshots.
func TestFaulty_PersistWhenState(t *testing.T) {
	m, c := NewFaulty(NewFullFake(nil, nil))
	c.FailPersistWhenState(func(s *states.State) bool {
		v := s.RootOutputValues["v"]
		return v != nil && v.Value.AsString() == "boom"
	}, FaultDisk)

	if err := m.WriteState(faultyTestState("ok")); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if err := m.PersistState(nil); err != nil {
		t.Fatalf("checkpoint for unrelated state should succeed: %s", err)
	}

	if err := m.WriteState(faultyTestState("boom")); err != nil {
		t.Fatalf("unexpected error: %s", err)
	}
	if err := m.PersistState(nil); !errors.Is(err, ErrInjectedDisk) {
		t.Fatalf("expected disk fault for matching checkpoint, got %v", err)
	}

	c.Recover()
	if err := m.PersistState(nil); err != nil {
		t.Fatalf("persist after recovery failed: %s", err)
	}
}

func TestFaulty_RefreshDiskError(t *testing.T) {
	m, c := NewFaulty(NewFullFake(nil, nil))
	c.FailNext(FaultRefreshState, FaultDisk, 1)
	if err := m.RefreshState(); !errors.Is(err, ErrInjectedDisk) {
		t.Fatalf("expected ErrInjectedDisk, got %v", err)
	}
	if err := m.RefreshState(); err != nil {
		t.Fatalf("second refresh should succeed: %s", err)
	}
}

// Lock and Unlock, as well as snapshot metadata, delegate to the wrapped
// manager so the wrapper is a drop-in replacement.
func TestFaulty_LockAndMetaPassthrough(t *testing.T) {
	inner := NewFullFake(nil, nil)
	m, _ := NewFaulty(inner)

	id, err := m.Lock(NewLockInfo())
	if err != nil {
		t.Fatalf("lock failed: %s", err)
	}
	if err := m.Unlock(id); err != nil {
		t.Fatalf("unlock failed: %s", err)
	}

	s := faultyTestState("meta")
	if err := m.WriteState(s); err != nil {
		t.Fatalf("write failed: %s", err)
	}
	if err := m.PersistState(nil); err != nil {
		t.Fatalf("persist failed: %s", err)
	}
	if _, ok := interface{}(m).(PersistentMeta); !ok {
		t.Fatal("Faulty should implement PersistentMeta")
	}
	if _, ok := interface{}(m).(IntermediateStateConditionalPersister); !ok {
		t.Fatal("Faulty should implement IntermediateStateConditionalPersister")
	}
}

// Toggles faults from many goroutines while other goroutines perform
// checkpoint operations. Run with -race to exercise the synchronization.
func TestFaulty_Concurrent(t *testing.T) {
	m, c := NewFaulty(NewFullFake(nil, nil))
	base := faultyTestState("concurrent")
	if err := m.WriteState(base); err != nil {
		t.Fatalf("initial write failed: %s", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				switch j % 4 {
				case 0:
					c.Disconnect()
				case 1:
					c.Reconnect()
				case 2:
					c.FailNext(FaultPersistState, FaultNetwork, 1)
				}
				_ = m.PersistState(nil)
				_ = m.WriteState(base)
			}
		}(i)
	}
	wg.Wait()

	c.Recover()
	if err := m.WriteState(base); err != nil {
		t.Fatalf("write after recovery failed: %s", err)
	}
	if err := m.PersistState(nil); err != nil {
		t.Fatalf("persist after recovery failed: %s", err)
	}
	if got, want := c.Failures(FaultPersistState) > 0, true; got != want {
		t.Fatal("expected at least one injected network failure during the run")
	}
	if got := c.Attempts(FaultPersistState); got < c.Failures(FaultPersistState) {
		t.Fatalf("attempt count %d is below failure count %d", got, c.Failures(FaultPersistState))
	}
}
