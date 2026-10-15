// Copyright IBM Corp. 2026
// SPDX-License-Identifier: BUSL-1.1

package remote

import (
	"errors"
	"sync"
	"testing"

	"github.com/zclconf/go-cty/cty"

	"github.com/hashicorp/terraform/internal/addrs"
	"github.com/hashicorp/terraform/internal/states"
	"github.com/hashicorp/terraform/internal/tfdiags"
)

// errFlakyNetwork is the synthetic error reported by flakyClient while the
// simulated remote storage connection is down.
var errFlakyNetwork = errors.New("simulated network outage: connection to remote state storage lost")

// flakyClient is an in-memory [Client] whose Get and Put calls can be made
// to fail, either for the duration of a simulated outage or for a fixed
// number of transient failures, to exercise the remote state manager's
// checkpoint/recovery behavior. It is safe for concurrent use.
type flakyClient struct {
	mu sync.Mutex

	current   []byte
	connected bool

	// failGets/failPuts count down transient failures while the connection
	// itself is considered up.
	failGets int
	failPuts int

	getAttempts int
	getFailures int
	putAttempts int
	putFailures int
}

var _ Client = (*flakyClient)(nil)

func (c *flakyClient) Get() (*Payload, tfdiags.Diagnostics) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.getAttempts++
	if !c.connected || c.failGets > 0 {
		if c.failGets > 0 {
			c.failGets--
		}
		c.getFailures++
		return nil, tfdiags.Diagnostics{}.Append(tfdiags.Sourceless(
			tfdiags.Error,
			"Failed to read remote state",
			errFlakyNetwork.Error(),
		))
	}
	if c.current == nil {
		return nil, nil
	}
	return &Payload{Data: append([]byte(nil), c.current...)}, nil
}

func (c *flakyClient) Put(data []byte) tfdiags.Diagnostics {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.putAttempts++
	if !c.connected || c.failPuts > 0 {
		if c.failPuts > 0 {
			c.failPuts--
		}
		c.putFailures++
		return tfdiags.Diagnostics{}.Append(tfdiags.Sourceless(
			tfdiags.Error,
			"Failed to persist remote state",
			errFlakyNetwork.Error(),
		))
	}
	c.current = append([]byte(nil), data...)
	return nil
}

func (c *flakyClient) Delete() tfdiags.Diagnostics {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.current = nil
	return nil
}

func (c *flakyClient) disconnect() {
	c.mu.Lock()
	c.connected = false
	c.mu.Unlock()
}

func (c *flakyClient) connect() {
	c.mu.Lock()
	c.connected = true
	c.mu.Unlock()
}

// During an outage every checkpoint attempt fails and nothing reaches
// durable storage; after reconnecting the persist succeeds and a brand-new
// manager (i.e. a restarted Terraform process) loads exactly the last
// consistently persisted checkpoint, with lineage preserved.
func TestRemoteState_NetworkOutageAndReconnect(t *testing.T) {
	client := &flakyClient{connected: true}
	mgr := &State{Client: client}

	checkpoint := states.NewState()
	checkpoint.SetOutputValue(
		addrs.OutputValue{Name: "v"}.Absolute(addrs.RootModuleInstance),
		cty.StringVal("checkpoint"), false,
	)
	if err := mgr.WriteState(checkpoint); err != nil {
		t.Fatalf("write failed: %s", err)
	}

	// Lose the connection: no checkpoint can land remotely. PersistState
	// first checks for an existing snapshot, so the failure may surface as
	// a failed Get or a failed Put depending on timing.
	client.disconnect()
	if err := mgr.PersistState(nil); err == nil {
		t.Fatal("persist should fail while the network is down")
	}
	if client.getFailures+client.putFailures == 0 {
		t.Fatal("expected failed Get or Put calls during the outage")
	}

	// Reconnect and retry the persist, as Terraform would on a resumed run.
	client.connect()
	if err := mgr.PersistState(nil); err != nil {
		t.Fatalf("persist after reconnect failed: %s", err)
	}
	lineage := mgr.lineage
	if lineage == "" {
		t.Fatal("persisted checkpoint has no lineage")
	}
	if mgr.serial == 0 {
		t.Fatal("persisted checkpoint has no serial")
	}

	// Simulate a process restart: a fresh manager over the same "remote"
	// storage must observe the last consistent checkpoint, and nothing
	// older or newer.
	restarted := &State{Client: client}
	if err := restarted.RefreshState(); err != nil {
		t.Fatalf("refresh after restart failed: %s", err)
	}
	got := restarted.State()
	if !got.Equal(checkpoint) {
		t.Fatalf("restart did not load the last consistent checkpoint\ngot:\n%s\nwant:\n%s", got, checkpoint)
	}
	if restarted.lineage != lineage {
		t.Fatalf("lineage changed across restart: got %q, want %q", restarted.lineage, lineage)
	}

	// Further checkpoints after the restart advance the serial within the
	// same lineage, so concurrent writers could still order snapshots.
	next := checkpoint.DeepCopy()
	next.SetOutputValue(
		addrs.OutputValue{Name: "v"}.Absolute(addrs.RootModuleInstance),
		cty.StringVal("checkpoint-2"), false,
	)
	if err := restarted.WriteState(next); err != nil {
		t.Fatalf("write failed: %s", err)
	}
	if err := restarted.PersistState(nil); err != nil {
		t.Fatalf("second persist failed: %s", err)
	}
	if restarted.lineage != lineage {
		t.Fatalf("lineage changed across persist: got %q, want %q", restarted.lineage, lineage)
	}
	if restarted.serial <= mgr.serial {
		t.Fatalf("serial did not advance after a new checkpoint: got %d, want > %d", restarted.serial, mgr.serial)
	}
}

// A small number of transient Put failures followed by success models a
// connection that drops and is immediately re-established while a single
// logical checkpoint operation is being retried.
func TestRemoteState_TransientPutFailures(t *testing.T) {
	client := &flakyClient{connected: true, failPuts: 2}
	mgr := &State{Client: client}

	checkpoint := states.NewState()
	checkpoint.SetOutputValue(
		addrs.OutputValue{Name: "v"}.Absolute(addrs.RootModuleInstance),
		cty.StringVal("retry"), false,
	)
	if err := mgr.WriteState(checkpoint); err != nil {
		t.Fatalf("write failed: %s", err)
	}

	// First two attempts hit the dropped connection and fail.
	if err := mgr.PersistState(nil); err == nil {
		t.Fatal("first persist attempt should fail")
	}
	if err := mgr.PersistState(nil); err == nil {
		t.Fatal("second persist attempt should fail")
	}
	if got, want := client.putFailures, 2; got != want {
		t.Fatalf("wrong number of failed puts: got %d, want %d", got, want)
	}

	// The retry after reconnect succeeds and the checkpoint is durable.
	if err := mgr.PersistState(nil); err != nil {
		t.Fatalf("persist after reconnect failed: %s", err)
	}
	if got, want := client.putAttempts, 3; got != want {
		t.Fatalf("wrong number of put attempts: got %d, want %d", got, want)
	}

	restarted := &State{Client: client}
	if err := restarted.RefreshState(); err != nil {
		t.Fatalf("refresh failed: %s", err)
	}
	if got := restarted.State(); !got.Equal(checkpoint) {
		t.Fatalf("durable checkpoint mismatch\ngot:\n%s\nwant:\n%s", got, checkpoint)
	}
}
