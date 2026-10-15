// Copyright IBM Corp. 2026
// SPDX-License-Identifier: BUSL-1.1

package terraform

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zclconf/go-cty/cty"

	"github.com/hashicorp/terraform/internal/addrs"
	"github.com/hashicorp/terraform/internal/configs"
	"github.com/hashicorp/terraform/internal/configs/configschema"
	"github.com/hashicorp/terraform/internal/plans"
	"github.com/hashicorp/terraform/internal/providers"
	provider_testing "github.com/hashicorp/terraform/internal/providers/testing"
	"github.com/hashicorp/terraform/internal/states"
	"github.com/hashicorp/terraform/internal/states/statemgr"
	"github.com/hashicorp/terraform/internal/tfdiags"
)

// These tests exercise Terraform's checkpoint/recovery behavior under
// injected faults: provider timeouts interrupted by cancellation, local
// disk failures, remote-storage network outages with reconnection, and
// failures occurring while resources are applied concurrently.
//
// The property under test is that after a fault Terraform resumes from the
// last *durable* consistent checkpoint (an intermediate state snapshot
// written through the statemgr) and never re-submits provider operations
// for resources already recorded in that checkpoint.

const flakyProviderSource = "terraform.io/test/flaky"

// faultNodeNames is the resource name order of the apply-fault fixture.
var faultNodeNames = []string{"a", "b", "c", "d"}

// ---------------------------------------------------------------------------
// Test provider: counts "submissions" (ApplyResourceChange calls) and can
// block on barriers, fail a number of times, or be stopped mid-apply.
// ---------------------------------------------------------------------------

// applyBarrier blocks an ApplyResourceChange call until the test releases
// it, or until the provider is stopped (simulating cancellation).
type applyBarrier struct {
	entered  chan struct{}
	released chan struct{}

	enterOnce   sync.Once
	releaseOnce sync.Once
}

func newApplyBarrier() *applyBarrier {
	return &applyBarrier{
		entered:  make(chan struct{}),
		released: make(chan struct{}),
	}
}

func (b *applyBarrier) await(stop <-chan struct{}) {
	b.enterOnce.Do(func() { close(b.entered) })
	select {
	case <-b.released:
	case <-stop:
	}
}

func (b *applyBarrier) release() { b.releaseOnce.Do(func() { close(b.released) }) }

type faultApplyRuntime struct {
	mu sync.Mutex

	// attempts counts ApplyResourceChange submissions per resource name,
	// across every simulated process restart.
	attempts map[string]int
	// successes counts submissions that returned a real new state.
	successes map[string]int
	// failNext holds remaining hard provider failures per resource name.
	failNext map[string]int
	// barriers, if present for a resource name, block its apply call.
	barriers map[string]*applyBarrier

	stopCh   chan struct{}
	stopOnce sync.Once
}

func newFaultApplyRuntime() *faultApplyRuntime {
	return &faultApplyRuntime{
		attempts:  map[string]int{},
		successes: map[string]int{},
		failNext:  map[string]int{},
		barriers:  map[string]*applyBarrier{},
		stopCh:    make(chan struct{}),
	}
}

func (rt *faultApplyRuntime) barrier(name string) *applyBarrier {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	b := newApplyBarrier()
	rt.barriers[name] = b
	return b
}

// failOnce arranges for the next apply of the named resource to return an
// error diagnostic (simulating a failed remote API call).
func (rt *faultApplyRuntime) failOnce(name string) {
	rt.mu.Lock()
	rt.failNext[name]++
	rt.mu.Unlock()
}

func (rt *faultApplyRuntime) stop() {
	rt.stopOnce.Do(func() { close(rt.stopCh) })
}

func (rt *faultApplyRuntime) snapshotAttempts() map[string]int {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	out := make(map[string]int, len(rt.attempts))
	for k, v := range rt.attempts {
		out[k] = v
	}
	return out
}

// provider builds a fresh mock provider. A fresh provider is created for
// every context, modeling a separate Terraform process after a restart,
// while the shared runtime preserves cross-run counters.
//
// The returned provider wraps provider_testing.MockProvider but overrides
// ApplyResourceChange so that a blocked call does not hold the mock's
// internal mutex: unlike the mock, real providers can serve concurrent
// calls up to the walk's parallelism limit, which the concurrent-failure
// test relies on.
func (rt *faultApplyRuntime) provider() providers.Interface {
	p := new(provider_testing.MockProvider)
	p.GetProviderSchemaResponse = &providers.GetProviderSchemaResponse{
		ResourceTypes: map[string]providers.Schema{
			"flaky_node": {
				Body: &configschema.Block{
					Attributes: map[string]*configschema.Attribute{
						"id": {
							Type:     cty.String,
							Computed: true,
						},
						"name": {
							Type:     cty.String,
							Optional: true,
						},
					},
				},
			},
		},
	}
	p.PlanResourceChangeFn = rt.planResourceChange
	p.StopFn = func() error {
		rt.stop()
		return nil
	}
	return &faultMockProvider{MockProvider: p, rt: rt}
}

type faultMockProvider struct {
	*provider_testing.MockProvider
	rt *faultApplyRuntime
}

func (p *faultMockProvider) ApplyResourceChange(req providers.ApplyResourceChangeRequest) providers.ApplyResourceChangeResponse {
	return p.rt.applyResourceChange(req)
}

func (rt *faultApplyRuntime) factory() providers.Factory {
	return func() (providers.Interface, error) {
		return rt.provider(), nil
	}
}

func (rt *faultApplyRuntime) planResourceChange(req providers.PlanResourceChangeRequest) providers.PlanResourceChangeResponse {
	// A destroy plan.
	if req.ProposedNewState.IsNull() {
		return providers.PlanResourceChangeResponse{
			PlannedState:   req.ProposedNewState,
			PlannedPrivate: req.PriorPrivate,
		}
	}
	// An object already recorded in the checkpoint state plans as a no-op,
	// which is what prevents duplicate submissions after a restart.
	if !req.PriorState.IsNull() {
		return providers.PlanResourceChangeResponse{PlannedState: req.PriorState}
	}
	planned := req.ProposedNewState.AsValueMap()
	planned["id"] = cty.UnknownVal(cty.String)
	return providers.PlanResourceChangeResponse{PlannedState: cty.ObjectVal(planned)}
}

func (rt *faultApplyRuntime) applyResourceChange(req providers.ApplyResourceChangeRequest) providers.ApplyResourceChangeResponse {
	// The provider protocol does not carry the resource address, so the
	// fixture identifies each instance via its "name" attribute.
	nameVal := req.Config.GetAttr("name")
	name := nameVal.AsString()

	rt.mu.Lock()
	rt.attempts[name]++
	barrier := rt.barriers[name]
	rt.mu.Unlock()

	// Block first so the test can deterministically arrange faults while
	// the call is in flight; cancellation (Stop) releases the barrier.
	if barrier != nil {
		barrier.await(rt.stopCh)
	}

	// Read any pending failure only after the barrier is released, so the
	// test decides the outcome of the in-flight call.
	rt.mu.Lock()
	fails := rt.failNext[name]
	if fails > 0 {
		rt.failNext[name]--
	}
	rt.mu.Unlock()
	if fails > 0 {
		return providers.ApplyResourceChangeResponse{
			Diagnostics: tfdiags.Diagnostics{}.Append(tfdiags.Sourceless(
				tfdiags.Error,
				"simulated provider fault",
				fmt.Sprintf("ApplyResourceChange for flaky_node.%s failed due to an injected fault", name),
			)),
		}
	}

	planned := req.PlannedState.AsValueMap()
	planned["id"] = cty.StringVal("flaky_node/" + name)

	rt.mu.Lock()
	rt.successes[name]++
	rt.mu.Unlock()

	return providers.ApplyResourceChangeResponse{NewState: cty.ObjectVal(planned)}
}

// ---------------------------------------------------------------------------
// Checkpoint hook: mirrors local.StateHook by writing every state update
// and immediately persisting it as a durable checkpoint. Transient network
// errors can optionally be retried in place, modeling transport-level
// reconnection performed by real remote-state clients.
// ---------------------------------------------------------------------------

type faultCheckpointHook struct {
	NilHook
	mu  sync.Mutex
	mgr statemgr.Full

	// persistRetries is the number of extra persist attempts after a
	// transient network error.
	persistRetries int
	// onPersistFailure, if set, is invoked before each retry (typically to
	// drive a reconnect).
	onPersistFailure func(error)

	checkpoints int32
}

var _ Hook = (*faultCheckpointHook)(nil)

func (h *faultCheckpointHook) PostStateUpdate(new *states.State) (HookAction, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if err := h.mgr.WriteState(new); err != nil {
		return HookActionHalt, err
	}

	var err error
	for attempt := 0; attempt <= h.persistRetries; attempt++ {
		err = h.mgr.PersistState(nil)
		if err == nil {
			atomic.AddInt32(&h.checkpoints, 1)
			return HookActionContinue, nil
		}
		if !errors.Is(err, statemgr.ErrInjectedNetwork) || attempt == h.persistRetries {
			break
		}
		if h.onPersistFailure != nil {
			h.onPersistFailure(err)
		}
	}
	return HookActionHalt, err
}

func (h *faultCheckpointHook) checkpointCount() int {
	return int(atomic.LoadInt32(&h.checkpoints))
}

// ---------------------------------------------------------------------------
// Small shared helpers
// ---------------------------------------------------------------------------

func flakyNodeAddr(name string) addrs.AbsResourceInstance {
	return addrs.Resource{
		Mode: addrs.ManagedResourceMode,
		Type: "flaky_node",
		Name: name,
	}.Instance(addrs.NoKey).Absolute(addrs.RootModuleInstance)
}

func (rt *faultApplyRuntime) newContext(t *testing.T, moduleName string, hooks []Hook, mgr statemgr.Full) (*Context, *configs.Config) {
	t.Helper()
	m := testModule(t, moduleName)
	ctx := testContext2(t, &ContextOpts{
		Hooks: hooks,
		Providers: map[addrs.Provider]providers.Factory{
			addrs.MustParseProviderSourceString(flakyProviderSource): rt.factory(),
		},
	})
	return ctx, m
}

func faultPlan(t *testing.T, ctx *Context, m *configs.Config, prior *states.State) *plans.Plan {
	t.Helper()
	plan, diags := ctx.Plan(m, prior, DefaultPlanOpts)
	if diags.HasErrors() {
		t.Fatalf("unexpected plan error: %s", diags.Err())
	}
	return plan
}

func faultReloadCheckpoint(t *testing.T, mgr statemgr.Full) *states.State {
	t.Helper()
	if err := mgr.RefreshState(); err != nil {
		t.Fatalf("failed to reload checkpoint: %s", err)
	}
	state := mgr.State()
	if state == nil {
		state = states.NewState()
	}
	return state
}

func faultCountCreates(plan *plans.Plan) int {
	n := 0
	for _, rc := range plan.Changes.Resources {
		if rc.Action == plans.Create {
			n++
		}
	}
	return n
}

func faultStateHasNodes(t *testing.T, state *states.State, names ...string) {
	t.Helper()
	for _, name := range names {
		if obj := state.ResourceInstance(flakyNodeAddr(name)); obj == nil || obj.Current == nil {
			t.Errorf("state is missing flaky_node.%s", name)
		}
	}
}

func faultStateNodeNames(state *states.State) map[string]bool {
	out := map[string]bool{}
	if state == nil {
		return out
	}
	for name := range faultAllNodeNames {
		if obj := state.ResourceInstance(flakyNodeAddr(name)); obj != nil && obj.Current != nil {
			out[name] = true
		}
	}
	return out
}

// faultAllNodeNames is the superset of names used by either fixture.
var faultAllNodeNames = map[string]bool{
	"a": true,
	"b": true,
	"c": true,
	"d": true,
	"e": true,
	"f": true,
}

func faultWaitFor(t *testing.T, what string, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// faultDiagHas reports whether the diagnostics contain the injected fault.
// Terraform converts hook errors into diagnostics, which preserve the
// message but not the original error identity, so we match on text.
func faultDiagHas(diags tfdiags.Diagnostics, sentinel error) bool {
	return diags.HasErrors() && strings.Contains(diags.Err().Error(), sentinel.Error())
}

// ---------------------------------------------------------------------------
// Timeout + cancellation: after a graceful stop, the operation resumes from
// the checkpoint and every resource is submitted at most once.
// ---------------------------------------------------------------------------

func TestContext2Apply_faultTimeoutCancel_resumeCheckpoint(t *testing.T) {
	mgr, _ := statemgr.NewFaulty(statemgr.NewFullFake(nil, nil))
	rt := newFaultApplyRuntime()

	// "b" is the call that appears to hang until a timeout prompts the user
	// to cancel the run.
	barrier := rt.barrier("b")

	h := &faultCheckpointHook{mgr: mgr}
	ctx, m := rt.newContext(t, "apply-fault", []Hook{h}, mgr)
	plan := faultPlan(t, ctx, m, states.NewState())

	done := make(chan struct{})
	var applyDiags tfdiags.Diagnostics
	go func() {
		defer close(done)
		_, applyDiags = ctx.Apply(plan, m, nil)
	}()

	select {
	case <-barrier.entered:
	case <-time.After(15 * time.Second):
		t.Fatal("ApplyResourceChange for flaky_node.b never started")
	}

	// Cancel while b is in flight: Core stops the provider and unwinds.
	ctx.Stop()

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("apply did not unwind after Stop")
	}
	if !applyDiags.HasErrors() {
		t.Fatal("expected the cancelled apply to report a halt error")
	}

	// Simulate a new Terraform process loading the last durable checkpoint.
	cp := faultReloadCheckpoint(t, mgr)
	checkpointed := faultStateNodeNames(cp)
	// b was in flight and completed during the graceful stop, so it is
	// always checkpointed. Depending on scheduling, a may or may not have
	// finished before the walk unwound; c depends on both a and b and d on
	// c, so neither can be in the checkpoint.
	if !checkpointed["b"] {
		t.Fatalf("checkpoint should contain completed flaky_node.b, got %v", checkpointed)
	}
	if checkpointed["c"] || checkpointed["d"] {
		t.Fatalf("checkpoint must not contain resources after the stopped flaky_node.b, got %v", checkpointed)
	}
	if checkpointed["a"] {
		// If a finished, c still could not have started because c needs b
		// too; the checkpoint may be {a,b}.
		if got, want := len(checkpointed), 2; got != want {
			t.Fatalf("unexpected checkpoint %v", checkpointed)
		}
	} else if got, want := len(checkpointed), 1; got != want {
		t.Fatalf("unexpected checkpoint %v", checkpointed)
	}
	beforeRestart := rt.snapshotAttempts()

	// Restart: re-plan against the checkpoint. Checkpointed resources must
	// not be submitted again.
	h2 := &faultCheckpointHook{mgr: mgr}
	ctx2, m2 := rt.newContext(t, "apply-fault", []Hook{h2}, mgr)
	plan2 := faultPlan(t, ctx2, m2, cp)
	if got, want := faultCountCreates(plan2), len(faultNodeNames)-len(checkpointed); got != want {
		t.Fatalf("restart plan should create the resources missing from the checkpoint: got %d creates, want %d (checkpoint=%v)", got, want, checkpointed)
	}

	state2, diags2 := ctx2.Apply(plan2, m2, nil)
	if diags2.HasErrors() {
		t.Fatalf("apply after restart failed: %s", diags2.Err())
	}
	faultStateHasNodes(t, state2, faultNodeNames...)

	for _, name := range faultNodeNames {
		if got, want := rt.attempts[name], 1; got != want {
			t.Errorf("flaky_node.%s was submitted %d times across restart; want exactly 1", name, got)
		}
		if checkpointed[name] {
			if rt.attempts[name]-beforeRestart[name] != 0 {
				t.Errorf("checkpointed flaky_node.%s was re-submitted after restart", name)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Hard disk failure mid-apply: the crashed process leaves the last
// successfully-written state file intact; a restarted process (new state
// manager over the same file) resumes from it without resubmitting the
// checkpointed resources.
// ---------------------------------------------------------------------------

func TestContext2Apply_faultDisk_restartFromCheckpoint(t *testing.T) {
	path := filepath.Join(t.TempDir(), "terraform.tfstate")
	mgr, ctrl := statemgr.NewFaulty(statemgr.NewFilesystem(path))
	lockID, err := mgr.Lock(statemgr.NewLockInfo())
	if err != nil {
		t.Fatalf("failed to lock state: %s", err)
	}
	rt := newFaultApplyRuntime()

	barrier := rt.barrier("c")
	h := &faultCheckpointHook{mgr: mgr}
	ctx, m := rt.newContext(t, "apply-fault", []Hook{h}, mgr)
	plan := faultPlan(t, ctx, m, states.NewState())

	done := make(chan struct{})
	var applyDiags tfdiags.Diagnostics
	go func() {
		defer close(done)
		_, applyDiags = ctx.Apply(plan, m, nil)
	}()

	select {
	case <-barrier.entered:
	case <-time.After(15 * time.Second):
		t.Fatal("ApplyResourceChange for flaky_node.c never started")
	}
	// c can only start once a and b have been applied and checkpointed.
	faultWaitFor(t, "a and b checkpoints", 15*time.Second, func() bool {
		return h.checkpointCount() == 2
	})

	// The disk dies: every subsequent state write fails. The last durable
	// file on disk remains the two-resource checkpoint.
	ctrl.FailNext(statemgr.FaultWriteState, statemgr.FaultDisk, 1<<20)
	barrier.release()

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("apply did not unwind after disk failure")
	}
	if !faultDiagHas(applyDiags, statemgr.ErrInjectedDisk) {
		t.Fatalf("expected injected disk error, got %s", applyDiags.Err())
	}

	// The crashed process releases its state lock and file handle before a
	// fresh process takes over.
	if err := mgr.Unlock(lockID); err != nil {
		t.Fatalf("failed to unlock crashed manager: %s", err)
	}

	// Simulate the disk being replaced and Terraform restarting: a brand
	// new, healthy manager loads the checkpoint from the same path.
	mgr2, _ := statemgr.NewFaulty(statemgr.NewFilesystem(path))
	restartLockID, err := mgr2.Lock(statemgr.NewLockInfo())
	if err != nil {
		t.Fatalf("failed to lock state after restart: %s", err)
	}
	defer mgr2.Unlock(restartLockID)
	cp := faultReloadCheckpoint(t, mgr2)
	checkpointed := faultStateNodeNames(cp)
	if !checkpointed["a"] || !checkpointed["b"] {
		t.Fatalf("last consistent checkpoint should be {a,b}, got %v", checkpointed)
	}
	if checkpointed["c"] || checkpointed["d"] {
		t.Fatalf("checkpoint must not contain resources applied after the disk failed, got %v", checkpointed)
	}
	metaBefore := mgr2.StateSnapshotMeta()
	if metaBefore.Lineage == "" {
		t.Fatal("checkpoint has no lineage")
	}
	beforeRestart := rt.snapshotAttempts()

	h2 := &faultCheckpointHook{mgr: mgr2}
	ctx2, m2 := rt.newContext(t, "apply-fault", []Hook{h2}, mgr2)
	plan2 := faultPlan(t, ctx2, m2, cp)
	if got, want := faultCountCreates(plan2), 2; got != want {
		t.Fatalf("restart plan should create only c and d, got %d creates", got)
	}

	state2, diags2 := ctx2.Apply(plan2, m2, nil)
	if diags2.HasErrors() {
		t.Fatalf("apply after restart failed: %s", diags2.Err())
	}
	faultStateHasNodes(t, state2, faultNodeNames...)

	// The no-duplicate-submission guarantee for checkpointed resources.
	for name := range checkpointed {
		if delta := rt.attempts[name] - beforeRestart[name]; delta != 0 {
			t.Errorf("checkpointed flaky_node.%s was re-submitted %d times after restart", name, delta)
		}
		if rt.successes[name] != 1 {
			t.Errorf("flaky_node.%s has %d successful submissions, want 1", name, rt.successes[name])
		}
	}
	// c was the in-flight operation whose result could not be checkpointed;
	// it is the only operation Terraform cannot know about, so the restart
	// replans it. d was never started and is submitted once.
	if got, want := rt.attempts["d"], 1; got != want {
		t.Errorf("flaky_node.d was submitted %d times, want 1", got)
	}

	// Lineage is preserved across the restart while serials advance. The
	// resumed state must also be durably readable by a further refresh.
	reloaded := faultReloadCheckpoint(t, mgr2)
	faultStateHasNodes(t, reloaded, faultNodeNames...)
	metaAfter := mgr2.StateSnapshotMeta()
	if metaAfter.Lineage != metaBefore.Lineage {
		t.Errorf("lineage changed across restart: got %q, want %q", metaAfter.Lineage, metaBefore.Lineage)
	}
	if metaAfter.Serial <= metaBefore.Serial {
		t.Errorf("serial did not advance after resumed apply: got %d, want > %d", metaAfter.Serial, metaBefore.Serial)
	}
}

// ---------------------------------------------------------------------------
// Remote storage network outage: intermediate checkpoints stop landing
// durably while disconnected; after reconnecting, a restart resumes from
// the last checkpoint without re-submitting checkpointed resources.
// ---------------------------------------------------------------------------

func TestContext2Apply_faultNetworkDisconnect_restartFromCheckpoint(t *testing.T) {
	mgr, ctrl := statemgr.NewFaulty(statemgr.NewFullFake(nil, nil))
	rt := newFaultApplyRuntime()

	barrier := rt.barrier("c")
	h := &faultCheckpointHook{mgr: mgr}
	ctx, m := rt.newContext(t, "apply-fault", []Hook{h}, mgr)
	plan := faultPlan(t, ctx, m, states.NewState())

	done := make(chan struct{})
	var applyDiags tfdiags.Diagnostics
	go func() {
		defer close(done)
		_, applyDiags = ctx.Apply(plan, m, nil)
	}()

	select {
	case <-barrier.entered:
	case <-time.After(15 * time.Second):
		t.Fatal("ApplyResourceChange for flaky_node.c never started")
	}
	faultWaitFor(t, "a and b checkpoints", 15*time.Second, func() bool {
		return h.checkpointCount() == 2
	})

	// Network to the remote state backend drops while c is in flight.
	ctrl.Disconnect()
	barrier.release()

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("apply did not unwind after network failure")
	}
	if !faultDiagHas(applyDiags, statemgr.ErrInjectedNetwork) {
		t.Fatalf("expected injected network error, got %s", applyDiags.Err())
	}

	// Reconnect and restart. RefreshState must load the durable two-resource
	// checkpoint rather than the unpersisted three-resource transient
	// snapshot.
	ctrl.Reconnect()
	cp := faultReloadCheckpoint(t, mgr)
	checkpointed := faultStateNodeNames(cp)
	if !checkpointed["a"] || !checkpointed["b"] {
		t.Fatalf("last consistent checkpoint should be {a,b}, got %v", checkpointed)
	}
	if checkpointed["c"] || checkpointed["d"] {
		t.Fatalf("unpersisted resources must not survive as checkpoint, got %v", checkpointed)
	}
	beforeRestart := rt.snapshotAttempts()

	h2 := &faultCheckpointHook{mgr: mgr}
	ctx2, m2 := rt.newContext(t, "apply-fault", []Hook{h2}, mgr)
	plan2 := faultPlan(t, ctx2, m2, cp)
	if got, want := faultCountCreates(plan2), 2; got != want {
		t.Fatalf("restart plan should create only c and d, got %d creates", got)
	}

	state2, diags2 := ctx2.Apply(plan2, m2, nil)
	if diags2.HasErrors() {
		t.Fatalf("apply after reconnect failed: %s", diags2.Err())
	}
	faultStateHasNodes(t, state2, faultNodeNames...)

	for name := range checkpointed {
		if delta := rt.attempts[name] - beforeRestart[name]; delta != 0 {
			t.Errorf("checkpointed flaky_node.%s was re-submitted %d times after reconnect", name, delta)
		}
	}
	if got, want := rt.attempts["d"], 1; got != want {
		t.Errorf("flaky_node.d was submitted %d times, want 1", got)
	}
}

// ---------------------------------------------------------------------------
// Transient network blip with in-place reconnection: the checkpoint hook
// retries the failed persist after reconnecting, the same run completes,
// and a subsequent restart has nothing left to do.
// ---------------------------------------------------------------------------

func TestContext2Apply_faultNetworkReconnect_retryCheckpoint(t *testing.T) {
	mgr, ctrl := statemgr.NewFaulty(statemgr.NewFullFake(nil, nil))
	rt := newFaultApplyRuntime()

	h := &faultCheckpointHook{
		mgr:            mgr,
		persistRetries: 3,
		onPersistFailure: func(error) {
			// Model the remote client re-establishing its connection before
			// the checkpoint is retried.
			ctrl.Reconnect()
		},
	}
	ctx, m := rt.newContext(t, "apply-fault", []Hook{h}, mgr)
	plan := faultPlan(t, ctx, m, states.NewState())

	// Exactly one checkpoint attempt hits a dropped connection; the
	// FailNext schedule restores the connection once consumed.
	ctrl.FailNext(statemgr.FaultPersistState, statemgr.FaultNetwork, 1)

	state, diags := ctx.Apply(plan, m, nil)
	if diags.HasErrors() {
		t.Fatalf("apply should complete after in-place reconnect: %s", diags.Err())
	}
	faultStateHasNodes(t, state, faultNodeNames...)
	if got, want := h.checkpointCount(), len(faultNodeNames); got != want {
		t.Fatalf("expected %d durable checkpoints, got %d", want, got)
	}
	if got, want := ctrl.Failures(statemgr.FaultPersistState), 1; got != want {
		t.Fatalf("expected exactly one failed persist, got %d", got)
	}

	// Restart from the final checkpoint: the plan must be a no-op and no
	// resource may be submitted again.
	cp := faultReloadCheckpoint(t, mgr)
	beforeRestart := rt.snapshotAttempts()

	ctx2, m2 := rt.newContext(t, "apply-fault", nil, mgr)
	plan2 := faultPlan(t, ctx2, m2, cp)
	if got := faultCountCreates(plan2); got != 0 {
		t.Fatalf("expected no-op plan after recovery, got %d creates", got)
	}
	if _, diags := ctx2.Apply(plan2, m2, nil); diags.HasErrors() {
		t.Fatalf("no-op apply failed: %s", diags.Err())
	}
	for name := range beforeRestart {
		if rt.attempts[name] != beforeRestart[name] {
			t.Errorf("flaky_node.%s was re-submitted after recovery", name)
		}
		if rt.attempts[name] != 1 {
			t.Errorf("flaky_node.%s was submitted %d times, want exactly 1", name, rt.attempts[name])
		}
	}
}

// ---------------------------------------------------------------------------
// Concurrent failure: six independent resources apply in parallel. "c" is
// held inside ApplyResourceChange while the other five complete and reach a
// durable checkpoint; when c is released it fails. The restart must only
// re-submit c, leaving the five checkpointed resources untouched.
// ---------------------------------------------------------------------------

func TestContext2Apply_faultConcurrent(t *testing.T) {
	parallelNames := []string{"a", "b", "c", "d", "e", "f"}

	mgr, _ := statemgr.NewFaulty(statemgr.NewFullFake(nil, nil))
	rt := newFaultApplyRuntime()

	barrier := rt.barrier("c")
	h := &faultCheckpointHook{mgr: mgr}
	ctx, m := rt.newContext(t, "apply-fault-parallel", []Hook{h}, mgr)
	plan := faultPlan(t, ctx, m, states.NewState())

	done := make(chan struct{})
	var applyDiags tfdiags.Diagnostics
	go func() {
		defer close(done)
		_, applyDiags = ctx.Apply(plan, m, nil)
	}()

	select {
	case <-barrier.entered:
	case <-time.After(15 * time.Second):
		t.Fatal("ApplyResourceChange for flaky_node.c never started")
	}
	// With the other five vertices unblocked and the walk parallelism well
	// above six, they must all finish while c is in flight.
	faultWaitFor(t, "five parallel checkpoints", 30*time.Second, func() bool {
		return h.checkpointCount() == len(parallelNames)-1
	})

	// Release c into a provider failure (e.g. a remote API error).
	rt.failOnce("c")
	barrier.release()

	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("apply did not unwind after concurrent failure")
	}
	if !applyDiags.HasErrors() {
		t.Fatal("expected the failed resource to fail the apply")
	}

	cp := faultReloadCheckpoint(t, mgr)
	checkpointed := faultStateNodeNames(cp)
	for _, name := range parallelNames {
		if name == "c" {
			if checkpointed[name] {
				t.Fatal("failed flaky_node.c must not appear in the checkpoint")
			}
			continue
		}
		if !checkpointed[name] {
			t.Fatalf("checkpoint should contain completed flaky_node.%s, got %v", name, checkpointed)
		}
	}
	beforeRestart := rt.snapshotAttempts()

	// Restart: only c remains to apply.
	h2 := &faultCheckpointHook{mgr: mgr}
	ctx2, m2 := rt.newContext(t, "apply-fault-parallel", []Hook{h2}, mgr)
	plan2 := faultPlan(t, ctx2, m2, cp)
	if got, want := faultCountCreates(plan2), 1; got != want {
		t.Fatalf("restart plan should retry only flaky_node.c, got %d creates", got)
	}

	state2, diags2 := ctx2.Apply(plan2, m2, nil)
	if diags2.HasErrors() {
		t.Fatalf("apply after restart failed: %s", diags2.Err())
	}
	faultStateHasNodes(t, state2, parallelNames...)

	for _, name := range parallelNames {
		if name == "c" {
			// One failed attempt in run one, one successful attempt after
			// the restart.
			if got, want := rt.attempts["c"], 2; got != want {
				t.Errorf("flaky_node.c was submitted %d times, want %d", got, want)
			}
			if got, want := rt.successes["c"], 1; got != want {
				t.Errorf("flaky_node.c has %d successful submissions, want %d", got, want)
			}
			continue
		}
		if got, want := rt.attempts[name], 1; got != want {
			t.Errorf("flaky_node.%s was submitted %d times, want exactly 1", name, got)
		}
		if delta := rt.attempts[name] - beforeRestart[name]; delta != 0 {
			t.Errorf("checkpointed flaky_node.%s was re-submitted %d times after concurrent failure", name, delta)
		}
	}
}
