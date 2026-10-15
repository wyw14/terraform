// Copyright IBM Corp. 2014, 2026
// SPDX-License-Identifier: BUSL-1.1

package terraform

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/zclconf/go-cty/cty"

	"github.com/hashicorp/terraform/internal/addrs"
	"github.com/hashicorp/terraform/internal/configs/configschema"
	"github.com/hashicorp/terraform/internal/plans"
	"github.com/hashicorp/terraform/internal/providers"
	testing_provider "github.com/hashicorp/terraform/internal/providers/testing"
	"github.com/hashicorp/terraform/internal/states"
	"github.com/hashicorp/terraform/internal/tfdiags"
)

// refreshStateTestSchema returns the schema shared by the tests in this file.
// It mixes a plain attribute and a sensitive one so that sensitivity handling
// during refresh can be verified.
func refreshStateTestSchema() *configschema.Block {
	return &configschema.Block{
		Attributes: map[string]*configschema.Attribute{
			"id": {
				Type:     cty.String,
				Computed: true,
			},
			"arg": {
				Type:     cty.String,
				Optional: true,
			},
			"secret": {
				Type:      cty.String,
				Optional:  true,
				Sensitive: true,
			},
		},
	}
}

func refreshStateTestProvider(t *testing.T) *testing_provider.MockProvider {
	t.Helper()
	p := simpleMockProvider()
	p.GetProviderSchemaResponse = &providers.GetProviderSchemaResponse{
		Provider: providers.Schema{Body: simpleTestSchema()},
		ResourceTypes: map[string]providers.Schema{
			"test_object": {
				Body: refreshStateTestSchema(),
			},
		},
	}
	p.PlanResourceChangeFn = func(req providers.PlanResourceChangeRequest) providers.PlanResourceChangeResponse {
		return providers.PlanResourceChangeResponse{
			PlannedState: req.ProposedNewState,
		}
	}
	return p
}

func refreshStateTestContext(t *testing.T, p *testing_provider.MockProvider, parallelism int) *Context {
	t.Helper()
	return testContext2(t, &ContextOpts{
		Parallelism: parallelism,
		Providers: map[addrs.Provider]providers.Factory{
			addrs.NewDefaultProvider("test"): testProviderFuncFixed(p),
		},
	})
}

func refreshStateTestState() *states.State {
	addr := mustResourceInstanceAddr("test_object.a")
	return states.BuildState(func(s *states.SyncState) {
		s.SetResourceInstanceCurrent(addr, &states.ResourceInstanceObjectSrc{
			AttrsJSON: []byte(`{"id":"existing","arg":"VALID","secret":"shh"}`),
			Status:    states.ObjectReady,
		}, mustProviderConfig(`provider["registry.terraform.io/hashicorp/test"]`))
	})
}

// A remote object which the provider reports as no longer existing must be
// removed from the state, even when the resource is still present in the
// configuration.
func TestContext2Refresh_removedObjectDroppedFromState(t *testing.T) {
	m := testModuleInline(t, map[string]string{
		"main.tf": `
resource "test_object" "a" {
  arg = "config"
}
`,
	})

	p := refreshStateTestProvider(t)
	p.ReadResourceFn = func(req providers.ReadResourceRequest) providers.ReadResourceResponse {
		return providers.ReadResourceResponse{
			NewState: cty.NullVal(req.PriorState.Type()),
		}
	}

	ctx := refreshStateTestContext(t, p, 0)

	s, diags := ctx.Refresh(m, refreshStateTestState(), &PlanOpts{Mode: plans.NormalMode})
	if diags.HasErrors() {
		t.Fatalf("unexpected refresh errors: %s", diags.Err())
	}
	if mod := s.RootModule(); len(mod.Resources) != 0 {
		t.Fatalf("a resource which no longer exists remotely must not be retained in state, got %d resources", len(mod.Resources))
	}
}

// In refresh-only mode a deleted remote object must be removed from the prior
// state, and the refresh-only plan must not record any resource changes.
func TestContext2Refresh_refreshOnlyRemovesDeletedObject(t *testing.T) {
	m := testModuleInline(t, map[string]string{
		"main.tf": `
resource "test_object" "a" {
  arg = "config"
}
`,
	})

	p := refreshStateTestProvider(t)
	p.ReadResourceFn = func(req providers.ReadResourceRequest) providers.ReadResourceResponse {
		return providers.ReadResourceResponse{
			NewState: cty.NullVal(req.PriorState.Type()),
		}
	}

	addr := mustResourceInstanceAddr("test_object.a")
	ctx := refreshStateTestContext(t, p, 0)

	plan, diags := ctx.Plan(m, refreshStateTestState(), &PlanOpts{Mode: plans.RefreshOnlyMode})
	if diags.HasErrors() {
		t.Fatalf("unexpected refresh-only errors: %s", diags.Err())
	}
	if len(plan.Changes.Resources) != 0 {
		t.Fatalf("refresh-only plan must not contain resource changes, got %d", len(plan.Changes.Resources))
	}
	if inst := plan.PriorState.ResourceInstance(addr); inst != nil {
		t.Fatalf("deleted object must be removed from the refresh-only prior state, got %#v", inst)
	}
}

// When the provider cannot complete a read (a timeout or transport error) and
// returns an error alongside an incomplete result, the error must fail the
// plan and the prior valid attributes must survive in the best-effort prior
// state instead of being overwritten by the incomplete object.
func TestContext2Plan_refreshReadFailurePreservesPriorState(t *testing.T) {
	m := testModuleInline(t, map[string]string{
		"main.tf": `
resource "test_object" "a" {
  arg = "config"
}
`,
	})

	p := refreshStateTestProvider(t)
	p.ReadResourceFn = func(req providers.ReadResourceRequest) providers.ReadResourceResponse {
		var diags tfdiags.Diagnostics
		diags = diags.Append(fmt.Errorf("read timed out while waiting for the remote API"))
		return providers.ReadResourceResponse{
			// An incomplete result returned together with the error.
			NewState: cty.ObjectVal(map[string]cty.Value{
				"id":     cty.NullVal(cty.String),
				"arg":    cty.StringVal("INCOMPLETE"),
				"secret": cty.NullVal(cty.String),
			}),
			Diagnostics: diags,
		}
	}

	addr := mustResourceInstanceAddr("test_object.a")
	ctx := refreshStateTestContext(t, p, 0)

	plan, diags := ctx.Plan(m, refreshStateTestState(), DefaultPlanOpts)
	if !diags.HasErrors() {
		t.Fatal("a failed refresh read must cause the plan to report an error")
	}
	if plan == nil || !plan.Errored {
		t.Fatal("the returned plan must be flagged as errored and non-applyable")
	}

	inst := plan.PriorState.ResourceInstance(addr)
	if inst == nil || inst.Current == nil {
		t.Fatal("the prior resource must be retained after a failed refresh read")
	}
	obj, err := inst.Current.Decode(providers.Schema{Body: refreshStateTestSchema()})
	if err != nil {
		t.Fatalf("failed to decode retained state: %s", err)
	}
	if got := obj.Value.GetAttr("arg"); got.AsString() != "VALID" {
		t.Fatalf("valid attributes must not be overwritten by an incomplete read result, got %#v", got)
	}
}

// When a sensitive attribute changes during refresh the refreshed value must
// keep its sensitive marking, so the changed secret is never rendered or
// persisted as a plain value.
func TestContext2Plan_refreshSensitiveChangeStillMarked(t *testing.T) {
	m := testModuleInline(t, map[string]string{
		"main.tf": `
resource "test_object" "a" {
  arg = "config"
}
`,
	})

	p := refreshStateTestProvider(t)
	p.ReadResourceFn = func(req providers.ReadResourceRequest) providers.ReadResourceResponse {
		return providers.ReadResourceResponse{
			NewState: cty.ObjectVal(map[string]cty.Value{
				"id":     cty.StringVal("existing"),
				"arg":    cty.StringVal("old"),
				"secret": cty.StringVal("ROTATED"),
			}),
		}
	}

	ctx := refreshStateTestContext(t, p, 0)

	plan, diags := ctx.Plan(m, refreshStateTestState(), DefaultPlanOpts)
	if diags.HasErrors() {
		t.Fatalf("unexpected plan errors: %s", diags.Err())
	}

	wantPath := cty.Path{cty.GetAttrStep{Name: "secret"}}
	for _, rc := range plan.Changes.Resources {
		if rc.Addr.String() != "test_object.a" {
			continue
		}
		for _, sensitivePath := range rc.AfterSensitivePaths {
			if sensitivePath.Equals(wantPath) {
				return
			}
		}
		t.Fatalf("the refreshed sensitive attribute must remain marked, got sensitive paths %#v", rc.AfterSensitivePaths)
	}
	t.Fatal("expected a change for test_object.a")
}

// Multiple instances are refreshed in parallel. Cancelling while reads are
// still in flight must halt the walk with an error, ask the provider to stop,
// and must never let the canceled (incomplete) read results replace the valid
// prior attributes.
func TestContext2Plan_parallelRefreshStop(t *testing.T) {
	const count = 4

	m := testModuleInline(t, map[string]string{
		"main.tf": `
resource "test_object" "a" {
  count = 4
  arg   = "config"
}
`,
	})

	var (
		readsStarted int32
		stopCalled   int32
	)
	firstRead := make(chan struct{})
	stopCh := make(chan struct{})

	p := refreshStateTestProvider(t)
	p.ReadResourceFn = func(req providers.ReadResourceRequest) providers.ReadResourceResponse {
		if atomic.AddInt32(&readsStarted, 1) == 1 {
			close(firstRead)
		}
		// Simulate a blocked read against a slow remote API.
		<-stopCh
		var diags tfdiags.Diagnostics
		diags = diags.Append(fmt.Errorf("request canceled"))
		return providers.ReadResourceResponse{
			Diagnostics: diags,
		}
	}
	p.StopFn = func() error {
		atomic.StoreInt32(&stopCalled, 1)
		close(stopCh)
		return nil
	}

	state := states.NewState()
	root := state.EnsureModule(addrs.RootModuleInstance)
	for i := 0; i < count; i++ {
		testSetResourceInstanceCurrent(root,
			fmt.Sprintf("test_object.a[%d]", i),
			`{"id":"existing","arg":"VALID","secret":"shh"}`,
			`provider["registry.terraform.io/hashicorp/test"]`)
	}

	ctx := refreshStateTestContext(t, p, count)

	planCh := make(chan *plans.Plan, 1)
	errCh := make(chan tfdiags.Diagnostics, 1)
	go func() {
		plan, d := ctx.Plan(m, state, DefaultPlanOpts)
		planCh <- plan
		errCh <- d
	}()

	// Make sure at least one refresh is genuinely in flight before asking to
	// stop, so that the cancellation exercises the in-read path.
	<-firstRead
	ctx.Stop()

	if diags := <-errCh; !diags.HasErrors() {
		t.Fatal("a plan interrupted while refreshing must report an error")
	}
	if atomic.LoadInt32(&stopCalled) != 1 {
		t.Fatal("the provider's Stop method must be called when refresh is cancelled")
	}

	plan := <-planCh
	if plan == nil {
		t.Fatal("a best-effort plan should still be returned for error context")
	}

	// None of the instances may have their valid prior attributes replaced by
	// the canceled reads.
	for i := 0; i < count; i++ {
		addr := mustResourceInstanceAddr(fmt.Sprintf("test_object.a[%d]", i))
		inst := plan.PriorState.ResourceInstance(addr)
		if inst == nil || inst.Current == nil {
			continue
		}
		obj, err := inst.Current.Decode(providers.Schema{Body: refreshStateTestSchema()})
		if err != nil {
			t.Fatalf("failed to decode state for %s: %s", addr, err)
		}
		arg := obj.Value.GetAttr("arg")
		if !arg.IsNull() && arg.AsString() != "VALID" {
			t.Fatalf("%s had its valid attribute overwritten by a canceled read, got %#v", addr, arg)
		}
	}
}
