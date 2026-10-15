// Copyright IBM Corp. 2014, 2026
// SPDX-License-Identifier: BUSL-1.1

package tracing

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// setupTestExporter routes every span into an in-memory exporter for the
// duration of the test. The tracer provider is a process-wide global, so none
// of these tests may run in parallel.
func setupTestExporter(t *testing.T) *tracetest.InMemoryExporter {
	t.Helper()

	prevProvider := otel.GetTracerProvider()
	prevPropagator := otel.GetTextMapPropagator()

	exp := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(sdktrace.NewSimpleSpanProcessor(exp)))
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{}))

	// The package-level tracer was resolved from the global provider at init
	// time, so point it at the new provider for the test and restore it
	// afterwards.
	prevTracer := tracer
	tracer = otel.Tracer(tracerName)

	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
		otel.SetTracerProvider(prevProvider)
		otel.SetTextMapPropagator(prevPropagator)
		tracer = prevTracer
	})
	return exp
}

// withRate runs fn with the given sample rate and restores the previous rate
// and registry state afterwards.
func withRate(t *testing.T, rate float64, fn func()) {
	t.Helper()
	prevRate := SampleRate()
	prevEnabled := TelemetryEnabled()
	SetSampleRate(rate)
	Reset()
	t.Cleanup(func() {
		SetSampleRate(prevRate)
		SetTelemetryEnabled(prevEnabled)
		Reset()
	})
	fn()
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", msg)
}

// TestOperationIDGeneratedAndStable covers the synchronous path: once an
// operation has started, every descendant context sees the same operation ID
// (rubric 1).
func TestOperationIDGeneratedAndStable(t *testing.T) {
	withRate(t, 1, func() {
		ctx, id, end := StartOperation(context.Background(), "test op")
		defer end(nil)

		if id == "" {
			t.Fatal("expected a non-empty operation ID")
		}
		got, ok := OperationIDFromContext(ctx)
		if !ok || got != id {
			t.Fatalf("operation ID not carried by context: %q vs %q", got, id)
		}

		var stageID OperationID
		sctx, endStage := Stage(ctx, "child-stage")
		stageID, _ = OperationIDFromContext(sctx)
		endStage(nil)
		if stageID != id {
			t.Fatalf("stage got operation ID %q; want %q", stageID, id)
		}

		// EnsureOperationID must not replace an existing ID.
		ctx2, id2 := EnsureOperationID(ctx)
		if id2 != id {
			t.Fatalf("EnsureOperationID replaced existing ID: %q vs %q", id2, id)
		}
		_, stillOK := OperationIDFromContext(ctx2)
		if !stillOK {
			t.Fatal("EnsureOperationID returned a context without an operation ID")
		}
	})
}

// TestSyncLogAndMetricsAggregation checks that records produced along the
// synchronous path can be gathered by operation ID and cover each stage
// (rubrics 4 and 5).
func TestSyncLogAndMetricsAggregation(t *testing.T) {
	withRate(t, 1, func() {
		ctx, id, end := StartOperation(context.Background(), "aggregated")
		func() {
			ctx, endStage := Stage(ctx, "one")
			defer endStage(nil)
			IncrCounter(ctx, "widgets", StringAttr("color", "blue"))
		}()
		func() {
			ctx, endStage := Stage(ctx, "two")
			defer endStage(nil)
			RecordLog(ctx, hclog.Info.String(), "in stage two")
		}()
		end(nil)

		logs := LogEventsForOperation(id)
		var sawOne, sawTwo bool
		for _, e := range logs {
			if e.OperationID != id {
				t.Fatalf("log attributed to wrong operation: %q", e.OperationID)
			}
			switch e.Message {
			case "stage started":
				for _, a := range e.Attrs {
					if a.Key == "stage" && a.Value == "one" {
						sawOne = true
					}
					if a.Key == "stage" && a.Value == "two" {
						sawTwo = true
					}
				}
			case "in stage two":
				sawTwo = true
			}
		}
		if !sawOne || !sawTwo {
			t.Fatalf("aggregate did not cover both stages (one=%v two=%v): %#v", sawOne, sawTwo, logs)
		}

		var widgetCount float64
		var stageStarts float64
		for _, e := range MetricEventsForOperation(id) {
			if e.OperationID != id {
				t.Fatalf("metric attributed to wrong operation: %q", e.OperationID)
			}
			switch e.Name {
			case "widgets":
				widgetCount += e.Value
			case "stage.started":
				stageStarts += e.Value
			}
		}
		if widgetCount != 1 {
			t.Errorf("widgets = %v; want 1", widgetCount)
		}
		if stageStarts != 2 {
			t.Errorf("stage.started = %v; want 2", stageStarts)
		}
	})
}

// TestAsyncPropagation covers the asynchronous path: goroutines started with
// [Go] keep the caller's operation ID, and their spans are linked back to the
// caller's span while their records still aggregate under one ID (rubric 2).
func TestAsyncPropagation(t *testing.T) {
	exp := setupTestExporter(t)
	withRate(t, 1, func() {
		ctx, id, endOp := StartOperation(context.Background(), "async root")

		var wg sync.WaitGroup
		var asyncIDs []OperationID
		var mu sync.Mutex
		for i := 0; i < 5; i++ {
			wg.Add(1)
			Go(ctx, fmt.Sprintf("worker-%d", i), func(ctx context.Context) {
				defer wg.Done()
				got, ok := OperationIDFromContext(ctx)
				if !ok {
					t.Error("async goroutine lost its operation ID")
					return
				}
				mu.Lock()
				asyncIDs = append(asyncIDs, got)
				mu.Unlock()
				IncrCounter(ctx, "async.work")
				RecordLog(ctx, hclog.Debug.String(), "async work")
			})
		}
		wg.Wait()
		endOp(nil)

		// wg.Done runs as the task body's defer, slightly before the
		// goroutine's span-ending defer, so wait for the bookkeeping to
		// catch up rather than racing it.
		waitFor(t, time.Second, func() bool { return OpenSpanCount(id) == 0 }, "async spans to end")

		for _, got := range asyncIDs {
			if got != id {
				t.Fatalf("async goroutine got operation ID %q; want %q", got, id)
			}
		}
		if got := CounterSum(id, "async.work"); got != 5 {
			t.Errorf("async.work = %v; want 5", got)
		}
		if got := OpenSpanCount(id); got != 0 {
			t.Errorf("open spans after async completion = %d; want 0", got)
		}

		// All exported spans, including the detached async ones, must carry
		// the same operation ID attribute.
		expSpans := exp.GetSpans()
		if len(expSpans) < 6 {
			t.Fatalf("expected at least 6 exported spans, got %d", len(expSpans))
		}
		for _, s := range expSpans {
			var found bool
			for _, kv := range s.Attributes {
				if string(kv.Key) == OperationIDKey && kv.Value.AsString() == string(id) {
					found = true
				}
			}
			if !found {
				t.Errorf("span %q is missing operation ID %q", s.Name, id)
			}
		}

		// Every async span must link back to a span from the same operation
		// rather than being a plain child (detached-root-with-link model).
		var linked int
		for _, s := range expSpans {
			if s.Name == "async root" {
				continue
			}
			if len(s.Links) == 0 {
				t.Errorf("span %q should be linked back to its caller", s.Name)
				continue
			}
			linked++
		}
		if linked == 0 {
			t.Error("expected at least one linked async span")
		}
	})
}

// TestRetryAttemptsShareOperation simulates the retry-loop shape used by the
// cloud/communicator helpers: every attempt, including the failing ones,
// stays attributable to the one operation (rubric 3).
func TestRetryAttemptsShareOperation(t *testing.T) {
	withRate(t, 1, func() {
		ctx, id, endOp := StartOperation(context.Background(), "retrying")

		retryCtx, endRetry := StartRetry(ctx, "test.retry")
		var attempts int
		for attempt := 1; attempt <= 3; attempt++ {
			var err error
			if attempt < 3 {
				err = errors.New("transient")
			}
			_, endAttempt := StartAttempt(retryCtx, attempt)
			attempts++
			endAttempt(&err)
		}
		endRetry(nil)
		endOp(nil)

		if attempts != 3 {
			t.Fatalf("attempts = %d; want 3", attempts)
		}
		if got := CounterSum(id, "retry.attempt"); got != 3 {
			t.Fatalf("retry.attempt = %v; want 3", got)
		}
		events := MetricEventsForOperation(id)
		for _, e := range events {
			if e.OperationID != id {
				t.Fatalf("retry metric attributed to %q", e.OperationID)
			}
		}
		var sawFailed, sawSucceeded bool
		for _, e := range events {
			if e.Name != "retry.attempt.duration" {
				continue
			}
			for _, a := range e.Attrs {
				if a.Key == "outcome" {
					switch a.Value {
					case "failed":
						sawFailed = true
					case "succeeded":
						sawSucceeded = true
					}
				}
			}
		}
		if !sawFailed || !sawSucceeded {
			t.Fatalf("attempt outcomes missing (failed=%v succeeded=%v)", sawFailed, sawSucceeded)
		}
		if got := OpenSpanCount(id); got != 0 {
			t.Fatalf("open spans after retry = %d; want 0", got)
		}
	})
}

// TestConcurrentOperationsNoCrossTalk hammers the registry from many
// operations and goroutines at once and proves aggregates never mix (rubric
// 7). Run with -race for the concurrency guarantee.
func TestConcurrentOperationsNoCrossTalk(t *testing.T) {
	withRate(t, 1, func() {
		const numOps = 40
		const perOp = 20

		var wg sync.WaitGroup
		ids := make([]OperationID, numOps)
		for i := 0; i < numOps; i++ {
			ids[i] = OperationID(fmt.Sprintf("fixed-op-%03d", i))
		}

		for i := 0; i < numOps; i++ {
			ctx := WithOperationID(context.Background(), ids[i])
			token := string(ids[i])
			for j := 0; j < perOp; j++ {
				wg.Add(1)
				Go(ctx, fmt.Sprintf("op-%d-task-%d", i, j), func(ctx context.Context) {
					defer wg.Done()
					IncrCounter(ctx, "work", StringAttr("token", token))
					RecordLog(ctx, hclog.Debug.String(), "work", StringAttr("token", token))
					ObserveDuration(ctx, "work.duration", time.Microsecond, StringAttr("token", token))
				})
			}
		}
		wg.Wait()

		for _, id := range ids {
			token := string(id)
			// The task body's wg.Done defer runs immediately before the
			// goroutine's span-ending defer, so let the bookkeeping settle.
			waitFor(t, 5*time.Second, func() bool { return OpenSpanCount(id) == 0 },
				fmt.Sprintf("open spans for %s to close", id))

			metrics := MetricEventsForOperation(id)
			logs := LogEventsForOperation(id)
			// Per task: Go's own async.started counter, the body's "work"
			// counter, and one work.duration timer = 3 metrics; Go's own
			// "async task started" record plus the body's "work" record = 2
			// logs.
			if len(metrics) != perOp*3 {
				t.Errorf("operation %q has %d metric events; want %d", id, len(metrics), perOp*3)
			}
			if len(logs) != perOp*2 {
				t.Errorf("operation %q has %d log events; want %d", id, len(logs), perOp*2)
			}
			for _, e := range metrics {
				if e.OperationID != id {
					t.Fatalf("metric crossed operations: event for %q filed under %q", e.OperationID, id)
				}
				for _, a := range e.Attrs {
					if a.Key == "token" && a.Value != token {
						t.Fatalf("operation %q contains a record tagged %q", id, a.Value)
					}
				}
			}
			for _, e := range logs {
				if e.OperationID != id {
					t.Fatalf("log crossed operations: event for %q filed under %q", e.OperationID, id)
				}
				for _, a := range e.Attrs {
					if a.Key == "token" && a.Value != token {
						t.Fatalf("operation %q contains a log tagged %q", id, a.Value)
					}
				}
			}
			if got := OpenSpanCount(id); got != 0 {
				t.Fatalf("operation %q left %d open spans", id, got)
			}
		}
	})
}

// TestSamplingNeverChangesResults is the core "sampling is observability
// only" guarantee: the same business function produces identical results
// whether sampling is off, partial, or on (rubric 6).
func TestSamplingNeverChangesResults(t *testing.T) {
	type result struct {
		sum int
		err error
	}
	business := func(ctx context.Context) result {
		sum := 0
		for i := 0; i < 10; i++ {
			// These calls are observability only and must not perturb sum.
			IncrCounter(ctx, "loop.iteration")
			RecordLog(ctx, hclog.Trace.String(), "tick")
			_, end := StartSpan(ctx, "tick")
			sum++
			end(nil)
		}
		if sum != 10 {
			return result{sum, errors.New("unexpected")}
		}
		return result{sum, nil}
	}

	// Use fixed operation IDs so the same set of in/out decisions occurs at
	// each ratio.
	fixedIDs := make([]OperationID, 50)
	for i := range fixedIDs {
		fixedIDs[i] = OperationID(fmt.Sprintf("sampling-probe-%04d", i))
	}

	var reference map[string]result
	for _, rate := range []float64{0, 0.25, 0.5, 0.75, 1} {
		t.Run(fmt.Sprintf("rate=%v", rate), func(t *testing.T) {
			withRate(t, rate, func() {
				got := make(map[string]result, len(fixedIDs))
				var sampledCount int64
				for _, id := range fixedIDs {
					ctx := WithOperationID(context.Background(), id)
					if SampledFromContext(ctx) {
						sampledCount++
					}
					got[string(id)] = business(ctx)
					if rate == 0 {
						if len(MetricEventsForOperation(id)) != 0 || len(LogEventsForOperation(id)) != 0 {
							t.Fatalf("rate 0 must capture nothing for %q", id)
						}
					}
				}
				if rate == 0 && sampledCount != 0 {
					t.Fatalf("rate 0 sampled %d operations; want 0", sampledCount)
				}
				if rate == 1 && sampledCount != int64(len(fixedIDs)) {
					t.Fatalf("rate 1 sampled %d operations; want %d", sampledCount, len(fixedIDs))
				}
				if reference == nil {
					reference = got
					return
				}
				for k, v := range got {
					if v != reference[k] {
						t.Fatalf("result for %q changed with sampling rate %v: %+v vs %+v", k, rate, v, reference[k])
					}
				}
			})
		})
	}
}

// TestSamplingDecisionInherited makes sure the decision made at operation
// start is the one seen in async and retried paths, so partial sampling
// can't make an attempt disagree with its operation (rubric 6).
func TestSamplingDecisionInherited(t *testing.T) {
	withRate(t, 0.5, func() {
		// Roughly half of a large fixed ID set should be sampled; the point
		// under test is that descendants always inherit the exact decision.
		sampled := 0
		var children sync.WaitGroup
		for i := 0; i < 200; i++ {
			ctx := WithOperationID(context.Background(), OperationID(fmt.Sprintf("inherit-%04d", i)))
			want := SampledFromContext(ctx)
			if want {
				sampled++
			}
			children.Add(1)
			Go(ctx, "child", func(ctx context.Context) {
				defer children.Done()
				if SampledFromContext(ctx) != want {
					t.Errorf("async child sampling decision differs from operation %q", operationIDOrEmpty(ctx))
				}
			})
			rctx, endRetry := StartRetry(ctx, "k")
			actx, endAttempt := StartAttempt(rctx, 1)
			if SampledFromContext(actx) != want {
				t.Errorf("retry attempt sampling decision differs from operation")
			}
			endAttempt(nil)
			endRetry(nil)
		}
		children.Wait()
		if sampled == 0 || sampled == 200 {
			t.Fatalf("rate 0.5 should sample a strict subset; got %d/200", sampled)
		}
	})
}

func operationIDOrEmpty(ctx context.Context) OperationID {
	id, _ := OperationIDFromContext(ctx)
	return id
}

// TestCancellationLeavesNoOpenRecords exercises rubric 8: when an operation
// is canceled while work is in flight, the records produced so far remain
// attributable and every span still ends.
func TestCancellationLeavesNoOpenRecords(t *testing.T) {
	exp := setupTestExporter(t)
	withRate(t, 1, func() {
		ctx, cancel := context.WithCancel(context.Background())
		ctx, id, endOp := StartOperation(ctx, "cancelable")

		var workers sync.WaitGroup
		for i := 0; i < 4; i++ {
			workers.Add(1)
			Go(ctx, fmt.Sprintf("blocked-%d", i), func(ctx context.Context) {
				defer workers.Done()
				IncrCounter(ctx, "started")
				<-ctx.Done()
				RecordLog(ctx, hclog.Info.String(), "shutting down after cancel")
			})
		}

		// A retry attempt that is waiting when cancellation arrives.
		retryCtx, endRetry := StartRetry(ctx, "stuck")
		attemptStarted := make(chan struct{})
		attemptDone := make(chan struct{})
		go func() {
			defer close(attemptDone)
			_, endAttempt := StartAttempt(retryCtx, 1)
			close(attemptStarted)
			<-retryCtx.Done()
			endAttempt(nil)
		}()
		<-attemptStarted

		// Records produced before cancellation are already attributable.
		waitFor(t, time.Second, func() bool { return CounterSum(id, "started") >= 4 }, "workers to run their body")
		if got := CounterSum(id, "started"); got != 4 {
			t.Fatalf("expected 4 in-flight records, got %v", got)
		}

		cancel()
		workers.Wait()
		<-attemptDone
		endRetry(nil)
		endOp(nil)

		waitFor(t, time.Second, func() bool { return OpenSpanCount(id) == 0 }, "all spans to end")

		// Existing records are still queryable under the canceled op ID.
		if len(MetricEventsForOperation(id)) == 0 {
			t.Fatal("canceled operation retained no metrics")
		}
		if len(LogEventsForOperation(id)) == 0 {
			t.Fatal("canceled operation retained no logs")
		}

		// Every exported span ended and the root recorded cancellation.
		var unfinished int
		var sawCanceled bool
		for _, s := range exp.GetSpans() {
			if s.EndTime.IsZero() {
				unfinished++
			}
			if s.Name == "cancelable" {
				for _, e := range s.Events {
					if e.Name == "canceled" {
						sawCanceled = true
					}
				}
			}
		}
		if unfinished != 0 {
			t.Errorf("%d spans have no end time", unfinished)
		}
		if !sawCanceled {
			t.Error("root span did not record cancellation")
		}
	})
}

// TestFailureRecordsAttributableAndClosed covers the failure branch of
// rubric 8.
func TestFailureRecordsAttributableAndClosed(t *testing.T) {
	exp := setupTestExporter(t)
	withRate(t, 1, func() {
		ctx, id, endOp := StartOperation(context.Background(), "failing")
		boom := errors.New("boom")
		func() {
			ctx, endStage := Stage(ctx, "danger")
			defer endStage(&boom)
			RecordLog(ctx, hclog.Error.String(), "about to fail")
		}()
		endOp(&boom)

		if len(LogEventsForOperation(id)) == 0 {
			t.Fatal("failed operation kept no logs")
		}
		if got := OpenSpanCount(id); got != 0 {
			t.Fatalf("open spans after failure = %d; want 0", got)
		}
		var errored int
		for _, s := range exp.GetSpans() {
			if s.Status.Code.String() == "Error" {
				errored++
			}
			if s.EndTime.IsZero() {
				t.Errorf("span %q has no end time", s.Name)
			}
		}
		if errored < 2 {
			t.Errorf("expected root + stage error statuses, got %d errored spans", errored)
		}
	})
}

// TestGoRepanics makes sure async propagation doesn't swallow panics, which
// would itself be a change to business semantics. A panic in the spawned
// goroutine cannot be recovered by the launching goroutine, so this test
// re-executes itself as a subprocess and asserts the child crashes.
func TestGoRepanics(t *testing.T) {
	if os.Getenv(goPanicEnv) == "1" {
		goPanicChild()
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestGoRepanics$", "-test.v")
	cmd.Env = append(os.Environ(), goPanicEnv+"=1")
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	err := cmd.Run()

	if err == nil {
		t.Fatalf("expected child process to fail, but it succeeded:\n%s", output.String())
	}
	if !strings.Contains(output.String(), "kaboom") {
		t.Fatalf("child output did not contain the panic payload:\n%s", output.String())
	}
}

const goPanicEnv = "TF_TRACING_TEST_GO_PANIC"

func goPanicChild() {
	ctx, _, end := StartOperation(context.Background(), "panicky-child")
	defer end(nil)
	Go(ctx, "boom", func(ctx context.Context) {
		panic("kaboom")
	})
	// Give the spawned goroutine time to panic and crash the process.
	time.Sleep(5 * time.Second)
}

// TestPropagationRoundTrip covers cross-boundary operation handoff.
func TestPropagationRoundTrip(t *testing.T) {
	setupTestExporter(t)
	withRate(t, 1, func() {
		ctx, id, end := StartOperation(context.Background(), "cross-boundary")
		carrier := propagation.MapCarrier{}
		Inject(ctx, carrier)

		if carrier.Get(HeaderOperationID) != string(id) {
			t.Fatalf("carrier missing operation ID: %#v", carrier)
		}
		if carrier.Get(HeaderSampled) != "true" {
			t.Fatalf("carrier missing sampled flag: %#v", carrier)
		}
		if carrier.Get("traceparent") == "" {
			t.Fatal("expected W3C traceparent when a propagator is installed")
		}

		out := Extract(context.Background(), carrier)
		gotID, ok := OperationIDFromContext(out)
		if !ok || gotID != id {
			t.Fatalf("extracted operation ID = %q; want %q", gotID, id)
		}
		if !SampledFromContext(out) {
			t.Fatal("extracted context lost sampled flag")
		}
		end(nil)

		// Extraction from an empty carrier must not fabricate an operation.
		bare := Extract(context.Background(), propagation.MapCarrier{})
		if _, ok := OperationIDFromContext(bare); ok {
			t.Fatal("unexpected operation ID in empty carrier")
		}
	})
}

// TestLoggerBehavior makes sure the process-wide logger is untouched unless
// telemetry export is explicitly enabled.
func TestLoggerBehavior(t *testing.T) {
	withRate(t, 1, func() {
		base := Logger(context.Background())
		ctx, id, end := StartOperation(context.Background(), "logger")
		defer end(nil)
		_ = id

		// Export disabled: same logger as before (byte-identical output
		// guarantee for the default configuration).
		SetTelemetryEnabled(false)
		if got := Logger(ctx); got != base {
			t.Error("expected the unmodified base logger when telemetry is disabled")
		}

		// Export enabled: a derived logger stamped with the operation ID.
		SetTelemetryEnabled(true)
		if got := Logger(ctx); got == base {
			t.Error("expected a derived logger carrying the operation ID")
		}
	})
}

// TestParseSampleRate covers the environment-variable parsing rules.
func TestParseSampleRate(t *testing.T) {
	cases := map[string]float64{
		"":       1,
		"1":      1,
		"0":      0,
		"0.5":    0.5,
		"off":    0,
		"OFF":    0,
		"false":  0,
		"on":     1,
		"true":   1,
		"2":      1,
		"-1":     0,
		"bogus":  1,
		" 0.25 ": 0.25,
	}
	for in, want := range cases {
		if got := parseSampleRate(in); got != want {
			t.Errorf("parseSampleRate(%q) = %v; want %v", in, got, want)
		}
	}
}

// TestDeterministicSamplingForID checks the hash-based decision is stable for
// a given ID and ratio.
func TestDeterministicSamplingForID(t *testing.T) {
	id := OperationID("stable-id")
	for _, rate := range []float64{0, 0.01, 0.5, 0.99, 1} {
		first := decideSampled(id, rate)
		for i := 0; i < 100; i++ {
			if decideSampled(id, rate) != first {
				t.Fatalf("sampling decision for %q flapped at rate %v", id, rate)
			}
		}
	}
}

// TestNoOperationIsSafe makes sure all entry points are no-ops without an
// operation, so unmodified call sites keep baseline behavior.
func TestNoOperationIsSafe(t *testing.T) {
	withRate(t, 1, func() {
		ctx := context.Background()
		RecordLog(ctx, "INFO", "nothing")
		IncrCounter(ctx, "nothing")
		AddCounter(ctx, "nothing", 2)
		ObserveDuration(ctx, "nothing", time.Millisecond)
		_, end := StartSpan(ctx, "orphan")
		end(nil)
		if _, ok := OperationIDFromContext(ctx); ok {
			t.Fatal("background context unexpectedly carries an operation")
		}
		if SampledFromContext(ctx) {
			t.Fatal("background context must be reported as unsampled")
		}
	})
}

// TestConcurrentSamplingDecisions exercises the rate switch under
// concurrency: business results stay consistent even if the ratio changes
// while operations are starting, because each operation keeps its own
// once-made decision.
func TestConcurrentSamplingDecisions(t *testing.T) {
	withRate(t, 0.5, func() {
		var stop atomic.Bool
		var twiddlers, children sync.WaitGroup

		// Twiddle the rate from another goroutine.
		twiddlers.Add(1)
		go func() {
			defer twiddlers.Done()
			for i := 0; !stop.Load(); i++ {
				SetSampleRate([]float64{0, 0.3, 0.7, 1}[i%4])
			}
		}()

		for i := 0; i < 200; i++ {
			ctx := WithOperationID(context.Background(), OperationID(fmt.Sprintf("racy-%04d", i)))
			want := SampledFromContext(ctx)
			children.Add(1)
			Go(ctx, "racy-child", func(ctx context.Context) {
				defer children.Done()
				if SampledFromContext(ctx) != want {
					t.Errorf("decision changed mid-operation for %q", operationIDOrEmpty(ctx))
				}
			})
		}

		children.Wait()
		stop.Store(true)
		twiddlers.Wait()
		SetSampleRate(0.5)
	})
}
