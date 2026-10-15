// Copyright IBM Corp. 2014, 2026
// SPDX-License-Identifier: BUSL-1.1

// Package tracing carries a correlation identity (the operation ID) through
// every execution path involved in a single Terraform operation: synchronous
// command stages, detached asynchronous tasks, graph vertices, and retry
// attempts.
//
// The operation ID travels as an ordinary context value, so it survives the
// detached-root spans that some asynchronous subsystems (such as
// internal/promising) deliberately create with trace links instead of a
// parent/child relationship.
//
// The package also provides two small aggregation surfaces that did not exist
// in the baseline:
//
//   - an in-process registry of log records keyed by operation ID, so that all
//     log records produced for one operation can be gathered together even
//     though the underlying logger is a process-wide singleton; and
//   - an in-process metrics registry, likewise keyed by operation ID.
//
// Whether those records are captured is governed by a per-operation sampling
// decision that is made exactly once, when the operation begins, and is then
// inherited by every synchronous, asynchronous, and retried descendant. The
// sampling decision only ever affects observability data; it is never read by
// business logic and therefore cannot change business results.
package tracing

import (
	"context"
	"hash/fnv"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/google/uuid"
)

// OperationIDKey is the field name under which the operation ID is attached to
// tracing spans, log records, metrics, and (when crossing a process boundary)
// propagation carriers. Keeping a single name means records gathered from any
// of those surfaces join on the same key.
const OperationIDKey = "terraform.operation_id"

// EnvSampleRate is the environment variable used to configure the sampling
// ratio for an operation. It accepts a value between 0 and 1, the strings
// "off"/"on" (or "false"/"true"), and defaults to 1 (sample everything) when
// unset or invalid.
//
// The value only changes how much observability data is captured or exported.
// It never affects the result of an operation.
const EnvSampleRate = "TF_TRACE_SAMPLE_RATE"

// OperationID identifies a single logical Terraform operation (typically one
// CLI invocation, but also any explicitly-started nested operation). All
// synchronous stages, asynchronous descendants, and retry attempts of that
// operation share the same ID.
type OperationID string

// operationState is the value stored in a context. It is carried by pointer
// so that every descendant path shares exactly one immutable decision; there
// is no per-goroutine state that could become inconsistent under
// concurrency.
type operationState struct {
	id      OperationID
	sampled bool
}

type operationContextKeyType struct{}

var operationContextKey operationContextKeyType

// sampleRate holds the configured sampling ratio as the bits of a float64,
// so it can be read and written concurrently without a lock.
var sampleRate atomic.Uint64

// telemetryEnabled reports whether an external trace exporter was configured
// (see SetTelemetryEnabled). Even when disabled the operation ID is still
// propagated and records may still be captured in-process, but log output to
// the process-wide logger stays byte-for-byte identical to the baseline.
var telemetryEnabled atomic.Bool

func init() {
	sampleRate.Store(math.Float64bits(parseSampleRate(os.Getenv(EnvSampleRate))))
}

// NewOperationID generates a fresh, globally-unique operation ID.
func NewOperationID() OperationID {
	return OperationID(uuid.NewString())
}

// WithOperationID returns a context carrying the given operation ID, making
// the sampling decision for it up-front. The decision is deterministic for a
// given ID at a given sample ratio, so every descendant path (including
// goroutines and retry attempts) observes the same one.
//
// If the context already carries an operation ID it is replaced; callers
// wanting to inherit an existing ID should use EnsureOperationID instead.
func WithOperationID(ctx context.Context, id OperationID) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if id == "" {
		id = NewOperationID()
	}
	return context.WithValue(ctx, operationContextKey, &operationState{
		id:      id,
		sampled: decideSampled(id, SampleRate()),
	})
}

// EnsureOperationID returns the context unchanged (apart from normalizing a
// nil context to context.Background) if it already carries an operation ID,
// and otherwise starts a brand new operation by generating an ID and making
// its sampling decision. The returned ID is the operation ID that the
// context now carries in either case.
func EnsureOperationID(ctx context.Context) (context.Context, OperationID) {
	if ctx == nil {
		ctx = context.Background()
	}
	if st := stateFromContext(ctx); st != nil {
		return ctx, st.id
	}
	id := NewOperationID()
	return WithOperationID(ctx, id), id
}

// OperationIDFromContext returns the operation ID carried by the context, if
// any.
func OperationIDFromContext(ctx context.Context) (OperationID, bool) {
	if st := stateFromContext(ctx); st != nil {
		return st.id, true
	}
	return "", false
}

// SampledFromContext reports whether the operation carried by the context was
// selected for sampling. Contexts without an operation are reported as not
// sampled, so observability calls become cheap no-ops.
func SampledFromContext(ctx context.Context) bool {
	if st := stateFromContext(ctx); st != nil {
		return st.sampled
	}
	return false
}

// SetSampleRate overrides the sampling ratio configured from the environment.
// Values are clamped to the range [0, 1]. The override does not change
// decisions already made for in-flight operations.
func SetSampleRate(rate float64) {
	sampleRate.Store(math.Float64bits(normalizeRate(rate)))
}

// SampleRate returns the currently configured sampling ratio.
func SampleRate() float64 {
	return math.Float64frombits(sampleRate.Load())
}

// SetTelemetryEnabled records whether an external trace exporter is active.
// This is called from the CLI entry point only; other code should treat it
// as read-only.
func SetTelemetryEnabled(enabled bool) {
	telemetryEnabled.Store(enabled)
}

// TelemetryEnabled reports whether external telemetry export was configured.
func TelemetryEnabled() bool {
	return telemetryEnabled.Load()
}

// decideSampled makes the single sampling decision for an operation. A ratio
// of at least 1 always samples, a ratio of 0 never does, and intermediate
// ratios use a deterministic hash of the operation ID so that all paths of a
// single operation reach the same decision, and flipping the switch does not
// make different attempts of one retried operation disagree with each other.
func decideSampled(id OperationID, rate float64) bool {
	rate = normalizeRate(rate)
	if rate >= 1 {
		return true
	}
	if rate <= 0 {
		return false
	}
	h := fnv.New32a()
	h.Write([]byte(string(id)))
	bucket := float64(h.Sum32()) / float64(1<<32)
	return bucket < rate
}

func normalizeRate(rate float64) float64 {
	switch {
	case math.IsNaN(rate) || rate <= 0:
		return 0
	case rate >= 1:
		return 1
	default:
		return rate
	}
}

func parseSampleRate(raw string) float64 {
	raw = strings.TrimSpace(strings.ToLower(raw))
	switch raw {
	case "":
		return 1
	case "off", "false", "no", "disabled":
		return 0
	case "on", "true", "yes", "enabled":
		return 1
	}
	rate, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		log.Printf("[WARN] invalid %s value %q: must be a number between 0 and 1; defaulting to sample everything", EnvSampleRate, raw)
		return 1
	}
	return normalizeRate(rate)
}

func stateFromContext(ctx context.Context) *operationState {
	if ctx == nil {
		return nil
	}
	st, _ := ctx.Value(operationContextKey).(*operationState)
	return st
}
