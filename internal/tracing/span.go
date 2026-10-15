// Copyright IBM Corp. 2014, 2026
// SPDX-License-Identifier: BUSL-1.1

package tracing

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/terraform/internal/logging"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// tracerName follows the repository convention of naming tracers after their
// package path.
const tracerName = "github.com/hashicorp/terraform/internal/tracing"

// tracer is the OpenTelemetry tracer used for spans in this package. With no
// exporter configured it resolves to the global no-op tracer, so span calls
// in the default configuration are effectively free.
var tracer = otel.Tracer(tracerName)

// EndFunc ends a span (and the bookkeeping around it). If a pointer to the
// caller's error is passed then a non-nil error marks the span as errored; a
// canceled or expired context is recorded as cancellation instead. EndFunc
// is safe to call more than once: only the first call has an effect.
type EndFunc func(errp *error)

func normalizeCtx(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func operationIDAttr(ctx context.Context) []attribute.KeyValue {
	if id, ok := OperationIDFromContext(ctx); ok {
		return []attribute.KeyValue{attribute.String(OperationIDKey, string(id))}
	}
	return nil
}

// StartSpan starts a span that carries the context's operation ID as an
// attribute, and pairs it with open/closed bookkeeping in the registry. It is
// the shared primitive behind [StartOperation], [Stage], [StartRetry],
// [StartAttempt], [StartVertex], and [Go].
func StartSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, EndFunc) {
	ctx = normalizeCtx(ctx)
	if attrs := operationIDAttr(ctx); len(attrs) > 0 {
		opts = append(opts, trace.WithAttributes(attrs...))
	}
	ctx, span := tracer.Start(ctx, name, opts...)

	var ended atomic.Bool
	spanBegin(ctx)

	end := func(errp *error) {
		if !ended.CompareAndSwap(false, true) {
			return
		}

		switch {
		case errp != nil && *errp != nil:
			span.RecordError(*errp)
			span.SetStatus(codes.Error, (*errp).Error())
		case ctx.Err() != nil:
			// The operation was interrupted. Record this as an event rather
			// than an error status: cancellation is an expected outcome, not
			// a fault.
			span.AddEvent("canceled", trace.WithAttributes(
				attribute.String("cancel.reason", ctx.Err().Error()),
			))
		}
		span.End()
		spanEnd(ctx)
	}

	return ctx, end
}

// StartOperation begins one logical operation. It is the synchronous root of
// the correlation tree: an ID is generated (unless the context already
// carries one), the single sampling decision for the operation is made, and a
// root span is opened.
//
// The returned context must be passed to every descendant stage, goroutine,
// and retry loop so that they all share the operation's identity. The
// returned function ends the operation and is idempotent.
func StartOperation(ctx context.Context, name string) (context.Context, OperationID, EndFunc) {
	ctx, id := EnsureOperationID(normalizeCtx(ctx))
	ctx, end := StartSpan(ctx, name)
	RecordLog(ctx, hclog.Info.String(), "operation started", StringAttr("operation", name))
	IncrCounter(ctx, "operation.start", StringAttr("operation", name))
	return ctx, id, end
}

// Stage marks one synchronous stage of an operation, such as handing the
// operation from the command layer to a backend. It opens a span, records a
// log record and a counter both at entry and at exit, and observes the
// stage's duration. The business result is untouched.
func Stage(ctx context.Context, name string, attrs ...Attr) (context.Context, EndFunc) {
	ctx = normalizeCtx(ctx)
	start := time.Now()

	// Translate our portable attrs into span attributes as well.
	spanAttrs := make([]attribute.KeyValue, 0, len(attrs))
	for _, a := range attrs {
		spanAttrs = append(spanAttrs, attribute.String(a.Key, a.Value))
	}
	ctx, spanEnd := StartSpan(ctx, "stage: "+name, trace.WithAttributes(spanAttrs...))
	RecordLog(ctx, hclog.Debug.String(), "stage started", append(attrs, StringAttr("stage", name))...)
	IncrCounter(ctx, "stage.started", append(attrs, StringAttr("stage", name))...)

	end := func(errp *error) {
		outcome := "succeeded"
		switch {
		case errp != nil && *errp != nil:
			outcome = "failed"
		case ctx.Err() != nil:
			outcome = "canceled"
		}
		ObserveDuration(ctx, "stage.duration", time.Since(start), append(attrs, StringAttr("stage", name), StringAttr("outcome", outcome))...)
		RecordLog(ctx, hclog.Debug.String(), "stage "+outcome, append(attrs, StringAttr("stage", name))...)
		spanEnd(errp)
	}
	return ctx, end
}

// Go runs fn in a new goroutine that inherits the operation identity of ctx.
//
// As with internal/promising.AsyncTask the span is deliberately detached from
// the caller's span tree (WithNewRoot) but linked back to it, because an
// asynchronous task can outlive the span that started it; unlike the trace
// relationship, the operation ID and sampling decision travel as context
// values and are never detached.
//
// If fn panics the span is still ended and the panic is re-raised, so Go does
// not change panic semantics.
func Go(ctx context.Context, name string, fn func(ctx context.Context), attrs ...Attr) {
	ctx = normalizeCtx(ctx)
	spanAttrs := make([]trace.SpanStartOption, 0, 2)
	callerSpanContext := trace.SpanFromContext(ctx).SpanContext()
	if callerSpanContext.IsValid() {
		spanAttrs = append(spanAttrs,
			trace.WithNewRoot(),
			trace.WithLinks(trace.Link{SpanContext: callerSpanContext}),
		)
	}
	if len(attrs) > 0 {
		kv := make([]attribute.KeyValue, 0, len(attrs))
		for _, a := range attrs {
			kv = append(kv, attribute.String(a.Key, a.Value))
		}
		spanAttrs = append(spanAttrs, trace.WithAttributes(kv...))
	}

	asyncCtx, end := StartSpan(ctx, "async: "+name, spanAttrs...)
	RecordLog(asyncCtx, hclog.Debug.String(), "async task started", StringAttr("task", name))
	IncrCounter(asyncCtx, "async.started", StringAttr("task", name))

	go func() {
		var err error
		defer end(&err)
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("panic in async task %q: %v", name, r)
				panic(r) // re-raise after the span is closed by the defer above
			}
		}()
		fn(asyncCtx)
	}()
}

// StartRetry opens the overall span covering a retry loop. The loop's
// individual attempts are opened with [StartAttempt] and therefore share the
// operation ID while remaining individually distinguishable.
//
// The context returned must be used inside the retry goroutine; context
// values are safe to share across goroutines.
func StartRetry(ctx context.Context, kind string) (context.Context, EndFunc) {
	ctx = normalizeCtx(ctx)
	return StartSpan(ctx, "retry", trace.WithAttributes(
		attribute.String("retry.kind", kind),
	))
}

// StartAttempt opens one attempt of a retry loop. It records an attempt
// counter at entry, observes the attempt duration at exit, and attributes
// every record to the same operation ID as the surrounding loop, so a
// retried operation never produces orphaned records.
func StartAttempt(ctx context.Context, attempt int) (context.Context, EndFunc) {
	ctx = normalizeCtx(ctx)
	start := time.Now()
	attrs := []Attr{StringAttr("retry.attempt", fmt.Sprintf("%d", attempt))}
	ctx, spanEnd := StartSpan(ctx, "retry attempt", trace.WithAttributes(
		attribute.Int("retry.attempt", attempt),
	))
	IncrCounter(ctx, "retry.attempt", attrs...)
	RecordLog(ctx, hclog.Trace.String(), "retry attempt started", attrs...)

	return ctx, func(errp *error) {
		outcome := "succeeded"
		switch {
		case errp != nil && *errp != nil:
			outcome = "failed"
		case ctx.Err() != nil:
			outcome = "canceled"
		}
		ObserveDuration(ctx, "retry.attempt.duration", time.Since(start), append(attrs, StringAttr("outcome", outcome))...)
		RecordLog(ctx, hclog.Trace.String(), "retry attempt "+outcome, attrs...)
		spanEnd(errp)
	}
}

// StartVertex opens the span for one graph vertex visit, which runs in its
// own goroutine. It attributes the visit (and its duration) to the graph
// walk's operation even though vertices execute concurrently.
func StartVertex(ctx context.Context, vertexName string) (context.Context, EndFunc) {
	ctx = normalizeCtx(ctx)
	start := time.Now()
	attrs := []Attr{StringAttr("dag.vertex", vertexName)}
	ctx, spanEnd := StartSpan(ctx, "dag vertex", trace.WithAttributes(
		attribute.String("dag.vertex", vertexName),
	))
	IncrCounter(ctx, "dag.vertex.started", attrs...)
	return ctx, func(errp *error) {
		ObserveDuration(ctx, "dag.vertex.duration", time.Since(start), attrs...)
		spanEnd(errp)
	}
}

// Logger returns a logger that automatically stamps every line with the
// context's operation ID. When no operation is active, or external telemetry
// export is not enabled, the unmodified process-wide logger is returned so
// that log output stays exactly as it was before tracing was introduced.
func Logger(ctx context.Context) hclog.Logger {
	l := logging.HCLogger()
	if !TelemetryEnabled() {
		return l
	}
	if id, ok := OperationIDFromContext(ctx); ok {
		return l.With(OperationIDKey, string(id))
	}
	return l
}
