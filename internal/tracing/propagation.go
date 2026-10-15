// Copyright IBM Corp. 2014, 2026
// SPDX-License-Identifier: BUSL-1.1

package tracing

import (
	"context"
	"strconv"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// Carrier is the minimal interface a transport (HTTP headers, gRPC
// metadata, a message envelope, ...) must implement for operation context
// propagation. It matches [go.opentelemetry.io/otel/propagation.TextMapCarrier].
type Carrier = propagation.TextMapCarrier

// Header names used for the Terraform-level correlation identity. These are
// independent of the W3C traceparent headers, which describe span linkage
// rather than operation ownership.
const (
	HeaderOperationID = "terraform-operation-id"
	HeaderSampled     = "terraform-operation-sampled"
)

// Inject writes the operation's correlation identity (and, when configured,
// the W3C trace context) into the carrier. Injecting is always safe: with no
// propagator installed the OpenTelemetry call is a no-op, and with no
// operation on the context only the no-op part happens.
//
// It never injects anything derived from business data, and therefore cannot
// affect business results.
func Inject(ctx context.Context, carrier Carrier) {
	if ctx == nil || carrier == nil {
		return
	}
	// This is a no-op unless the CLI has installed a propagator (i.e. when
	// OTLP export was enabled), matching the baseline behavior.
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	if st := stateFromContext(ctx); st != nil {
		carrier.Set(HeaderOperationID, string(st.id))
		carrier.Set(HeaderSampled, strconv.FormatBool(st.sampled))
	}
}

// Extract returns a context carrying both any W3C trace context and the
// operation identity found in the carrier. When the carrier carries no
// operation ID the context is returned (effectively) unchanged, so a caller
// can start a brand-new operation in that case.
func Extract(ctx context.Context, carrier Carrier) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if carrier == nil {
		return ctx
	}
	ctx = otel.GetTextMapPropagator().Extract(ctx, carrier)
	if raw := carrier.Get(HeaderOperationID); raw != "" {
		sampled := true
		if v := carrier.Get(HeaderSampled); v != "" {
			// An unparseable value conservatively means "sampled", which is
			// the safe default because it only affects captured records.
			sampled = v != "false" && v != "0"
		}
		ctx = context.WithValue(ctx, operationContextKey, &operationState{
			id:      OperationID(raw),
			sampled: sampled,
		})
	}
	return ctx
}
