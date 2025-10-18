package otel

import (
	"context"

	"github.com/pavliha/aircast-sdk/pkg/relay"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// TracingMiddleware creates middleware that extracts trace context from requests
// and creates spans for request/response handling
func TracingMiddleware(tracer trace.Tracer) relay.Middleware {
	return func(next relay.ActionHandler) relay.ActionHandler {
		return func(ctx context.Context, req *relay.Request, res relay.ResponseWriter) error {
			// Extract trace context from request
			ctx = ExtractTraceContext(ctx, req.TraceContext)

			// Start a new span for this request
			spanName := "request." + req.Action
			ctx, span := tracer.Start(ctx, spanName,
				trace.WithSpanKind(trace.SpanKindServer),
			)
			defer span.End()

			// Add request attributes to span
			span.SetAttributes(
				attribute.String("request.action", req.Action),
				attribute.String("request.id", req.RequestID),
				attribute.String("request.session_id", req.SessionID),
				attribute.String("request.source", req.Source),
			)

			// Execute the handler with the instrumented context
			err := next(ctx, req, res)

			// Record error if any
			if err != nil {
				span.RecordError(err)
			}

			return err
		}
	}
}

// EventTracingMiddleware creates middleware for event handlers with tracing
func EventTracingMiddleware(tracer trace.Tracer) func(relay.EventHandler) relay.EventHandler {
	return func(next relay.EventHandler) relay.EventHandler {
		return func(ctx context.Context, event *relay.EventRequest) error {
			// Extract trace context from event
			ctx = ExtractTraceContext(ctx, event.TraceContext)

			// Start a new span for this event
			spanName := "event." + event.Action
			ctx, span := tracer.Start(ctx, spanName,
				trace.WithSpanKind(trace.SpanKindConsumer),
			)
			defer span.End()

			// Add event attributes to span
			span.SetAttributes(
				attribute.String("event.action", event.Action),
				attribute.String("event.session_id", event.SessionID),
				attribute.String("event.source", event.Source),
			)

			// Execute the handler with the instrumented context
			err := next(ctx, event)

			// Record error if any
			if err != nil {
				span.RecordError(err)
			}

			return err
		}
	}
}

// ExtractTraceContext extracts W3C trace context from the trace context map
// and injects it into the Go context
func ExtractTraceContext(ctx context.Context, traceCtx map[string]string) context.Context {
	if len(traceCtx) == 0 {
		return ctx
	}

	// Use W3C Trace Context propagator
	propagator := otel.GetTextMapPropagator()
	carrier := propagation.MapCarrier(traceCtx)

	return propagator.Extract(ctx, carrier)
}

// InjectTraceContext extracts trace context from the Go context and
// returns it as a map suitable for message TraceContext field
func InjectTraceContext(ctx context.Context) map[string]string {
	propagator := otel.GetTextMapPropagator()
	carrier := propagation.MapCarrier{}

	propagator.Inject(ctx, carrier)

	if len(carrier) == 0 {
		return nil
	}

	return map[string]string(carrier)
}
