# OpenTelemetry Integration

This package provides OpenTelemetry middleware for the Aircast SDK, enabling distributed tracing across your message-based system.

## Features

- **Automatic Span Creation**: Creates spans for every request and event
- **W3C Trace Context Propagation**: Extracts and injects W3C trace context from/to messages
- **Error Recording**: Automatically records errors in spans
- **Middleware Pattern**: Easy integration with the SDK's router

## Installation

The OpenTelemetry integration is included in the SDK. Simply import it:

```go
import "github.com/pavliha/aircast-sdk/pkg/otel"
```

## Quick Start

### 1. Setup OpenTelemetry

First, initialize OpenTelemetry with your preferred exporter (Jaeger, Zipkin, OTLP, etc.):

```go
package main

import (
	"context"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func initTracer() (*sdktrace.TracerProvider, error) {
	// Create stdout exporter (for demo - use Jaeger/OTLP in production)
	exporter, err := stdouttrace.New(
		stdouttrace.WithPrettyPrint(),
	)
	if err != nil {
		return nil, err
	}

	// Create tracer provider
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
	)

	// Set global tracer provider
	otel.SetTracerProvider(tp)

	// Set W3C Trace Context propagator
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return tp, nil
}
```

### 2. Add Middleware to Router

Apply the tracing middleware to your relay router:

```go
package main

import (
	"context"
	"github.com/pavliha/aircast-sdk/pkg/otel"
	"github.com/pavliha/aircast-sdk/pkg/relay"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
)

func main() {
	// Initialize tracer
	tp, err := initTracer()
	if err != nil {
		panic(err)
	}
	defer tp.Shutdown(context.Background())

	// Get tracer
	tracer := otel.Tracer("aircast-service")

	// Create router
	router := relay.NewRouter(
		logrus.NewEntry(logrus.New()),
		yourResponseFactory,
		yourErrorSender,
	)

	// Add tracing middleware globally
	router.Use(otel.TracingMiddleware(tracer))

	// Register handlers as usual
	router.HandleRequest("device.status", handleDeviceStatus)
	router.HandleEvent("device.connected", handleDeviceConnected)
}
```

### 3. Propagate Trace Context in Messages

When sending messages, inject the trace context from your current span:

```go
func handleDeviceStatus(ctx context.Context, req *relay.Request, res relay.ResponseWriter) error {
	// Your business logic here...

	// When sending an event, inject trace context
	event := message.NewEvent().
		WithAction("device.status.updated").
		WithSource(message.SystemDevice).
		WithDestination(message.DestinationWeb).
		WithPayload(map[string]any{"status": "online"}).
		WithTraceContext(otel.InjectTraceContext(ctx)).  // Inject current span context
		Build()

	return client.Send(event)
}
```

## API Reference

### `TracingMiddleware(tracer trace.Tracer) relay.Middleware`

Creates middleware for action handlers that:
- Extracts W3C trace context from incoming requests
- Creates a server span for the request
- Adds request metadata as span attributes
- Records errors if the handler fails

**Usage:**
```go
router.Use(otel.TracingMiddleware(tracer))
```

### `EventTracingMiddleware(tracer trace.Tracer) func(relay.EventHandler) relay.EventHandler`

Creates middleware for event handlers with similar tracing capabilities.

**Usage:**
```go
// Wrap individual event handlers
handler := otel.EventTracingMiddleware(tracer)(yourEventHandler)
router.HandleEvent("event.name", handler)
```

### `ExtractTraceContext(ctx context.Context, traceCtx map[string]string) context.Context`

Extracts W3C trace context from a trace context map and injects it into the Go context.

**Usage:**
```go
ctx = otel.ExtractTraceContext(ctx, request.TraceContext)
```

### `InjectTraceContext(ctx context.Context) map[string]string`

Extracts trace context from the current Go context and returns it as a map suitable for message `TraceContext` fields.

**Usage:**
```go
traceCtx := otel.InjectTraceContext(ctx)
message.WithTraceContext(traceCtx)
```

## Example: Full Integration

```go
package main

import (
	"context"
	"github.com/pavliha/aircast-sdk/pkg/message"
	"github.com/pavliha/aircast-sdk/pkg/otel"
	"github.com/pavliha/aircast-sdk/pkg/relay"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func initTracer() (*sdktrace.TracerProvider, error) {
	exporter, err := otlptracegrpc.New(context.Background())
	if err != nil {
		return nil, err
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return tp, nil
}

func main() {
	// Initialize OpenTelemetry
	tp, err := initTracer()
	if err != nil {
		log.Fatal(err)
	}
	defer tp.Shutdown(context.Background())

	// Create tracer
	tracer := otel.Tracer("my-service")

	// Create router with tracing middleware
	router := relay.NewRouter(
		log.NewEntry(log.New()),
		createResponseWriter,
		sendError,
	)

	// Add global tracing middleware
	router.Use(otel.TracingMiddleware(tracer))

	// Register handlers
	router.HandleRequest("device.get_status", func(ctx context.Context, req *relay.Request, res relay.ResponseWriter) error {
		// This request is already traced!
		// The span contains request.action, request.id, request.room_id, etc.

		status := getDeviceStatus() // Your logic

		return res.SendSuccess(status)
	})

	// Start processing messages
	// router.ProcessRequest(ctx, requestMessage)
}
```

## Span Attributes

The middleware automatically adds the following attributes to spans:

### Request Spans
- `request.action`: The action name
- `request.id`: The request ID
- `request.room_id`: The session/channel ID
- `request.source`: The source of the request (web, device, api)

### Event Spans
- `event.action`: The event action name
- `event.room_id`: The session/channel ID
- `event.source`: The source of the event

## Best Practices

1. **Always set W3C propagator**: Use `otel.SetTextMapPropagator(propagation.TraceContext{})` to ensure proper trace context propagation

2. **Inject context when sending messages**: Always use `otel.InjectTraceContext(ctx)` when creating messages to maintain trace continuity

3. **Use structured logging**: Combine OpenTelemetry with structured logging for better observability

4. **Configure sampling**: In high-traffic scenarios, configure sampling in your tracer provider

5. **Use appropriate exporters**:
   - Development: stdout exporter
   - Production: OTLP, Jaeger, or Zipkin

## Compatibility

This package is compatible with:
- OpenTelemetry Go v1.38.0+
- W3C Trace Context standard
- All major OpenTelemetry backends (Jaeger, Zipkin, Honeycomb, DataDog, etc.)
