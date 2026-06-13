package otel

import (
	"context"
	"errors"
	"testing"

	"github.com/pavliha/aircast-sdk/pkg/relay"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	sdktrace "go.opentelemetry.io/otel/trace"
)

func TestTracingMiddleware(t *testing.T) {
	// Setup test tracer
	spanRecorder := tracetest.NewSpanRecorder()
	tracerProvider := trace.NewTracerProvider(
		trace.WithSpanProcessor(spanRecorder),
	)
	tracer := tracerProvider.Tracer("test-tracer")

	t.Run("creates span for request", func(t *testing.T) {
		// Create middleware
		middleware := TracingMiddleware(tracer)

		// Create a test handler
		handlerCalled := false
		handler := func(ctx context.Context, req *relay.Request) (any, error) {
			handlerCalled = true
			return nil, nil
		}

		// Wrap handler with middleware
		wrappedHandler := middleware(handler)

		// Create test request
		req := &relay.Request{
			Action:    "test.action",
			RequestID: "req-123",
			RoomID:    "session-456",
			Source:    "web",
		}

		// Execute
		_, err := wrappedHandler(context.Background(), req)

		// Assert
		assert.NoError(t, err)
		assert.True(t, handlerCalled)

		// Check that span was created
		spans := spanRecorder.Ended()
		require.Len(t, spans, 1)
		assert.Equal(t, "request.test.action", spans[0].Name())
	})

	t.Run("extracts trace context from request", func(t *testing.T) {
		spanRecorder.Reset()

		// Setup W3C Trace Context propagator
		otel.SetTextMapPropagator(propagation.TraceContext{})

		// Create middleware
		middleware := TracingMiddleware(tracer)

		// Create handler that checks context
		var extractedCtx context.Context
		handler := func(ctx context.Context, req *relay.Request) (any, error) {
			extractedCtx = ctx
			return nil, nil
		}

		// Wrap handler
		wrappedHandler := middleware(handler)

		// Create request with trace context
		traceContext := map[string]string{
			"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		}
		req := &relay.Request{
			Action:       "test.action",
			RequestID:    "req-123",
			TraceContext: traceContext,
		}

		// Execute
		_, err := wrappedHandler(context.Background(), req)

		// Assert
		assert.NoError(t, err)
		assert.NotNil(t, extractedCtx)

		// Verify that trace context was extracted
		spanCtx := sdktrace.SpanFromContext(extractedCtx).SpanContext()
		assert.True(t, spanCtx.IsValid())
	})

	t.Run("records error in span", func(t *testing.T) {
		spanRecorder.Reset()

		// Create middleware
		middleware := TracingMiddleware(tracer)

		// Create handler that returns error
		expectedErr := errors.New("test error")
		handler := func(ctx context.Context, req *relay.Request) (any, error) {
			return nil, expectedErr
		}

		// Wrap handler
		wrappedHandler := middleware(handler)

		// Create test request
		req := &relay.Request{
			Action:    "test.action",
			RequestID: "req-123",
		}

		// Execute
		_, err := wrappedHandler(context.Background(), req)

		// Assert
		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)

		// Check that error was recorded in span
		spans := spanRecorder.Ended()
		require.Len(t, spans, 1)
		assert.NotEmpty(t, spans[0].Events())
	})
}

func TestEventTracingMiddleware(t *testing.T) {
	// Setup test tracer
	spanRecorder := tracetest.NewSpanRecorder()
	tracerProvider := trace.NewTracerProvider(
		trace.WithSpanProcessor(spanRecorder),
	)
	tracer := tracerProvider.Tracer("test-tracer")

	t.Run("creates span for event", func(t *testing.T) {
		// Create middleware
		middleware := EventTracingMiddleware(tracer)

		// Create handler
		handlerCalled := false
		handler := func(ctx context.Context, event *relay.EventRequest) error {
			handlerCalled = true
			return nil
		}

		// Wrap handler
		wrappedHandler := middleware(handler)

		// Create test event
		event := &relay.EventRequest{
			Action: "test.event",
			RoomID: "session-456",
			Source: "device",
		}

		// Execute
		err := wrappedHandler(context.Background(), event)

		// Assert
		assert.NoError(t, err)
		assert.True(t, handlerCalled)

		// Check that span was created
		spans := spanRecorder.Ended()
		require.Len(t, spans, 1)
		assert.Equal(t, "event.test.event", spans[0].Name())
	})
}
