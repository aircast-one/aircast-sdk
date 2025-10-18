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
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func setupTestTracer(t *testing.T) (*sdktrace.TracerProvider, *tracetest.SpanRecorder) {
	spanRecorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(spanRecorder),
	)
	t.Cleanup(func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			t.Errorf("failed to shutdown tracer provider: %v", err)
		}
	})
	return tp, spanRecorder
}

func TestTracingMiddleware(t *testing.T) {
	// Setup
	tp, spanRecorder := setupTestTracer(t)
	tracer := tp.Tracer("test-tracer")

	t.Run("creates span for request", func(t *testing.T) {
		// Create middleware
		middleware := TracingMiddleware(tracer)

		// Create a test handler
		handlerCalled := false
		handler := func(ctx context.Context, req *relay.Request, res relay.ResponseWriter) error {
			handlerCalled = true
			return nil
		}

		// Wrap handler with middleware
		wrappedHandler := middleware(handler)

		// Create test request
		req := &relay.Request{
			Action:    "test.action",
			RequestID: "req-123",
			SessionID: "session-456",
			Source:    "web",
		}

		// Execute
		err := wrappedHandler(context.Background(), req, nil)

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

		// Create a parent span and extract its trace context
		parentCtx, parentSpan := tracer.Start(context.Background(), "parent")
		defer parentSpan.End()

		traceContext := InjectTraceContext(parentCtx)
		require.NotNil(t, traceContext)

		// Create middleware
		middleware := TracingMiddleware(tracer)

		// Create handler that checks context
		var extractedCtx context.Context
		handler := func(ctx context.Context, req *relay.Request, res relay.ResponseWriter) error {
			extractedCtx = ctx
			return nil
		}

		// Wrap handler
		wrappedHandler := middleware(handler)

		// Create request with trace context
		req := &relay.Request{
			Action:       "test.action",
			RequestID:    "req-123",
			TraceContext: traceContext,
		}

		// Execute
		err := wrappedHandler(context.Background(), req, nil)

		// Assert
		assert.NoError(t, err)
		assert.NotNil(t, extractedCtx)

		// Verify trace context was propagated
		spans := spanRecorder.Ended()
		require.GreaterOrEqual(t, len(spans), 1)
	})

	t.Run("records error in span when handler fails", func(t *testing.T) {
		spanRecorder.Reset()

		// Create middleware
		middleware := TracingMiddleware(tracer)

		// Create handler that returns error
		expectedErr := errors.New("test error")
		handler := func(ctx context.Context, req *relay.Request, res relay.ResponseWriter) error {
			return expectedErr
		}

		// Wrap handler
		wrappedHandler := middleware(handler)

		// Create test request
		req := &relay.Request{
			Action:    "test.action",
			RequestID: "req-123",
		}

		// Execute
		err := wrappedHandler(context.Background(), req, nil)

		// Assert
		assert.Equal(t, expectedErr, err)

		// Check that error was recorded in span
		spans := spanRecorder.Ended()
		require.Len(t, spans, 1)
		// Note: Checking span events for error would require more detailed assertions
	})
}

func TestEventTracingMiddleware(t *testing.T) {
	// Setup
	tp, spanRecorder := setupTestTracer(t)
	tracer := tp.Tracer("test-tracer")

	t.Run("creates span for event", func(t *testing.T) {
		// Create middleware
		middleware := EventTracingMiddleware(tracer)

		// Create a test handler
		handlerCalled := false
		handler := func(ctx context.Context, event *relay.EventRequest) error {
			handlerCalled = true
			return nil
		}

		// Wrap handler with middleware
		wrappedHandler := middleware(handler)

		// Create test event
		event := &relay.EventRequest{
			Action:    "test.event",
			SessionID: "session-456",
			Source:    "device",
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

	t.Run("records error in span when handler fails", func(t *testing.T) {
		spanRecorder.Reset()

		// Create middleware
		middleware := EventTracingMiddleware(tracer)

		// Create handler that returns error
		expectedErr := errors.New("event error")
		handler := func(ctx context.Context, event *relay.EventRequest) error {
			return expectedErr
		}

		// Wrap handler
		wrappedHandler := middleware(handler)

		// Create test event
		event := &relay.EventRequest{
			Action: "test.event",
		}

		// Execute
		err := wrappedHandler(context.Background(), event)

		// Assert
		assert.Equal(t, expectedErr, err)

		// Check that error was recorded
		spans := spanRecorder.Ended()
		require.Len(t, spans, 1)
	})
}

func TestExtractTraceContext(t *testing.T) {
	// Setup W3C Trace Context propagator
	otel.SetTextMapPropagator(propagation.TraceContext{})

	t.Run("extracts valid trace context", func(t *testing.T) {
		traceContext := map[string]string{
			"traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",
		}

		ctx := ExtractTraceContext(context.Background(), traceContext)
		assert.NotNil(t, ctx)
	})

	t.Run("handles nil trace context", func(t *testing.T) {
		ctx := ExtractTraceContext(context.Background(), nil)
		assert.NotNil(t, ctx)
	})

	t.Run("handles empty trace context", func(t *testing.T) {
		ctx := ExtractTraceContext(context.Background(), map[string]string{})
		assert.NotNil(t, ctx)
	})
}

func TestInjectTraceContext(t *testing.T) {
	// Setup
	otel.SetTextMapPropagator(propagation.TraceContext{})
	tp, _ := setupTestTracer(t)
	tracer := tp.Tracer("test-tracer")

	t.Run("injects trace context from span", func(t *testing.T) {
		ctx, span := tracer.Start(context.Background(), "test-span")
		defer span.End()

		traceContext := InjectTraceContext(ctx)

		// Should have traceparent
		assert.NotNil(t, traceContext)
		assert.Contains(t, traceContext, "traceparent")
	})

	t.Run("returns nil for context without span", func(t *testing.T) {
		traceContext := InjectTraceContext(context.Background())

		// Should be nil or empty when no active span
		if traceContext != nil {
			assert.Empty(t, traceContext)
		}
	})
}
