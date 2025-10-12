package relay

import (
	"context"
	"errors"
	"testing"

	"github.com/pavliha/aircast-sdk/pkg/message"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockResponseWriter for testing
type MockResponseWriter struct {
	mock.Mock
}

func (m *MockResponseWriter) SendSuccess(payload any) error {
	args := m.Called(payload)
	return args.Error(0)
}

func (m *MockResponseWriter) SendError(code string, message string) error {
	args := m.Called(code, message)
	return args.Error(0)
}

// Test helper functions

func createTestRouter() (*Router, *mockResponseFactory, *mockErrorSender) {
	logger := logrus.NewEntry(logrus.New())
	logger.Logger.SetLevel(logrus.PanicLevel) // Suppress logs in tests

	respFactory := &mockResponseFactory{}
	errSender := &mockErrorSender{}

	router := NewRouter(logger, respFactory.factory, errSender.sender)

	return router, respFactory, errSender
}

type mockResponseFactory struct {
	responseWriter ResponseWriter
}

func (m *mockResponseFactory) factory(ctx context.Context, req *Request, logger *logrus.Entry) ResponseWriter {
	if m.responseWriter != nil {
		return m.responseWriter
	}
	return &MockResponseWriter{}
}

type mockErrorSender struct {
	lastError error
}

func (m *mockErrorSender) sender(ctx context.Context, req *Request, code, message string) error {
	return m.lastError
}

// Tests

func TestNewRouter(t *testing.T) {
	logger := logrus.NewEntry(logrus.New())
	respFactory := func(ctx context.Context, req *Request, logger *logrus.Entry) ResponseWriter {
		return &MockResponseWriter{}
	}
	errSender := func(ctx context.Context, req *Request, code, message string) error {
		return nil
	}

	router := NewRouter(logger, respFactory, errSender)

	assert.NotNil(t, router)
	assert.NotNil(t, router.routes)
	assert.NotNil(t, router.middlewares)
	assert.NotNil(t, router.eventRoutes)
	assert.NotNil(t, router.eventMiddlewares)
}

func TestRouter_HandleRequest_BasicRegistration(t *testing.T) {
	router, _, _ := createTestRouter()

	handlerCalled := false
	handler := func(ctx context.Context, req *Request, res ResponseWriter) error {
		handlerCalled = true
		return nil
	}

	router.HandleRequest("test.action", handler)

	registeredHandler, exists := router.GetHandler("test.action")
	assert.True(t, exists)
	assert.NotNil(t, registeredHandler)

	// Execute handler
	mockRes := &MockResponseWriter{}
	err := registeredHandler(context.Background(), &Request{}, mockRes)
	assert.NoError(t, err)
	assert.True(t, handlerCalled)
}

func TestRouter_HandleRequest_WithGlobalMiddleware(t *testing.T) {
	router, _, _ := createTestRouter()

	var executionOrder []string

	// Middleware 1
	middleware1 := func(next ActionHandler) ActionHandler {
		return func(ctx context.Context, req *Request, res ResponseWriter) error {
			executionOrder = append(executionOrder, "mw1_before")
			err := next(ctx, req, res)
			executionOrder = append(executionOrder, "mw1_after")
			return err
		}
	}

	// Middleware 2
	middleware2 := func(next ActionHandler) ActionHandler {
		return func(ctx context.Context, req *Request, res ResponseWriter) error {
			executionOrder = append(executionOrder, "mw2_before")
			err := next(ctx, req, res)
			executionOrder = append(executionOrder, "mw2_after")
			return err
		}
	}

	// Handler
	handler := func(ctx context.Context, req *Request, res ResponseWriter) error {
		executionOrder = append(executionOrder, "handler")
		return nil
	}

	// Register global middleware
	router.UseRequestMiddleware(middleware1)
	router.UseRequestMiddleware(middleware2)

	// Register handler
	router.HandleRequest("test.action", handler)

	// Execute
	h, _ := router.GetHandler("test.action")
	err := h(context.Background(), &Request{}, &MockResponseWriter{})

	assert.NoError(t, err)
	assert.Equal(t, []string{
		"mw2_before",
		"mw1_before",
		"handler",
		"mw1_after",
		"mw2_after",
	}, executionOrder)
}

func TestRouter_ProcessRequest_Success(t *testing.T) {
	router, respFactory, _ := createTestRouter()

	mockRes := &MockResponseWriter{}
	mockRes.On("SendSuccess", map[string]any{"result": "success"}).Return(nil)
	respFactory.responseWriter = mockRes

	handlerCalled := false
	router.HandleRequest("test.action", func(ctx context.Context, req *Request, res ResponseWriter) error {
		handlerCalled = true
		return res.SendSuccess(map[string]any{"result": "success"})
	})

	msg := message.RequestMessage{
		Action:    "test.action",
		RequestID: "req-123",
		ChannelID: "session-123",
		Source:    "web",
		Payload:   map[string]any{"data": "test"},
	}

	err := router.ProcessRequest(context.Background(), msg)
	assert.NoError(t, err)
	assert.True(t, handlerCalled)
	mockRes.AssertExpectations(t)
}

func TestRouter_ProcessRequest_UnknownAction(t *testing.T) {
	router, _, errSender := createTestRouter()

	errSender.lastError = nil

	msg := message.RequestMessage{
		Action:    "unknown.action",
		RequestID: "req-123",
		ChannelID: "session-123",
		Source:    "web",
	}

	err := router.ProcessRequest(context.Background(), msg)
	assert.NoError(t, err) // errorSender returned nil
}

func TestRouter_ProcessRequest_InvalidMessage(t *testing.T) {
	router, _, _ := createTestRouter()

	msg := message.RequestMessage{
		Action:    "test.action",
		RequestID: "", // Missing required field
		ChannelID: "session-123",
		Source:    "web",
	}

	err := router.ProcessRequest(context.Background(), msg)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "request ID is required")
}

func TestRouter_ProcessRequest_HandlerError(t *testing.T) {
	router, respFactory, _ := createTestRouter()

	mockRes := &MockResponseWriter{}
	mockRes.On("SendError", "TEST_ERROR", "test error").Return(nil)
	respFactory.responseWriter = mockRes

	router.HandleRequest("test.action", func(ctx context.Context, req *Request, res ResponseWriter) error {
		return res.SendError("TEST_ERROR", "test error")
	})

	msg := message.RequestMessage{
		Action:    "test.action",
		RequestID: "req-123",
		ChannelID: "session-123",
		Source:    "web",
	}

	err := router.ProcessRequest(context.Background(), msg)
	assert.NoError(t, err)
	mockRes.AssertExpectations(t)
}

func TestRouter_HandleEvent_Registration(t *testing.T) {
	router, _, _ := createTestRouter()

	eventCalled := false
	handler := func(ctx context.Context, event *EventRequest) error {
		eventCalled = true
		return nil
	}

	router.HandleEvent("test.event", handler)

	msg := message.EventMessage{
		Action:    "test.event",
		ChannelID: "session-123",
		Source:    "web",
		Payload:   map[string]any{"data": "test"},
	}

	err := router.ProcessEvent(context.Background(), msg)
	assert.NoError(t, err)
	assert.True(t, eventCalled)
}

func TestRouter_ProcessEvent_UnknownAction(t *testing.T) {
	router, _, _ := createTestRouter()

	msg := message.EventMessage{
		Action:    "unknown.event",
		ChannelID: "session-123",
		Source:    "web",
	}

	err := router.ProcessEvent(context.Background(), msg)
	assert.NoError(t, err) // Unknown events are silently ignored
}

func TestRouter_ProcessEvent_HandlerError(t *testing.T) {
	router, _, _ := createTestRouter()

	expectedError := errors.New("handler error")
	router.HandleEvent("test.event", func(ctx context.Context, event *EventRequest) error {
		return expectedError
	})

	msg := message.EventMessage{
		Action:    "test.event",
		ChannelID: "session-123",
		Source:    "web",
	}

	err := router.ProcessEvent(context.Background(), msg)
	assert.Error(t, err)
	assert.Equal(t, expectedError, err)
}

func TestRouter_UseEventMiddleware(t *testing.T) {
	router, _, _ := createTestRouter()

	var executionOrder []string

	middleware := func(next EventHandler) EventHandler {
		return func(ctx context.Context, event *EventRequest) error {
			executionOrder = append(executionOrder, "middleware_before")
			err := next(ctx, event)
			executionOrder = append(executionOrder, "middleware_after")
			return err
		}
	}

	handler := func(ctx context.Context, event *EventRequest) error {
		executionOrder = append(executionOrder, "handler")
		return nil
	}

	router.UseEventMiddleware(middleware)
	router.HandleEvent("test.event", handler)

	msg := message.EventMessage{
		Action:    "test.event",
		ChannelID: "session-123",
		Source:    "web",
	}

	err := router.ProcessEvent(context.Background(), msg)
	assert.NoError(t, err)
	assert.Equal(t, []string{
		"middleware_before",
		"handler",
		"middleware_after",
	}, executionOrder)
}

// Handler Adaptation Tests

func TestRouter_AdaptHandler_NoArgFunction(t *testing.T) {
	router, respFactory, _ := createTestRouter()

	mockRes := &MockResponseWriter{}
	mockRes.On("SendSuccess", "result").Return(nil)
	respFactory.responseWriter = mockRes

	// Register func() any
	router.HandleRequest("test.action", func() any {
		return "result"
	})

	msg := message.RequestMessage{
		Action:    "test.action",
		RequestID: "req-123",
		ChannelID: "session-123",
		Source:    "web",
	}

	err := router.ProcessRequest(context.Background(), msg)
	assert.NoError(t, err)
	mockRes.AssertExpectations(t)
}

func TestRouter_AdaptHandler_NoArgWithErrorFunction(t *testing.T) {
	router, respFactory, _ := createTestRouter()

	mockRes := &MockResponseWriter{}
	mockRes.On("SendSuccess", "result").Return(nil)
	respFactory.responseWriter = mockRes

	// Register func() (any, error)
	router.HandleRequest("test.action", func() (any, error) {
		return "result", nil
	})

	msg := message.RequestMessage{
		Action:    "test.action",
		RequestID: "req-123",
		ChannelID: "session-123",
		Source:    "web",
	}

	err := router.ProcessRequest(context.Background(), msg)
	assert.NoError(t, err)
	mockRes.AssertExpectations(t)
}

func TestRouter_AdaptHandler_NoArgWithErrorFunction_Error(t *testing.T) {
	router, respFactory, _ := createTestRouter()

	mockRes := &MockResponseWriter{}
	mockRes.On("SendError", "SERVICE_UNAVAILABLE", "test error").Return(nil)
	respFactory.responseWriter = mockRes

	// Register func() (any, error) that returns error
	router.HandleRequest("test.action", func() (any, error) {
		return nil, errors.New("test error")
	})

	msg := message.RequestMessage{
		Action:    "test.action",
		RequestID: "req-123",
		ChannelID: "session-123",
		Source:    "web",
	}

	err := router.ProcessRequest(context.Background(), msg)
	assert.NoError(t, err)
	mockRes.AssertExpectations(t)
}

func TestRouter_AdaptHandler_SingleArgFunction(t *testing.T) {
	router, respFactory, _ := createTestRouter()

	mockRes := &MockResponseWriter{}
	mockRes.On("SendSuccess", "processed: test").Return(nil)
	respFactory.responseWriter = mockRes

	// Register func(any) any
	router.HandleRequest("test.action", func(payload any) any {
		payloadMap := payload.(map[string]any)
		return "processed: " + payloadMap["input"].(string)
	})

	msg := message.RequestMessage{
		Action:    "test.action",
		RequestID: "req-123",
		ChannelID: "session-123",
		Source:    "web",
		Payload:   map[string]any{"input": "test"},
	}

	err := router.ProcessRequest(context.Background(), msg)
	assert.NoError(t, err)
	mockRes.AssertExpectations(t)
}

func TestRouter_AdaptHandler_SingleArgWithErrorFunction(t *testing.T) {
	router, respFactory, _ := createTestRouter()

	mockRes := &MockResponseWriter{}
	mockRes.On("SendSuccess", "processed").Return(nil)
	respFactory.responseWriter = mockRes

	// Register func(any) (any, error)
	router.HandleRequest("test.action", func(payload any) (any, error) {
		return "processed", nil
	})

	msg := message.RequestMessage{
		Action:    "test.action",
		RequestID: "req-123",
		ChannelID: "session-123",
		Source:    "web",
		Payload:   map[string]any{"input": "test"},
	}

	err := router.ProcessRequest(context.Background(), msg)
	assert.NoError(t, err)
	mockRes.AssertExpectations(t)
}

func TestRouter_AdaptHandler_SingleArgWithErrorFunction_Error(t *testing.T) {
	router, respFactory, _ := createTestRouter()

	mockRes := &MockResponseWriter{}
	mockRes.On("SendError", "SERVICE_UNAVAILABLE", "test error").Return(nil)
	respFactory.responseWriter = mockRes

	// Register func(any) (any, error) that returns error
	router.HandleRequest("test.action", func(payload any) (any, error) {
		return nil, errors.New("test error")
	})

	msg := message.RequestMessage{
		Action:    "test.action",
		RequestID: "req-123",
		ChannelID: "session-123",
		Source:    "web",
		Payload:   map[string]any{"input": "test"},
	}

	err := router.ProcessRequest(context.Background(), msg)
	assert.NoError(t, err)
	mockRes.AssertExpectations(t)
}

func TestRouter_HandleRequest_Panic_NoComponents(t *testing.T) {
	router, _, _ := createTestRouter()

	assert.Panics(t, func() {
		router.HandleRequest("test.action")
	})
}

func TestRouter_HandleRequest_Panic_InvalidComponent(t *testing.T) {
	router, _, _ := createTestRouter()

	assert.Panics(t, func() {
		router.HandleRequest("test.action", "not a handler")
	})
}

func TestRouter_HandleRequest_Panic_InvalidMiddleware(t *testing.T) {
	router, _, _ := createTestRouter()

	handler := func(ctx context.Context, req *Request, res ResponseWriter) error {
		return nil
	}

	assert.Panics(t, func() {
		router.HandleRequest("test.action", "not middleware", handler)
	})
}

func TestRouter_TraceContext_Propagation(t *testing.T) {
	router, respFactory, _ := createTestRouter()

	mockRes := &MockResponseWriter{}
	mockRes.On("SendSuccess", mock.Anything).Return(nil)
	respFactory.responseWriter = mockRes

	var receivedTraceContext map[string]string
	router.HandleRequest("test.action", func(ctx context.Context, req *Request, res ResponseWriter) error {
		receivedTraceContext = req.TraceContext
		return res.SendSuccess(map[string]any{"ok": true})
	})

	msg := message.RequestMessage{
		Action:    "test.action",
		RequestID: "req-123",
		ChannelID: "session-123",
		Source:    "web",
		TraceContext: map[string]string{
			"traceparent": "00-trace-id-span-id-01",
		},
	}

	err := router.ProcessRequest(context.Background(), msg)
	assert.NoError(t, err)
	assert.Equal(t, "00-trace-id-span-id-01", receivedTraceContext["traceparent"])
}

func TestRouter_EventTraceContext_Propagation(t *testing.T) {
	router, _, _ := createTestRouter()

	var receivedTraceContext map[string]string
	router.HandleEvent("test.event", func(ctx context.Context, event *EventRequest) error {
		receivedTraceContext = event.TraceContext
		return nil
	})

	msg := message.EventMessage{
		Action:    "test.event",
		ChannelID: "session-123",
		Source:    "web",
		TraceContext: map[string]string{
			"traceparent": "00-event-trace-id-span-id-01",
		},
	}

	err := router.ProcessEvent(context.Background(), msg)
	assert.NoError(t, err)
	assert.Equal(t, "00-event-trace-id-span-id-01", receivedTraceContext["traceparent"])
}
