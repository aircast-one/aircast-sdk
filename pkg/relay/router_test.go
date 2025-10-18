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

// MockClient for testing
type MockClient struct {
	mock.Mock
}

func (m *MockClient) Send(msg any, sessionId *message.ChannelID) error {
	args := m.Called(msg, sessionId)
	return args.Error(0)
}

func (m *MockClient) SendMessageToChannel(id message.ChannelID, msg any) error {
	args := m.Called(id, msg)
	return args.Error(0)
}

func (m *MockClient) SendBroadcastMessage(msg any) error {
	args := m.Called(msg)
	return args.Error(0)
}

func (m *MockClient) SendResponse(req *message.RequestMessage, payload any) error {
	args := m.Called(req, payload)
	return args.Error(0)
}

func (m *MockClient) SendErrorToChannel(req *message.RequestMessage, payload message.ErrorResponse) error {
	args := m.Called(req, payload)
	return args.Error(0)
}

func (m *MockClient) SendEventToChannel(action message.MessageAction, payload any, destination message.MessageDestination, sessionID message.ChannelID) error {
	args := m.Called(action, payload, destination, sessionID)
	return args.Error(0)
}

func (m *MockClient) Listen(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *MockClient) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockClient) IsClosed() bool {
	args := m.Called()
	return args.Bool(0)
}

func (m *MockClient) ReadMessage() <-chan any {
	args := m.Called()
	return args.Get(0).(<-chan any)
}

func (m *MockClient) RegisterWill(will message.WillMessage) error {
	args := m.Called(will)
	return args.Error(0)
}

func (m *MockClient) ClearWill() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockClient) SendRawJSON(jsonBytes []byte) error {
	args := m.Called(jsonBytes)
	return args.Error(0)
}

// Test helper functions

func createTestRouter() (*Router, *MockClient) {
	logger := logrus.NewEntry(logrus.New())
	logger.Logger.SetLevel(logrus.PanicLevel) // Suppress logs in tests

	client := &MockClient{}
	router := NewRouter(logger, client)

	return router, client
}

// Tests

func TestNewRouter(t *testing.T) {
	logger := logrus.NewEntry(logrus.New())
	client := &MockClient{}

	router := NewRouter(logger, client)

	assert.NotNil(t, router)
	assert.NotNil(t, router.routes)
	assert.NotNil(t, router.middlewares)
	assert.NotNil(t, router.eventRoutes)
	assert.NotNil(t, router.eventMiddlewares)
	assert.NotNil(t, router.client)
}

func TestRouter_HandleRequest_BasicRegistration(t *testing.T) {
	router, _ := createTestRouter()

	handlerCalled := false
	handler := func(ctx context.Context, req *Request) (any, error) {
		handlerCalled = true
		return map[string]string{"status": "ok"}, nil
	}

	router.HandleRequest("test.action", handler)

	registeredHandler, exists := router.GetHandler("test.action")
	assert.True(t, exists)
	assert.NotNil(t, registeredHandler)

	// Execute handler
	payload, err := registeredHandler(context.Background(), &Request{})
	assert.NoError(t, err)
	assert.NotNil(t, payload)
	assert.True(t, handlerCalled)
}

func TestRouter_HandleRequest_WithGlobalMiddleware(t *testing.T) {
	router, _ := createTestRouter()

	var executionOrder []string

	// Middleware 1
	middleware1 := func(next ActionHandler) ActionHandler {
		return func(ctx context.Context, req *Request) (any, error) {
			executionOrder = append(executionOrder, "mw1_before")
			payload, err := next(ctx, req)
			executionOrder = append(executionOrder, "mw1_after")
			return payload, err
		}
	}

	// Middleware 2
	middleware2 := func(next ActionHandler) ActionHandler {
		return func(ctx context.Context, req *Request) (any, error) {
			executionOrder = append(executionOrder, "mw2_before")
			payload, err := next(ctx, req)
			executionOrder = append(executionOrder, "mw2_after")
			return payload, err
		}
	}

	// Handler
	handler := func(ctx context.Context, req *Request) (any, error) {
		executionOrder = append(executionOrder, "handler")
		return nil, nil
	}

	// Register global middleware
	router.UseRequestMiddleware(middleware1)
	router.UseRequestMiddleware(middleware2)

	// Register handler
	router.HandleRequest("test.action", handler)

	// Execute
	h, _ := router.GetHandler("test.action")
	_, err := h(context.Background(), &Request{})

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
	router, client := createTestRouter()

	client.On("SendResponse", mock.AnythingOfType("*message.RequestMessage"), map[string]any{"result": "success"}).Return(nil)

	handlerCalled := false
	router.HandleRequest("test.action", func(ctx context.Context, req *Request) (any, error) {
		handlerCalled = true
		return map[string]any{"result": "success"}, nil
	})

	msg := message.RequestMessage{
		Action:      "test.action",
		RequestID:   "req-123",
		ChannelID:   "session-123",
		Source:      "web",
		Destination: "device",
		Payload:     map[string]any{"data": "test"},
	}

	err := router.ProcessRequest(context.Background(), msg)
	assert.NoError(t, err)
	assert.True(t, handlerCalled)
	client.AssertExpectations(t)
}

func TestRouter_ProcessRequest_UnknownAction(t *testing.T) {
	router, client := createTestRouter()

	client.On("SendErrorToChannel", mock.AnythingOfType("*message.RequestMessage"), mock.MatchedBy(func(errResp message.ErrorResponse) bool {
		return errResp.Code == "UNKNOWN_ACTION"
	})).Return(nil)

	msg := message.RequestMessage{
		Action:      "unknown.action",
		RequestID:   "req-123",
		ChannelID:   "session-123",
		Source:      "web",
		Destination: "device",
	}

	err := router.ProcessRequest(context.Background(), msg)
	assert.NoError(t, err)
	client.AssertExpectations(t)
}

func TestRouter_ProcessRequest_InvalidMessage(t *testing.T) {
	router, client := createTestRouter()

	client.On("SendErrorToChannel", mock.AnythingOfType("*message.RequestMessage"), mock.MatchedBy(func(errResp message.ErrorResponse) bool {
		return errResp.Code == "INVALID_REQUEST"
	})).Return(nil)

	msg := message.RequestMessage{
		Action:      "test.action",
		RequestID:   "", // Missing required field
		ChannelID:   "session-123",
		Source:      "web",
		Destination: "device",
	}

	err := router.ProcessRequest(context.Background(), msg)
	assert.NoError(t, err)
	client.AssertExpectations(t)
}

func TestRouter_ProcessRequest_HandlerError(t *testing.T) {
	router, client := createTestRouter()

	client.On("SendErrorToChannel", mock.AnythingOfType("*message.RequestMessage"), mock.MatchedBy(func(errResp message.ErrorResponse) bool {
		return errResp.Code == "HANDLER_ERROR" && errResp.Message == "test error"
	})).Return(nil)

	router.HandleRequest("test.action", func(ctx context.Context, req *Request) (any, error) {
		return nil, errors.New("test error")
	})

	msg := message.RequestMessage{
		Action:      "test.action",
		RequestID:   "req-123",
		ChannelID:   "session-123",
		Source:      "web",
		Destination: "device",
	}

	err := router.ProcessRequest(context.Background(), msg)
	assert.NoError(t, err)
	client.AssertExpectations(t)
}

func TestRouter_HandleEvent_Registration(t *testing.T) {
	router, _ := createTestRouter()

	eventCalled := false
	handler := func(ctx context.Context, event *EventRequest) error {
		eventCalled = true
		return nil
	}

	router.HandleEvent("test.event", handler)

	msg := message.EventMessage{
		Action:      "test.event",
		ChannelID:   "session-123",
		Source:      "web",
		Destination: "device",
		Payload:     map[string]any{"data": "test"},
	}

	err := router.ProcessEvent(context.Background(), msg)
	assert.NoError(t, err)
	assert.True(t, eventCalled)
}

func TestRouter_ProcessEvent_UnknownAction(t *testing.T) {
	router, _ := createTestRouter()

	msg := message.EventMessage{
		Action:      "unknown.event",
		ChannelID:   "session-123",
		Source:      "web",
		Destination: "device",
	}

	err := router.ProcessEvent(context.Background(), msg)
	assert.NoError(t, err) // Unknown events are silently ignored
}

func TestRouter_ProcessEvent_HandlerError(t *testing.T) {
	router, _ := createTestRouter()

	expectedError := errors.New("handler error")
	router.HandleEvent("test.event", func(ctx context.Context, event *EventRequest) error {
		return expectedError
	})

	msg := message.EventMessage{
		Action:      "test.event",
		ChannelID:   "session-123",
		Source:      "web",
		Destination: "device",
	}

	err := router.ProcessEvent(context.Background(), msg)
	assert.Error(t, err)
	assert.Equal(t, expectedError, err)
}

func TestRouter_UseEventMiddleware(t *testing.T) {
	router, _ := createTestRouter()

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
		Action:      "test.event",
		ChannelID:   "session-123",
		Source:      "web",
		Destination: "device",
	}

	err := router.ProcessEvent(context.Background(), msg)
	assert.NoError(t, err)
	assert.Equal(t, []string{
		"middleware_before",
		"handler",
		"middleware_after",
	}, executionOrder)
}

func TestRouter_AdaptSimpleHandlers(t *testing.T) {
	router, client := createTestRouter()

	client.On("SendResponse", mock.AnythingOfType("*message.RequestMessage"), "test result").Return(nil)

	// Test adapting func() any
	router.HandleRequest("simple.noargs", func() any {
		return "test result"
	})

	msg := message.RequestMessage{
		Action:      "simple.noargs",
		RequestID:   "req-123",
		ChannelID:   "session-123",
		Source:      "web",
		Destination: "device",
	}

	err := router.ProcessRequest(context.Background(), msg)
	assert.NoError(t, err)
	client.AssertExpectations(t)
}
